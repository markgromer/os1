const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_MAX_RESULTS = 12;
const MAX_METADATA_BYTES = 64 * 1024;
const MAX_HANDOFF_BYTES = 256 * 1024;
const MAX_HANDOFF_TEXT = 1800;
const MAX_REQUEST_TEXT = 800;
const MAX_CONTEXT_ITEMS = 18;
const MAX_CONTEXT_TEXT = 1800;

function humanizeWorkspaceName(value) {
  return String(value || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function parseGitStatus(value, limit = 30) {
  const lines = String(value || '').split(/\r?\n/).filter(Boolean);
  return {
    count: lines.length,
    entries: lines.slice(0, Math.max(1, Number(limit) || 30)).map((line) => ({
      status: line.slice(0, 2).trim(),
      file: line.slice(3).trim(),
    })),
  };
}

function sessionDayDirectories(root, nowMs, days) {
  const output = [];
  for (let offset = 0; offset < days; offset += 1) {
    const date = new Date(nowMs - (offset * 24 * 60 * 60 * 1000));
    const year = String(date.getFullYear());
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    output.push(path.join(root, year, month, day));
  }
  return [...new Set(output)];
}

function readFirstJsonLine(filePath) {
  let handle;
  try {
    handle = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(MAX_METADATA_BYTES);
    const bytesRead = fs.readSync(handle, buffer, 0, buffer.length, 0);
    const firstLine = buffer.subarray(0, bytesRead).toString('utf8').split(/\r?\n/, 1)[0];
    return firstLine ? JSON.parse(firstLine) : null;
  } catch {
    return null;
  } finally {
    if (handle !== undefined) {
      try { fs.closeSync(handle); } catch {}
    }
  }
}

function compactText(value, limit = MAX_HANDOFF_TEXT) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, Math.max(1, Number(limit) || MAX_HANDOFF_TEXT));
}

function extractTextParts(value, output = []) {
  if (!value || output.join(' ').length > MAX_HANDOFF_TEXT * 2) return output;
  if (typeof value === 'string') {
    const clean = compactText(value, MAX_HANDOFF_TEXT);
    if (clean) output.push(clean);
    return output;
  }
  if (Array.isArray(value)) {
    for (const item of value) extractTextParts(item, output);
    return output;
  }
  if (typeof value === 'object') {
    for (const key of ['text', 'content', 'message', 'summary', 'final_output', 'finalOutput']) {
      if (Object.hasOwn(value, key)) extractTextParts(value[key], output);
    }
  }
  return output;
}

function eventLooksAssistantHandoff(event) {
  const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
  const role = String(payload.role || payload.author?.role || event?.role || '').toLowerCase();
  const type = String(event?.type || payload.type || '').toLowerCase();
  const status = String(payload.status || event?.status || '').toLowerCase();
  if (role && role !== 'assistant') return false;
  if (role === 'assistant') return true;
  return /assistant|final|result|completion|handoff/.test(type) || /completed|complete|succeeded|success/.test(status);
}

function classifyHandoffSummary(summary) {
  const lower = String(summary || '').toLowerCase();
  if (/\b(fixed|resolved|confirmed|applied|ready|no pending migrations|returns ready|should load)\b/.test(lower)) return 'ready_for_mark';
  if (/\b(blocked|failed|error|missing|unauthorized|500|crash)\b/.test(lower)) return 'blocked';
  return summary ? 'handoff' : '';
}

function eventLooksUserRequest(event) {
  const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
  const role = String(payload.role || payload.author?.role || event?.role || '').toLowerCase();
  if (role !== 'user') return false;
  const type = String(event?.type || payload.type || '').toLowerCase();
  if (type && !/message|response_item|event_msg/.test(type)) return false;
  return true;
}

function cleanUserRequestText(value) {
  const clean = compactText(value, MAX_REQUEST_TEXT);
  if (!clean) return '';
  const lower = clean.toLowerCase();
  if (lower.startsWith('<environment_context') || lower.startsWith('<permissions instructions')) return '';
  if (lower.startsWith('<recommended_plugins') || lower.startsWith('<collaboration_mode')) return '';
  if (lower.includes('the following is the codex agent history')) return '';
  if (lower.includes('>>> transcript') || lower.includes('>>> approval request')) return '';
  if (lower.includes('planned action json') && lower.includes('sandbox_permissions')) return '';
  if (/^<[^>]+>$/.test(clean)) return '';
  return clean;
}

function redactContextText(value) {
  return compactText(value, MAX_CONTEXT_TEXT)
    .replace(/\b(sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9_]{16,})\b/g, '[redacted credential]')
    .replace(/\b(AUTHORIZATION|API[_ -]?KEY|TOKEN|PASSWORD|SECRET)\s*[:=]\s*[^\s,;]+/gi, '$1=[redacted]')
    .replace(/SECRET TRANSCRIPT CONTENT/gi, '[redacted sensitive content]');
}

function readSessionRollingContext(filePath) {
  let stat;
  try { stat = fs.statSync(filePath); } catch { return []; }
  if (!stat.isFile()) return [];
  let handle;
  try {
    handle = fs.openSync(filePath, 'r');
    const bytes = Math.min(MAX_HANDOFF_BYTES, stat.size);
    const buffer = Buffer.alloc(bytes);
    fs.readSync(handle, buffer, 0, bytes, Math.max(0, stat.size - bytes));
    const output = [];
    for (const line of buffer.toString('utf8').split(/\r?\n/).filter(Boolean)) {
      let event;
      try { event = JSON.parse(line); } catch { continue; }
      const payload = event?.payload && typeof event.payload === 'object' ? event.payload : {};
      const role = String(payload.role || payload.author?.role || event?.role || '').toLowerCase();
      if (role !== 'user' && role !== 'assistant') continue;
      let content = extractTextParts(payload).join(' ');
      if (role === 'user') content = cleanUserRequestText(content);
      content = redactContextText(content);
      if (!content) continue;
      output.push({
        role,
        content,
        at: typeof event.timestamp === 'string' ? event.timestamp.slice(0, 40) : '',
      });
    }
    return output.slice(-MAX_CONTEXT_ITEMS);
  } catch {
    return [];
  } finally {
    if (handle !== undefined) {
      try { fs.closeSync(handle); } catch {}
    }
  }
}

function readSessionLatestUserRequest(filePath) {
  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch {
    return null;
  }
  if (!stat.isFile()) return null;
  let handle;
  try {
    handle = fs.openSync(filePath, 'r');
    const bytes = Math.min(MAX_HANDOFF_BYTES, stat.size);
    const buffer = Buffer.alloc(bytes);
    fs.readSync(handle, buffer, 0, bytes, Math.max(0, stat.size - bytes));
    const lines = buffer.toString('utf8').split(/\r?\n/).filter(Boolean);
    for (let index = lines.length - 1; index >= 0; index -= 1) {
      let event;
      try { event = JSON.parse(lines[index]); } catch { continue; }
      if (!eventLooksUserRequest(event)) continue;
      const request = cleanUserRequestText(extractTextParts(event.payload || event).join(' '));
      if (!request) continue;
      return {
        request,
        requestedAt: typeof event.timestamp === 'string' ? event.timestamp : stat.mtime.toISOString(),
      };
    }
  } catch {
    return null;
  } finally {
    if (handle !== undefined) {
      try { fs.closeSync(handle); } catch {}
    }
  }
  return null;
}

function readSessionHandoffSummary(filePath) {
  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch {
    return null;
  }
  if (!stat.isFile()) return null;
  let handle;
  try {
    handle = fs.openSync(filePath, 'r');
    const bytes = Math.min(MAX_HANDOFF_BYTES, stat.size);
    const buffer = Buffer.alloc(bytes);
    fs.readSync(handle, buffer, 0, bytes, Math.max(0, stat.size - bytes));
    const lines = buffer.toString('utf8').split(/\r?\n/).filter(Boolean);
    for (let index = lines.length - 1; index >= 0; index -= 1) {
      let event;
      try { event = JSON.parse(lines[index]); } catch { continue; }
      if (!eventLooksAssistantHandoff(event)) continue;
      const summary = compactText(extractTextParts(event.payload || event).join(' '), MAX_HANDOFF_TEXT);
      if (!summary) continue;
      return {
        summary,
        status: classifyHandoffSummary(summary),
        observedAt: stat.mtime.toISOString(),
      };
    }
  } catch {
    return null;
  } finally {
    if (handle !== undefined) {
      try { fs.closeSync(handle); } catch {}
    }
  }
  return null;
}

function parseSessionMetadata(filePath, nowMs, maxAgeMs) {
  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch {
    return null;
  }
  if (!stat.isFile() || nowMs - stat.mtimeMs > maxAgeMs) return null;

  const event = readFirstJsonLine(filePath);
  const payload = event?.type === 'session_meta' && event.payload && typeof event.payload === 'object'
    ? event.payload
    : null;
  if (!payload || typeof payload.source !== 'string') return null;

  const workspacePath = String(payload.cwd || '').trim();
  if (!workspacePath || !path.isAbsolute(workspacePath)) return null;
  try {
    if (!fs.statSync(workspacePath).isDirectory()) return null;
  } catch {
    return null;
  }

  const folderName = path.basename(workspacePath);
  const handoff = readSessionHandoffSummary(filePath);
  const rollingContext = readSessionRollingContext(filePath);
  return {
    sessionId: String(payload.id || '').trim().slice(0, 160),
    workspacePath,
    folderName,
    projectName: humanizeWorkspaceName(folderName),
    modifiedAt: stat.mtime.toISOString(),
    source: String(payload.source || '').trim().slice(0, 80),
    originator: String(payload.originator || '').trim().slice(0, 120),
    rollingContext,
    ...(handoff ? {
      handoffSummary: handoff.summary,
      handoffStatus: handoff.status,
      handoffObservedAt: handoff.observedAt,
    } : {}),
  };
}

function discoverRecentCodexWorkspaces({
  codexHome = process.env.CODEX_HOME || path.join(os.homedir(), '.codex'),
  nowMs = Date.now(),
  maxAgeMs = DEFAULT_MAX_AGE_MS,
  maxResults = DEFAULT_MAX_RESULTS,
  days = 8,
  maxPerWorkspace = 1,
} = {}) {
  const sessionsRoot = path.join(codexHome, 'sessions');
  const candidates = [];
  for (const directory of sessionDayDirectories(sessionsRoot, nowMs, days)) {
    let entries = [];
    try {
      entries = fs.readdirSync(directory, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.endsWith('.jsonl')) continue;
      const metadata = parseSessionMetadata(path.join(directory, entry.name), nowMs, maxAgeMs);
      if (metadata) candidates.push(metadata);
    }
  }

  candidates.sort((left, right) => right.modifiedAt.localeCompare(left.modifiedAt));
  const seenSessions = new Set();
  const workspaceCounts = new Map();
  const output = [];
  for (const candidate of candidates) {
    const sessionKey = String(candidate.sessionId || '').trim();
    if (sessionKey && seenSessions.has(sessionKey)) continue;
    if (sessionKey) seenSessions.add(sessionKey);
    const workspaceKey = path.resolve(candidate.workspacePath).toLowerCase();
    const count = workspaceCounts.get(workspaceKey) || 0;
    if (count >= Math.max(1, Math.min(10, Number(maxPerWorkspace) || 1))) continue;
    workspaceCounts.set(workspaceKey, count + 1);
    output.push(candidate);
    if (output.length >= Math.max(1, Math.min(30, Number(maxResults) || DEFAULT_MAX_RESULTS))) break;
  }
  return output;
}

module.exports = { discoverRecentCodexWorkspaces, humanizeWorkspaceName, parseGitStatus, parseSessionMetadata, readSessionHandoffSummary, readSessionLatestUserRequest, readSessionRollingContext };
