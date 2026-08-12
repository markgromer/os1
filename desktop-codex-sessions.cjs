const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_MAX_RESULTS = 12;
const MAX_METADATA_BYTES = 64 * 1024;

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
  return {
    sessionId: String(payload.id || '').trim().slice(0, 160),
    workspacePath,
    folderName,
    projectName: humanizeWorkspaceName(folderName),
    modifiedAt: stat.mtime.toISOString(),
    source: String(payload.source || '').trim().slice(0, 80),
    originator: String(payload.originator || '').trim().slice(0, 120),
  };
}

function discoverRecentCodexWorkspaces({
  codexHome = process.env.CODEX_HOME || path.join(os.homedir(), '.codex'),
  nowMs = Date.now(),
  maxAgeMs = DEFAULT_MAX_AGE_MS,
  maxResults = DEFAULT_MAX_RESULTS,
  days = 8,
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
  const seen = new Set();
  const output = [];
  for (const candidate of candidates) {
    const key = path.resolve(candidate.workspacePath).toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(candidate);
    if (output.length >= Math.max(1, Math.min(30, Number(maxResults) || DEFAULT_MAX_RESULTS))) break;
  }
  return output;
}

module.exports = { discoverRecentCodexWorkspaces, humanizeWorkspaceName, parseGitStatus, parseSessionMetadata };
