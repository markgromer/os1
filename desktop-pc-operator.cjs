const { execFile, spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_MAX_RESULTS = 40;
const DEFAULT_MAX_VISITED = 20_000;
const DEFAULT_TIMEOUT_MS = 7_500;
const MAX_READ_BYTES = 60_000;

const SKIP_DIRECTORIES = new Set([
  '$recycle.bin', 'system volume information', 'windows', 'winsxs', 'node_modules',
  '.git', '.next', 'dist', 'build', 'coverage', '.cache', '__pycache__',
]);

const BINARY_EXTENSIONS = new Set([
  '.7z', '.avi', '.bin', '.bmp', '.class', '.db', '.dll', '.doc', '.docx', '.exe',
  '.gif', '.gz', '.ico', '.jar', '.jpeg', '.jpg', '.lock', '.map', '.mov', '.mp3',
  '.mp4', '.msi', '.obj', '.pdf', '.png', '.ppt', '.pptx', '.pyc', '.rar', '.sqlite',
  '.tar', '.ttf', '.wav', '.woff', '.woff2', '.xls', '.xlsx', '.zip',
]);

const SENSITIVE_PATH_PATTERNS = [
  /(^|[\\/])\.env(?:\.|$)/i,
  /(^|[\\/])\.ssh[\\/](?!config(?:$|[\\/]))/i,
  /(^|[\\/])\.aws[\\/](?:credentials|config)(?:$|[\\/])/i,
  /(^|[\\/])\.config[\\/](?:gh|gcloud)[\\/]/i,
  /(^|[\\/])appdata[\\/].*(?:login data|cookies|web data|credentials)/i,
  /(^|[\\/])(?:auth|credentials?|secrets?|tokens?)(?:\.[^\\/]+)?$/i,
  /(?:id_rsa|id_ed25519|private[-_.]?key|api[-_.]?key)(?:\.[^\\/]+)?$/i,
];

function comparablePath(value) {
  const resolved = path.resolve(String(value || ''));
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function pathWithin(root, candidate) {
  const relative = path.relative(comparablePath(root), comparablePath(candidate));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function canonicalExistingPath(value) {
  const candidate = String(value || '').trim();
  if (!candidate || !path.isAbsolute(candidate)) return { ok: false, error: 'An absolute path is required' };
  try {
    const canonicalPath = fs.realpathSync.native(path.resolve(candidate));
    return { ok: true, path: canonicalPath, stat: fs.statSync(canonicalPath) };
  } catch {
    return { ok: false, error: 'Path does not exist or is unavailable' };
  }
}

function uniqueExistingRoots(values) {
  const roots = [];
  for (const value of Array.isArray(values) ? values : []) {
    const resolved = canonicalExistingPath(value);
    if (!resolved.ok || !resolved.stat.isDirectory()) continue;
    if (!roots.some((root) => comparablePath(root) === comparablePath(resolved.path))) roots.push(resolved.path);
  }
  return roots;
}

function defaultFullPcRoots() {
  const candidates = [];
  if (process.platform === 'win32') {
    const systemDrive = String(process.env.SystemDrive || path.parse(os.homedir()).root || 'C:').replace(/[\\/]+$/, '');
    candidates.push(`${systemDrive}\\`);
  } else {
    candidates.push('/');
  }
  return uniqueExistingRoots(candidates);
}

function createPcAccessPolicy({ fullPcAccess = false, pcAccessRoots = [], workspaceRoots = [] } = {}) {
  const full = fullPcAccess === true;
  const configuredRoots = uniqueExistingRoots(pcAccessRoots);
  const roots = configuredRoots.length
    ? configuredRoots
    : full
      ? defaultFullPcRoots()
      : uniqueExistingRoots(workspaceRoots);
  return {
    fullPcAccess: full,
    roots,
    capabilities: [
      'inventory', 'search_files', 'read_text_file', 'list_directory',
      'open_file_or_folder', 'open_http_url', 'list_applications', 'launch_installed_application',
    ],
  };
}

function validatePcPath(value, policy, { kind = 'any' } = {}) {
  const resolved = canonicalExistingPath(value);
  if (!resolved.ok) return resolved;
  const roots = Array.isArray(policy?.roots) ? policy.roots : [];
  if (!roots.length || !roots.some((root) => pathWithin(root, resolved.path))) {
    return { ok: false, error: 'Path is outside the authorized PC roots' };
  }
  if (kind === 'file' && !resolved.stat.isFile()) return { ok: false, error: 'Path is not a file' };
  if (kind === 'directory' && !resolved.stat.isDirectory()) return { ok: false, error: 'Path is not a directory' };
  return resolved;
}

function isSensitivePath(value) {
  const candidate = String(value || '');
  return SENSITIVE_PATH_PATTERNS.some((pattern) => pattern.test(candidate));
}

function safeLimit(value, fallback = DEFAULT_MAX_RESULTS, maximum = 100) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(1, Math.min(maximum, Math.floor(number))) : fallback;
}

function relativeDisplay(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative || path.basename(candidate) || candidate;
}

function directoryShouldBeSkipped(name, includeSystem) {
  if (includeSystem) return false;
  return SKIP_DIRECTORIES.has(String(name || '').toLowerCase());
}

function searchPcFiles(payload, policy) {
  const query = String(payload?.query || '').trim().toLowerCase();
  if (!query) return { ok: false, error: 'A file or folder name query is required' };
  const maxResults = safeLimit(payload?.limit);
  const maxVisited = safeLimit(payload?.maxVisited, DEFAULT_MAX_VISITED, 100_000);
  const maxDepth = safeLimit(payload?.maxDepth, 8, 20);
  const timeoutMs = safeLimit(payload?.timeoutMs, DEFAULT_TIMEOUT_MS, 20_000);
  const includeSystem = payload?.includeSystem === true;
  const requestedRoot = String(payload?.root || '').trim();
  const roots = requestedRoot
    ? (() => {
        const validated = validatePcPath(requestedRoot, policy, { kind: 'directory' });
        return validated.ok ? [validated.path] : validated;
      })()
    : [...(policy?.roots || [])];
  if (!Array.isArray(roots)) return roots;
  if (!roots.length) return { ok: false, error: 'No PC access roots are configured' };

  const startedAt = Date.now();
  const queue = roots.map((root) => ({ root, directory: root, depth: 0 }));
  const results = [];
  let visited = 0;
  let denied = 0;
  let timedOut = false;
  while (queue.length && results.length < maxResults && visited < maxVisited) {
    if (Date.now() - startedAt >= timeoutMs) {
      timedOut = true;
      break;
    }
    const current = queue.shift();
    let entries = [];
    try { entries = fs.readdirSync(current.directory, { withFileTypes: true }); }
    catch { denied += 1; continue; }
    for (const entry of entries) {
      visited += 1;
      if (visited > maxVisited || results.length >= maxResults) break;
      const fullPath = path.join(current.directory, entry.name);
      const nameLower = entry.name.toLowerCase();
      if (nameLower.includes(query)) {
        let size = 0;
        let modifiedAt = '';
        try {
          const stat = fs.statSync(fullPath);
          size = stat.isFile() ? stat.size : 0;
          modifiedAt = stat.mtime.toISOString();
        } catch {}
        results.push({
          name: entry.name,
          path: fullPath,
          relativePath: relativeDisplay(current.root, fullPath),
          type: entry.isDirectory() ? 'directory' : entry.isFile() ? 'file' : 'other',
          size,
          modifiedAt,
          sensitive: isSensitivePath(fullPath),
        });
      }
      if (entry.isDirectory() && current.depth < maxDepth && !directoryShouldBeSkipped(entry.name, includeSystem)) {
        queue.push({ root: current.root, directory: fullPath, depth: current.depth + 1 });
      }
    }
  }
  return {
    ok: true,
    query,
    roots,
    results,
    visited,
    denied,
    truncated: queue.length > 0,
    timedOut,
    elapsedMs: Date.now() - startedAt,
  };
}

function listPcDirectory(payload, policy) {
  const validated = validatePcPath(payload?.path, policy, { kind: 'directory' });
  if (!validated.ok) return validated;
  const limit = safeLimit(payload?.limit, 100, 300);
  let entries;
  try { entries = fs.readdirSync(validated.path, { withFileTypes: true }); }
  catch (error) { return { ok: false, error: String(error?.message || error) }; }
  return {
    ok: true,
    path: validated.path,
    entries: entries.slice(0, limit).map((entry) => ({
      name: entry.name,
      path: path.join(validated.path, entry.name),
      type: entry.isDirectory() ? 'directory' : entry.isFile() ? 'file' : 'other',
      sensitive: isSensitivePath(path.join(validated.path, entry.name)),
    })),
    truncated: entries.length > limit,
  };
}

function readPcTextFile(payload, policy) {
  const validated = validatePcPath(payload?.path, policy, { kind: 'file' });
  if (!validated.ok) return validated;
  if (isSensitivePath(validated.path)) {
    return { ok: false, approvalRequired: true, sensitive: true, error: 'Credential and secret-bearing files are not relayed to the hosted Marcus service' };
  }
  if (BINARY_EXTENSIONS.has(path.extname(validated.path).toLowerCase())) {
    return { ok: false, error: 'Only bounded text files can be relayed through this action' };
  }
  const maxBytes = safeLimit(payload?.maxBytes, 30_000, MAX_READ_BYTES);
  if (validated.stat.size > 5 * 1024 * 1024) return { ok: false, error: 'File is too large for bounded text inspection' };
  try {
    const buffer = Buffer.alloc(Math.min(maxBytes, validated.stat.size));
    const fd = fs.openSync(validated.path, 'r');
    const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, 0);
    fs.closeSync(fd);
    if (buffer.subarray(0, bytesRead).includes(0)) return { ok: false, error: 'File appears to be binary' };
    return {
      ok: true,
      path: validated.path,
      size: validated.stat.size,
      modifiedAt: validated.stat.mtime.toISOString(),
      content: buffer.subarray(0, bytesRead).toString('utf8'),
      truncated: validated.stat.size > bytesRead,
    };
  } catch (error) {
    return { ok: false, error: String(error?.message || error) };
  }
}

function startMenuRoots() {
  return uniqueExistingRoots([
    path.join(process.env.APPDATA || '', 'Microsoft', 'Windows', 'Start Menu', 'Programs'),
    path.join(process.env.ProgramData || '', 'Microsoft', 'Windows', 'Start Menu', 'Programs'),
  ]);
}

function listInstalledApplications(payload = {}) {
  const query = String(payload.query || '').trim().toLowerCase();
  const limit = safeLimit(payload.limit, 80, 200);
  const roots = startMenuRoots();
  const queue = roots.map((root) => ({ root, directory: root, depth: 0 }));
  const applications = [];
  while (queue.length && applications.length < limit) {
    const current = queue.shift();
    let entries = [];
    try { entries = fs.readdirSync(current.directory, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const fullPath = path.join(current.directory, entry.name);
      if (entry.isDirectory() && current.depth < 5) {
        queue.push({ root: current.root, directory: fullPath, depth: current.depth + 1 });
      } else if (entry.isFile() && /\.(?:lnk|appref-ms|url)$/i.test(entry.name)) {
        const name = entry.name.replace(/\.(?:lnk|appref-ms|url)$/i, '');
        if (!query || name.toLowerCase().includes(query)) {
          applications.push({ name, path: fullPath, source: relativeDisplay(current.root, fullPath) });
          if (applications.length >= limit) break;
        }
      }
    }
  }
  applications.sort((left, right) => left.name.localeCompare(right.name));
  return { ok: true, query, applications, roots, truncated: applications.length >= limit };
}

function launchDetached(command, args = []) {
  try {
    const child = spawn(command, args.map(String), { detached: true, stdio: 'ignore', windowsHide: false });
    child.unref();
    return { ok: true, pid: child.pid || 0 };
  } catch (error) {
    return { ok: false, error: String(error?.message || error) };
  }
}

function openPcItem(payload, policy) {
  const target = String(payload?.target || payload?.path || '').trim();
  if (/^https?:\/\//i.test(target)) {
    try { new URL(target); } catch { return { ok: false, error: 'A valid HTTP or HTTPS URL is required' }; }
    const result = process.platform === 'win32'
      ? launchDetached('explorer.exe', [target])
      : launchDetached('xdg-open', [target]);
    return { ...result, target, type: 'url' };
  }
  const validated = validatePcPath(target, policy);
  if (!validated.ok) return validated;
  const result = process.platform === 'win32'
    ? launchDetached('explorer.exe', [validated.path])
    : launchDetached('xdg-open', [validated.path]);
  return { ...result, target: validated.path, type: validated.stat.isDirectory() ? 'directory' : 'file' };
}

function launchInstalledApplication(payload) {
  const requestedName = String(payload?.name || '').trim();
  const requestedPath = String(payload?.applicationPath || '').trim();
  let application = null;
  if (requestedPath) {
    const found = listInstalledApplications({ limit: 200 }).applications.find((item) => comparablePath(item.path) === comparablePath(requestedPath));
    if (found) application = found;
  }
  if (!application && requestedName) {
    const candidates = listInstalledApplications({ query: requestedName, limit: 100 }).applications;
    const normalized = requestedName.toLowerCase();
    application = candidates.find((item) => item.name.toLowerCase() === normalized)
      || (candidates.length === 1 ? candidates[0] : null);
    if (!application && candidates.length > 1) {
      return { ok: false, ambiguous: true, error: 'More than one installed application matches that name', candidates: candidates.slice(0, 12) };
    }
  }
  if (!application) return { ok: false, error: 'Installed application was not found in the Windows Start menu' };
  const result = process.platform === 'win32'
    ? launchDetached('explorer.exe', [application.path])
    : launchDetached('xdg-open', [application.path]);
  return { ...result, application };
}

function getPcInventory(policy) {
  const applications = listInstalledApplications({ limit: 200 });
  const roots = (policy?.roots || []).map((root) => {
    try {
      const stat = fs.statSync(root);
      return { path: root, available: stat.isDirectory() };
    } catch { return { path: root, available: false }; }
  });
  return {
    ok: true,
    host: os.hostname(),
    platform: process.platform,
    release: os.release(),
    fullPcAccess: policy?.fullPcAccess === true,
    roots,
    capabilities: [...(policy?.capabilities || [])],
    installedApplicationCount: applications.applications.length,
    installedApplicationCountTruncated: applications.truncated,
  };
}

module.exports = {
  createPcAccessPolicy,
  getPcInventory,
  isSensitivePath,
  launchInstalledApplication,
  listInstalledApplications,
  listPcDirectory,
  openPcItem,
  pathWithin,
  readPcTextFile,
  searchPcFiles,
  validatePcPath,
};
