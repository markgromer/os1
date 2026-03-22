#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────
// M.A.R.C.U.S. Desktop Agent
// ─────────────────────────────────────────────────────────────
// Captures the active window title, process name, OS idle time,
// and - when an editor is active - the workspace path, git info,
// project structure, and recently modified files. Relays it all
// to a remote M.A.R.C.U.S. server so desktop awareness works
// even when the server is hosted on Render (Linux).
//
// Usage:
//   node desktop-agent.cjs <SERVER_URL> <ADMIN_TOKEN>
//
// Example:
//   node desktop-agent.cjs https://your-app.onrender.com yourSecretToken
//
// The agent runs until you press Ctrl+C.
// ─────────────────────────────────────────────────────────────
const { execFile, exec } = require('child_process');
const https = require('https');
const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');

const SERVER_URL = (process.argv[2] || process.env.MARCUS_SERVER_URL || '').trim();
const ADMIN_TOKEN = (process.argv[3] || process.env.ADMIN_TOKEN || '').trim();

if (!SERVER_URL) {
  console.error('Usage: node desktop-agent.cjs <SERVER_URL> [ADMIN_TOKEN]');
  console.error('  SERVER_URL: e.g. https://your-app.onrender.com');
  console.error('  ADMIN_TOKEN: the same token your server uses for auth');
  process.exit(1);
}

const POLL_MS = 5000;
const WORKSPACE_SCAN_INTERVAL_MS = 30_000; // full workspace scan every 30s
const RELAY_PATH = '/api/desktop-context/relay';

// ── PowerShell capture script ──────────────────────────────────
const SCRIPT_DIR = path.join(os.tmpdir(), 'marcus-agent');
const SCRIPT_PATH = path.join(SCRIPT_DIR, 'desktop-context.ps1');
const PS_SCRIPT = `
$cs = @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public class DesktopInfo {
    [DllImport("user32.dll")] static extern IntPtr GetForegroundWindow();
    [DllImport("user32.dll", CharSet=CharSet.Unicode)] static extern int GetWindowText(IntPtr h, StringBuilder t, int c);
    [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr h, out uint pid);
    [DllImport("user32.dll")] static extern bool GetLastInputInfo(ref LASTINPUTINFO p);
    [StructLayout(LayoutKind.Sequential)]
    struct LASTINPUTINFO { public uint cbSize; public uint dwTime; }
    public static string Query() {
        var sb = new StringBuilder(512);
        IntPtr hw = GetForegroundWindow();
        GetWindowText(hw, sb, 512);
        uint pid; GetWindowThreadProcessId(hw, out pid);
        string pn = ""; try { pn = System.Diagnostics.Process.GetProcessById((int)pid).ProcessName; } catch {}
        var li = new LASTINPUTINFO(); li.cbSize = (uint)Marshal.SizeOf(li);
        GetLastInputInfo(ref li);
        uint idle = ((uint)Environment.TickCount - li.dwTime) / 1000;
        return sb.ToString() + "||" + pn + "||" + idle;
    }
}
"@
try { Add-Type -TypeDefinition $cs -ErrorAction Stop } catch {}
[DesktopInfo]::Query()
`.trim();

try { fs.mkdirSync(SCRIPT_DIR, { recursive: true }); } catch {}
fs.writeFileSync(SCRIPT_PATH, PS_SCRIPT, 'utf8');

// ── Editor process detection ────────────────────────────────────
const EDITOR_PROCESSES = /^(code|cursor|devenv|webstorm64|idea64|pycharm64|phpstorm64|clion64|rider64|goland64|sublime_text|atom)$/i;
const SKIP_DIRS = new Set(['node_modules', '.git', 'dist', 'build', '.next', '__pycache__', '.venv', 'venv', '.idea', '.vs', 'coverage', '.parcel-cache', '.turbo', 'out']);

// Cache for workspace scanning (avoid rescanning every 5s)
let lastWorkspacePath = '';
let lastWorkspaceScanAt = 0;
let cachedWorkspaceInfo = null;
let cachedFileContents = null;
let cachedGitDiff = '';
let lastFileContentsAt = 0;

function extractWorkspaceFromTitle(windowTitle) {
  const parts = windowTitle.split(' - ').map(p => p.trim()).filter(Boolean);
  if (parts.length < 2) return '';
  // Strip editor name tail
  const editorTail = /visual studio code|vscode|cursor|webstorm|intellij|pycharm|phpstorm|clion|rider|goland|sublime text|atom/i;
  if (parts.length >= 2 && /^insiders$/i.test(parts[parts.length - 1]) && /visual studio code/i.test(parts[parts.length - 2])) {
    parts.splice(-2);
  }
  if (parts.length && editorTail.test(parts[parts.length - 1])) parts.pop();
  // Clean segments
  const cleaned = parts.map(p => p.replace(/^[●◉]\s*/, '').replace(/\s*\[(?:SSH|WSL|Remote|Dev Container|Codespace|Tunnel)[^\]]*\]\s*$/i, '').replace(/\s*\(Workspace\)\s*$/i, '').trim()).filter(Boolean);
  // Last non-file segment is typically the workspace/folder name
  for (let i = cleaned.length - 1; i >= 0; i--) {
    if (!/\.[a-z0-9]{1,6}$/i.test(cleaned[i])) return cleaned[i];
  }
  return cleaned[cleaned.length - 1] || '';
}

// ── Find VS Code workspace folder using its storage DB ──────────
function findWorkspacePath(workspaceName) {
  if (!workspaceName) return '';
  const appDataPath = process.env.APPDATA || '';
  if (!appDataPath) return '';

  // Check VS Code and Cursor storage locations
  const storagePaths = [
    path.join(appDataPath, 'Code', 'User', 'globalStorage', 'storage.json'),
    path.join(appDataPath, 'Code - Insiders', 'User', 'globalStorage', 'storage.json'),
    path.join(appDataPath, 'Cursor', 'User', 'globalStorage', 'storage.json'),
  ];

  const nameLower = workspaceName.toLowerCase().replace(/[-_\s]+/g, ' ').trim();

  for (const sp of storagePaths) {
    try {
      if (!fs.existsSync(sp)) continue;
      const raw = fs.readFileSync(sp, 'utf8');
      const data = JSON.parse(raw);

      // Modern VS Code: profileAssociations.workspaces has URIs as keys
      const paWorkspaces = data?.profileAssociations?.workspaces;
      if (paWorkspaces && typeof paWorkspaces === 'object') {
        for (const uri of Object.keys(paWorkspaces)) {
          try {
            if (!uri.startsWith('file:///')) continue;
            const folderPath = decodeURIComponent(uri.replace('file:///', '').replace(/\//g, path.sep));
            const folderName = path.basename(folderPath).toLowerCase().replace(/[-_\s]+/g, ' ').trim();
            if (folderName === nameLower) return folderPath;
          } catch {}
        }
      }

      // Legacy VS Code: openedPathsList.entries or openedPathsList.workspaces3
      const entries = data?.openedPathsList?.entries || data?.openedPathsList?.workspaces3 || [];
      for (const entry of entries) {
        const uri = typeof entry === 'string' ? entry : (entry?.folderUri || entry?.configPath || '');
        if (!uri) continue;
        try {
          let folderPath = '';
          if (uri.startsWith('file:///')) {
            folderPath = decodeURIComponent(uri.replace('file:///', '').replace(/\//g, path.sep));
          } else if (/^[a-zA-Z]:/.test(uri)) {
            folderPath = uri;
          }
          if (!folderPath) continue;
          const folderName = path.basename(folderPath).toLowerCase().replace(/[-_\s]+/g, ' ').trim();
          if (folderName === nameLower) return folderPath;
        } catch {}
      }
    } catch {}
  }
  return '';
}

// ── Run a git command in a directory ────────────────────────────
function gitCmd(cwd, args) {
  return new Promise((resolve) => {
    execFile('git', args, { cwd, windowsHide: true, timeout: 5000 }, (err, stdout) => {
      resolve(err ? '' : String(stdout || '').trim());
    });
  });
}

// ── Scan a workspace directory for structure + git info ─────────
async function scanWorkspace(wsPath) {
  if (!wsPath || !fs.existsSync(wsPath)) return null;

  const info = {
    workspacePath: wsPath,
    folderName: path.basename(wsPath),
    gitBranch: '',
    gitStatus: [],
    gitRecentCommits: [],
    recentFiles: [],
    structure: [],
  };

  // Git branch
  info.gitBranch = await gitCmd(wsPath, ['rev-parse', '--abbrev-ref', 'HEAD']);

  // Git status (changed/staged files)
  const statusRaw = await gitCmd(wsPath, ['status', '--porcelain', '-u']);
  if (statusRaw) {
    info.gitStatus = statusRaw.split('\n').slice(0, 30).map(line => {
      const status = line.slice(0, 2).trim();
      const file = line.slice(3).trim();
      return { status, file };
    });
  }

  // Recent commits (last 5)
  const logRaw = await gitCmd(wsPath, ['log', '--oneline', '-5', '--no-decorate']);
  if (logRaw) {
    info.gitRecentCommits = logRaw.split('\n').map(l => l.trim()).filter(Boolean);
  }

  // Recently modified files (last 10 minutes, max 20)
  const tenMinAgo = new Date(Date.now() - 10 * 60 * 1000);
  try {
    const allFiles = [];
    const walk = (dir, depth = 0) => {
      if (depth > 3 || allFiles.length > 50) return;
      let entries;
      try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
      for (const e of entries) {
        if (SKIP_DIRS.has(e.name) || e.name.startsWith('.')) continue;
        const full = path.join(dir, e.name);
        if (e.isDirectory()) {
          walk(full, depth + 1);
        } else if (e.isFile()) {
          try {
            const stat = fs.statSync(full);
            if (stat.mtime >= tenMinAgo) {
              allFiles.push({ file: path.relative(wsPath, full), mtime: stat.mtime.toISOString() });
            }
          } catch {}
        }
      }
    };
    walk(wsPath);
    allFiles.sort((a, b) => b.mtime.localeCompare(a.mtime));
    info.recentFiles = allFiles.slice(0, 20).map(f => f.file);
  } catch {}

  // Top-level directory listing
  try {
    const entries = fs.readdirSync(wsPath, { withFileTypes: true });
    const dirs = [];
    const files = [];
    for (const e of entries) {
      if (e.name.startsWith('.') || SKIP_DIRS.has(e.name)) continue;
      if (e.isDirectory()) dirs.push(e.name + '/');
      else files.push(e.name);
    }
    info.structure = [...dirs.sort(), ...files.sort()].slice(0, 40);
  } catch {}

  return info;
}

// ── Read contents of recently modified files ────────────────────
function readRecentFileContents(wsPath, recentFiles) {
  const contents = {};
  const MAX_FILES = 5;
  const MAX_BYTES = 15_000;
  for (const rel of recentFiles.slice(0, MAX_FILES)) {
    try {
      const full = path.join(wsPath, rel);
      const stat = fs.statSync(full);
      if (stat.size > 100_000) continue;
      let text = fs.readFileSync(full, 'utf8');
      if (text.length > MAX_BYTES) text = text.slice(0, MAX_BYTES) + '\n... (truncated)';
      contents[rel] = text;
    } catch {}
  }
  return contents;
}

// ── Get unified git diff of uncommitted work ────────────────────
async function getGitDiff(wsPath) {
  // staged + unstaged diff
  const diff = await gitCmd(wsPath, ['diff', 'HEAD']);
  if (!diff) return '';
  return diff.length > 25_000 ? diff.slice(0, 25_000) + '\n... (diff truncated)' : diff;
}

// ── Capture desktop context via PowerShell ──────────────────────
function captureDesktop() {
  return new Promise((resolve) => {
    execFile(
      'powershell.exe',
      ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', SCRIPT_PATH],
      { windowsHide: true, timeout: 5000 },
      (err, stdout) => {
        if (err) return resolve(null);
        const parts = String(stdout || '').trim().split('||');
        resolve({
          windowTitle: (parts[0] || '').trim(),
          processName: (parts[1] || '').trim().toLowerCase(),
          idleSeconds: Math.max(0, Number(parts[2]) || 0),
        });
      }
    );
  });
}

// ── Send data to the server ─────────────────────────────────────
function relay(data) {
  return new Promise((resolve) => {
    const body = JSON.stringify(data);
    const url = new URL(RELAY_PATH, SERVER_URL);
    const isHttps = url.protocol === 'https:';
    const mod = isHttps ? https : http;

    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    };
    if (ADMIN_TOKEN) {
      headers['Authorization'] = `Bearer ${ADMIN_TOKEN}`;
    }

    const req = mod.request(
      {
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname,
        method: 'POST',
        headers,
        timeout: 8000,
      },
      (res) => {
        let buf = '';
        res.on('data', (d) => { buf += d; });
        res.on('end', () => resolve({ status: res.statusCode, body: buf }));
      }
    );

    req.on('error', (e) => resolve({ status: 0, body: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ status: 0, body: 'timeout' }); });
    req.write(body);
    req.end();
  });
}

// ── Main loop ───────────────────────────────────────────────────
let consecutive = 0;
let lastTitle = '';

async function tick() {
  const ctx = await captureDesktop();
  if (!ctx) {
    if (++consecutive >= 3) {
      process.stdout.write('  [!] Desktop capture failing - is this Windows?\r');
    }
    return;
  }
  consecutive = 0;

  const brief = ctx.windowTitle.length > 60
    ? ctx.windowTitle.slice(0, 57) + '...'
    : ctx.windowTitle;

  if (ctx.windowTitle !== lastTitle) {
    const ts = new Date().toLocaleTimeString();
    console.log(`[${ts}] ${ctx.processName} | ${brief} | idle ${ctx.idleSeconds}s`);
    lastTitle = ctx.windowTitle;
  }

  // When an editor is active, capture workspace context
  const isEditor = EDITOR_PROCESSES.test(ctx.processName);
  let workspace = null;

  if (isEditor) {
    const wsName = extractWorkspaceFromTitle(ctx.windowTitle);
    let wsPath = '';

    // Try to find the actual folder path
    if (wsName) {
      wsPath = findWorkspacePath(wsName);
    }

    // Rescan workspace if it changed or if it's time for a periodic refresh
    const now = Date.now();
    if (wsPath && (wsPath !== lastWorkspacePath || (now - lastWorkspaceScanAt) > WORKSPACE_SCAN_INTERVAL_MS)) {
      const ts = new Date().toLocaleTimeString();
      console.log(`[${ts}] Scanning workspace: ${wsPath}`);
      cachedWorkspaceInfo = await scanWorkspace(wsPath);
      lastWorkspacePath = wsPath;
      lastWorkspaceScanAt = now;
    } else if (!wsPath && wsName) {
      // Couldn't find the path but have the name
      cachedWorkspaceInfo = { workspacePath: '', folderName: wsName, gitBranch: '', gitStatus: [], gitRecentCommits: [], recentFiles: [], structure: [] };
      lastWorkspacePath = '';
    }

    workspace = cachedWorkspaceInfo;

    // Deep context: read file contents + git diff (alongside scan, so same interval)
    if (workspace && workspace.workspacePath) {
      const now2 = Date.now();
      if (wsPath === lastWorkspacePath && cachedFileContents && (now2 - lastFileContentsAt) < WORKSPACE_SCAN_INTERVAL_MS) {
        workspace = { ...workspace, fileContents: cachedFileContents, gitDiff: cachedGitDiff };
      } else {
        const fc = readRecentFileContents(wsPath, workspace.recentFiles || []);
        const gd = await getGitDiff(wsPath);
        cachedFileContents = fc;
        cachedGitDiff = gd;
        lastFileContentsAt = now2;
        workspace = { ...workspace, fileContents: fc, gitDiff: gd };
        if (Object.keys(fc).length) {
          const ts2 = new Date().toLocaleTimeString();
          console.log(`[${ts2}] Read ${Object.keys(fc).length} file(s) + diff (${gd.length} chars)`);
        }
      }
    }
  } else {
    // Not in an editor - clear workspace cache
    if (lastWorkspacePath) {
      lastWorkspacePath = '';
      cachedWorkspaceInfo = null;
    }
  }

  // Build relay payload
  const payload = {
    windowTitle: ctx.windowTitle,
    processName: ctx.processName,
    idleSeconds: ctx.idleSeconds,
  };
  if (workspace) {
    payload.workspace = workspace;
  }

  const result = await relay(payload);
  if (result.status === 401) {
    console.error('[!] 401 Unauthorized - check your ADMIN_TOKEN');
  } else if (result.status && result.status !== 200) {
    console.error(`[!] Server returned ${result.status}: ${result.body.slice(0, 120)}`);
  }
}

console.log('');
console.log('  M.A.R.C.U.S. Desktop Agent');
console.log(`  Server: ${SERVER_URL}`);
console.log(`  Auth:   ${ADMIN_TOKEN ? 'Bearer token set' : 'no token (local mode)'}`);
console.log(`  Poll:   every ${POLL_MS / 1000}s`);
console.log('  Press Ctrl+C to stop.');
console.log('');

tick();
setInterval(tick, POLL_MS);
