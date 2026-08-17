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
const { execFile, exec, spawn } = require('child_process');
const https = require('https');
const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');
const readline = require('readline');
const { discoverRecentCodexWorkspaces, parseGitStatus } = require('./desktop-codex-sessions.cjs');
const { localPackageBinInvocation, npmCliInvocation } = require('./desktop-node-cli.cjs');
const { MarcusBrowserBridge } = require('./desktop-marcus-browser.cjs');
const {
  createPcAccessPolicy,
  createPcDirectory,
  deletePcItem,
  getPcInventory,
  launchInstalledApplication,
  listInstalledApplications,
  listPcDirectory,
  movePcItem,
  openPcItem,
  readPcTextFile,
  runPcPowerShell,
  searchPcFiles,
  toDesktopActionOutcome,
  writePcTextFile,
} = require('./desktop-pc-operator.cjs');

const SERVER_URL = (process.argv[2] || process.env.MARCUS_SERVER_URL || '').trim();
const DEFAULT_ADMIN_TOKEN_FILE = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  'M.A.R.C.U.S',
  'mobile-live-admin-token.txt',
);
const DEFAULT_DESKTOP_CONFIG_FILE = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  'M.A.R.C.U.S',
  'desktop-agent.json',
);

function desktopAgentConfigFile() {
  return String(process.env.MARCUS_DESKTOP_CONFIG_FILE || DEFAULT_DESKTOP_CONFIG_FILE).trim();
}

function readDesktopAgentConfig() {
  const configFile = desktopAgentConfigFile();
  if (!configFile) return {};
  try {
    const value = JSON.parse(fs.readFileSync(configFile, 'utf8').replace(/^\uFEFF/, ''));
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  } catch {
    return {};
  }
}

function booleanSetting(environmentName, configValue, fallback = false) {
  const environmentValue = String(process.env[environmentName] || '').trim().toLowerCase();
  if (environmentValue) return environmentValue === 'true';
  return typeof configValue === 'boolean' ? configValue : fallback;
}

function listSetting(environmentName, configValue) {
  const environmentValue = String(process.env[environmentName] || '').trim();
  if (environmentValue) return environmentValue.split(path.delimiter).map((value) => value.trim()).filter(Boolean);
  return Array.isArray(configValue) ? configValue.map((value) => String(value || '').trim()).filter(Boolean) : [];
}

const DESKTOP_CONFIG = readDesktopAgentConfig();

function readAdminTokenFile() {
  const tokenFile = String(process.env.MARCUS_ADMIN_TOKEN_FILE || DEFAULT_ADMIN_TOKEN_FILE).trim();
  if (!tokenFile) return '';
  try {
    return fs.readFileSync(tokenFile, 'utf8').trim();
  } catch {
    return '';
  }
}

const ADMIN_TOKEN = (process.argv[3] || process.env.ADMIN_TOKEN || readAdminTokenFile()).trim();
const DESKTOP_AGENT_ID = String(process.env.MARCUS_DESKTOP_AGENT_ID || DESKTOP_CONFIG.agentId || os.hostname()).trim().slice(0, 200);
const ALLOWED_WORKSPACE_ROOT_VALUES = listSetting('MARCUS_ALLOWED_WORKSPACE_ROOTS', DESKTOP_CONFIG.allowedWorkspaceRoots);
const ALLOW_BROAD_WORKSPACE_ROOTS = booleanSetting('MARCUS_ALLOW_BROAD_WORKSPACE_ROOTS', DESKTOP_CONFIG.allowBroadWorkspaceRoots);
let FULL_PC_ACCESS = booleanSetting('MARCUS_FULL_PC_ACCESS', DESKTOP_CONFIG.fullPcAccess);
let PC_ACCESS_ROOT_VALUES = listSetting('MARCUS_PC_ACCESS_ROOTS', DESKTOP_CONFIG.pcAccessRoots);
const CODEX_MONITOR_MODE = String(process.env.MARCUS_CODEX_MONITOR_MODE || DESKTOP_CONFIG.codexMonitorMode || 'kiosk').trim().toLowerCase();
const NEW_PROJECT_ROOT = String(process.env.MARCUS_NEW_PROJECT_ROOT
  || DESKTOP_CONFIG.newProjectRoot
  || path.join(os.homedir(), 'OneDrive', 'Documents', 'Marcus Projects')).trim();
let PC_ACCESS_POLICY = createPcAccessPolicy({
  fullPcAccess: FULL_PC_ACCESS,
  pcAccessRoots: PC_ACCESS_ROOT_VALUES,
  workspaceRoots: ALLOWED_WORKSPACE_ROOT_VALUES,
});

function normalizedRootKey(value) {
  const resolved = path.resolve(String(value || ''));
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function sameRoots(left, right) {
  const normalize = (values) => (Array.isArray(values) ? values : []).map(normalizedRootKey).sort();
  return JSON.stringify(normalize(left)) === JSON.stringify(normalize(right));
}

function persistDesktopAgentConfig(patch = {}) {
  const configFile = desktopAgentConfigFile();
  if (!configFile) throw new Error('The desktop agent config path is unavailable.');
  const current = readDesktopAgentConfig();
  const next = { ...current, ...patch, version: 1, updatedAt: new Date().toISOString() };
  const directory = path.dirname(configFile);
  const temporary = `${configFile}.${process.pid}.${Date.now()}.tmp`;
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(temporary, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
  try {
    fs.renameSync(temporary, configFile);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
  return next;
}

function validatePcAccessRootShape(value) {
  const candidate = String(value || '').trim();
  if (process.platform === 'win32') return /^[A-Za-z]:[\\/]?$/.test(candidate);
  return candidate === '/';
}

function configurePcAccess(payload = {}) {
  if (Object.prototype.hasOwnProperty.call(process.env, 'MARCUS_FULL_PC_ACCESS')
    || Object.prototype.hasOwnProperty.call(process.env, 'MARCUS_PC_ACCESS_ROOTS')) {
    return { ok: false, error: 'PC access is overridden by environment variables and cannot be changed durably by an approved operation.' };
  }
  const requestedRoots = Array.isArray(payload.pcAccessRoots)
    ? [...new Set(payload.pcAccessRoots.map((value) => String(value || '').trim()).filter(Boolean))]
    : [];
  if (!requestedRoots.length || !requestedRoots.every(validatePcAccessRootShape)) {
    return { ok: false, error: 'The approved PC access target must contain exact local drive roots.' };
  }
  const fullPcAccess = payload.fullPcAccess === true;
  if (!fullPcAccess) return { ok: false, error: 'This action only supports the exact approved full-PC grant.' };
  const nextPolicy = createPcAccessPolicy({ fullPcAccess, pcAccessRoots: requestedRoots, workspaceRoots: ALLOWED_WORKSPACE_ROOT_VALUES });
  if (!nextPolicy.roots.length || nextPolicy.roots.length !== requestedRoots.length) {
    return { ok: false, error: 'One or more approved PC access roots do not exist or are unavailable.' };
  }
  try {
    persistDesktopAgentConfig({ fullPcAccess, pcAccessRoots: nextPolicy.roots });
  } catch (error) {
    return { ok: false, error: `The desktop access policy could not be persisted: ${String(error?.message || error)}` };
  }
  FULL_PC_ACCESS = true;
  PC_ACCESS_ROOT_VALUES = [...nextPolicy.roots];
  PC_ACCESS_POLICY = nextPolicy;
  return {
    ok: true,
    details: {
      scope: 'full_pc', fullPcAccess: true, pcAccessRoots: [...nextPolicy.roots],
      capabilities: [...nextPolicy.capabilities], policyVersion: Number(payload.policyVersion) || 1,
      persisted: true, runtimeApplied: true, credentialContentBlocked: true,
      arbitraryShellExecutionAllowed: false, consequentialActionsRemainApprovalGated: true,
    },
  };
}

function verifyPcAccess(payload = {}) {
  const expectedRoots = Array.isArray(payload.pcAccessRoots)
    ? payload.pcAccessRoots.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  const persisted = readDesktopAgentConfig();
  const persistedPolicy = createPcAccessPolicy({
    fullPcAccess: persisted.fullPcAccess === true,
    pcAccessRoots: Array.isArray(persisted.pcAccessRoots) ? persisted.pcAccessRoots : [],
    workspaceRoots: ALLOWED_WORKSPACE_ROOT_VALUES,
  });
  const verified = payload.fullPcAccess === true
    && FULL_PC_ACCESS === true
    && PC_ACCESS_POLICY.fullPcAccess === true
    && persisted.fullPcAccess === true
    && sameRoots(expectedRoots, PC_ACCESS_POLICY.roots)
    && sameRoots(expectedRoots, persistedPolicy.roots);
  return verified
    ? {
        ok: true,
        details: {
          verified: true, scope: 'full_pc', fullPcAccess: true, pcAccessRoots: [...PC_ACCESS_POLICY.roots],
          capabilities: [...PC_ACCESS_POLICY.capabilities], persisted: true, runtimeApplied: true,
          credentialContentBlocked: true, arbitraryShellExecutionAllowed: false,
          consequentialActionsRemainApprovalGated: true,
        },
      }
    : { ok: false, error: 'The runtime and persisted PC access policies do not match the exact approved target.' };
}

function validateBoundPcAccessAction(action = {}) {
  const payload = action?.payload && typeof action.payload === 'object' ? action.payload : {};
  const operationId = String(payload.operationId || '').trim();
  return Boolean(
    operationId
    && String(action.requestedBy || '').trim() === `operation:${operationId}`
    && String(payload.businessKey || '').trim()
    && String(payload.stepId || '').trim()
    && String(payload.idempotencyKey || '').trim()
    && String(payload.desktopAgentId || '').trim() === DESKTOP_AGENT_ID
  );
}

if (!SERVER_URL) {
  console.error('Usage: node desktop-agent.cjs <SERVER_URL> [ADMIN_TOKEN]');
  console.error('  SERVER_URL: e.g. https://your-app.onrender.com');
  console.error(`  ADMIN_TOKEN: optional; defaults to ${DEFAULT_ADMIN_TOKEN_FILE}`);
  process.exit(1);
}

if (!ADMIN_TOKEN && new URL(SERVER_URL).protocol === 'https:') {
  console.error(`ADMIN_TOKEN is required for a remote server. Add it to ${DEFAULT_ADMIN_TOKEN_FILE}.`);
  process.exit(1);
}

const POLL_MS = 5000;
const BROWSER_ACTION_POLL_MS = 350;
const BROWSER_FRAME_INTERVAL_MS = 1200;
const WORKSPACE_SCAN_INTERVAL_MS = 30_000; // full workspace scan every 30s
const CODEX_WORKSPACE_SCAN_INTERVAL_MS = 30_000;
const SYSTEM_HEALTH_INTERVAL_MS = 60_000; // system health check every 60s
const RELAY_PATH = '/api/desktop-context/relay';
const ALLOWED_NPM_SCRIPTS = new Set(['install', 'dev', 'build', 'test', 'lint', 'typecheck']);
const marcusBrowser = new MarcusBrowserBridge({
  debugPort: process.env.MARCUS_CHROME_DEBUG_PORT || DESKTOP_CONFIG.chromeDebugPort,
  profileRoot: process.env.MARCUS_CHROME_PROFILE_ROOT || DESKTOP_CONFIG.chromeProfileRoot,
  defaultUrl: process.env.MARCUS_CHROME_DEFAULT_URL || DESKTOP_CONFIG.chromeDefaultUrl,
});

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
let cachedCodexWorkspaces = [];
let lastCodexWorkspaceScanAt = 0;

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
    try {
      execFile('git', args, { cwd, windowsHide: true, timeout: 5000 }, (err, stdout) => {
        resolve(err ? '' : String(stdout || '').trimEnd());
      });
    } catch {
      resolve('');
    }
  });
}

function runLocalCommand(cwd, command, args, timeout = 60_000) {
  return new Promise((resolve) => {
    try {
      execFile(command, args, { cwd, windowsHide: true, timeout }, (err, stdout, stderr) => {
        resolve({
          ok: !err,
          code: err?.code ?? 0,
          stdout: String(stdout || '').slice(-8000),
          stderr: String(stderr || err?.message || '').slice(-8000),
        });
      });
    } catch (error) {
      resolve({ ok: false, code: error?.code || 1, stdout: '', stderr: String(error?.message || error) });
    }
  });
}

function readPackageScripts(cwd) {
  try {
    const packagePath = path.join(cwd, 'package.json');
    if (!fs.existsSync(packagePath)) return {};
    const pkg = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
    return pkg && typeof pkg.scripts === 'object' && pkg.scripts ? pkg.scripts : {};
  } catch {
    return {};
  }
}

function comparablePath(value) {
  const resolved = path.resolve(value);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function pathWithin(root, candidate) {
  const relative = path.relative(comparablePath(root), comparablePath(candidate));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function broadWorkspaceRoots() {
  const home = path.resolve(os.homedir());
  return new Set([
    path.parse(home).root, home, path.join(home, 'Documents'), path.join(home, 'OneDrive'), path.join(home, 'OneDrive', 'Documents'),
  ].map(comparablePath));
}

function allowedWorkspaceRoots() {
  const broad = broadWorkspaceRoots();
  const roots = [];
  for (const value of ALLOWED_WORKSPACE_ROOT_VALUES) {
    let canonical;
    try { canonical = fs.realpathSync.native(path.resolve(value)); } catch { continue; }
    if (!ALLOW_BROAD_WORKSPACE_ROOTS && broad.has(comparablePath(canonical))) continue;
    if (!roots.some((root) => comparablePath(root) === comparablePath(canonical))) roots.push(canonical);
  }
  return roots;
}

function validateWorkspaceFolder(projectPath, binding = {}) {
  const target = String(projectPath || '').trim();
  if (!target) return { ok: false, error: 'Path required' };
  if (binding.desktopAgentId && binding.desktopAgentId !== DESKTOP_AGENT_ID) return { ok: false, error: 'Desktop agent identity does not match the registered workspace binding' };
  if (binding.requireProjectBinding && !String(binding.projectRegistryId || '').trim()) return { ok: false, error: 'Project registry binding is required' };
  let canonical;
  try {
    canonical = fs.realpathSync.native(path.resolve(target));
    const stat = fs.statSync(canonical);
    if (!stat.isDirectory()) return { ok: false, error: 'Path is not a folder' };
  } catch {
    return { ok: false, error: 'Path does not exist' };
  }
  const roots = allowedWorkspaceRoots();
  if (!roots.length) return { ok: false, error: 'No safe MARCUS_ALLOWED_WORKSPACE_ROOTS are configured' };
  if (!roots.some((root) => pathWithin(root, canonical))) return { ok: false, error: 'Path is outside the configured workspace roots' };
  return { ok: true, path: canonical };
}

async function preparePublish(projectPath) {
  const valid = validateWorkspaceFolder(projectPath);
  if (!valid.ok) return valid;
  const cwd = valid.path;

  const inside = await gitCmd(cwd, ['rev-parse', '--is-inside-work-tree']);
  if (inside !== 'true') return { ok: false, error: 'Folder is not a git repository' };

  const branch = await gitCmd(cwd, ['rev-parse', '--abbrev-ref', 'HEAD']);
  const upstream = await gitCmd(cwd, ['rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{u}']);
  const remote = await gitCmd(cwd, ['remote', 'get-url', 'origin']);
  const statusRaw = await gitCmd(cwd, ['status', '--porcelain', '--untracked-files=normal']);
  const diffStat = await gitCmd(cwd, ['diff', '--stat', 'HEAD']);
  const recentCommitsRaw = await gitCmd(cwd, ['log', '--oneline', '-5', '--no-decorate']);
  const scripts = readPackageScripts(cwd);

  const changes = parseGitStatus(statusRaw, 80).entries;

  return {
    ok: true,
    details: {
      path: cwd,
      branch,
      upstream,
      remote,
      hasChanges: changes.length > 0,
      changes,
      diffStat,
      packageScripts: Object.keys(scripts),
      recentCommits: recentCommitsRaw ? recentCommitsRaw.split('\n') : [],
      recommended: {
        buildScript: scripts.build ? 'build' : '',
        testScript: scripts.test ? 'test' : '',
      },
    },
  };
}

async function runNpmScript(cwd, scriptName) {
  const clean = String(scriptName || '').trim();
  if (!clean) return { ok: true, skipped: true };
  if (!/^[A-Za-z0-9:_-]+$/.test(clean)) return { ok: false, stderr: 'Invalid npm script name' };
  if (!ALLOWED_NPM_SCRIPTS.has(clean)) return { ok: false, stderr: `npm script "${clean}" is not allowlisted` };
  const scripts = readPackageScripts(cwd);
  if (!scripts[clean]) return { ok: false, stderr: `package.json has no "${clean}" script` };
  const invocation = npmCliInvocation(['run', clean]);
  if (!invocation.ok) return { ok: false, stderr: invocation.error };
  return await runLocalCommand(cwd, invocation.command, invocation.args, 120_000);
}

async function publishProjectChanges(payload) {
  const authorizedActions = new Set(Array.isArray(payload?.authorizedActions) ? payload.authorizedActions.map((item) => String(item || '').trim()) : []);
  if (payload?.commit !== true || !authorizedActions.has('commit')) {
    return { ok: false, error: 'The queued payload lacks exact commit authorization' };
  }
  if (payload?.push !== false && !authorizedActions.has('push')) {
    return { ok: false, error: 'The queued payload requests push without exact push authorization' };
  }
  const valid = validateWorkspaceFolder(payload?.path, payload || {});
  if (!valid.ok) return valid;
  const cwd = valid.path;

  const inside = await gitCmd(cwd, ['rev-parse', '--is-inside-work-tree']);
  if (inside !== 'true') return { ok: false, error: 'Folder is not a git repository' };

  const commitMessage = String(payload?.commitMessage || '').trim();
  if (!commitMessage) return { ok: false, error: 'Commit message required' };

  const before = await preparePublish(cwd);
  if (!before.ok) return before;
  if (!before.details.hasChanges) {
    return { ok: false, error: 'No local changes to commit', details: before.details };
  }

  const testResult = await runNpmScript(cwd, payload?.testScript);
  if (!testResult.ok) return { ok: false, error: 'Test script failed', details: { before: before.details, testResult } };

  const buildResult = await runNpmScript(cwd, payload?.buildScript);
  if (!buildResult.ok) return { ok: false, error: 'Build script failed', details: { before: before.details, testResult, buildResult } };

  const addResult = await runLocalCommand(cwd, 'git', ['add', '-A'], 30_000);
  if (!addResult.ok) return { ok: false, error: 'git add failed', details: { before: before.details, addResult } };

  const commitResult = await runLocalCommand(cwd, 'git', ['commit', '-m', commitMessage], 60_000);
  if (!commitResult.ok) return { ok: false, error: 'git commit failed', details: { before: before.details, testResult, buildResult, commitResult } };

  const branch = await gitCmd(cwd, ['rev-parse', '--abbrev-ref', 'HEAD']);
  let pushResult = { ok: true, skipped: true };
  if (payload?.push !== false) {
    pushResult = await runLocalCommand(cwd, 'git', ['push', '-u', 'origin', branch], 120_000);
    if (!pushResult.ok) return { ok: false, error: 'git push failed', details: { before: before.details, testResult, buildResult, commitResult, pushResult, branch } };
  }

  const after = await preparePublish(cwd);
  return {
    ok: true,
    details: {
      before: before.details,
      after: after.details,
      testResult,
      buildResult,
      commitResult,
      pushResult,
      branch,
    },
  };
}

async function runProjectScript(payload) {
  const valid = validateWorkspaceFolder(payload?.path, { ...payload, requireProjectBinding: true });
  if (!valid.ok) return valid;
  const cwd = valid.path;
  const scriptName = String(payload?.scriptName || '').trim();
  if (!scriptName) return { ok: false, error: 'scriptName is required' };

  const result = await runNpmScript(cwd, scriptName);
  return {
    ok: result.ok,
    error: result.ok ? '' : `npm script "${scriptName}" failed`,
    details: {
      path: cwd,
      scriptName,
      result,
    },
  };
}

function defaultCloneParentPath() {
  const docs = path.join(os.homedir(), 'OneDrive', 'Documents');
  if (fs.existsSync(docs)) return docs;
  return path.join(os.homedir(), 'Documents');
}

function deriveRepoFolderName(repoUrl) {
  const raw = String(repoUrl || '').trim().replace(/\.git$/i, '');
  const parts = raw.split(/[/:\\]/).filter(Boolean);
  const last = parts[parts.length - 1] || 'repo';
  return last.replace(/[^A-Za-z0-9._-]+/g, '-').slice(0, 80) || 'repo';
}

async function cloneGithubProject(payload) {
  const repoUrl = String(payload?.repoUrl || '').trim();
  if (!/^((https:\/\/github\.com\/[^/\s]+\/[^/\s]+?(\.git)?)|(git@github\.com:[^/\s]+\/[^/\s]+?(\.git)?))$/i.test(repoUrl)) {
    return { ok: false, error: 'A GitHub HTTPS or SSH clone URL is required' };
  }

  const parentRaw = String(payload?.parentPath || '').trim();
  const parentPath = parentRaw || defaultCloneParentPath();
  let parentStat = null;
  try {
    parentStat = fs.statSync(parentPath);
  } catch {
    return { ok: false, error: `Parent folder does not exist: ${parentPath}` };
  }
  if (!parentStat.isDirectory()) return { ok: false, error: 'Parent path is not a folder' };

  const folderNameRaw = String(payload?.folderName || '').trim();
  const folderName = (folderNameRaw || deriveRepoFolderName(repoUrl)).replace(/[^A-Za-z0-9._ -]+/g, '-').trim();
  const destination = path.resolve(parentPath, folderName);
  if (!destination.startsWith(path.resolve(parentPath))) return { ok: false, error: 'Invalid destination folder' };

  if (fs.existsSync(destination)) {
    const gitDir = path.join(destination, '.git');
    const alreadyRepo = fs.existsSync(gitDir);
    const openResult = payload?.openInVsCode === false ? { ok: true, skipped: true } : await openVsCode(destination);
    return {
      ok: alreadyRepo && openResult.ok,
      error: alreadyRepo ? (openResult.ok ? '' : openResult.error) : 'Destination exists and is not a git repository',
      details: { repoUrl, destination, alreadyExisted: true, openResult },
    };
  }

  const cloneResult = await runLocalCommand(parentPath, 'git', ['clone', repoUrl, folderName], 180_000);
  if (!cloneResult.ok) {
    return { ok: false, error: 'git clone failed', details: { repoUrl, destination, cloneResult } };
  }

  const prep = await preparePublish(destination);
  const openResult = payload?.openInVsCode === false ? { ok: true, skipped: true } : await openVsCode(destination);
  return {
    ok: openResult.ok,
    error: openResult.ok ? '' : openResult.error,
    details: { repoUrl, destination, cloneResult, prepare: prep.details || null, openResult },
  };
}

async function setPerformanceProfile(payload) {
  const mode = String(payload?.mode || '').trim().toLowerCase();
  const powerSchemeByMode = {
    balanced: 'SCHEME_BALANCED',
    performance: 'SCHEME_MIN',
    'power-saver': 'SCHEME_MAX',
    optimize: 'SCHEME_BALANCED',
  };
  const scheme = powerSchemeByMode[mode];
  if (!scheme) return { ok: false, error: 'Invalid performance mode' };

  const details = { mode, scheme, steps: [] };
  const power = await runLocalCommand(process.cwd(), 'powercfg', ['/setactive', scheme], 20_000);
  details.steps.push({ name: 'powercfg', ...power });

  if (mode === 'optimize') {
    const dns = await runLocalCommand(process.cwd(), 'powershell.exe', ['-NoProfile', '-Command', 'Clear-DnsClientCache'], 20_000);
    details.steps.push({ name: 'dns-cache', ...dns });
  }

  const ok = details.steps.every((s) => s.ok);
  return {
    ok,
    error: ok ? '' : 'One or more performance actions failed',
    details,
  };
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
  const statusRaw = await gitCmd(wsPath, ['status', '--porcelain', '--untracked-files=normal']);
  info.gitStatus = parseGitStatus(statusRaw, 30).entries;

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

// ── Binary file extensions to skip ──────────────────────────────
const BINARY_EXT = new Set(['.png','.jpg','.jpeg','.gif','.bmp','.ico','.svg','.woff','.woff2','.ttf','.eot','.mp3','.mp4','.wav','.avi','.mov','.zip','.tar','.gz','.rar','.7z','.exe','.dll','.so','.dylib','.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.db','.sqlite','.pyc','.class','.o','.obj','.min.js','.min.css','.map','.lock']);

// ── Extract active filename from VS Code title ─────────────────
function extractActiveFileFromTitle(windowTitle) {
  const parts = windowTitle.split(' - ').map(p => p.trim()).filter(Boolean);
  if (!parts.length) return '';
  const first = parts[0].replace(/^[●◉]\s*/, '').trim();
  if (/\.[a-z0-9]{1,8}$/i.test(first)) return first;
  return '';
}

// ── Read a single file safely ───────────────────────────────────
function readFileSafe(fullPath, maxBytes = 20_000) {
  try {
    const ext = path.extname(fullPath).toLowerCase();
    if (BINARY_EXT.has(ext)) return null;
    const stat = fs.statSync(fullPath);
    if (!stat.isFile() || stat.size > 200_000) return null;
    let text = fs.readFileSync(fullPath, 'utf8');
    if (text.length > maxBytes) text = text.slice(0, maxBytes) + '\n... (truncated)';
    return text;
  } catch { return null; }
}

// ── Read active file + all sibling files in the same directory ──
function readActiveContext(wsPath, activeFileName) {
  const contents = {};
  if (!wsPath || !activeFileName) return contents;

  // Find the active file in the workspace
  let activeRelPath = '';
  const findFile = (dir, depth = 0) => {
    if (depth > 4 || activeRelPath) return;
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      if (activeRelPath) return;
      if (SKIP_DIRS.has(e.name) || e.name.startsWith('.')) continue;
      const full = path.join(dir, e.name);
      if (e.isFile() && e.name === activeFileName) {
        activeRelPath = path.relative(wsPath, full);
        return;
      }
      if (e.isDirectory()) findFile(full, depth + 1);
    }
  };
  findFile(wsPath);

  if (!activeRelPath) return contents;

  // Read the active file (larger limit - this is what they're working on)
  const activeFullPath = path.join(wsPath, activeRelPath);
  const activeContent = readFileSafe(activeFullPath, 30_000);
  if (activeContent) contents[activeRelPath] = activeContent;

  // Read ALL sibling files in the same directory
  const activeDir = path.dirname(activeFullPath);
  let totalSize = activeContent ? activeContent.length : 0;
  const MAX_TOTAL = 120_000;
  try {
    const siblings = fs.readdirSync(activeDir, { withFileTypes: true });
    for (const e of siblings) {
      if (totalSize >= MAX_TOTAL) break;
      if (!e.isFile()) continue;
      const full = path.join(activeDir, e.name);
      const rel = path.relative(wsPath, full);
      if (rel === activeRelPath) continue;
      const text = readFileSafe(full, 15_000);
      if (text) {
        contents[rel] = text;
        totalSize += text.length;
      }
    }
  } catch {}

  return contents;
}

// ── Read key project config files ───────────────────────────────
function readProjectConfigFiles(wsPath) {
  const configs = {};
  const configNames = ['package.json', 'requirements.txt', 'Pipfile', 'pyproject.toml', 'Cargo.toml', 'go.mod', 'composer.json', 'Gemfile', 'README.md', 'readme.md', '.env.example', 'render.yaml', 'Dockerfile', 'docker-compose.yml'];
  for (const name of configNames) {
    const full = path.join(wsPath, name);
    const text = readFileSafe(full, 8_000);
    if (text) configs[name] = text;
  }
  return configs;
}

// ── HTTP helper for GET requests ────────────────────────────────
function httpGet(urlPath) {
  return new Promise((resolve) => {
    const url = new URL(urlPath, SERVER_URL);
    const isHttps = url.protocol === 'https:';
    const mod = isHttps ? https : http;
    const headers = {};
    if (ADMIN_TOKEN) headers['Authorization'] = `Bearer ${ADMIN_TOKEN}`;
    const req = mod.get({ hostname: url.hostname, port: url.port || (isHttps ? 443 : 80), path: `${url.pathname}${url.search}`, headers, timeout: 5000 }, (res) => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => { try { resolve(JSON.parse(buf)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
  });
}

// ── Fulfil file-read requests from Marcus (exploring deeper) ────
function openVsCode(projectPath) {
  return new Promise((resolve) => {
    const target = String(projectPath || '').trim();
    if (!target) return resolve({ ok: false, error: 'Path required' });

    let stat = null;
    try {
      stat = fs.statSync(target);
    } catch {
      return resolve({ ok: false, error: 'Path does not exist' });
    }
    if (!stat.isDirectory()) return resolve({ ok: false, error: 'Path is not a folder' });

    const tryLaunch = (cmds) => {
      const cmd = cmds.shift();
      if (!cmd) return resolve({ ok: false, error: 'VS Code command not found. Install the code command or add it to PATH.' });
      try {
        execFile(cmd, [target], { windowsHide: false, timeout: 5000 }, (err) => {
          if (!err) return resolve({ ok: true });
          tryLaunch(cmds);
        });
      } catch {
        tryLaunch(cmds);
      }
    };

    tryLaunch([
      path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Microsoft VS Code', 'Code.exe'),
      path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Microsoft VS Code', 'bin', 'code.cmd'),
      path.join(process.env.ProgramFiles || '', 'Microsoft VS Code', 'Code.exe'),
      'code',
      'code.cmd',
    ].filter(Boolean));
  });
}

const activeLocalCodexJobs = new Map();

function chromeExecutable() {
  const candidates = [
    path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env['ProgramFiles(x86)'] || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env.ProgramFiles || '', 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
  ];
  return candidates.find((candidate) => candidate && fs.existsSync(candidate)) || '';
}

function validateNewWorkspacePath(projectPath) {
  const target = path.resolve(String(projectPath || '').trim());
  if (!path.isAbsolute(target) || !path.basename(target)) return { ok: false, error: 'A valid absolute project path is required' };
  const roots = allowedWorkspaceRoots();
  if (!roots.length) return { ok: false, error: 'No MARCUS_ALLOWED_WORKSPACE_ROOTS are configured' };
  const root = roots.find((candidate) => pathWithin(candidate, target) && comparablePath(candidate) !== comparablePath(target));
  if (!root) return { ok: false, error: 'The new project path is outside the configured workspace roots' };
  const relative = path.relative(root, target);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return { ok: false, error: 'Invalid new project path' };
  return { ok: true, path: target, root };
}

async function createProjectWorkspace(payload) {
  const valid = validateNewWorkspacePath(payload?.path);
  if (!valid.ok) return valid;
  const destination = valid.path;
  const binding = {
    operationId: String(payload?.operationId || '').trim(),
    projectRegistryId: String(payload?.projectRegistryId || '').trim(),
  };
  const markerPath = path.join(destination, '.git', 'marcus-project.json');
  try {
    if (fs.existsSync(destination)) {
      const entries = fs.readdirSync(destination);
      if (entries.length) {
        let marker = null;
        try { marker = JSON.parse(fs.readFileSync(markerPath, 'utf8')); } catch {}
        const sameOperation = marker
          && binding.operationId
          && binding.projectRegistryId
          && String(marker.operationId || '') === binding.operationId
          && String(marker.projectRegistryId || '') === binding.projectRegistryId;
        if (!sameOperation) {
          return { ok: false, error: 'The destination already exists and is not bound to this exact Marcus project operation' };
        }
      }
    } else {
      fs.mkdirSync(destination, { recursive: true });
    }
  } catch (error) {
    return { ok: false, error: `Project folder could not be created: ${String(error?.message || error)}` };
  }
  let gitResult = { ok: true, skipped: true };
  if (payload?.initializeGit !== false && !fs.existsSync(path.join(destination, '.git'))) {
    gitResult = await runLocalCommand(destination, 'git', ['init', '-b', 'main'], 30_000);
    if (!gitResult.ok) return { ok: false, error: 'git init failed', details: { destination, gitResult } };
  }
  if (!fs.existsSync(path.join(destination, '.git'))) {
    return { ok: false, error: 'The project workspace is not a Git repository' };
  }
  const marcusNotePath = path.join(destination, 'marcus.txt');
  if (!fs.existsSync(marcusNotePath)) {
    const projectName = String(payload?.projectName || path.basename(destination)).trim().slice(0, 300);
    const date = new Date().toISOString().slice(0, 10);
    const note = [
      'MARCUS PROJECT NOTE',
      '',
      `Project: ${projectName}`,
      `Workspace: ${destination}`,
      'Status: active',
      `Last reviewed: ${date}`,
      '',
      'Purpose:',
      'MARCUS created this workspace for a new project. Replace this sentence with the durable project objective as the implementation becomes clear.',
      '',
      'Standing instruction:',
      'Read this repository and this note before reporting. Append concise dated context after meaningful work. Keep credentials and raw secrets out.',
      '',
      'Append Log:',
      '',
      date,
      '- MARCUS created the project workspace, initialized project memory, and bound it to the durable project operation.',
      '',
    ].join('\n');
    fs.writeFileSync(marcusNotePath, note, { encoding: 'utf8', flag: 'wx' });
  }
  fs.writeFileSync(markerPath, `${JSON.stringify({ ...binding, createdAt: new Date().toISOString() }, null, 2)}\n`, 'utf8');
  const openResult = payload?.openInVsCode === false ? { ok: true, skipped: true } : await openVsCode(destination);
  let canonicalPath = destination;
  try { canonicalPath = fs.realpathSync.native(destination); } catch {}
  return {
    ok: openResult.ok,
    error: openResult.ok ? '' : openResult.error,
    details: {
      projectName: String(payload?.projectName || path.basename(destination)).slice(0, 300),
      registeredPath: String(payload?.path || destination),
      canonicalPath,
      destination,
      marcusNotePath,
      gitResult,
      openResult,
    },
  };
}

async function connectGithubRepository(payload) {
  const valid = validateWorkspaceFolder(payload?.path, { ...payload, requireProjectBinding: true });
  if (!valid.ok) return valid;
  const repoUrl = String(payload?.repoUrl || '').trim();
  if (!/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+(?:\.git)?$/i.test(repoUrl)) {
    return { ok: false, error: 'A valid GitHub repository URL is required' };
  }
  const current = await gitCmd(valid.path, ['remote', 'get-url', 'origin']);
  const args = current ? ['remote', 'set-url', 'origin', repoUrl] : ['remote', 'add', 'origin', repoUrl];
  const result = await runLocalCommand(valid.path, 'git', args, 30_000);
  return { ok: result.ok, error: result.ok ? '' : 'Could not configure the GitHub origin', details: { path: valid.path, repoUrl, previousOrigin: current, result } };
}

function cloudflareDeploymentUrl(output) {
  const text = String(output || '');
  const matches = text.match(/https:\/\/[A-Za-z0-9.-]+(?:\.workers\.dev|\.pages\.dev)(?:\/[^\s]*)?/gi) || [];
  return matches[matches.length - 1] || '';
}

async function deployCloudflareProject(payload) {
  const valid = validateWorkspaceFolder(payload?.path, { ...payload, requireProjectBinding: true });
  if (!valid.ok) return valid;
  const configNames = ['wrangler.jsonc', 'wrangler.json', 'wrangler.toml'];
  if (!configNames.some((name) => fs.existsSync(path.join(valid.path, name)))) {
    return { ok: false, error: 'The project has no Wrangler configuration. Codex must prepare a Cloudflare Worker or Pages project first.' };
  }
  const invocation = localPackageBinInvocation(valid.path, 'wrangler', 'wrangler', ['deploy']);
  if (!invocation.ok) return { ok: false, error: invocation.error, details: { path: valid.path } };
  const result = await runLocalCommand(valid.path, invocation.command, invocation.args, 240_000);
  const deploymentUrl = cloudflareDeploymentUrl(`${result.stdout}\n${result.stderr}`);
  return {
    ok: result.ok && Boolean(deploymentUrl),
    error: result.ok ? (deploymentUrl ? '' : 'Wrangler completed without reporting a live URL') : 'Cloudflare deployment failed',
    details: { path: valid.path, deploymentUrl, result },
  };
}

function openCodexMonitor(url) {
  const target = String(url || '').trim();
  if (!/^https?:\/\//i.test(target)) return { ok: false, error: 'A valid Codex monitor URL is required' };
  const executable = chromeExecutable();
  if (!executable) return { ok: false, error: 'Chrome or Edge was not found' };
  try {
    const args = CODEX_MONITOR_MODE === 'kiosk'
      ? ['--new-window', '--kiosk', target]
      : ['--new-window', `--app=${target}`];
    const child = spawn(executable, args, { detached: true, stdio: 'ignore', windowsHide: false });
    child.unref();
    return { ok: true, mode: CODEX_MONITOR_MODE === 'kiosk' ? 'kiosk' : 'app' };
  } catch (error) {
    return { ok: false, error: String(error?.message || error) };
  }
}

function codexEventDetails(event, state) {
  if (!event || typeof event !== 'object') return;
  const type = String(event.type || '').trim();
  if (type === 'thread.started') state.threadId = String(event.thread_id || event.threadId || '').trim();
  if (type === 'item.completed' && event.item?.type === 'agent_message') {
    state.finalOutput = String(event.item?.text || event.item?.content || '').trim().slice(0, 40_000);
  }
  if (type === 'error') state.error = String(event.message || event.error?.message || event.error || 'Codex reported an error').slice(0, 8_000);
}

function codexExecutable() {
  const configured = String(process.env.CODEX_CLI_PATH || '').trim();
  if (configured && fs.existsSync(configured)) return configured;
  const candidates = [
    path.join(process.env.APPDATA || '', 'npm', 'codex.cmd'),
    path.join(process.env.APPDATA || '', 'npm', 'codex.exe'),
  ];
  for (const extensionRoot of [
    path.join(os.homedir(), '.vscode', 'extensions'),
    path.join(os.homedir(), '.vscode-insiders', 'extensions'),
  ]) {
    try {
      const extensions = fs.readdirSync(extensionRoot, { withFileTypes: true })
        .filter((entry) => entry.isDirectory() && /^openai\.chatgpt-/i.test(entry.name))
        .map((entry) => entry.name)
        .sort((left, right) => right.localeCompare(left, undefined, { numeric: true }));
      for (const extension of extensions) {
        candidates.push(path.join(extensionRoot, extension, 'bin', 'windows-x86_64', 'codex.exe'));
      }
    } catch {}
  }
  return candidates.find((candidate) => candidate && fs.existsSync(candidate)) || 'codex';
}

async function collectLocalCodexGitEvidence(cwd) {
  const [statusRaw, diffStat, branch] = await Promise.all([
    gitCmd(cwd, ['status', '--porcelain', '--untracked-files=normal']),
    gitCmd(cwd, ['diff', '--stat', 'HEAD']),
    gitCmd(cwd, ['rev-parse', '--abbrev-ref', 'HEAD']),
  ]);
  const parsed = parseGitStatus(statusRaw, 300);
  const changedFiles = parsed.entries.map((entry) => `${entry.status} ${entry.file}`);
  return {
    branch,
    changedFiles,
    diffSummary: String(diffStat || changedFiles.join('\n')).slice(0, 40_000),
  };
}

function startCodexProcess(payload, { resume = false } = {}) {
  const jobId = String(payload?.jobId || '').trim();
  const cwd = String(payload?.path || '').trim();
  const prompt = String(resume ? payload?.message : payload?.prompt || '').trim();
  if (!jobId || !cwd || !prompt) return { ok: false, error: 'Codex job id, workspace, and prompt are required' };
  if (activeLocalCodexJobs.has(jobId)) return { ok: true, details: { jobId, alreadyRunning: true } };

  const state = {
    jobId,
    threadId: String(payload?.threadId || '').trim(),
    finalOutput: '',
    error: '',
    events: [],
    flushTimer: null,
  };
  const flush = async (status = 'running', extra = {}) => {
    if (state.flushTimer) clearTimeout(state.flushTimer);
    state.flushTimer = null;
    const events = state.events.splice(0, 100);
    await relay({
      jobId,
      desktopAgentId: DESKTOP_AGENT_ID,
      status,
      threadId: state.threadId,
      finalOutput: state.finalOutput,
      error: state.error,
      events,
      ...extra,
    }, '/api/desktop-context/codex-updates');
    if (state.events.length) state.flushTimer = setTimeout(() => { void flush(status); }, 250);
  };
  const queueEvent = (event) => {
    codexEventDetails(event, state);
    state.events.push(event);
    if (state.events.length >= 20) void flush('running');
    else if (!state.flushTimer) state.flushTimer = setTimeout(() => { void flush('running'); }, 350);
  };

  const args = resume
    ? ['exec', '--json', '--sandbox', 'workspace-write', '--cd', cwd, 'resume', state.threadId, '-']
    : ['exec', '--json', '--sandbox', 'workspace-write', '--cd', cwd, '--skip-git-repo-check', '-'];
  let child;
  try {
    child = spawn(codexExecutable(), args, { cwd, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
  } catch (error) {
    return { ok: false, error: `Codex could not start: ${String(error?.message || error)}` };
  }
  activeLocalCodexJobs.set(jobId, { child, state, cwd });
  const stdout = readline.createInterface({ input: child.stdout });
  stdout.on('line', (line) => {
    const text = String(line || '').trim();
    if (!text) return;
    try { queueEvent(JSON.parse(text)); }
    catch { queueEvent({ type: 'codex.output', text: text.slice(0, 8_000) }); }
  });
  child.stderr.on('data', (chunk) => {
    const text = String(chunk || '').trim();
    if (text) {
      state.error = `${state.error}\n${text}`.trim().slice(-8_000);
      queueEvent({ type: 'codex.stderr', text: text.slice(0, 8_000) });
    }
  });
  child.on('error', (error) => {
    state.error = String(error?.message || error).slice(0, 8_000);
  });
  child.on('close', async (code, signal) => {
    activeLocalCodexJobs.delete(jobId);
    stdout.close();
    const evidence = await collectLocalCodexGitEvidence(cwd);
    const status = code === 0 ? 'completed' : signal ? 'cancelled' : 'failed';
    if (code !== 0 && !state.error) state.error = `Codex exited with code ${code}.`;
    queueEvent({ type: 'desktop_codex.completed', status, code, signal: signal || '', evidence });
    await flush(status, { ...evidence, completedAt: new Date().toISOString() });
  });
  child.stdin.end(prompt);
  queueEvent({ type: 'desktop_codex.started', jobId, workspacePath: cwd, resumed: resume });
  void flush('running');
  return { ok: true, details: { jobId, pid: child.pid, workspacePath: cwd, resumed: resume } };
}

function startLocalCodexJob(payload) {
  const monitor = openCodexMonitor(payload?.monitorUrl);
  const result = startCodexProcess(payload);
  return { ...result, details: { ...(result.details || {}), monitor } };
}

function followupLocalCodexJob(payload) {
  if (!String(payload?.threadId || '').trim()) return { ok: false, error: 'The Codex thread id is not available for follow-up' };
  const monitor = openCodexMonitor(payload?.monitorUrl);
  const result = startCodexProcess(payload, { resume: true });
  return { ...result, details: { ...(result.details || {}), monitor } };
}

function cancelLocalCodexJob(payload) {
  const jobId = String(payload?.jobId || '').trim();
  const active = activeLocalCodexJobs.get(jobId);
  if (!active) return { ok: true, details: { jobId, alreadyStopped: true } };
  try {
    active.child.kill();
    return { ok: true, details: { jobId, signalSent: true } };
  } catch (error) {
    return { ok: false, error: String(error?.message || error) };
  }
}

let desktopActionCheckInFlight = false;

async function checkDesktopActions() {
  if (desktopActionCheckInFlight) return;
  desktopActionCheckInFlight = true;
  try {
    const result = await httpGet(`/api/desktop-context/actions?agentId=${encodeURIComponent(DESKTOP_AGENT_ID)}`);
    const actions = Array.isArray(result?.actions) ? result.actions : [];
    if (!actions.length) return;

    const responses = [];
    for (const action of actions) {
      const id = String(action?.id || '').trim();
      const type = String(action?.type || '').trim();
      if (!id || !type) continue;

      let outcome = { ok: false, error: `Unsupported desktop action: ${type}` };
      const pathAction = [
        'open-vscode', 'prepare-publish', 'publish-project-changes', 'run-project-script',
        'validate-workspace', 'start-local-codex-job', 'followup-local-codex-job',
        'connect-github-repository', 'deploy-cloudflare-project',
      ].includes(type);
      if (pathAction) {
        const operationBound = String(action?.requestedBy || '').startsWith('operation:');
        const valid = validateWorkspaceFolder(action?.payload?.path, { ...action?.payload, requireProjectBinding: operationBound || type === 'validate-workspace' });
        if (!valid.ok) {
          outcome = valid;
          if (type === 'validate-workspace') outcome.details = { registeredPath: String(action?.payload?.registeredPath || ''), canonicalPath: '' };
          responses.push({
            id, type, jobId: String(action?.payload?.jobId || ''), businessKey: String(action?.payload?.businessKey || ''), operationId: String(action?.payload?.operationId || ''),
            stepId: String(action?.payload?.stepId || ''), projectRegistryId: String(action?.payload?.projectRegistryId || ''),
            desktopAgentId: DESKTOP_AGENT_ID, idempotencyKey: String(action?.payload?.idempotencyKey || ''),
            attemptNumber: Number(action?.payload?.attemptNumber ?? 0), ...outcome,
          });
          continue;
        }
        action.payload.path = valid.path;
      }
      if (type === 'open-vscode') {
        outcome = await openVsCode(action?.payload?.path);
      } else if (type === 'prepare-publish') {
        outcome = await preparePublish(action?.payload?.path);
      } else if (type === 'publish-project-changes') {
        outcome = await publishProjectChanges(action?.payload || {});
      } else if (type === 'run-project-script') {
        outcome = await runProjectScript(action?.payload || {});
      } else if (type === 'validate-workspace') {
        outcome = {
          ok: true,
          details: {
            challengeId: String(action?.payload?.challengeId || id),
            canonicalPath: action.payload.path,
            registeredPath: String(action?.payload?.registeredPath || ''),
            businessKey: String(action?.payload?.businessKey || ''),
            projectRegistryId: String(action?.payload?.projectRegistryId || ''),
            desktopAgentId: DESKTOP_AGENT_ID,
          },
        };
      } else if (type === 'clone-github-project') {
        outcome = await cloneGithubProject(action?.payload || {});
      } else if (type === 'set-performance-profile') {
        outcome = await setPerformanceProfile(action?.payload || {});
      } else if (type === 'start-local-codex-job') {
        outcome = startLocalCodexJob(action?.payload || {});
      } else if (type === 'followup-local-codex-job') {
        outcome = followupLocalCodexJob(action?.payload || {});
      } else if (type === 'cancel-local-codex-job') {
        outcome = cancelLocalCodexJob(action?.payload || {});
      } else if (type === 'create-project-workspace') {
        outcome = await createProjectWorkspace(action?.payload || {});
      } else if (type === 'connect-github-repository') {
        outcome = await connectGithubRepository(action?.payload || {});
      } else if (type === 'deploy-cloudflare-project') {
        outcome = await deployCloudflareProject(action?.payload || {});
      } else if (type === 'configure-pc-access') {
        outcome = validateBoundPcAccessAction(action)
          ? configurePcAccess(action?.payload || {})
          : { ok: false, error: 'PC access changes require an exact durable operation binding for this desktop agent.' };
      } else if (type === 'verify-pc-access') {
        outcome = validateBoundPcAccessAction(action)
          ? verifyPcAccess(action?.payload || {})
          : { ok: false, error: 'PC access verification requires an exact durable operation binding for this desktop agent.' };
      } else if (type === 'pc-inventory') {
        outcome = getPcInventory(PC_ACCESS_POLICY);
      } else if (type === 'pc-search-files') {
        outcome = searchPcFiles(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-list-directory') {
        outcome = listPcDirectory(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-read-text-file') {
        outcome = readPcTextFile(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-list-applications') {
        outcome = listInstalledApplications(action?.payload || {});
      } else if (type === 'pc-open-item') {
        outcome = openPcItem(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-launch-application') {
        outcome = launchInstalledApplication(action?.payload || {});
      } else if (type === 'pc-write-text-file') {
        outcome = writePcTextFile(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-create-directory') {
        outcome = createPcDirectory(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-move-item') {
        outcome = movePcItem(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-delete-item') {
        outcome = deletePcItem(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'pc-run-powershell') {
        outcome = await runPcPowerShell(action?.payload || {}, PC_ACCESS_POLICY);
      } else if (type === 'marcus-browser-open' || type === 'marcus-browser-command') {
        outcome = await marcusBrowser.command(action?.payload || {});
      }
      if (type.startsWith('pc-')) outcome = toDesktopActionOutcome(outcome);

      responses.push({
        id, type, jobId: String(action?.payload?.jobId || ''), businessKey: String(action?.payload?.businessKey || ''), operationId: String(action?.payload?.operationId || ''),
        stepId: String(action?.payload?.stepId || ''), projectRegistryId: String(action?.payload?.projectRegistryId || ''),
        desktopAgentId: DESKTOP_AGENT_ID, idempotencyKey: String(action?.payload?.idempotencyKey || ''),
        attemptNumber: Number(action?.payload?.attemptNumber ?? 0), ...outcome,
      });
      const ts = new Date().toLocaleTimeString();
      console.log(`[${ts}] Desktop action ${type}: ${outcome.ok ? 'ok' : outcome.error}`);
    }

    if (responses.length) {
      await relay({ agentId: DESKTOP_AGENT_ID, results: responses }, '/api/desktop-context/action-results');
    }
  } catch {
  } finally {
    desktopActionCheckInFlight = false;
  }
}

async function checkFileRequests(wsPath) {
  if (!wsPath) return;
  try {
    const result = await httpGet('/api/desktop-context/file-requests');
    if (!result?.requests?.length) return;

    const responses = {};
    for (const r of result.requests) {
      const reqPath = String(r.path || '').trim();
      if (!reqPath) continue;
      // Safety: resolve both paths through the filesystem so traversal and
      // symlink targets cannot escape into a sibling path with the same prefix.
      let resolved = '';
      let workspaceRoot = '';
      try {
        workspaceRoot = fs.realpathSync(wsPath);
        resolved = fs.realpathSync(path.resolve(wsPath, reqPath));
      } catch {
        continue;
      }
      const relative = path.relative(workspaceRoot, resolved);
      if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) continue;
      const fullPath = resolved;

      try {
        const stat = fs.statSync(fullPath);
        if (stat.isFile()) {
          const text = readFileSafe(fullPath, 25_000);
          if (text) responses[reqPath] = text;
        } else if (stat.isDirectory()) {
          const entries = fs.readdirSync(fullPath, { withFileTypes: true });
          const listing = [];
          let dirTotal = 0;
          for (const e of entries) {
            if (SKIP_DIRS.has(e.name) || e.name.startsWith('.')) continue;
            if (e.isDirectory()) {
              listing.push(e.name + '/');
            } else if (e.isFile()) {
              const childFull = path.join(fullPath, e.name);
              const childRel = path.relative(wsPath, childFull);
              const text = readFileSafe(childFull, 15_000);
              if (text && dirTotal + text.length < 100_000) {
                responses[childRel] = text;
                dirTotal += text.length;
              }
              listing.push(e.name);
            }
          }
          responses[reqPath + '/__listing__'] = listing.join('\n');
        }
      } catch {}
    }

    if (Object.keys(responses).length) {
      const ts = new Date().toLocaleTimeString();
      console.log(`[${ts}] Fulfilled ${Object.keys(responses).length} file request(s) from Marcus`);
      await relay({ fileResponses: responses }, '/api/desktop-context/file-responses');
    }
  } catch {}
}

// ── Get unified git diff of uncommitted work ────────────────────
async function getGitDiff(wsPath) {
  const diff = await gitCmd(wsPath, ['diff', 'HEAD']);
  if (!diff) return '';
  return diff.length > 25_000 ? diff.slice(0, 25_000) + '\n... (diff truncated)' : diff;
}

// ── Capture desktop context via PowerShell ──────────────────────
function captureDesktop() {
  return new Promise((resolve) => {
    try {
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
    } catch {
      resolve(null);
    }
  });
}

async function scanCodexWorkspaceSummary(session) {
  const wsPath = String(session?.workspacePath || '').trim();
  if (!wsPath) return session;
  const [gitBranch, gitRemote, statusRaw, recentCommitsRaw] = await Promise.all([
    gitCmd(wsPath, ['rev-parse', '--abbrev-ref', 'HEAD']),
    gitCmd(wsPath, ['remote', 'get-url', 'origin']),
    gitCmd(wsPath, ['status', '--porcelain', '--untracked-files=normal']),
    gitCmd(wsPath, ['log', '--oneline', '-3', '--no-decorate']),
  ]);
  const parsedStatus = parseGitStatus(statusRaw, 30);
  return {
    ...session,
    gitBranch,
    gitRemote,
    gitStatusCount: parsedStatus.count,
    gitStatus: parsedStatus.entries,
    gitRecentCommits: recentCommitsRaw ? recentCommitsRaw.split('\n').map((line) => line.trim()).filter(Boolean) : [],
    latestUserRequest: typeof session.latestUserRequest === 'string' ? session.latestUserRequest.slice(0, 800) : '',
    latestUserRequestAt: typeof session.latestUserRequestAt === 'string' ? session.latestUserRequestAt.slice(0, 40) : '',
  };
}

// ── Send data to the server ─────────────────────────────────────
function relay(data, customPath) {
  return new Promise((resolve) => {
    const body = JSON.stringify(data);
    const url = new URL(customPath || RELAY_PATH, SERVER_URL);
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

// ── System health monitoring ────────────────────────────────────
const HEALTH_SCRIPT_PATH = path.join(SCRIPT_DIR, 'system-health.ps1');
const HEALTH_PS_SCRIPT = `
$ErrorActionPreference = 'SilentlyContinue'
$out = @{}

# CPU usage (sampled over ~1s)
try {
  $cpu = (Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average
  $out.cpuPercent = [math]::Round($cpu, 1)
} catch { $out.cpuPercent = -1 }

# Memory
try {
  $os = Get-CimInstance Win32_OperatingSystem
  $totalGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
  $freeGB = [math]::Round($os.FreePhysicalMemory / 1MB, 1)
  $usedGB = [math]::Round($totalGB - $freeGB, 1)
  $out.memoryTotalGB = $totalGB
  $out.memoryUsedGB = $usedGB
  $out.memoryPercent = [math]::Round(($usedGB / $totalGB) * 100, 1)
} catch { $out.memoryPercent = -1 }

# Disk usage (all fixed drives)
try {
  $disks = Get-CimInstance Win32_LogicalDisk -Filter "DriveType=3" | ForEach-Object {
    @{
      drive = $_.DeviceID
      totalGB = [math]::Round($_.Size / 1GB, 1)
      freeGB = [math]::Round($_.FreeSpace / 1GB, 1)
      usedPercent = [math]::Round((($_.Size - $_.FreeSpace) / $_.Size) * 100, 1)
    }
  }
  $out.disks = @($disks)
} catch { $out.disks = @() }

# Windows Defender status
try {
  $def = Get-MpComputerStatus
  $out.defender = @{
    enabled = [bool]$def.AntivirusEnabled
    realTimeProtection = [bool]$def.RealTimeProtectionEnabled
    defsUpToDate = [bool]$def.AntivirusSignatureLastUpdated -and ((Get-Date) - $def.AntivirusSignatureLastUpdated).TotalDays -lt 3
    lastScan = if ($def.FullScanEndTime) { $def.FullScanEndTime.ToString('o') } else { '' }
    quickScanAge = if ($def.QuickScanEndTime) { [math]::Round(((Get-Date) - $def.QuickScanEndTime).TotalHours, 1) } else { -1 }
  }
} catch { $out.defender = @{ enabled = $false; error = 'unavailable' } }

# Recent Defender threat detections (last 7 days)
try {
  $threats = Get-MpThreatDetection | Where-Object { $_.InitialDetectionTime -gt (Get-Date).AddDays(-7) } | Select-Object -First 10 | ForEach-Object {
    @{
      threat = (Get-MpThreat -ThreatID $_.ThreatID).ThreatName
      time = $_.InitialDetectionTime.ToString('o')
      action = $_.ThreatStatusID
    }
  }
  $out.recentThreats = @($threats)
} catch { $out.recentThreats = @() }

# Failed login attempts (last 2 hours, Event ID 4625)
try {
  $fails = Get-WinEvent -FilterHashtable @{LogName='Security'; Id=4625; StartTime=(Get-Date).AddHours(-2)} -MaxEvents 20 | ForEach-Object {
    $xml = [xml]$_.ToXml()
    $ip = ($xml.Event.EventData.Data | Where-Object { $_.Name -eq 'IpAddress' }).'#text'
    $user = ($xml.Event.EventData.Data | Where-Object { $_.Name -eq 'TargetUserName' }).'#text'
    @{ time = $_.TimeCreated.ToString('o'); user = $user; sourceIp = $ip }
  }
  $out.failedLogins = @($fails)
} catch { $out.failedLogins = @() }

# Firewall status
try {
  $fw = Get-NetFirewallProfile | ForEach-Object { @{ profile = $_.Name; enabled = [bool]$_.Enabled } }
  $out.firewall = @($fw)
} catch { $out.firewall = @() }

# Top processes by CPU (top 5, excluding idle/system)
try {
  $topCpu = Get-Process | Where-Object { $_.ProcessName -notin 'Idle','System','_Total' } | Sort-Object CPU -Descending | Select-Object -First 5 | ForEach-Object {
    @{ name = $_.ProcessName; cpu = [math]::Round($_.CPU, 1); memMB = [math]::Round($_.WorkingSet64 / 1MB, 0) }
  }
  $out.topProcesses = @($topCpu)
} catch { $out.topProcesses = @() }

# Top processes by memory (top 5)
try {
  $topMem = Get-Process | Where-Object { $_.ProcessName -notin 'Idle','System','_Total' } | Sort-Object WorkingSet64 -Descending | Select-Object -First 5 | ForEach-Object {
    @{ name = $_.ProcessName; memMB = [math]::Round($_.WorkingSet64 / 1MB, 0) }
  }
  $out.topMemProcesses = @($topMem)
} catch { $out.topMemProcesses = @() }

# Unusual listening ports (exclude common ones)
try {
  $common = @(80,443,3000,3030,5000,5173,8080,8443,135,139,445,5040,5357,7680,1900)
  $listeners = Get-NetTCPConnection -State Listen | Where-Object { $_.LocalPort -notin $common -and $_.LocalAddress -ne '::1' -and $_.LocalAddress -ne '127.0.0.1' } | Select-Object -First 15 | ForEach-Object {
    $proc = try { (Get-Process -Id $_.OwningProcess).ProcessName } catch { 'unknown' }
    @{ port = $_.LocalPort; process = $proc; address = $_.LocalAddress }
  }
  $out.unusualListeners = @($listeners)
} catch { $out.unusualListeners = @() }

# System uptime
try {
  $boot = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
  $out.uptimeHours = [math]::Round(((Get-Date) - $boot).TotalHours, 1)
} catch { $out.uptimeHours = -1 }

$out | ConvertTo-Json -Depth 4 -Compress
`.trim();

try { fs.writeFileSync(HEALTH_SCRIPT_PATH, HEALTH_PS_SCRIPT, 'utf8'); } catch {}

let cachedSystemHealth = null;
let lastSystemHealthAt = 0;

function captureSystemHealth() {
  return new Promise((resolve) => {
    try {
      execFile(
        'powershell.exe',
        ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', HEALTH_SCRIPT_PATH],
        { windowsHide: true, timeout: 15_000 },
        (err, stdout) => {
          if (err) return resolve(null);
          try {
            const data = JSON.parse(stdout.trim());
            data.collectedAt = new Date().toISOString();
            resolve(data);
          } catch {
            resolve(null);
          }
        }
      );
    } catch {
      resolve(null);
    }
  });
}

// ── Main loop ───────────────────────────────────────────────────
let consecutive = 0;
let lastTitle = '';

async function tick() {
  await checkDesktopActions();

  const capturedDesktop = await captureDesktop();
  if (!capturedDesktop) {
    if (++consecutive >= 3) {
      process.stdout.write('  [!] Desktop capture failing - is this Windows?\r');
    }
  } else {
    consecutive = 0;
  }
  const ctx = capturedDesktop || { windowTitle: '', processName: '', idleSeconds: 0 };

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

    // Deep context: active file + sibling dir + project configs + git diff
    if (workspace && workspace.workspacePath) {
      const now2 = Date.now();
      if (wsPath === lastWorkspacePath && cachedFileContents && (now2 - lastFileContentsAt) < WORKSPACE_SCAN_INTERVAL_MS) {
        workspace = { ...workspace, fileContents: cachedFileContents, gitDiff: cachedGitDiff };
      } else {
        const activeFile = extractActiveFileFromTitle(ctx.windowTitle);
        const activeCtx = readActiveContext(wsPath, activeFile);
        const configCtx = readProjectConfigFiles(wsPath);
        const fc = { ...configCtx, ...activeCtx };
        const gd = await getGitDiff(wsPath);
        cachedFileContents = fc;
        cachedGitDiff = gd;
        lastFileContentsAt = now2;
        workspace = { ...workspace, fileContents: fc, gitDiff: gd, activeFile: activeFile || '' };
        const ts2 = new Date().toLocaleTimeString();
        console.log(`[${ts2}] Context: ${Object.keys(fc).length} file(s)${activeFile ? ' (active: ' + activeFile + ')' : ''} + diff (${gd.length} chars)`);
      }

      // Check for file exploration requests from Marcus
      await checkFileRequests(wsPath);
    }
  } else {
    // Not in an editor - clear workspace cache
    if (lastWorkspacePath) {
      lastWorkspacePath = '';
      cachedWorkspaceInfo = null;
    }
  }

  // Build relay payload
  const codexNow = Date.now();
  if (!cachedCodexWorkspaces.length || (codexNow - lastCodexWorkspaceScanAt) > CODEX_WORKSPACE_SCAN_INTERVAL_MS) {
    const sessions = discoverRecentCodexWorkspaces({ maxResults: 12 });
    cachedCodexWorkspaces = await Promise.all(sessions.map((session) => scanCodexWorkspaceSummary(session)));
    lastCodexWorkspaceScanAt = codexNow;
  }
  const payload = {
    agentId: DESKTOP_AGENT_ID,
    desktopAuthorization: {
      scope: FULL_PC_ACCESS ? 'full_pc' : 'workspace_roots',
      broadWorkspaceRootsAllowed: ALLOW_BROAD_WORKSPACE_ROOTS,
      allowedRoots: ALLOWED_WORKSPACE_ROOT_VALUES,
      newProjectRoot: NEW_PROJECT_ROOT,
      fullPcAccess: FULL_PC_ACCESS,
      pcAccessRoots: PC_ACCESS_POLICY.roots,
      capabilities: PC_ACCESS_POLICY.capabilities,
    },
    windowTitle: ctx.windowTitle,
    processName: ctx.processName,
    idleSeconds: ctx.idleSeconds,
    codexWorkspaces: cachedCodexWorkspaces,
  };
  if (workspace) {
    payload.workspace = workspace;
  }

  // System health (collected on slower interval)
  const now3 = Date.now();
  if (!cachedSystemHealth || (now3 - lastSystemHealthAt) > SYSTEM_HEALTH_INTERVAL_MS) {
    const health = await captureSystemHealth();
    if (health) {
      cachedSystemHealth = health;
      lastSystemHealthAt = now3;
      const ts3 = new Date().toLocaleTimeString();
      const alerts = [];
      if (health.cpuPercent > 90) alerts.push(`CPU ${health.cpuPercent}%`);
      if (health.memoryPercent > 90) alerts.push(`RAM ${health.memoryPercent}%`);
      if (health.recentThreats?.length) alerts.push(`${health.recentThreats.length} threat(s)`);
      if (health.failedLogins?.length) alerts.push(`${health.failedLogins.length} failed login(s)`);
      if (alerts.length) console.log(`[${ts3}] HEALTH ALERT: ${alerts.join(', ')}`);
    }
  }
  if (cachedSystemHealth) {
    payload.systemHealth = cachedSystemHealth;
  }

  const result = await relay(payload);
  if (result.status === 401) {
    console.error('[!] 401 Unauthorized - check your ADMIN_TOKEN');
  } else if (result.status && result.status !== 200) {
    console.error(`[!] Server returned ${result.status}: ${result.body.slice(0, 120)}`);
  }
}

let browserRelayInFlight = false;

async function relayMarcusBrowser() {
  if (browserRelayInFlight) return;
  browserRelayInFlight = true;
  try {
    const browser = await marcusBrowser.capture();
    await relay({
      agentId: DESKTOP_AGENT_ID,
      ...browser,
      observedAt: new Date().toISOString(),
    }, '/api/marcus/browser/relay');
  } catch {
    // The regular desktop relay stays online when Chrome is closed or restarting.
  } finally {
    browserRelayInFlight = false;
  }
}

console.log('');
console.log('  M.A.R.C.U.S. Desktop Agent');
console.log(`  Server: ${SERVER_URL}`);
console.log(`  Auth:   ${ADMIN_TOKEN ? 'Bearer token set' : 'no token (local mode)'}`);
console.log(`  Poll:   every ${POLL_MS / 1000}s`);
console.log(`  Chrome: dedicated MARCUS profile bridge on 127.0.0.1:${marcusBrowser.debugPort}`);
console.log(`  PC:     ${FULL_PC_ACCESS ? `full access (${PC_ACCESS_POLICY.roots.join(', ')})` : 'workspace roots only'}`);
console.log('  Press Ctrl+C to stop.');
console.log('');

async function runLoop() {
  try {
    await tick();
  } catch (error) {
    console.error(`[!] Relay tick failed: ${String(error?.message || error).slice(0, 200)}`);
  } finally {
    setTimeout(runLoop, POLL_MS);
  }
}

runLoop();

async function runBrowserActionLoop() {
  try { await checkDesktopActions(); } finally { setTimeout(runBrowserActionLoop, BROWSER_ACTION_POLL_MS); }
}

async function runBrowserRelayLoop() {
  try { await relayMarcusBrowser(); } finally { setTimeout(runBrowserRelayLoop, BROWSER_FRAME_INTERVAL_MS); }
}

runBrowserActionLoop();
runBrowserRelayLoop();
