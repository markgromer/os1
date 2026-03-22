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
const SYSTEM_HEALTH_INTERVAL_MS = 60_000; // system health check every 60s
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
    const req = mod.get({ hostname: url.hostname, port: url.port || (isHttps ? 443 : 80), path: url.pathname, headers, timeout: 5000 }, (res) => {
      let buf = '';
      res.on('data', d => buf += d);
      res.on('end', () => { try { resolve(JSON.parse(buf)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
  });
}

// ── Fulfil file-read requests from Marcus (exploring deeper) ────
async function checkFileRequests(wsPath) {
  if (!wsPath) return;
  try {
    const result = await httpGet('/api/desktop-context/file-requests');
    if (!result?.requests?.length) return;

    const responses = {};
    for (const r of result.requests) {
      const reqPath = String(r.path || '').trim();
      if (!reqPath) continue;
      const fullPath = path.join(wsPath, reqPath);

      // Safety: must stay within workspace
      const resolved = path.resolve(fullPath);
      if (!resolved.startsWith(path.resolve(wsPath))) continue;

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
  const payload = {
    windowTitle: ctx.windowTitle,
    processName: ctx.processName,
    idleSeconds: ctx.idleSeconds,
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

console.log('');
console.log('  M.A.R.C.U.S. Desktop Agent');
console.log(`  Server: ${SERVER_URL}`);
console.log(`  Auth:   ${ADMIN_TOKEN ? 'Bearer token set' : 'no token (local mode)'}`);
console.log(`  Poll:   every ${POLL_MS / 1000}s`);
console.log('  Press Ctrl+C to stop.');
console.log('');

tick();
setInterval(tick, POLL_MS);
