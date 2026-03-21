#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────
// M.A.R.C.U.S. Desktop Agent
// ─────────────────────────────────────────────────────────────
// Captures the active window title, process name, and OS idle
// time on Windows and relays it to a remote M.A.R.C.U.S. server
// so desktop awareness works even when the server is hosted on
// Render (or any non-Windows host).
//
// Usage:
//   node desktop-agent.cjs <SERVER_URL> <ADMIN_TOKEN>
//
// Example:
//   node desktop-agent.cjs https://your-app.onrender.com yourSecretToken
//
// The agent runs until you press Ctrl+C.
// ─────────────────────────────────────────────────────────────
const { execFile } = require('child_process');
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

  const result = await relay(ctx);
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
