#!/usr/bin/env node
const { execFile } = require('child_process');
const fs = require('fs');
const https = require('https');
const http = require('http');
const os = require('os');
const path = require('path');

const SERVER_URL = String(process.argv[2] || process.env.MARCUS_SERVER_URL || '').trim().replace(/\/$/, '');
const CONFIG_DIR = path.join(process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'), 'M.A.R.C.U.S');
const TOKEN_FILE = String(process.env.MARCUS_ADMIN_TOKEN_FILE || path.join(CONFIG_DIR, 'mobile-live-admin-token.txt')).trim();
const STATE_FILE = String(process.env.MARCUS_WATCHDOG_STATE_FILE || path.join(CONFIG_DIR, 'desktop-agent-watchdog.json')).trim();
const TASK_NAME = 'MARCUS-DesktopAgent';
const POLL_MS = 4000;

function readText(filename) {
  try { return fs.readFileSync(filename, 'utf8').replace(/^\uFEFF/, '').trim(); } catch { return ''; }
}

function readState() {
  try { return JSON.parse(readText(STATE_FILE)) || {}; } catch { return {}; }
}

function writeState(patch = {}) {
  const next = { ...readState(), ...patch, updatedAt: new Date().toISOString() };
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  const temporary = `${STATE_FILE}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
  fs.renameSync(temporary, STATE_FILE);
}

function request(method, pathname, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(pathname, `${SERVER_URL}/`);
    const payload = body === undefined ? '' : JSON.stringify(body);
    const transport = url.protocol === 'https:' ? https : http;
    const req = transport.request(url, {
      method,
      headers: {
        Accept: 'application/json',
        ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {}),
        ...(readText(TOKEN_FILE) ? { Authorization: `Bearer ${readText(TOKEN_FILE)}` } : {}),
      },
      timeout: 15000,
    }, (res) => {
      let text = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { text += chunk; });
      res.on('end', () => {
        let data = {};
        try { data = JSON.parse(text); } catch {}
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(data);
        else reject(new Error(data.error || `HTTP ${res.statusCode}`));
      });
    });
    req.on('timeout', () => req.destroy(new Error('Request timed out')));
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function restartDesktopAgent() {
  return new Promise((resolve, reject) => {
    const command = `Stop-ScheduledTask -TaskName '${TASK_NAME}' -ErrorAction SilentlyContinue; Start-ScheduledTask -TaskName '${TASK_NAME}'`;
    execFile('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', command], { windowsHide: true, timeout: 20000 }, (error) => {
      if (error) reject(error); else resolve();
    });
  });
}

let polling = false;
async function poll() {
  if (polling) return;
  polling = true;
  try {
    const status = await request('GET', '/api/marcus/desktop-agent/restart');
    const requestId = String(status.request?.id || '');
    if (!requestId || requestId === String(readState().handledRequestId || '')) return;
    await restartDesktopAgent();
    writeState({ handledRequestId: requestId, restartedAt: new Date().toISOString(), error: '' });
    await request('POST', '/api/marcus/desktop-agent/restart/ack', { requestId, ok: true });
  } catch (error) {
    writeState({ error: String(error?.message || error).slice(0, 500) });
  } finally {
    polling = false;
  }
}

if (!SERVER_URL) throw new Error('A MARCUS server URL is required.');
console.log(`MARCUS desktop-agent watchdog polling ${SERVER_URL}`);
void poll();
const interval = setInterval(() => void poll(), POLL_MS);
process.on('SIGINT', () => { clearInterval(interval); process.exit(0); });
process.on('SIGTERM', () => { clearInterval(interval); process.exit(0); });
