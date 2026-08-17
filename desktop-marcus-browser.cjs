const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const WebSocket = require('ws');

const DEFAULT_DEBUG_PORT = 9333;
const DEFAULT_URL = 'https://mail.google.com/';
const MAX_FRAME_BASE64_LENGTH = 390_000;

function safeHttpUrl(value, fallback = '') {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  try {
    const parsed = new URL(raw);
    return ['http:', 'https:'].includes(parsed.protocol) ? parsed.toString() : fallback;
  } catch {
    return fallback;
  }
}

function chromeExecutable() {
  const candidates = [
    path.join(process.env.ProgramFiles || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env['ProgramFiles(x86)'] || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Google', 'Chrome', 'Application', 'chrome.exe'),
  ];
  return candidates.find((candidate) => candidate && fs.existsSync(candidate)) || '';
}

function getJson(port, pathname, timeoutMs = 1_500) {
  return new Promise((resolve, reject) => {
    const request = http.get({
      hostname: '127.0.0.1',
      port,
      path: pathname,
      timeout: timeoutMs,
    }, (response) => {
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => {
        if (response.statusCode < 200 || response.statusCode >= 300) {
          reject(new Error(`Chrome debugging endpoint returned ${response.statusCode}`));
          return;
        }
        try { resolve(JSON.parse(body)); } catch { reject(new Error('Chrome debugging endpoint returned invalid JSON')); }
      });
    });
    request.on('error', reject);
    request.on('timeout', () => request.destroy(new Error('Chrome debugging endpoint timed out')));
  });
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class CdpSession {
  constructor(webSocketUrl) {
    this.webSocketUrl = webSocketUrl;
    this.socket = null;
    this.connecting = null;
    this.nextId = 1;
    this.pending = new Map();
  }

  async connect() {
    if (this.socket?.readyState === WebSocket.OPEN) return;
    if (this.connecting) return this.connecting;
    this.connecting = new Promise((resolve, reject) => {
      const socket = new WebSocket(this.webSocketUrl, { handshakeTimeout: 2_500 });
      const fail = (error) => {
        if (this.socket === socket) this.socket = null;
        reject(error);
      };
      socket.once('open', () => {
        socket.removeListener('error', fail);
        this.socket = socket;
        socket.on('message', (raw) => this.onMessage(raw));
        socket.on('close', () => this.onClose(new Error('Chrome page connection closed')));
        socket.on('error', () => {});
        resolve();
      });
      socket.once('error', fail);
    }).finally(() => { this.connecting = null; });
    return this.connecting;
  }

  onMessage(raw) {
    let message;
    try { message = JSON.parse(String(raw)); } catch { return; }
    if (!message.id || !this.pending.has(message.id)) return;
    const pending = this.pending.get(message.id);
    this.pending.delete(message.id);
    clearTimeout(pending.timer);
    if (message.error) pending.reject(new Error(message.error.message || 'Chrome command failed'));
    else pending.resolve(message.result || {});
  }

  onClose(error) {
    this.socket = null;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }

  async send(method, params = {}, timeoutMs = 5_000) {
    await this.connect();
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Chrome command ${method} timed out`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.socket.send(JSON.stringify({ id, method, params }), (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      });
    });
  }

  close() {
    try { this.socket?.close(); } catch {}
    this.onClose(new Error('Chrome page connection replaced'));
  }
}

class MarcusBrowserBridge {
  constructor({
    debugPort = DEFAULT_DEBUG_PORT,
    profileRoot = path.join(process.env.LOCALAPPDATA || os.tmpdir(), 'M.A.R.C.U.S', 'MarcusBrowserProfile'),
    defaultUrl = DEFAULT_URL,
  } = {}) {
    this.debugPort = Math.max(1024, Math.min(65535, Number(debugPort) || DEFAULT_DEBUG_PORT));
    this.profileRoot = path.resolve(String(profileRoot || '').trim());
    this.defaultUrl = safeHttpUrl(defaultUrl, DEFAULT_URL);
    this.activeTargetId = '';
    this.session = null;
    this.sessionTargetId = '';
    this.launchedAt = 0;
    this.lastError = '';
  }

  async debuggingEndpoint() {
    try {
      const version = await getJson(this.debugPort, '/json/version');
      return {
        reachable: true,
        chrome: Boolean(version?.webSocketDebuggerUrl && /(?:Chrome|Chromium)\//i.test(String(version?.Browser || ''))),
      };
    } catch {
      return { reachable: false, chrome: false };
    }
  }

  async debuggingReady() {
    return (await this.debuggingEndpoint()).chrome;
  }

  async ensureBrowser(url = '') {
    const endpoint = await this.debuggingEndpoint();
    if (endpoint.chrome) return true;
    if (endpoint.reachable) throw new Error(`Local port ${this.debugPort} is already used by a non-Chrome debugger. Set MARCUS_CHROME_DEBUG_PORT to a free localhost port.`);
    const executable = chromeExecutable();
    if (!executable) throw new Error('Google Chrome was not found on this PC.');
    fs.mkdirSync(this.profileRoot, { recursive: true });
    const targetUrl = safeHttpUrl(url, this.defaultUrl);
    const args = [
      `--user-data-dir=${this.profileRoot}`,
      '--profile-directory=Default',
      `--remote-debugging-port=${this.debugPort}`,
      '--remote-debugging-address=127.0.0.1',
      '--no-first-run',
      '--disable-features=Translate',
      '--new-window',
      '--window-size=1280,800',
      targetUrl,
    ];
    const child = spawn(executable, args, { detached: true, stdio: 'ignore', windowsHide: false });
    child.unref();
    this.launchedAt = Date.now();
    for (let attempt = 0; attempt < 20; attempt += 1) {
      await wait(250);
      if (await this.debuggingReady()) return true;
    }
    throw new Error('MARCUS Chrome is open without browser control. Close that one Chrome window once; the desktop bridge will restart the same profile with control enabled.');
  }

  async pages() {
    const targets = await getJson(this.debugPort, '/json/list', 2_000);
    return (Array.isArray(targets) ? targets : []).filter((target) => target?.type === 'page' && target.webSocketDebuggerUrl);
  }

  async page() {
    await this.ensureBrowser();
    const pages = await this.pages();
    if (!pages.length) throw new Error('MARCUS Chrome has no controllable page.');
    const selected = pages.find((target) => target.id === this.activeTargetId)
      || pages.find((target) => /^https?:/i.test(target.url || ''))
      || pages[0];
    this.activeTargetId = selected.id;
    if (!this.session || this.sessionTargetId !== selected.id || this.session.webSocketUrl !== selected.webSocketDebuggerUrl) {
      this.session?.close();
      this.session = new CdpSession(selected.webSocketDebuggerUrl);
      this.sessionTargetId = selected.id;
    }
    return { target: selected, session: this.session };
  }

  async command(payload = {}) {
    const command = String(payload.command || '').trim().toLowerCase();
    const requestedUrl = safeHttpUrl(payload.url);
    await this.ensureBrowser(requestedUrl || undefined);
    const { target, session } = await this.page();
    let result = {};
    if (command === 'open' || command === 'navigate') {
      if (!requestedUrl) throw new Error('A valid http or https URL is required.');
      result = await session.send('Page.navigate', { url: requestedUrl });
    } else if (command === 'back' || command === 'forward') {
      const history = await session.send('Page.getNavigationHistory');
      const offset = command === 'back' ? -1 : 1;
      const entry = history.entries?.[history.currentIndex + offset];
      if (entry?.id !== undefined) result = await session.send('Page.navigateToHistoryEntry', { entryId: entry.id });
    } else if (command === 'refresh') {
      result = await session.send('Page.reload', { ignoreCache: false });
    } else if (command === 'click') {
      const x = Math.max(0, Math.min(10_000, Number(payload.x) || 0));
      const y = Math.max(0, Math.min(10_000, Number(payload.y) || 0));
      await session.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
      result = await session.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
    } else if (command === 'scroll') {
      result = await session.send('Input.dispatchMouseEvent', {
        type: 'mouseWheel',
        x: Math.max(0, Math.min(10_000, Number(payload.x) || 0)),
        y: Math.max(0, Math.min(10_000, Number(payload.y) || 0)),
        deltaX: Math.max(-2_000, Math.min(2_000, Number(payload.deltaX) || 0)),
        deltaY: Math.max(-2_000, Math.min(2_000, Number(payload.deltaY) || 0)),
      });
    } else if (command === 'type') {
      const text = String(payload.text || '').slice(0, 4_000);
      if (!text) throw new Error('Text is required.');
      const sensitive = await this.sensitiveFieldFocused(session);
      if (sensitive) throw new Error('Password entry is blocked from the remote bridge. Type it in the visible MARCUS Chrome window.');
      result = await session.send('Input.insertText', { text });
    } else if (command === 'key') {
      const key = String(payload.key || '').slice(0, 40);
      const keys = {
        Enter: { code: 'Enter', windowsVirtualKeyCode: 13 },
        Tab: { code: 'Tab', windowsVirtualKeyCode: 9 },
        Backspace: { code: 'Backspace', windowsVirtualKeyCode: 8 },
        Escape: { code: 'Escape', windowsVirtualKeyCode: 27 },
        ArrowUp: { code: 'ArrowUp', windowsVirtualKeyCode: 38 },
        ArrowDown: { code: 'ArrowDown', windowsVirtualKeyCode: 40 },
        ArrowLeft: { code: 'ArrowLeft', windowsVirtualKeyCode: 37 },
        ArrowRight: { code: 'ArrowRight', windowsVirtualKeyCode: 39 },
      };
      if (!keys[key]) throw new Error('That key is not allowed through the browser bridge.');
      await session.send('Input.dispatchKeyEvent', { type: 'rawKeyDown', key, ...keys[key] });
      result = await session.send('Input.dispatchKeyEvent', { type: 'keyUp', key, ...keys[key] });
    } else {
      throw new Error(`Unsupported MARCUS browser command: ${command || 'missing'}`);
    }
    this.lastError = '';
    return { ok: true, details: { command, targetId: target.id, result } };
  }

  async sensitiveFieldFocused(session) {
    try {
      const result = await session.send('Runtime.evaluate', {
        expression: "Boolean(document.activeElement && document.activeElement.tagName === 'INPUT' && String(document.activeElement.type).toLowerCase() === 'password')",
        returnByValue: true,
      });
      return result?.result?.value === true;
    } catch {
      return false;
    }
  }

  async capture() {
    try {
      const { target, session } = await this.page();
      const metrics = await session.send('Page.getLayoutMetrics');
      const viewport = metrics?.cssVisualViewport || metrics?.cssLayoutViewport || {};
      const sensitive = await this.sensitiveFieldFocused(session);
      if (sensitive) {
        return {
          ok: true,
          connected: true,
          sensitive: true,
          title: String(target.title || '').slice(0, 300),
          url: safeHttpUrl(target.url),
          viewportWidth: Math.max(1, Math.round(Number(viewport.clientWidth) || 1280)),
          viewportHeight: Math.max(1, Math.round(Number(viewport.clientHeight) || 720)),
          frameBase64: '',
          error: '',
        };
      }
      let screenshot = await session.send('Page.captureScreenshot', {
        format: 'jpeg', quality: 48, fromSurface: true, captureBeyondViewport: false,
      }, 8_000);
      if (String(screenshot.data || '').length > MAX_FRAME_BASE64_LENGTH) {
        screenshot = await session.send('Page.captureScreenshot', {
          format: 'jpeg', quality: 32, fromSurface: true, captureBeyondViewport: false,
        }, 8_000);
      }
      const frameBase64 = String(screenshot.data || '');
      if (frameBase64.length > MAX_FRAME_BASE64_LENGTH) throw new Error('Chrome frame is too large to relay safely. Resize the MARCUS Chrome window smaller.');
      this.lastError = '';
      return {
        ok: true,
        connected: true,
        sensitive: false,
        title: String(target.title || '').slice(0, 300),
        url: safeHttpUrl(target.url),
        viewportWidth: Math.max(1, Math.round(Number(viewport.clientWidth) || 1280)),
        viewportHeight: Math.max(1, Math.round(Number(viewport.clientHeight) || 720)),
        frameBase64,
        error: '',
      };
    } catch (error) {
      this.lastError = String(error?.message || error).slice(0, 500);
      return { ok: false, connected: false, sensitive: false, title: '', url: '', frameBase64: '', error: this.lastError };
    }
  }
}

module.exports = {
  MarcusBrowserBridge,
  safeHttpUrl,
};
