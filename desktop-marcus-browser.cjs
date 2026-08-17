const { spawn } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const WebSocket = require('ws');

const DEFAULT_DEBUG_PORT = 9333;
const DEFAULT_URL = 'https://mail.google.com/';
const MAX_FRAME_BASE64_LENGTH = 390_000;
const MAX_VISIBLE_TEXT_LENGTH = 6_000;

function liveContextKind(value) {
  const url = safeHttpUrl(value);
  if (!url) return '';
  const parsed = new URL(url);
  const host = parsed.hostname.toLowerCase();
  if (host === 'mail.google.com') return 'gmail';
  if (host === 'meet.google.com') return 'google-meet';
  if (host === 'teams.microsoft.com' || host === 'teams.live.com') return 'teams';
  if (host === 'www.skool.com' || host === 'skool.com') return 'skool';
  if (host === 'app.zoom.us' || host.endsWith('.zoom.us')) return 'zoom';
  if (host === 'www.youtube.com' || host === 'youtube.com') return 'youtube';
  if (host === 'www.tiktok.com' || host === 'tiktok.com') return 'tiktok';
  return '';
}

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
    if (command === 'open') {
      if (!requestedUrl) throw new Error('A valid http or https URL is required.');
      result = await session.send('Target.createTarget', { url: requestedUrl });
      if (result?.targetId) {
        this.activeTargetId = result.targetId;
        this.session?.close();
        this.session = null;
        this.sessionTargetId = '';
      }
    } else if (command === 'navigate') {
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
    } else if (command === 'activate') {
      const label = String(payload.label || '').replace(/\s+/g, ' ').trim().slice(0, 240);
      if (!label) throw new Error('Visible link or button text is required.');
      if (await this.sensitiveFieldFocused(session)) throw new Error('Visible actions are blocked while a password field is focused.');
      result = await session.send('Runtime.evaluate', {
        expression: `(() => {
          const wanted = ${JSON.stringify(label.toLowerCase())};
          const candidates = [...document.querySelectorAll('a,button,[role="button"],[role="link"],tr')]
            .filter((element) => {
              const text = String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const rect = element.getBoundingClientRect();
              const style = getComputedStyle(element);
              return text.includes(wanted) && rect.width > 0 && rect.height > 0 && rect.bottom >= 0 && rect.top <= innerHeight
                && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
            })
            .sort((left, right) => {
              const leftControl = left.matches('a,button,[role="button"],[role="link"]') ? 0 : 1;
              const rightControl = right.matches('a,button,[role="button"],[role="link"]') ? 0 : 1;
              return leftControl - rightControl || String(left.innerText || '').length - String(right.innerText || '').length;
            });
          const target = candidates[0];
          if (!target) return { activated: false };
          target.click();
          return {
            activated: true,
            tag: target.tagName,
            text: String(target.innerText || target.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 240),
            href: target.tagName === 'A' && /^https?:/i.test(String(target.href || '')) ? String(target.href).slice(0, 4_000) : ''
          };
        })()`,
        returnByValue: true,
      });
      if (!result?.result?.value?.activated) throw new Error(`No visible link or button matched: ${label}`);
    } else if (command === 'read') {
      result = await this.readVisiblePage(session, target.url, { viewports: payload.viewports });
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
    } else if (command === 'fill') {
      const targetLabel = String(payload.target || '').replace(/\s+/g, ' ').trim().slice(0, 240);
      const text = String(payload.text || '').slice(0, 4_000);
      if (!text) throw new Error('Text is required.');
      if (await this.sensitiveFieldFocused(session)) {
        throw new Error('Password entry is blocked from the remote bridge. Type it in the visible MARCUS Chrome window.');
      }
      const focusEditor = (wantedLabel = '') => session.send('Runtime.evaluate', {
        expression: `(() => {
          const wanted = ${JSON.stringify(String(wantedLabel || '').toLowerCase())};
          const candidates = [...document.querySelectorAll('textarea,input:not([type="password"]),[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]')]
            .filter((element) => {
              const rect = element.getBoundingClientRect();
              const style = getComputedStyle(element);
              if (element.disabled || element.readOnly || rect.width <= 0 || rect.height <= 0 || rect.bottom < 0 || rect.top > innerHeight) return false;
              if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
              if (!wanted) return true;
              const details = [
                element.getAttribute('aria-label'), element.getAttribute('placeholder'), element.getAttribute('name'),
                element.getAttribute('data-placeholder'), element.closest('form,[role="dialog"],section')?.innerText,
              ].filter(Boolean).join(' ').replace(/\\s+/g, ' ').toLowerCase();
              return details.includes(wanted) || wanted.includes(details.slice(0, 120));
            })
            .sort((left, right) => left.getBoundingClientRect().top - right.getBoundingClientRect().top);
          const target = candidates[0];
          if (!target) return { focused: false };
          target.scrollIntoView({ block: 'center', inline: 'nearest' });
          target.focus();
          if (typeof target.select === 'function') target.select();
          else {
            const selection = window.getSelection();
            const range = document.createRange();
            range.selectNodeContents(target);
            selection.removeAllRanges();
            selection.addRange(range);
          }
          return {
            focused: true,
            tag: target.tagName,
            contentEditable: target.isContentEditable,
            label: String(target.getAttribute('aria-label') || target.getAttribute('placeholder') || target.getAttribute('data-placeholder') || '').slice(0, 240)
          };
        })()`,
        returnByValue: true,
      });
      let focused = await focusEditor(targetLabel);
      if (!focused?.result?.value?.focused && targetLabel && !/^(post|publish|send|submit)$/i.test(targetLabel)) {
        const opened = await session.send('Runtime.evaluate', {
          expression: `(() => {
            const wanted = ${JSON.stringify(targetLabel.toLowerCase())};
            const candidates = [...document.querySelectorAll('a,button,[role="button"],[role="link"]')]
              .filter((element) => {
                const text = String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const rect = element.getBoundingClientRect();
                const style = getComputedStyle(element);
                return text.includes(wanted) && rect.width > 0 && rect.height > 0 && rect.bottom >= 0 && rect.top <= innerHeight
                  && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
              })
              .sort((left, right) => String(left.innerText || '').length - String(right.innerText || '').length);
            if (!candidates[0]) return { activated: false };
            candidates[0].click();
            return { activated: true };
          })()`,
          returnByValue: true,
        });
        if (opened?.result?.value?.activated) {
          await wait(600);
          focused = await focusEditor('');
        }
      }
      if (!focused?.result?.value?.focused) {
        throw new Error(targetLabel ? `No visible editor matched: ${targetLabel}` : 'No visible browser editor is available.');
      }
      await session.send('Input.insertText', { text });
      result = { ...focused.result.value, insertedChars: text.length };
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

  async visiblePageText(session) {
    try {
      const result = await session.send('Runtime.evaluate', {
        expression: `(() => {
          const blocked = 'SCRIPT,STYLE,NOSCRIPT,INPUT,TEXTAREA,SELECT,OPTION,[contenteditable="true"],[contenteditable="plaintext-only"]';
          const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_TEXT);
          const lines = [];
          let total = 0;
          for (let node = walker.nextNode(); node && total < ${MAX_VISIBLE_TEXT_LENGTH}; node = walker.nextNode()) {
            const parent = node.parentElement;
            if (!parent || parent.closest(blocked) || parent.getAttribute('aria-hidden') === 'true') continue;
            const style = getComputedStyle(parent);
            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) continue;
            const rect = parent.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0 || rect.bottom < 0 || rect.top > innerHeight || rect.right < 0 || rect.left > innerWidth) continue;
            const text = String(node.nodeValue || '').replace(/\\s+/g, ' ').trim();
            if (!text) continue;
            lines.push(text);
            total += text.length + 1;
          }
          return lines.join('\\n').slice(0, ${MAX_VISIBLE_TEXT_LENGTH});
        })()`,
        returnByValue: true,
      });
      return String(result?.result?.value || '').trim().slice(0, MAX_VISIBLE_TEXT_LENGTH);
    } catch {
      return '';
    }
  }

  async readVisiblePage(session, url, { viewports = 8 } = {}) {
    const contextKind = liveContextKind(url);
    if (!contextKind) throw new Error('This page is outside the approved MARCUS visible-context sites.');
    if (await this.sensitiveFieldFocused(session)) throw new Error('Page reading is blocked while a password field is focused.');
    const count = Math.max(1, Math.min(12, Number(viewports) || 8));
    const position = await session.send('Runtime.evaluate', { expression: 'Number(window.scrollY) || 0', returnByValue: true });
    const originalY = Number(position?.result?.value) || 0;
    const sections = [];
    try {
      await session.send('Runtime.evaluate', { expression: 'window.scrollTo(0, 0)' });
      for (let index = 0; index < count; index += 1) {
        await wait(index === 0 ? 150 : 300);
        const visible = await this.visiblePageText(session);
        if (visible && !sections.includes(visible)) sections.push(visible);
        const movement = await session.send('Runtime.evaluate', {
          expression: '(() => { const before = window.scrollY; window.scrollBy(0, Math.max(320, Math.floor(innerHeight * 0.82))); return { before, after: window.scrollY, max: Math.max(0, document.documentElement.scrollHeight - innerHeight) }; })()',
          returnByValue: true,
        });
        const scroll = movement?.result?.value || {};
        if (Number(scroll.after) <= Number(scroll.before) || Number(scroll.after) >= Number(scroll.max)) {
          const finalVisible = await this.visiblePageText(session);
          if (finalVisible && !sections.includes(finalVisible)) sections.push(finalVisible);
          break;
        }
      }
    } finally {
      await session.send('Runtime.evaluate', { expression: `window.scrollTo(0, ${Math.max(0, originalY)})` }).catch(() => {});
    }
    return {
      contextKind,
      visibleText: sections.join('\n\n--- next viewport ---\n\n').slice(0, 16_000),
      viewportsRead: sections.length,
    };
  }

  async capture() {
    try {
      const { target, session } = await this.page();
      const metrics = await session.send('Page.getLayoutMetrics');
      const viewport = metrics?.cssVisualViewport || metrics?.cssLayoutViewport || {};
      const sensitive = await this.sensitiveFieldFocused(session);
      const contextKind = sensitive ? '' : liveContextKind(target.url);
      const visibleText = contextKind ? await this.visiblePageText(session) : '';
      const contextVersion = visibleText
        ? crypto.createHash('sha256').update(`${contextKind}\n${visibleText}`).digest('hex').slice(0, 20)
        : '';
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
          contextKind: '',
          visibleText: '',
          contextVersion: '',
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
        contextKind,
        visibleText,
        contextVersion,
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
  liveContextKind,
  safeHttpUrl,
};
