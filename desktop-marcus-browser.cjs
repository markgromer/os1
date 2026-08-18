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
const MARCUS_BROWSER_ACTION_TYPES = new Set([
  'marcus-browser-open',
  'marcus-browser-command',
  'marcus-browser-publish',
]);

function isMarcusBrowserActionType(value) {
  return MARCUS_BROWSER_ACTION_TYPES.has(String(value || '').trim());
}

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

function safeObservableUrl(value) {
  const safe = safeHttpUrl(value);
  if (!safe) return '';
  const parsed = new URL(safe);
  for (const key of [...parsed.searchParams.keys()]) {
    if (/(?:^|_)(?:pwd|password|passcode|token|code|secret|api_?key|auth|authorization|signature|zak|obf)(?:$|_)/i.test(key)) {
      parsed.searchParams.delete(key);
    }
  }
  if (/\b(?:pwd|password|passcode|token|code|secret|auth|signature|zak|obf)\b/i.test(parsed.hash)) {
    parsed.hash = '';
  }
  return parsed.toString();
}

function redactVisibleText(value) {
  return String(value || '')
    .replace(/https?:\/\/\S+/gi, (rawUrl) => {
      const suffix = /[),.!?]$/.test(rawUrl) ? rawUrl.at(-1) : '';
      const url = suffix ? rawUrl.slice(0, -1) : rawUrl;
      return `${safeObservableUrl(url) || '[redacted url]'}${suffix}`;
    })
    .replace(/\b\d{4,8}\b(?=[^\n]{0,100}\b(?:verification|security|login|sign[- ]?in|two[- ]?factor|2fa|one[- ]?time|otp|passcode|reset|code)\b)/gi, '[redacted code]')
    .replace(/\b((?:verification|security|login|sign[- ]?in|two[- ]?factor|2fa|one[- ]?time|otp|passcode|reset|code)[^\n]{0,100})\b\d{4,8}\b/gi, '$1[redacted code]');
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
    composerOpenTimeoutMs = 15_000,
    composerFocusTimeoutMs = 10_000,
    composerPollMs = 350,
  } = {}) {
    this.debugPort = Math.max(1024, Math.min(65535, Number(debugPort) || DEFAULT_DEBUG_PORT));
    this.profileRoot = path.resolve(String(profileRoot || '').trim());
    this.defaultUrl = safeHttpUrl(defaultUrl, DEFAULT_URL);
    this.composerOpenTimeoutMs = Math.max(50, Math.min(30_000, Number(composerOpenTimeoutMs) || 15_000));
    this.composerFocusTimeoutMs = Math.max(50, Math.min(30_000, Number(composerFocusTimeoutMs) || 10_000));
    this.composerPollMs = Math.max(1, Math.min(2_000, Number(composerPollMs) || 350));
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

  async page(preferredContextKind = '') {
    await this.ensureBrowser();
    const pages = await this.pages();
    if (!pages.length) throw new Error('MARCUS Chrome has no controllable page.');
    const active = pages.find((target) => target.id === this.activeTargetId);
    const selected = (preferredContextKind && active && liveContextKind(active.url) === preferredContextKind ? active : null)
      || (preferredContextKind ? pages.find((target) => liveContextKind(target.url) === preferredContextKind) : null)
      || active
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

  async pageForUrl(value) {
    const requestedUrl = safeHttpUrl(value);
    if (!requestedUrl) throw new Error('A valid approved publication URL is required.');
    await this.ensureBrowser(requestedUrl);
    const requested = new URL(requestedUrl);
    const pages = await this.pages();
    const exact = pages.find((target) => {
      const candidate = safeHttpUrl(target.url);
      if (!candidate) return false;
      const parsed = new URL(candidate);
      return parsed.origin === requested.origin
        && parsed.pathname.replace(/\/$/, '') === requested.pathname.replace(/\/$/, '')
        && parsed.search === requested.search;
    });
    const sameSite = pages.find((target) => liveContextKind(target.url) === liveContextKind(requestedUrl));
    const selected = exact || sameSite;
    if (!selected) {
      const current = await this.page();
      const created = await current.session.send('Target.createTarget', { url: requestedUrl });
      if (!created?.targetId) throw new Error('MARCUS Chrome could not open the approved browser page.');
      this.activeTargetId = created.targetId;
      this.session?.close();
      this.session = null;
      this.sessionTargetId = '';
      await wait(1_200);
      const opened = await this.page(liveContextKind(requestedUrl));
      if (liveContextKind(opened.target.url) !== liveContextKind(requestedUrl)) {
        throw new Error('The approved browser page opened, but no controllable tab was found.');
      }
      return opened;
    }
    this.activeTargetId = selected.id;
    const page = await this.page(liveContextKind(requestedUrl));
    if (!exact) {
      await page.session.send('Page.navigate', { url: requestedUrl });
      await wait(1_200);
      page.target.url = requestedUrl;
    }
    return page;
  }

  async command(payload = {}) {
    const command = String(payload.command || '').trim().toLowerCase();
    const requestedUrl = safeHttpUrl(payload.url);
    await this.ensureBrowser(requestedUrl || undefined);
    const preferredContextKind = ['prepare-post', 'prepare-reply'].includes(command) ? 'skool' : '';
    const { target, session } = command === 'publish-approved-draft'
      ? await this.pageForUrl(requestedUrl)
      : preferredContextKind && requestedUrl
        ? await this.pageForUrl(requestedUrl)
        : await this.page(preferredContextKind);
    if (preferredContextKind && liveContextKind(target.url) !== preferredContextKind) {
      throw new Error(`No controllable MARCUS Chrome ${preferredContextKind} tab is available.`);
    }
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
    } else if (command === 'observe-community') {
      result = await this.observeCommunityPage(session, target.url, { viewports: payload.viewports });
    } else if (command === 'inspect-notifications') {
      result = await this.inspectCommunityNotifications(session, target.url);
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
          const wantsStandalonePost = /\\b(?:main feed|feed editor|post editor|write something|standalone|new post|first post)\\b/.test(wanted)
            && !/\\b(?:comment|reply)\\b/.test(wanted);
          const standalonePostMatch = (details) => /\\b(?:write something|start a post|create post|post something|what do you want to share)\\b/.test(details);
          const replyOrCommentMatch = (details) => /\\b(?:comment|reply|respond|leave a comment|write a comment|add a comment)\\b/.test(details);
          const candidates = [...document.querySelectorAll('textarea,input:not([type="password"]),[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]')]
            .filter((element) => {
              const rect = element.getBoundingClientRect();
              const style = getComputedStyle(element);
              if (element.disabled || element.readOnly || rect.width <= 0 || rect.height <= 0 || rect.bottom < 0 || rect.top > innerHeight) return false;
              if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
              const fieldPurpose = [
                element.getAttribute('aria-label'), element.getAttribute('placeholder'), element.getAttribute('name'),
              ].filter(Boolean).join(' ').toLowerCase();
              if (!wanted && element.tagName === 'INPUT' && (fieldPurpose.includes('search') || fieldPurpose.includes('filter'))) return false;
              if (!wanted) return true;
              const details = [
                element.getAttribute('aria-label'), element.getAttribute('placeholder'), element.getAttribute('name'),
                element.getAttribute('data-placeholder'), element.closest('form,[role="dialog"],section')?.innerText,
              ].filter(Boolean).join(' ').replace(/\\s+/g, ' ').toLowerCase();
              if (wantsStandalonePost && replyOrCommentMatch(details)) return false;
              if (wantsStandalonePost && standalonePostMatch(details)) return true;
              return details.includes(wanted) || wanted.includes(details.slice(0, 120));
            })
            .sort((left, right) => {
              const leftEditor = left.matches('textarea,[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]:not(input)') ? 0 : 1;
              const rightEditor = right.matches('textarea,[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]:not(input)') ? 0 : 1;
              return leftEditor - rightEditor || right.getBoundingClientRect().top - left.getBoundingClientRect().top;
            });
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
            const wantsStandalonePost = /\\b(?:main feed|feed editor|post editor|write something|standalone|new post|first post)\\b/.test(wanted)
              && !/\\b(?:comment|reply)\\b/.test(wanted);
            const candidates = [...document.querySelectorAll('a,button,[role="button"],[role="link"]')]
              .filter((element) => {
                const text = String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const rect = element.getBoundingClientRect();
                const style = getComputedStyle(element);
                const matches = text.includes(wanted) || (wantsStandalonePost && /\\b(?:write something|start a post|create post|post something)\\b/.test(text));
                if (wantsStandalonePost && /\\b(?:comment|reply|respond)\\b/.test(text)) return false;
                return matches && rect.width > 0 && rect.height > 0 && rect.bottom >= 0 && rect.top <= innerHeight
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
          focused = await focusEditor(targetLabel);
        }
      }
      if (!focused?.result?.value?.focused) {
        throw new Error(targetLabel ? `No visible editor matched: ${targetLabel}` : 'No visible browser editor is available.');
      }
      await session.send('Input.insertText', { text });
      result = { ...focused.result.value, insertedChars: text.length };
    } else if (command === 'prepare-post') {
      const text = String(payload.text || '').slice(0, 4_000);
      result = await this.prepareStandalonePost(session, target, { text });
    } else if (command === 'prepare-reply') {
      const thread = String(payload.thread || '').replace(/\s+/g, ' ').trim().slice(0, 240);
      const text = String(payload.text || '').slice(0, 4_000);
      result = await this.prepareReply(session, target, { thread, text });
    } else if (command === 'publish-approved-draft') {
      result = await this.publishApprovedDraft(session, target, payload);
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

  async prepareStandalonePost(session, target, { text }) {
    if (!text) throw new Error('Text is required.');
    if (liveContextKind(target.url) !== 'skool') {
      throw new Error('Standalone post preparation is available only on the approved Skool page.');
    }
    if (await this.sensitiveFieldFocused(session)) {
      throw new Error('Password entry is blocked from the remote bridge. Type it in the visible MARCUS Chrome window.');
    }

    const currentUrl = new URL(String(target.url || ''));
    const pathParts = currentUrl.pathname.split('/').filter(Boolean);
    if (!pathParts.length) throw new Error('The Skool community could not be resolved from the visible page.');
    const communityUrl = `${currentUrl.origin}/${pathParts[0]}`;
    await session.send('Page.navigate', { url: communityUrl }, 4_000);

    const opened = await this.evaluateUntil(session, {
      expression: `(() => {
        const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
        const wanted = /^(?:write something|start a post|create post|post something|what do you want to share)(?:\.{3}|[.!?])?$/;
        const candidates = [...document.querySelectorAll('button,[role="button"],textarea,[contenteditable="true"],[contenteditable="plaintext-only"]')]
          .map((element) => ({
            element,
            label: normalize(element.getAttribute('aria-label') || element.getAttribute('placeholder') || element.getAttribute('data-placeholder') || element.innerText || element.textContent).replace(/\u2026/g, '...'),
          }))
          .filter(({ element, label }) => {
            const rect = element.getBoundingClientRect();
            const style = getComputedStyle(element);
            return wanted.test(label) && rect.width > 0 && rect.height > 0
              && rect.bottom >= 0 && rect.top <= innerHeight
              && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0
              && !element.closest('article');
          })
          .sort((left, right) => left.label.length - right.label.length
            || (left.element.getBoundingClientRect().width * left.element.getBoundingClientRect().height)
              - (right.element.getBoundingClientRect().width * right.element.getBoundingClientRect().height));
        const control = candidates[0]?.element;
        if (!control) return { activated: false };
        control.scrollIntoView({ block: 'center', inline: 'nearest' });
        control.click();
        return { activated: true, label: candidates[0].label, href: location.href };
      })()`,
      returnByValue: true,
    }, {
      timeoutMs: this.composerOpenTimeoutMs,
      accept: (value) => value?.result?.value?.activated === true,
    });
    if (!opened?.result?.value?.activated) {
      throw new Error('The ScoopOS main feed is visible, but its standalone post composer could not be opened.');
    }
    const focus = await this.evaluateUntil(session, {
      expression: `(() => {
        const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
        const visible = (element) => {
          const rect = element.getBoundingClientRect();
          const style = getComputedStyle(element);
          return !element.disabled && !element.readOnly && rect.width > 0 && rect.height > 0
            && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
        };
        const exactPostControl = (root) => [...root.querySelectorAll('button,[role="button"],input[type="submit"]')]
          .some((button) => visible(button) && /^(?:post|publish)$/.test(normalize(button.innerText || button.value || button.textContent)));
        const composerContainer = (editor) => {
          let current = editor;
          for (let depth = 0; current && depth < 8; depth += 1, current = current.parentElement) {
            if (exactPostControl(current)) return current;
          }
          return null;
        };
        const candidates = [...document.querySelectorAll('textarea,[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]:not(input)')]
          .filter(visible)
          .map((editor) => ({ editor, container: composerContainer(editor) }))
          .filter(({ editor, container }) => {
            if (!container || editor.closest('article')) return false;
            const details = normalize([
              editor.getAttribute('aria-label'), editor.getAttribute('placeholder'), editor.getAttribute('data-placeholder'),
              container.getAttribute('aria-label'), container.innerText,
            ].filter(Boolean).join(' '));
            return !/\\b(?:leave|write|add) (?:a )?(?:comment|reply)\\b|\\breply to\\b|\\bview \\d+ more replies\\b/.test(details);
          })
          .sort((left, right) => Number(Boolean(right.editor.closest('[role="dialog"]'))) - Number(Boolean(left.editor.closest('[role="dialog"]'))));
        const selected = candidates[0];
        if (!selected) return { focused: false };
        const editor = selected.editor;
        editor.scrollIntoView({ block: 'center', inline: 'nearest' });
        editor.focus();
        if (typeof editor.select === 'function') editor.select();
        else {
          const selection = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(editor);
          selection.removeAllRanges();
          selection.addRange(range);
        }
        return {
          focused: true,
          surface: 'standalone-feed-composer',
          communityRoot: location.pathname.replace(/\\/$/, '').split('/').filter(Boolean).length === 1,
          href: location.href,
          label: String(editor.getAttribute('aria-label') || editor.getAttribute('placeholder') || editor.getAttribute('data-placeholder') || '').slice(0, 240),
        };
      })()`,
      returnByValue: true,
    }, {
      timeoutMs: this.composerFocusTimeoutMs,
      accept: (value) => value?.result?.value?.focused === true && value?.result?.value?.communityRoot === true,
    });
    if (!focus?.result?.value?.focused || !focus?.result?.value?.communityRoot) {
      throw new Error('The standalone Skool feed composer was not verified. No draft was inserted.');
    }

    await session.send('Input.insertText', { text });
    const verified = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const expected = ${JSON.stringify(text.replace(/\r\n/g, '\n'))};
        const editor = document.activeElement;
        if (!editor || !editor.matches('textarea,[contenteditable="true"],[contenteditable="plaintext-only"],[role="textbox"]:not(input)')) {
          return { verified: false };
        }
        const actual = String(editor.value ?? editor.innerText ?? editor.textContent ?? '').replace(/\\r\\n/g, '\\n');
        const communityRoot = location.pathname.replace(/\\/$/, '').split('/').filter(Boolean).length === 1;
        const inThread = Boolean(editor.closest('article'));
        let container = editor;
        let hasPostControl = false;
        for (let depth = 0; container && depth < 8; depth += 1, container = container.parentElement) {
          hasPostControl = [...container.querySelectorAll('button,[role="button"],input[type="submit"]')].some((button) => {
            const label = String(button.innerText || button.value || button.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
            const rect = button.getBoundingClientRect();
            return /^(?:post|publish)$/.test(label) && !button.disabled && rect.width > 0 && rect.height > 0;
          });
          if (hasPostControl) break;
        }
        return { verified: actual === expected && communityRoot && !inThread && hasPostControl, chars: actual.length, communityRoot, inThread, hasPostControl, href: location.href };
      })()`,
      returnByValue: true,
    }, 4_000);
    if (!verified?.result?.value?.verified) {
      throw new Error('The standalone Skool draft failed exact composer read-back. MARCUS will not claim it is ready.');
    }
    return {
      ...focus.result.value,
      verified: true,
      insertedChars: text.length,
      href: verified.result.value.href || focus.result.value.href || communityUrl,
    };
  }

  async evaluateUntil(session, params, { timeoutMs = 10_000, accept = () => true } = {}) {
    const deadline = Date.now() + Math.max(50, Number(timeoutMs) || 10_000);
    let latest = null;
    let latestError = null;
    do {
      try {
        const remaining = Math.max(500, deadline - Date.now());
        latest = await session.send('Runtime.evaluate', params, Math.min(4_000, remaining));
        if (accept(latest)) return latest;
      } catch (error) {
        latestError = error;
      }
      if (Date.now() >= deadline) break;
      await wait(Math.min(this.composerPollMs, Math.max(1, deadline - Date.now())));
    } while (Date.now() < deadline);
    if (latest) return latest;
    if (latestError) throw latestError;
    return null;
  }

  async replaceVisibleEditor(session, text) {
    const focused = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const candidates = [...document.querySelectorAll('textarea,[contenteditable=true],[contenteditable=plaintext-only],[role=textbox]:not(input)')]
          .filter((element) => {
            const rect = element.getBoundingClientRect();
            const style = getComputedStyle(element);
            return !element.disabled && !element.readOnly && rect.width > 0 && rect.height > 0
              && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
          })
          .sort((left, right) => right.getBoundingClientRect().top - left.getBoundingClientRect().top);
        const editor = candidates[0];
        if (!editor) return { focused: false };
        editor.scrollIntoView({ block: 'center', inline: 'nearest' });
        editor.focus();
        if (typeof editor.select === 'function') editor.select();
        else {
          const selection = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(editor);
          selection.removeAllRanges();
          selection.addRange(range);
        }
        return { focused: true };
      })()`,
      returnByValue: true,
    });
    if (!focused?.result?.value?.focused) throw new Error('The approved publication editor is not visible.');
    await session.send('Input.insertText', { text });
  }

  async publishApprovedDraft(session, target, payload = {}) {
    const publicationId = String(payload.publicationId || '').trim().slice(0, 120);
    const mode = payload.mode === 'reply' ? 'reply' : 'post';
    const thread = String(payload.thread || payload.target || '').replace(/\s+/g, ' ').trim().slice(0, 240);
    const text = String(payload.text || '').slice(0, 4_000);
    const submitLabel = String(payload.submitLabel || '').replace(/\s+/g, ' ').trim().slice(0, 80);
    if (!publicationId || !text || !/^(post|publish|send|submit|reply|comment)(\b|\s)/i.test(submitLabel)) {
      throw new Error('The approved publication payload is incomplete.');
    }
    if (await this.sensitiveFieldFocused(session)) {
      throw new Error('Approved publication is blocked while a password field is focused.');
    }
    if (mode === 'reply') await this.prepareReply(session, target, { thread, text });
    else if (liveContextKind(target.url || payload.url) === 'skool') await this.prepareStandalonePost(session, target, { text });
    else await this.replaceVisibleEditor(session, text);

    const verified = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const expected = ${JSON.stringify(text.replace(/\r\n/g, '\n'))};
        const editor = document.activeElement;
        if (!editor || !editor.matches('textarea,[contenteditable=true],[contenteditable=plaintext-only],[role=textbox]:not(input)')) {
          return { matches: false, chars: 0 };
        }
        const actual = String(editor.value ?? editor.innerText ?? editor.textContent ?? '').replace(/\\r\\n/g, '\\n');
        return { matches: actual === expected, chars: actual.length };
      })()`,
      returnByValue: true,
    });
    if (!verified?.result?.value?.matches) {
      throw new Error('The visible editor did not exactly match the approved draft. Nothing was published.');
    }

    const activated = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const wanted = ${JSON.stringify(submitLabel.toLowerCase())};
        const editor = document.activeElement;
        const editorRect = editor?.getBoundingClientRect?.() || { left: 0, top: 0, width: 0, height: 0 };
        const candidates = [...document.querySelectorAll('button,[role=button],input[type=submit]')]
          .filter((element) => {
            const label = String(element.innerText || element.value || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
            const rect = element.getBoundingClientRect();
            const style = getComputedStyle(element);
            return (label === wanted || label.startsWith(wanted + ' ')) && !element.disabled
              && element.getAttribute('aria-disabled') !== 'true' && rect.width > 0 && rect.height > 0
              && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
          })
          .sort((left, right) => {
            const a = left.getBoundingClientRect();
            const b = right.getBoundingClientRect();
            const distanceA = Math.abs(a.left - editorRect.left) + Math.abs(a.top - editorRect.top);
            const distanceB = Math.abs(b.left - editorRect.left) + Math.abs(b.top - editorRect.top);
            return distanceA - distanceB;
          });
        const button = candidates[0];
        if (!button) return { activated: false };
        button.click();
        return { activated: true, label: String(button.innerText || button.value || button.textContent || '').replace(/\\s+/g, ' ').trim() };
      })()`,
      returnByValue: true,
    });
    if (!activated?.result?.value?.activated) {
      throw new Error(`The approved ${submitLabel} control is not visible. Nothing was published.`);
    }
    await wait(700);
    return {
      publicationId,
      published: true,
      mode,
      submitLabel: activated.result.value.label || submitLabel,
      chars: text.length,
      url: safeObservableUrl(target.url || payload.url),
    };
  }

  async prepareReply(session, target, { thread, text }) {
    if (!thread) throw new Error('A visible thread title or distinctive title fragment is required.');
    if (!text) throw new Error('Text is required.');
    if (liveContextKind(target.url) !== 'skool') {
      throw new Error('Thread reply preparation is available only on the approved Skool page.');
    }
    if (await this.sensitiveFieldFocused(session)) {
      throw new Error('Password entry is blocked from the remote bridge. Type it in the visible MARCUS Chrome window.');
    }

    const wantedThread = thread.toLowerCase();
    const surfaceExpression = [
      '(() => {',
      "  const editor = [...document.querySelectorAll('textarea,[contenteditable=true],[contenteditable=plaintext-only],[role=textbox]:not(input)')]",
      "    .some((element) => { const rect = element.getBoundingClientRect(); const style = getComputedStyle(element); return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden'; });",
      "  const latest = [...document.querySelectorAll('a,button,[role=button],[role=link]')]",
      "    .some((element) => String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase().includes('jump to latest comment'));",
      '  return editor || latest;',
      '})()',
    ].join('\n');
    const surface = await session.send('Runtime.evaluate', { expression: surfaceExpression, returnByValue: true }, 4_000);
    const openExpression = [
      '(() => {',
      '  const wanted = ' + JSON.stringify(wantedThread) + ';',
      '  const introRequest = /\\bintro(?:duction)?\\b/.test(wanted) || /\\bdrop your intro\\b/.test(wanted);',
      "  const candidates = [...document.querySelectorAll('a[href]')]",
      '    .filter((element) => {',
      "      const label = String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();",
      '      const rect = element.getBoundingClientRect();',
      '      const style = getComputedStyle(element);',
      "      const matches = label.includes(wanted) || (introRequest && (/\\bdrop your intro\\b/.test(label) || /\\bintroduction\\b/.test(label)));",
      "      return matches && rect.width > 0 && rect.height > 0 && style.display !== 'none'",
      "        && style.visibility !== 'hidden' && Number(style.opacity) !== 0;",
      '    })',
      '    .sort((left, right) => {',
      "      const leftText = String(left.innerText || left.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();",
      "      const rightText = String(right.innerText || right.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();",
      '      return (leftText === wanted ? 0 : 1) - (rightText === wanted ? 0 : 1) || leftText.length - rightText.length;',
      '    });',
      '  const link = candidates[0];',
      '  if (!link) return { activated: false };',
      "  link.scrollIntoView({ block: 'center', inline: 'nearest' });",
      '  link.click();',
      '  return {',
      '    activated: true,',
      "    text: String(link.innerText || link.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 240),",
      "    href: link.tagName === 'A' && /^https?:/i.test(String(link.href || '')) ? String(link.href).slice(0, 4000) : '',",
      '  };',
      '})()',
    ].join('\n');
    let opened = { current: true, text: thread, href: String(target.url || '') };
    if (!surface?.result?.value) {
      const currentUrl = new URL(String(target.url || ''));
      const pathParts = currentUrl.pathname.split('/').filter(Boolean);
      if (pathParts.length > 1) {
        await session.send('Page.navigate', { url: currentUrl.origin + '/' + pathParts[0] }, 4_000);
        await wait(900);
      }
      const located = await session.send('Runtime.evaluate', { expression: openExpression, returnByValue: true }, 4_000);
      const candidate = located?.result?.value || {};
      if (!candidate.activated || (candidate.href && liveContextKind(candidate.href) !== 'skool')) {
        throw new Error('No visible Skool thread matched: ' + thread);
      }
      opened = candidate;
      await wait(1_000);
    }

    const focusReplyEditor = () => session.send('Runtime.evaluate', {
      expression: [
        '(() => {',
        "  const candidates = [...document.querySelectorAll('textarea,[contenteditable=true],[contenteditable=plaintext-only],[role=textbox]:not(input)')]",
        '    .filter((element) => {',
        '      const rect = element.getBoundingClientRect();',
        '      const style = getComputedStyle(element);',
        '      return !element.disabled && !element.readOnly && rect.width > 0 && rect.height > 0',
        "        && style.display !== 'none'",
        "        && style.visibility !== 'hidden' && Number(style.opacity) !== 0;",
        '    })',
        '    .sort((left, right) => right.getBoundingClientRect().top - left.getBoundingClientRect().top);',
        '  const editor = candidates[0];',
        '  if (!editor) return { focused: false };',
        "  editor.scrollIntoView({ block: 'center', inline: 'nearest' });",
        '  editor.focus();',
        "  if (typeof editor.select === 'function') editor.select();",
        '  else {',
        '    const selection = window.getSelection();',
        '    const range = document.createRange();',
        '    range.selectNodeContents(editor);',
        '    selection.removeAllRanges();',
        '    selection.addRange(range);',
        '  }',
        '  return {',
        '    focused: true,',
        '    tag: editor.tagName,',
        '    contentEditable: editor.isContentEditable,',
        "    label: String(editor.getAttribute('aria-label') || editor.getAttribute('placeholder') || editor.getAttribute('data-placeholder') || '').slice(0, 240),",
        '  };',
        '})()',
      ].join('\n'),
      returnByValue: true,
    });

    let focused = await focusReplyEditor();
    let movedToLatest = false;
    if (!focused?.result?.value?.focused) {
      const latestExpression = [
        '(() => {',
        "  const button = [...document.querySelectorAll('a,button,[role=button],[role=link]')].find((element) => {",
        "    const label = String(element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();",
        '    const rect = element.getBoundingClientRect();',
        '    const style = getComputedStyle(element);',
        "    return label.includes('jump to latest comment') && rect.width > 0 && rect.height > 0",
        "      && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;",
        '  });',
        '  if (!button) return { activated: false };',
        "  button.scrollIntoView({ block: 'center', inline: 'nearest' });",
        '  button.click();',
        '  return { activated: true };',
        '})()',
      ].join('\n');
      const latest = await session.send('Runtime.evaluate', { expression: latestExpression, returnByValue: true });
      movedToLatest = Boolean(latest?.result?.value?.activated);
      if (movedToLatest) await wait(900);
      focused = await focusReplyEditor();
    }
    if (!focused?.result?.value?.focused) {
      throw new Error('The ' + thread + ' thread opened, but its visible reply editor was not available.');
    }
    await session.send('Input.insertText', { text });
    return {
      thread: opened.text || thread,
      href: opened.href || '',
      movedToLatest,
      editor: focused.result.value,
      insertedChars: text.length,
    };
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
      return redactVisibleText(result?.result?.value).trim().slice(0, MAX_VISIBLE_TEXT_LENGTH);
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

  async observeCommunityPage(session, url, { viewports = 8 } = {}) {
    if (liveContextKind(url) !== 'skool') throw new Error('Community observation is available only on the approved Skool page.');
    if (await this.sensitiveFieldFocused(session)) throw new Error('Community observation is blocked while a password field is focused.');
    const parsed = new URL(String(url || ''));
    const community = parsed.pathname.split('/').filter(Boolean)[0] || 'unknown';
    const count = Math.max(1, Math.min(12, Number(viewports) || 8));
    const position = await session.send('Runtime.evaluate', { expression: 'Number(window.scrollY) || 0', returnByValue: true });
    const originalY = Number(position?.result?.value) || 0;
    const collected = new Map();
    try {
      await session.send('Runtime.evaluate', { expression: 'window.scrollTo(0, 0)' });
      for (let index = 0; index < count; index += 1) {
        await wait(index === 0 ? 200 : 350);
        const snapshot = await session.send('Runtime.evaluate', {
          expression: `(() => {
            const clean = (value, max = 1500) => String(value || '').replace(/\\s+/g, ' ').trim().slice(0, max);
            const visible = (element) => {
              if (!element || element.closest('textarea,input,select,[contenteditable="true"],[contenteditable="plaintext-only"]')) return false;
              const rect = element.getBoundingClientRect();
              const style = getComputedStyle(element);
              return rect.width > 40 && rect.height > 20 && rect.bottom >= 0 && rect.top <= innerHeight
                && style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
            };
            const selectors = [
              'article', '[data-testid*="post"]', '[data-testid*="comment"]',
              '[class*="PostCard"]', '[class*="post-card"]', '[class*="CommentCard"]', '[class*="comment-card"]'
            ].join(',');
            let candidates = [...document.querySelectorAll(selectors)].filter(visible);
            if (!candidates.length) {
              candidates = [...document.querySelectorAll('main > div > div, main section > div')]
                .filter((element) => visible(element) && clean(element.innerText).length >= 80);
            }
            return candidates.slice(0, 80).map((element) => {
              const profile = [...element.querySelectorAll('a[href]')].find((link) => {
                const href = String(link.href || '');
                const label = clean(link.innerText || link.textContent, 200);
                return label && (/\\/@[^/]+/i.test(href) || /\\/(?:member|profile|user)s?\\//i.test(href));
              });
              const sourceLink = [...element.querySelectorAll('a[href]')].find((link) => {
                const href = String(link.href || '');
                return href.startsWith(location.origin) && !/\\/@[^/]+/i.test(href);
              });
              const details = clean([element.getAttribute('data-testid'), element.className].join(' '), 500).toLowerCase();
              const text = clean(element.innerText || element.textContent);
              const title = clean(element.querySelector('h1,h2,h3,h4,[role="heading"]')?.innerText, 300);
              const kind = details.includes('comment') ? 'comment' : details.includes('reply') ? 'reply' : 'post';
              const counts = [...element.querySelectorAll('button,[role="button"]')].map((button) => clean(button.innerText || button.textContent, 100));
              const numberNear = (word) => {
                const match = counts.join(' | ').match(new RegExp('(?:\\\\d+[,.]?\\\\d*\\\\s*)?' + word + '|' + word + '\\\\s*(\\\\d+[,.]?\\\\d*)', 'i'));
                const numeric = String(match?.[0] || '').match(/\\d+[,.]?\\d*/)?.[0];
                return numeric ? Number(numeric.replace(/,/g, '')) || 0 : 0;
              };
              return {
                author: clean(profile?.innerText || profile?.textContent, 200),
                authorUrl: String(profile?.href || '').slice(0, 2000),
                kind,
                title,
                text,
                sourceUrl: String(sourceLink?.href || location.href).slice(0, 2000),
                reactions: numberNear('like|reaction'),
                comments: numberNear('comment'),
                replies: numberNear('repl(?:y|ies)')
              };
            }).filter((item) => item.author && (item.text || item.title));
          })()`,
          returnByValue: true,
        }, 6_000);
        for (const item of (Array.isArray(snapshot?.result?.value) ? snapshot.result.value : [])) {
          const author = redactVisibleText(item?.author).replace(/\s+/g, ' ').trim().slice(0, 200);
          const contentSummary = redactVisibleText(item?.text).replace(/\s+/g, ' ').trim().slice(0, 1_500);
          const sourceUrl = safeObservableUrl(item?.sourceUrl) || safeObservableUrl(url);
          if (!author || !contentSummary) continue;
          const sourceKey = crypto.createHash('sha256').update(`${author}\n${sourceUrl}\n${contentSummary}`).digest('hex');
          if (collected.has(sourceKey)) continue;
          collected.set(sourceKey, {
            sourceKey,
            platform: 'skool',
            community,
            member: { displayName: author, profileUrl: safeObservableUrl(item?.authorUrl) },
            kind: ['post', 'comment', 'reply'].includes(item?.kind) ? item.kind : 'other',
            sourceTitle: redactVisibleText(item?.title).replace(/\s+/g, ' ').trim().slice(0, 300),
            sourceUrl,
            contentSummary,
            engagement: {
              reactions: Math.max(0, Number(item?.reactions) || 0),
              comments: Math.max(0, Number(item?.comments) || 0),
              replies: Math.max(0, Number(item?.replies) || 0),
            },
            observedAt: new Date().toISOString(),
          });
        }
        const movement = await session.send('Runtime.evaluate', {
          expression: '(() => { const before = window.scrollY; window.scrollBy(0, Math.max(320, Math.floor(innerHeight * 0.82))); return { before, after: window.scrollY, max: Math.max(0, document.documentElement.scrollHeight - innerHeight) }; })()',
          returnByValue: true,
        });
        const scroll = movement?.result?.value || {};
        if (Number(scroll.after) <= Number(scroll.before) || Number(scroll.after) >= Number(scroll.max)) break;
      }
    } finally {
      await session.send('Runtime.evaluate', { expression: `window.scrollTo(0, ${Math.max(0, originalY)})` }).catch(() => {});
    }
    return {
      contextKind: 'skool',
      community,
      observations: [...collected.values()].slice(0, 200),
      observedCount: collected.size,
      viewportsRequested: count,
    };
  }

  async inspectCommunityNotifications(session, url) {
    if (liveContextKind(url) !== 'skool') throw new Error('Community notifications are available only on the approved Skool page.');
    if (await this.sensitiveFieldFocused(session)) throw new Error('Notification inspection is blocked while a password field is focused.');
    const parsed = new URL(String(url || ''));
    const community = parsed.pathname.split('/').filter(Boolean)[0] || 'unknown';
    const opened = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const visible = (element) => { const rect = element.getBoundingClientRect(); const style = getComputedStyle(element); return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden'; };
        const controls = [...document.querySelectorAll('a[href],button,[role="button"]')].filter(visible);
        const target = controls.find((element) => /notification/i.test(String(element.getAttribute('aria-label') || element.title || element.innerText || '')))
          || controls.find((element) => /notification/i.test(String(element.href || '')));
        if (!target) return { activated: false };
        target.click();
        return { activated: true, href: String(target.href || location.href).slice(0, 2000) };
      })()`,
      returnByValue: true,
    });
    if (opened?.result?.value?.activated) await wait(800);
    const snapshot = await session.send('Runtime.evaluate', {
      expression: `(() => {
        const clean = (value, max = 1000) => String(value || '').replace(/\\s+/g, ' ').trim().slice(0, max);
        const visible = (element) => { const rect = element.getBoundingClientRect(); const style = getComputedStyle(element); return rect.width > 40 && rect.height > 18 && rect.bottom >= 0 && rect.top <= innerHeight && style.display !== 'none' && style.visibility !== 'hidden'; };
        let candidates = [...document.querySelectorAll('[data-testid*="notification"], [class*="Notification"], [class*="notification"], [role="listitem"]')].filter(visible);
        if (!candidates.length) candidates = [...document.querySelectorAll('main a[href], [role="dialog"] a[href]')].filter((element) => visible(element) && clean(element.innerText).length >= 20);
        return candidates.slice(0, 100).map((element) => {
          const profile = [...element.querySelectorAll('a[href]')].find((link) => /\\/@[^/]+/i.test(String(link.href || '')));
          const link = element.matches('a[href]') ? element : element.querySelector('a[href]');
          const text = clean(element.innerText || element.textContent);
          const lower = text.toLowerCase();
          const kind = lower.includes('repl') ? 'reply' : lower.includes('mention') ? 'mention' : lower.includes('comment') ? 'comment' : lower.includes('like') || lower.includes('react') ? 'reaction' : 'other';
          return { actor: clean(profile?.innerText || profile?.textContent, 200), actorUrl: String(profile?.href || '').slice(0, 2000), summary: text, sourceUrl: String(link?.href || location.href).slice(0, 2000), kind };
        }).filter((item) => item.summary);
      })()`,
      returnByValue: true,
    }, 6_000);
    const notifications = [];
    const seen = new Set();
    for (const item of (Array.isArray(snapshot?.result?.value) ? snapshot.result.value : [])) {
      const summary = redactVisibleText(item?.summary).replace(/\s+/g, ' ').trim().slice(0, 1_000);
      const sourceUrl = safeObservableUrl(item?.sourceUrl) || safeObservableUrl(url);
      const sourceKey = crypto.createHash('sha256').update(`${sourceUrl}\n${summary}`).digest('hex');
      if (!summary || seen.has(sourceKey)) continue;
      seen.add(sourceKey);
      notifications.push({
        sourceKey,
        platform: 'skool',
        community,
        actor: item?.actor ? { displayName: redactVisibleText(item.actor).slice(0, 200), profileUrl: safeObservableUrl(item?.actorUrl) } : null,
        kind: item?.kind || 'other',
        summary,
        sourceUrl,
        observedAt: new Date().toISOString(),
      });
    }
    return { contextKind: 'skool', community, notifications, notificationCount: notifications.length };
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
          url: safeObservableUrl(target.url),
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
        url: safeObservableUrl(target.url),
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
  isMarcusBrowserActionType,
  liveContextKind,
  safeHttpUrl,
  safeObservableUrl,
  redactVisibleText,
};
