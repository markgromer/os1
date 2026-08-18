import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';

const require = createRequire(import.meta.url);
const {
  MarcusBrowserBridge, isMarcusBrowserActionType, liveContextKind, safeHttpUrl, safeObservableUrl, redactVisibleText,
} = require('../desktop-marcus-browser.cjs');

test('desktop dispatcher recognizes every MARCUS browser action type', () => {
  assert.equal(isMarcusBrowserActionType('marcus-browser-open'), true);
  assert.equal(isMarcusBrowserActionType('marcus-browser-command'), true);
  assert.equal(isMarcusBrowserActionType('marcus-browser-publish'), true);
  assert.equal(isMarcusBrowserActionType('marcus-meeting-note'), false);
});

test('MARCUS browser bridge accepts only HTTP(S) navigation', () => {
  assert.equal(safeHttpUrl('https://www.skool.com'), 'https://www.skool.com/');
  assert.equal(safeHttpUrl('http://127.0.0.1:3030/live-presence.html'), 'http://127.0.0.1:3030/live-presence.html');
  assert.equal(safeHttpUrl('javascript:alert(1)'), '');
  assert.equal(safeHttpUrl('file:///C:/Users/markg/secret.txt'), '');
});

test('MARCUS browser observations redact credential-like URL parameters', () => {
  const observed = safeObservableUrl('https://app.zoom.us/wc/123/start?fromPWA=1&pwd=meeting-secret&token=private');
  assert.equal(observed, 'https://app.zoom.us/wc/123/start?fromPWA=1');
  assert.equal(
    safeObservableUrl('https://www.skool.com/localgiants/drop-your-intro?view=latest'),
    'https://www.skool.com/localgiants/drop-your-intro?view=latest',
  );
});

test('MARCUS browser visible text redacts one-time codes and credential URLs', () => {
  const observed = redactVisibleText('049325 is your Zoom verification code. Join https://us06web.zoom.us/j/123?pwd=secret&from=mail');
  assert.doesNotMatch(observed, /049325|pwd=secret/);
  assert.match(observed, /\[redacted code\]/);
  assert.match(observed, /from=mail/);
});

test('MARCUS browser bridge uses the dedicated non-conflicting localhost port', () => {
  const bridge = new MarcusBrowserBridge();
  assert.equal(bridge.debugPort, 9333);
  assert.match(bridge.profileRoot, /M\.A\.R\.C\.U\.S[\\/]MarcusBrowserProfile$/i);
});

test('MARCUS browser bridge observes only approved live-site pages', () => {
  assert.equal(liveContextKind('https://app.zoom.us/wc/123'), 'zoom');
  assert.equal(liveContextKind('https://www.skool.com/community'), 'skool');
  assert.equal(liveContextKind('https://meet.google.com/abc-defg-hij'), 'google-meet');
  assert.equal(liveContextKind('https://mail.google.com/mail/u/0/#inbox'), 'gmail');
  assert.equal(liveContextKind('https://example.com/private'), '');
});

test('MARCUS browser bridge selects the requested signed-in site tab instead of Chrome list order', async () => {
  const bridge = new MarcusBrowserBridge();
  bridge.ensureBrowser = async () => true;
  bridge.pages = async () => [
    { id: 'zoom-tab', type: 'page', url: 'https://us05web.zoom.us/profile', webSocketDebuggerUrl: 'ws://127.0.0.1/zoom' },
    { id: 'skool-tab', type: 'page', url: 'https://www.skool.com/localgiants', webSocketDebuggerUrl: 'ws://127.0.0.1/skool' },
  ];
  const selected = await bridge.page('skool');
  assert.equal(selected.target.id, 'skool-tab');
  assert.equal(bridge.activeTargetId, 'skool-tab');
  bridge.session.close();
});

test('MARCUS visible-page observation is bounded and uses a redacting DOM expression', async () => {
  const bridge = new MarcusBrowserBridge();
  let expression = '';
  const text = await bridge.visiblePageText({
    send: async (method, params) => {
      assert.equal(method, 'Runtime.evaluate');
      expression = params.expression;
      return { result: { value: 'x'.repeat(7_000) } };
    },
  });
  assert.equal(text.length, 6_000);
  assert.match(expression, /INPUT,TEXTAREA,SELECT/);
  assert.match(expression, /contenteditable/);
  assert.match(expression, /aria-hidden/);
});

test('MARCUS open command creates a new Chrome tab instead of replacing the active page', async () => {
  const bridge = new MarcusBrowserBridge();
  const calls = [];
  bridge.ensureBrowser = async () => true;
  bridge.page = async () => ({
    target: { id: 'old-tab' },
    session: {
      send: async (method, params) => {
        calls.push({ method, params });
        return { targetId: 'new-tab' };
      },
    },
  });
  const result = await bridge.command({ command: 'open', url: 'https://mail.google.com/' });
  assert.deepEqual(calls, [{ method: 'Target.createTarget', params: { url: 'https://mail.google.com/' } }]);
  assert.equal(bridge.activeTargetId, 'new-tab');
  assert.equal(result.ok, true);
});

test('MARCUS activate command matches bounded visible controls without script interpolation', async () => {
  const bridge = new MarcusBrowserBridge();
  let expression = '';
  bridge.ensureBrowser = async () => true;
  bridge.sensitiveFieldFocused = async () => false;
  bridge.page = async () => ({
    target: { id: 'gmail-tab' },
    session: {
      send: async (method, params) => {
        assert.equal(method, 'Runtime.evaluate');
        expression = params.expression;
        return { result: { value: { activated: true, tag: 'TR', text: 'Invitation' } } };
      },
    },
  });
  await bridge.command({ command: 'activate', label: 'Invite "quoted" text' });
  assert.match(expression, /Invite \\"quoted\\" text/i);
  assert.match(expression, /a,button/);
});

test('MARCUS fill command targets a visible editor and inserts text without submitting', async () => {
  const bridge = new MarcusBrowserBridge();
  const calls = [];
  bridge.ensureBrowser = async () => true;
  bridge.sensitiveFieldFocused = async () => false;
  bridge.page = async () => ({
    target: { id: 'skool-tab' },
    session: {
      send: async (method, params) => {
        calls.push({ method, params });
        if (method === 'Runtime.evaluate') {
          return { result: { value: { focused: true, tag: 'DIV', contentEditable: true, label: 'Write something' } } };
        }
        return {};
      },
    },
  });
  const result = await bridge.command({ command: 'fill', target: 'Write something', text: 'Prepared post text.' });
  assert.equal(result.ok, true);
  assert.match(calls[0].params.expression, /contenteditable/);
  assert.match(calls[0].params.expression, /write something/i);
  assert.match(calls[0].params.expression, /fieldPurpose\.includes\('search'\)/);
  assert.deepEqual(calls[1], { method: 'Input.insertText', params: { text: 'Prepared post text.' } });
});

test('MARCUS fill command can open a named composer before inserting the draft', async () => {
  const bridge = new MarcusBrowserBridge();
  let runtimeCall = 0;
  bridge.ensureBrowser = async () => true;
  bridge.sensitiveFieldFocused = async () => false;
  bridge.page = async () => ({
    target: { id: 'skool-tab' },
    session: {
      send: async (method) => {
        if (method === 'Input.insertText') return {};
        runtimeCall += 1;
        if (runtimeCall === 1) return { result: { value: { focused: false } } };
        if (runtimeCall === 2) return { result: { value: { activated: true } } };
        return { result: { value: { focused: true, tag: 'DIV', contentEditable: true, label: '' } } };
      },
    },
  });
  const result = await bridge.command({ command: 'fill', target: 'Write something', text: 'Prepared post text.' });
  assert.equal(result.details.result.insertedChars, 19);
  assert.equal(runtimeCall, 3);
});

test('MARCUS standalone feed post targeting avoids comment editors', async () => {
  const bridge = new MarcusBrowserBridge();
  let runtimeCall = 0;
  const expressions = [];
  bridge.ensureBrowser = async () => true;
  bridge.sensitiveFieldFocused = async () => false;
  bridge.page = async () => ({
    target: { id: 'skool-tab' },
    session: {
      send: async (method, params) => {
        if (method === 'Input.insertText') return {};
        expressions.push(params.expression);
        runtimeCall += 1;
        if (runtimeCall === 1) return { result: { value: { focused: false } } };
        if (runtimeCall === 2) return { result: { value: { activated: true } } };
        return { result: { value: { focused: true, tag: 'DIV', contentEditable: true, label: 'Write something' } } };
      },
    },
  });
  const result = await bridge.command({ command: 'fill', target: 'main feed editor', text: 'Standalone post text.' });
  assert.equal(result.ok, true);
  assert.equal(runtimeCall, 3);
  assert.match(expressions[0], /wantsStandalonePost/);
  assert.match(expressions[0], /replyOrCommentMatch/);
  assert.match(expressions[1], /write something\|start a post\|create post\|post something/);
  assert.match(expressions[2], /wantsStandalonePost/);
});

test('MARCUS thread reply workflow opens the thread and current comment editor before filling', async () => {
  const bridge = new MarcusBrowserBridge();
  const calls = [];
  let runtimeCall = 0;
  bridge.ensureBrowser = async () => true;
  bridge.sensitiveFieldFocused = async () => false;
  bridge.page = async () => ({
    target: { id: 'skool-tab', url: 'https://www.skool.com/localgiants' },
    session: {
      send: async (method, params) => {
        calls.push({ method, params });
        if (method === 'Input.insertText') return {};
        runtimeCall += 1;
        if (runtimeCall === 1) return { result: { value: false } };
        if (runtimeCall === 2) {
          return { result: { value: {
            activated: true,
            text: "Drop Your Intro (We're Not Here to Lurk)",
            href: 'https://www.skool.com/localgiants/drop-your-intro-were-not-here-to-lurk',
          } } };
        }
        if (runtimeCall === 3) return { result: { value: { focused: false } } };
        if (runtimeCall === 4) return { result: { value: { activated: true } } };
        return { result: { value: { focused: true, tag: 'DIV', contentEditable: true, label: '' } } };
      },
    },
  });

  const result = await bridge.command({
    command: 'prepare-reply',
    thread: 'Drop Your Intro',
    text: 'MARCUS introduction draft.',
  });
  assert.equal(result.ok, true);
  assert.equal(result.details.result.movedToLatest, true);
  assert.equal(result.details.result.insertedChars, 26);
  assert.match(calls[1].params.expression, /drop your intro/i);
  assert.match(calls[3].params.expression, /jump to latest comment/i);
  assert.deepEqual(calls.at(-1), { method: 'Input.insertText', params: { text: 'MARCUS introduction draft.' } });
});

test('approved browser publication verifies the exact draft before activating Comment', async () => {
  const bridge = new MarcusBrowserBridge();
  bridge.sensitiveFieldFocused = async () => false;
  let prepared = null;
  bridge.prepareReply = async (_session, _target, input) => {
    prepared = input;
    return { insertedChars: input.text.length };
  };
  let runtimeCall = 0;
  const session = {
    send: async (method, params) => {
      assert.equal(method, 'Runtime.evaluate');
      runtimeCall += 1;
      if (runtimeCall === 1) {
        assert.match(params.expression, /actual === expected/);
        return { result: { value: { matches: true, chars: 22 } } };
      }
      assert.match(params.expression, /button\.click/);
      return { result: { value: { activated: true, label: 'COMMENT' } } };
    },
  };
  const result = await bridge.publishApprovedDraft(session, { url: 'https://www.skool.com/localgiants/thread' }, {
    publicationId: 'publication-1',
    mode: 'reply',
    thread: 'Drop Your Intro',
    text: 'MARCUS approved reply.',
    submitLabel: 'Comment',
  });
  assert.deepEqual(prepared, { thread: 'Drop Your Intro', text: 'MARCUS approved reply.' });
  assert.equal(result.published, true);
  assert.equal(result.publicationId, 'publication-1');
  assert.equal(result.submitLabel, 'COMMENT');
});

test('approved browser publication refuses to click when visible text differs', async () => {
  const bridge = new MarcusBrowserBridge();
  bridge.sensitiveFieldFocused = async () => false;
  bridge.prepareReply = async () => ({});
  const session = { send: async () => ({ result: { value: { matches: false, chars: 8 } } }) };
  await assert.rejects(() => bridge.publishApprovedDraft(session, { url: 'https://www.skool.com/localgiants/thread' }, {
    publicationId: 'publication-2', mode: 'reply', thread: 'Intro', text: 'Exact approved text.', submitLabel: 'Comment',
  }), /exactly match the approved draft/i);
});

test('MARCUS page reader scans approved viewports and restores the original position', async () => {
  const bridge = new MarcusBrowserBridge();
  bridge.sensitiveFieldFocused = async () => false;
  let visibleCall = 0;
  bridge.visiblePageText = async () => `viewport ${++visibleCall}`;
  const expressions = [];
  const session = {
    send: async (method, params) => {
      assert.equal(method, 'Runtime.evaluate');
      expressions.push(params.expression);
      if (params.expression === 'Number(window.scrollY) || 0') return { result: { value: 450 } };
      if (params.returnByValue && params.expression.includes('window.scrollBy')) {
        return { result: { value: { before: 0, after: 800, max: 800 } } };
      }
      return { result: { value: null } };
    },
  };
  const result = await bridge.readVisiblePage(session, 'https://www.skool.com/localgiants', { viewports: 4 });
  assert.equal(result.contextKind, 'skool');
  assert.equal(result.viewportsRead, 2);
  assert.match(result.visibleText, /viewport 1[\s\S]*viewport 2/);
  assert.equal(expressions.at(-1), 'window.scrollTo(0, 450)');
  await assert.rejects(() => bridge.readVisiblePage(session, 'https://example.com/private'), /outside the approved/i);
});
