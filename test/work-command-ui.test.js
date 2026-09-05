import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';

const app = await fs.readFile(new URL('../public/app.js', import.meta.url), 'utf8');
const source = app.slice(app.indexOf('function renderWorkStatusReadout('), app.indexOf('function renderCommandSurface('));
const render = vm.runInNewContext(`${source}; renderWorkStatusReadout`, {
  safeText: (value) => String(value || ''),
  escapeHtml: (value) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;'),
});

test('Command work status is visible without a chat stream, escaped and business-scoped', () => {
  const readout = { businessKey: 'personal', observedAt: '2026-09-04T00:00:00Z', reply: '<b>Feedback fixture</b>\nReady, not authorized to advance: 1' };
  const html = render(readout, 'personal');
  assert.match(html, /role="status"/);
  assert.match(html, /&lt;b&gt;Feedback fixture&lt;\/b&gt;/);
  assert.match(html, /Ready, not authorized to advance: 1/);
  assert.doesNotMatch(html, /<b>|data-command=|data-signal|data-action/);
  assert.equal(render(readout, 'other-business'), '');
  assert.equal(render(null, 'personal'), '');
  assert.match(app, /renderWorkStatusReadout\(state\.workStatusReadout, normalizeBusinessKey\(state\.activeBusinessKey\)\)/);
});

test('Command ignores an in-flight work status response after switching businesses', async () => {
  const start = app.indexOf('async function sendOperationalCommand(');
  const commandSource = app.slice(start, app.indexOf('function recordChatMessage(', start));
  const ui = { activeBusinessKey: 'personal', workStatusReadout: null };
  const actions = [];
  let typingCleared = 0;
  let respond;
  const command = vm.runInNewContext(`${commandSource}; sendOperationalCommand`, {
    state: ui, safeText: (value) => String(value || ''), normalizeBusinessKey: (value) => value,
    getStoredBusinessKey: () => ui.activeBusinessKey,
    stopMarcusSpeech() {}, recordChatMessage: (role) => actions.push(role), addChatMessage() {},
    document: { getElementById: () => null }, setMarcusPresence() {}, showMarcusTypingIndicator() {}, removeMarcusTypingIndicator() { typingCleared++; },
    apiJson: () => new Promise((resolve) => { respond = resolve; }),
    renderMain: () => actions.push('render'), speakMarcus: () => actions.push('speak'),
  });
  const pending = command('Show tracked work status.');
  ui.activeBusinessKey = 'other-business';
  respond({ ok: true, intent: 'work_status', reply: 'Private first-business result' });
  await pending;
  assert.equal(ui.workStatusReadout, null);
  assert.deepEqual(actions, ['user']);
  assert.equal(typingCleared, 1);
});
