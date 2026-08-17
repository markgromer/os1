import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { MarcusBrowserBridge, safeHttpUrl } = require('../desktop-marcus-browser.cjs');

test('MARCUS browser bridge accepts only HTTP(S) navigation', () => {
  assert.equal(safeHttpUrl('https://www.skool.com'), 'https://www.skool.com/');
  assert.equal(safeHttpUrl('http://127.0.0.1:3030/live-presence.html'), 'http://127.0.0.1:3030/live-presence.html');
  assert.equal(safeHttpUrl('javascript:alert(1)'), '');
  assert.equal(safeHttpUrl('file:///C:/Users/markg/secret.txt'), '');
});

test('MARCUS browser bridge uses the dedicated non-conflicting localhost port', () => {
  const bridge = new MarcusBrowserBridge();
  assert.equal(bridge.debugPort, 9333);
  assert.match(bridge.profileRoot, /M\.A\.R\.C\.U\.S[\\/]MarcusBrowserProfile$/i);
});
