import test from 'node:test';
import assert from 'node:assert/strict';

import { classifyMarcusBrowserIntent } from '../marcus/browser_intent.js';

test('Skool inspection requests route to the live browser instead of project work', () => {
  assert.equal(
    classifyMarcusBrowserIntent('Look through the ScoopOS group on Skool and give me feedback on which type of post we should make.'),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent("Why aren't you looking at the browser window on your connected app?"),
    'marcus_browser_read',
  );
});

test('browser composition is prepared separately from explicitly approved submission', () => {
  assert.equal(
    classifyMarcusBrowserIntent('Write a Skool reply thanking Tanya for the commercial account question.'),
    'marcus_browser_fill',
  );
  assert.equal(classifyMarcusBrowserIntent('Post it', { pendingDraft: false }), '');
  assert.equal(classifyMarcusBrowserIntent('Post it', { pendingDraft: true }), 'marcus_browser_submit');
  assert.equal(classifyMarcusBrowserIntent('Approve and send the email', { pendingDraft: true }), '');
});

test('ordinary project requests do not become browser operations', () => {
  assert.equal(classifyMarcusBrowserIntent('Build the Scoop GPT settings page and deploy it.'), '');
  assert.equal(classifyMarcusBrowserIntent('Reply to the client by email.'), '');
});
