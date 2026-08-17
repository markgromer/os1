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

test('compound thread reply requests open the thread before preparing the reply', () => {
  assert.equal(
    classifyMarcusBrowserIntent('Head to the Skool community and make your introduction reply in the introduction tab.'),
    'marcus_browser_prepare_reply',
  );
  assert.equal(
    classifyMarcusBrowserIntent('Open the Tanya post on Skool and draft a reply.'),
    'marcus_browser_prepare_reply',
  );
});

test('ordinary project requests do not become browser operations', () => {
  assert.equal(classifyMarcusBrowserIntent('Build the Scoop GPT settings page and deploy it.'), '');
  assert.equal(classifyMarcusBrowserIntent('Reply to the client by email.'), '');
});
