import test from 'node:test';
import assert from 'node:assert/strict';

import {
  classifyMarcusBrowserIntent,
  resolveMarcusBrowserFollowupIntent,
  validateMarcusIntroductionDraft,
} from '../marcus/browser_intent.js';

test('Skool inspection requests route to the live browser instead of project work', () => {
  assert.equal(
    classifyMarcusBrowserIntent('Look through the ScoopOS group on Skool and give me feedback on which type of post we should make.'),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent("Why aren't you looking at the browser window on your connected app?"),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent('go to the main feed and check out the other posts, read a dozen or so, including comments, and start to think about future posts', { contextKind: 'skool' }),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent('no, the main feed on scoopos on skool'),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent('im switched your browser to the main feed', { contextKind: 'skool' }),
    'marcus_browser_read',
  );
  assert.equal(
    classifyMarcusBrowserIntent('open each, read all the comments, click read more ALWAYS', { contextKind: 'skool' }),
    'marcus_browser_read',
  );
});

test('browser composition is prepared separately from explicitly approved submission', () => {
  assert.equal(
    classifyMarcusBrowserIntent('Write a Skool reply thanking Tanya for the commercial account question.'),
    'marcus_browser_fill',
  );
  assert.equal(
    classifyMarcusBrowserIntent('draft your first post', { contextKind: 'skool' }),
    'marcus_browser_prepare_post',
  );
  assert.equal(
    classifyMarcusBrowserIntent('draft your first standalone post', { contextKind: 'skool' }),
    'marcus_browser_prepare_post',
  );
  assert.equal(
    classifyMarcusBrowserIntent('create the post', { contextKind: 'skool' }),
    'marcus_browser_prepare_post',
  );
  assert.equal(
    classifyMarcusBrowserIntent('Use your browser to draft your first post on the ScoopOS main feed on Skool.'),
    'marcus_browser_prepare_post',
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
  assert.equal(
    classifyMarcusBrowserIntent('Head to the Skool community and prepare your introduction reply in the introduction thread. Do not post it.'),
    'marcus_browser_prepare_reply',
  );
  assert.equal(
    classifyMarcusBrowserIntent('Make a reply post to the thread.', { contextKind: 'skool' }),
    'marcus_browser_prepare_reply',
  );
});

test('submission negation never becomes browser publication approval', () => {
  assert.notEqual(
    classifyMarcusBrowserIntent('Prepare the Skool reply, but do not post it.', { pendingDraft: true }),
    'marcus_browser_submit',
  );
  assert.notEqual(
    classifyMarcusBrowserIntent("Draft the Skool comment without publishing it.", { pendingDraft: true }),
    'marcus_browser_submit',
  );
});

test('MARCUS public introduction drafts cannot impersonate Mark or hide their AI identity', () => {
  assert.deepEqual(
    validateMarcusIntroductionDraft("Hi everyone, I'm Mark.", {
      requestMessage: 'Prepare your introduction reply on Skool.',
    }),
    { ok: false, error: 'MARCUS cannot introduce himself as Mark.' },
  );
  assert.deepEqual(
    validateMarcusIntroductionDraft("Hi, I'm MARCUS and I help with projects.", {
      requestMessage: 'Have MARCUS introduce himself on Skool.',
    }),
    { ok: false, error: 'MARCUS must identify himself openly as AI in his introduction.' },
  );
  assert.deepEqual(
    validateMarcusIntroductionDraft("Hi, I'm MARCUS, Mark's AI Chief of Staff.", {
      requestMessage: 'Prepare your introduction reply on Skool.',
    }),
    { ok: true },
  );
  assert.deepEqual(
    validateMarcusIntroductionDraft("Hi, I'm Mark.", {
      requestMessage: 'Help me write my introduction on Skool.',
    }),
    { ok: true },
  );
});

test('ordinary project requests do not become browser operations', () => {
  assert.equal(classifyMarcusBrowserIntent('Build the Scoop GPT settings page and deploy it.'), '');
  assert.equal(classifyMarcusBrowserIntent('Reply to the client by email.'), '');
});

test('browser follow-up approvals stay with the visible browser instead of approving Codex work', () => {
  const recentMessages = [
    {
      role: 'assistant',
      content: 'The ScoopOS Skool main feed is open now in the MARCUS browser. I can start reading through about a dozen recent posts and their comments to get a feel for content trends and conversations. Shall I summarize what I see and propose some future post ideas?',
    },
  ];
  assert.equal(resolveMarcusBrowserFollowupIntent('yes', recentMessages, { contextKind: 'skool' }), 'marcus_browser_read');
  assert.equal(resolveMarcusBrowserFollowupIntent('yes, i approve', recentMessages, { contextKind: 'skool' }), 'marcus_browser_read');
  assert.equal(resolveMarcusBrowserFollowupIntent('do it', recentMessages, { contextKind: 'skool' }), 'marcus_browser_read');
  assert.equal(
    resolveMarcusBrowserFollowupIntent('yes, do it, its MARCUS account you are logged in with', recentMessages, { contextKind: 'skool' }),
    'marcus_browser_read',
  );
});

test('browser follow-up resolver preserves publication safeguards', () => {
  const recentMessages = [
    { role: 'assistant', content: 'I prepared the Skool reply in the browser. Nothing has been posted.' },
  ];
  assert.equal(resolveMarcusBrowserFollowupIntent('Post it', recentMessages, { pendingDraft: true, contextKind: 'skool' }), 'marcus_browser_submit');
  assert.equal(resolveMarcusBrowserFollowupIntent('approve and send the email', recentMessages, { pendingDraft: true, contextKind: 'gmail' }), '');
});
