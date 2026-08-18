import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeMarcusSocialDraft } from '../marcus/social/editorial_quality.js';

const grounded = {
  sourceObservationIds: ['obs_source_123'],
  editorialAngle: 'Owners are not short on automation ideas; they are short on trustworthy handoffs between tools.',
  readerValue: 'The distinction gives operators a better way to choose which automation deserves attention first.',
};

test('MARCUS social quality rejects reusable engagement bait', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    title: 'Which handoff costs you the most time?',
    text: 'Running a service business is hard.\n\nVote for the handoff that costs your business the most time. I will take the top answer and share a workflow.',
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('generic-ranking-question'));
  assert.ok(result.issues.includes('engagement-bait-vote'));
  assert.ok(result.issues.includes('engagement-bait-followup'));
});

test('MARCUS social quality requires source evidence, an angle, and standalone reader value', () => {
  const result = analyzeMarcusSocialDraft({
    title: 'The software was not the bottleneck',
    text: 'Three owners described buying another tool.\n\nThe failure they described was the handoff between the tools they already had.',
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('missing-source-observation'));
  assert.ok(result.issues.includes('missing-editorial-angle'));
  assert.ok(result.issues.includes('missing-reader-value'));
});

test('MARCUS social quality accepts a grounded point of view without an engagement prompt', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    title: 'You probably do not need another app',
    text: 'I watched three ScoopOS conversations arrive at the same answer from different directions: buy or build one more tool.\n\nThe work was not stuck inside any tool. It was stuck in the handoff between the quote, the route, and the follow-up. That is a less exciting problem, which is probably why it survives longer.',
  });

  assert.deepEqual(result.issues, []);
  assert.equal(result.ok, true);
});

test('MARCUS social quality rejects hard-to-read sentence construction', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    title: 'The handoff is the product',
    text: `This sentence keeps going because it tries to explain the lead, the quote, the route, the follow-up, the customer, the technician, the software, the spreadsheet, the inbox, and every possible implication before giving the reader a place to breathe or a reason to care about the point.\n\nShort ending.`,
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('sentence-too-long'));
});
