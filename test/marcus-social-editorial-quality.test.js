import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeMarcusSocialDraft } from '../marcus/social/editorial_quality.js';

const grounded = {
  sourceObservationIds: ['obs_source_123'],
  editorialAngle: 'Owners are not short on automation ideas; they are short on trustworthy handoffs between tools.',
  readerValue: 'The distinction gives operators a better way to choose which automation deserves attention first.',
  sourceObservations: [{
    member: { displayName: 'Jeremy Casanave' },
    sourceTitle: 'Speed to Lead Testers Needed',
    contentSummary: 'Jeremy is testing ScooPilot with Facebook Ads and promises an AI response to new leads within 30 seconds.',
  }],
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
    text: 'Jeremy is testing ScooPilot against live Facebook Ads with a 30-second lead response. That is the useful part: a claim tied to a clock and real traffic.\n\nThe test matters more than the AI label. If it misses the handoff under live demand, the polished demo was never the product.',
  });

  assert.deepEqual(result.issues, []);
  assert.equal(result.ok, true);
});

test('MARCUS social quality rejects abstract ChatGPT business prose even when sources are cited', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    title: 'Building Real Business Systems, Not Just Polished Surfaces',
    text: "I've noticed a recurring tension between polished surface-level fixes and scalable business systems.\n\nThink of AI as a strategic enabler, not a magic wand. I will continue sharing what works under the hood.",
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('generic-observation-opener'));
  assert.ok(result.issues.includes('generic-ai-metaphor'));
  assert.ok(result.issues.includes('generic-strategy-language'));
  assert.ok(result.issues.includes('weak-source-grounding'));
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
