import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeMarcusSocialDraft, normalizeMarcusSocialDraftText } from '../marcus/social/editorial_quality.js';

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
    title: "ScooPilot's 30-second claim needs live traffic",
    text: "Jeremy is testing ScooPilot against live Facebook Ads with a 30-second lead response. That is the useful part: a claim tied to a clock and real traffic.\n\nThe test matters more than the AI label. If it misses the handoff under live demand, the polished demo was never the product.\n\nI'm MARCUS, Mark's AI chief of staff. I watch for the point where speed becomes customer-facing risk.",
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

test('MARCUS social quality rejects loose overlap that never names the cited source', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    sourceObservations: [{
      member: { displayName: 'Mark Gromer' },
      sourceTitle: 'Access is not ownership',
      contentSummary: 'A copied landing page retained technical fingerprints from a larger Next.js application.',
    }],
    title: 'Fast Fixes Don’t Build Reliable Automation',
    text: "Hi, I’m MARCUS, Mark’s AI chief of staff.\n\nI see a clear tension: quick automation wins get attention but often do not last.\n\nMark’s experience shows real automation means building solid workflows.\n\nIf you keep patching problems, it is time to build a foundation.",
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('missing-distinctive-source-anchor'));
  assert.ok(result.issues.includes('generic-tension-opener'));
  assert.ok(result.issues.includes('vague-attribution'));
});

test('MARCUS social quality rejects evidence wrapped in generic thought leadership', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    sourceObservationIds: ['obs_jeremy', 'obs_patty'],
    sourceObservations: [
      grounded.sourceObservations[0],
      {
        member: { displayName: 'Patty Shoults' },
        sourceTitle: 'Claude',
        contentSummary: 'Patty spent 90 minutes using Claude to clean up Gmail.',
      },
    ],
    title: 'Balancing Speed and Depth in Automation: Lessons from ScooPilot and Claude',
    text: "I'm MARCUS, Mark's AI chief of staff. Jeremy's ScooPilot demo showed AI answering leads in 30 seconds. Patty's Claude cleanup took 90 minutes.\n\nThat's the real choice: quick wins vs thorough upkeep.\n\nAutomation isn't about instant fixes alone. It's about backing fast moves with steady follow-up.\n\nMark's AI experience proves that speed without cleanup leads to chaos. Lasting results need both.\n\nFocus on blending fast action with organized process, not just flashy tools.",
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('generic-balanced-title'));
  assert.ok(result.issues.includes('generic-summary-transition'));
  assert.ok(result.issues.includes('unsupported-mark-attribution'));
  assert.ok(result.issues.includes('boilerplate-identity-opener'));
  assert.ok(result.issues.includes('missing-sharp-position'));
});

test('MARCUS social quality rejects symmetric slogans even with concrete sources', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    sourceObservationIds: ['obs_jeremy', 'obs_patty'],
    sourceObservations: [
      grounded.sourceObservations[0],
      {
        member: { displayName: 'Patty Shoults' },
        sourceTitle: 'Claude',
        contentSummary: 'Patty spent 90 minutes using Claude to clean up Gmail.',
      },
    ],
    title: 'Speed vs Patience: The Automation Balancing Act',
    text: "Jeremy's ScooPilot answered leads in 30 seconds. Patty spent 90 minutes cleaning Gmail with Claude.\n\nFast leads win business. Slow cleanup stops chaos.\n\nAs Mark's AI chief of staff, I see too many chase speed without cleanup.\n\nAutomation needs steady care. Balancing sprint and steady pace keeps operations smooth and customers happy.",
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('generic-balanced-title'));
  assert.ok(result.issues.includes('missing-sharp-position'));
  assert.ok(result.issues.includes('missing-public-ai-identity'));
});

test('MARCUS social quality rejects invented causality between unrelated observations', () => {
  const result = analyzeMarcusSocialDraft({
    ...grounded,
    sourceObservationIds: ['obs_jeremy', 'obs_patty'],
    sourceObservations: [
      grounded.sourceObservations[0],
      {
        member: { displayName: 'Patty Shoults' },
        sourceTitle: 'Claude',
        contentSummary: 'Patty spent 90 minutes using Claude to clean up Gmail.',
      },
    ],
    title: "Jeremy's 30-second lead response AI demands 90-minute cleanup",
    text: "Jeremy's ScooPilot answered a lead in 30 seconds. Patty used Claude to spend 90 minutes cleaning up Gmail.\n\nI'm MARCUS, Mark's AI chief of staff.\n\nSpeed without cleanup creates backlog risk and unpredictable failures.\n\nFast AI does not give permission to skip patient follow-up work.",
  });

  assert.equal(result.ok, false);
  assert.ok(result.issues.includes('unsupported-cross-source-causality'));
  assert.ok(result.issues.includes('identity-as-filler-paragraph'));
});

test('MARCUS social draft normalization folds a short identity line into the argument', () => {
  assert.equal(
    normalizeMarcusSocialDraftText("Jeremy tested ScooPilot against live leads.\n\nI'm MARCUS, Mark's AI chief of staff.\n\nCustomer-facing AI deserves more supervision."),
    "Jeremy tested ScooPilot against live leads.\n\nI'm MARCUS, Mark's AI chief of staff. Customer-facing AI deserves more supervision.",
  );
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
