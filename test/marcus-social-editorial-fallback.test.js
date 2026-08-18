import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeMarcusSocialDraft } from '../marcus/social/editorial_quality.js';
import { buildMarcusTestObservationDraft } from '../marcus/social/editorial_fallback.js';

test('MARCUS builds a source-faithful draft from an unverified live lead test', () => {
  const observation = {
    member: { displayName: 'Jeremy Casanave' },
    sourceTitle: 'Speed to Lead Testers Needed!',
    contentSummary: 'ScooPilot is testing an AI response to Facebook Ads leads within 30 seconds. It has been tested privately, but not live.',
  };
  const draft = buildMarcusTestObservationDraft(observation);

  assert.equal(draft.title, "ScooPilot's 30-second promise needs live traffic");
  assert.match(draft.text, /not against live ad traffic yet/i);
  assert.doesNotMatch(draft.text, /delivers|shipped|proven/i);
  const quality = analyzeMarcusSocialDraft({
    ...draft,
    sourceObservationIds: ['obs_jeremy'],
    sourceObservations: [observation],
  });
  assert.deepEqual(quality.issues, []);
});

test('MARCUS does not manufacture the live-test fallback for unrelated observations', () => {
  assert.equal(buildMarcusTestObservationDraft({
    member: { displayName: 'Patty Shoults' },
    sourceTitle: 'Claude',
    contentSummary: 'Patty spent 90 minutes cleaning Gmail with Claude.',
  }), null);
});
