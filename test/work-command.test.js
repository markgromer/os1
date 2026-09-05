import test from 'node:test';
import assert from 'node:assert/strict';
import { isWorkStatusCommand, buildWorkStatusResponse } from '../marcus/work/work_command.js';

test('work status intents never swallow mixed commands or execution requests', () => {
  for (const message of ['Show tracked work status.', 'What needs me?', 'What can continue?', 'What changed while I was away?']) assert.equal(isWorkStatusCommand(message), true);
  for (const message of ['Deploy the project', 'What needs me? Also deploy it.', 'Create a task to show tracked work status.', 'Show my action queue.']) assert.equal(isWorkStatusCommand(message), false);
});

test('the Command feedback response preserves empty coverage, authority, and evidence', () => {
  const empty = { trackedWorkCount: 0, needsMark: [], canContinue: [], anomalies: [], opportunities: [], uncertainty: ['No work items are tracked.'], away: { changes: [], truncated: false }, observedAt: '2026-09-04T00:00:00Z' };
  const response = buildWorkStatusResponse(empty, { items: [] });
  assert.match(response.reply, /No work items are tracked/);
  assert.match(response.reply, /read-only/);
  assert.deepEqual(response.suggestedActions, []);
  assert.equal(response.plainText, true);
  const populated = buildWorkStatusResponse({ ...empty, trackedWorkCount: 1, uncertainty: [], opportunities: [{ id: 'work-1', reason: 'Owner start required.', evidence: [{ type: 'work', id: 'work-1' }] }] }, { items: [{ id: 'work-1', objective: 'Review feedback' }] });
  assert.match(populated.reply, /Ready, not authorized to advance: 1/);
  assert.equal(populated.cards[0].title, 'Review feedback');
  assert.equal(populated.cards[0].readOnly, true);
  assert.deepEqual(populated.cards[0].evidence, [{ type: 'work', id: 'work-1' }]);
});
