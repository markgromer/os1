import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { formatJobPrimingManifest, selectJobPriming } from '../marcus/jobs/job_priming.js';
import { assessLockedDecisionConflict } from '../marcus/memory/locked_decisions.js';
import { WinningMethodStore } from '../marcus/memory/winning_method_store.js';
import { buildVoiceContinuityBrief } from '../marcus/voice/continuity_brief.js';
import { assessPostInterruptionAlignment } from '../marcus/voice/conversation_alignment.js';
import { containsSpokenMachineReference, sanitizeOperatorResultForSpeech } from '../marcus/voice/spoken_reference.js';

test('operator results retain machine evidence outside the speech-safe projection', () => {
  const raw = { ok: true, operationId: 'op_123456789', project: { name: 'Reggie', id: 'project_123456789' }, reply: 'PR #1847 is at https://example.test/pull/1847 for operation op_123456789.' };
  const spoken = sanitizeOperatorResultForSpeech(raw);
  assert.equal(raw.operationId, 'op_123456789');
  assert.equal(containsSpokenMachineReference(spoken), false);
  assert.match(spoken.reply, /Reggie pull request/i);
  assert.doesNotMatch(spoken.reply, /Operation Reggie|ready at Reggie/i);
});

test('voice continuity is bounded and suppresses private exchanges in public modes', () => {
  const input = { memories: [{ status: 'active', kind: 'preference', title: 'Relationship', content: 'Use earned teasing and fierce loyalty.' }], conversation: { activeProject: { name: 'Reggie' }, messages: [{ role: 'user', content: 'private running joke' }] } };
  assert.match(buildVoiceContinuityBrief(input), /private running joke/);
  assert.doesNotMatch(buildVoiceContinuityBrief({ ...input, personalityMode: 'public_assistant' }), /private running joke|earned teasing/);
});

test('job priming routes recurring work to a narrow context manifest', () => {
  assert.equal(selectJobPriming('Deploy the reviewed release').id, 'release_verification');
  assert.match(formatJobPrimingManifest('Write the PoopSites landing page copy'), /marketing_copy/);
  assert.match(formatJobPrimingManifest('Fix the mobile flow'), /Exclude unrelated project history/);
});

test('locked decision conflicts stop for permanent-change or exception clarification', () => {
  const conflict = assessLockedDecisionConflict('Bypass direct deployment authority for realtime voice', [{ id: 'mem_locked_123', status: 'active', kind: 'decision', title: '[Locked] Voice deployment authority', content: '[LOCKED] Realtime voice has no direct deployment authority.' }]);
  assert.equal(conflict.status, 'locked_decision_conflict');
  assert.match(conflict.reply, /permanent change or a one-time exception/i);
  assert.equal(assessLockedDecisionConflict('Explain the voice deployment authority', [{ status: 'active', kind: 'decision', title: '[Locked] Voice deployment authority', content: '[LOCKED] No direct deployment.' }]), null);
});

test('winning methods are stored only for recovered operations that complete', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-method-'));
  const store = new WinningMethodStore({ dataDir });
  assert.equal(await store.recordRecoveredOperation('personal', { status: 'completed', activityLog: [], title: 'Clean run' }), null);
  const saved = await store.recordRecoveredOperation('personal', { status: 'completed', projectRegistryId: 'reggie', title: 'Deploy Reggie', completedAt: '2026-08-17T00:00:00.000Z', activityLog: [{ type: 'operation_failed', message: 'Generic status polling failed.' }, { type: 'operation_completed', message: 'Verified through provider read-back.' }], verification: [{ type: 'url_health', status: 'passed' }] });
  assert.match(saved.deadEnd, /polling failed/);
  assert.match(saved.winningMethod, /provider read-back/);
  assert.equal((await store.list('personal')).length, 1);
});

test('semantic interruption audit rejects a stale answer and accepts the redirected turn', () => {
  const stale = assessPostInterruptionAlignment({ interruptedRequest: 'Audit Reggie font rendering', nextRequest: 'Actually check Marcus voice latency', answer: 'The Reggie font rendering is fixed.' });
  const aligned = assessPostInterruptionAlignment({ interruptedRequest: 'Audit Reggie font rendering', nextRequest: 'Actually check Marcus voice latency', answer: 'Marcus voice latency is healthy.' });
  assert.equal(stale.aligned, false);
  assert.equal(aligned.aligned, true);
});
