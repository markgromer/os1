import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createMarcusMobileOperationTracker,
  formatOperationTransition,
  selectTrackedOperation,
  shouldAnnounceOperationTransition,
  toMobileOperationSummary,
} from '../client/mobile-operation-tracker.js';

function memoryStorage(initial = {}) {
  const values = new Map(Object.entries(initial));
  return {
    getItem: (key) => values.get(key) || null,
    setItem: (key, value) => values.set(key, value),
  };
}

test('mobile operation summary exposes progress without audit or prompt data', () => {
  const summary = toMobileOperationSummary({
    id: 'op1', projectName: 'Reggie', title: 'Add setup modal', status: 'running', currentStepId: 'step2',
    artifacts: [{ content: 'secret source excerpt' }], metadata: { codexPrompt: 'private prompt' },
    approvals: [{ id: 'approval1', action: 'deploy_worker_version', riskLevel: 'high', reason: 'Exact version deployment requires approval.', status: 'pending', secret: 'do-not-return' }],
    steps: [
      { id: 'step1', title: 'Audit', type: 'internal', status: 'completed' },
      { id: 'step2', title: 'Implement with Codex', type: 'codex', status: 'running' },
      { id: 'step3', title: 'Verify', type: 'verification', status: 'pending' },
    ],
    verification: [{ type: 'test', status: 'pending', required: true }],
  });
  assert.equal(summary.currentStep.title, 'Implement with Codex');
  assert.deepEqual(summary.progress, { completed: 1, total: 3, percent: 33 });
  assert.deepEqual(summary.verificationSummary, { required: 1, passed: 0, failed: 0, needsManualReview: 0, pending: 1 });
  assert.equal(Object.hasOwn(summary, 'artifacts'), false);
  assert.equal(Object.hasOwn(summary, 'metadata'), false);
  assert.deepEqual(summary.pendingApproval, {
    id: 'approval1', action: 'deploy_worker_version', riskLevel: 'high', reason: 'Exact version deployment requires approval.', expiresAt: '',
  });
  assert.equal(Object.hasOwn(summary.pendingApproval, 'secret'), false);
});

test('mobile tracker emits each persisted state transition once and follows newer active work', async () => {
  const operation = {
    id: 'op1', projectName: 'Reggie', status: 'awaiting_provider', updatedAt: '2026-08-12T10:00:00Z',
    progress: { completed: 1, total: 3, percent: 33 },
    currentStep: { title: 'Implement with Codex', type: 'codex', status: 'running' },
    verificationSummary: { required: 0, passed: 0, failed: 0, needsManualReview: 0 },
  };
  let payload = { operations: [operation] };
  const transitions = [];
  const rendered = [];
  const tracker = createMarcusMobileOperationTracker({
    loadSummaries: async () => payload,
    onRender: (item) => rendered.push(item?.status || 'none'),
    onTransition: (item) => transitions.push(item.status),
    storage: memoryStorage(),
    timers: { setTimeout: () => 1, clearTimeout: () => {} },
  });
  await tracker.start();
  await tracker.refresh();
  assert.deepEqual(transitions, []);
  payload = { operations: [{ ...operation, status: 'verifying', progress: { completed: 2, total: 3, percent: 67 }, currentStep: { title: 'Verify', type: 'verification', status: 'running' } }] };
  await tracker.refresh();
  await tracker.refresh();
  assert.deepEqual(transitions, ['verifying']);
  payload = { operations: [{ ...payload.operations[0], status: 'completed', progress: { completed: 3, total: 3, percent: 100 }, currentStep: { title: 'Verify', type: 'verification', status: 'completed' }, verificationSummary: { required: 3, passed: 3, failed: 0, needsManualReview: 0 } }] };
  await tracker.refresh();
  assert.deepEqual(transitions, ['verifying', 'completed']);
  assert.match(formatOperationTransition(payload.operations[0]), /completed with persisted verification/i);
  assert.equal(shouldAnnounceOperationTransition(payload.operations[0]), true);
  assert.equal(shouldAnnounceOperationTransition({ ...payload.operations[0], status: 'running' }), false);
  payload = { operations: [{ id: 'op2', projectName: 'Freedom Scoopers', status: 'running', progress: { completed: 0, total: 3 } }, payload.operations[0]] };
  const selected = selectTrackedOperation(payload.operations, 'op1');
  assert.equal(selected.id, 'op2');
  assert.ok(rendered.includes('completed'));
});
