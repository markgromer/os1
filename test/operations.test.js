import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import express from 'express';

import { ApprovalPolicy } from '../marcus/approvals/approval_policy.js';
import { registerOperationsRoutes } from '../marcus/api/operations_routes.js';
import { withoutProjectExecutionDeferrals } from '../marcus/core/request_intent.js';
import { createOperationsEngine } from '../marcus/operations/operation_engine.js';
import { executeMarcusOperationTool, shouldCreateDurableOperationForRequest } from '../marcus/operations/marcus_operation_tools.js';
import { normalizeOperation, requiredVerificationPassed } from '../marcus/operations/operation_types.js';

async function withEngine(callback, options = {}) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-operations-test-'));
  const legacyByBusiness = options.legacyByBusiness || {};
  const create = () => createOperationsEngine({
    dataDir,
    getLegacyProjects: async (businessKey) => legacyByBusiness[businessKey] || [],
    queueDesktopAction: options.queueDesktopAction || null,
  });
  try {
    return await callback(create(), { dataDir, create });
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
}

test('operation normalization rejects arbitrary enum values, bounds data, and redacts secrets', () => {
  const operation = normalizeOperation({
    businessKey: '../../Other Business',
    status: 'model_says_done',
    riskLevel: 'safe-trust-me',
    objective: 'Ship safely',
    steps: [{ status: 'instant_success', type: 'shell', input: { apiKey: 'secret-value', nested: { token: 'abc123' } }, output: 'password=hunter2' }],
  });
  assert.equal(operation.businessKey, 'other-business');
  assert.equal(operation.status, 'draft');
  assert.equal(operation.riskLevel, 'low');
  assert.equal(operation.steps[0].status, 'pending');
  assert.equal(operation.steps[0].type, 'internal');
  assert.equal(operation.steps[0].input.apiKey, '[REDACTED]');
  assert.match(operation.steps[0].output, /REDACTED/);
});

test('state transitions reject restarting a completed operation', async () => {
  await withEngine(async (engine) => {
    const operation = await engine.createOperation('personal', { title: 'Done', objective: 'Done', status: 'completed' });
    await engine.store.update('personal', operation.id, (draft) => { draft.status = 'completed'; draft.completedAt = new Date().toISOString(); return draft; });
    await assert.rejects(() => engine.startOperation('personal', operation.id), /cannot start from completed/i);
  });
});

test('operation and registry stores enforce business isolation', async () => {
  await withEngine(async (engine) => {
    const personal = await engine.createOperation('personal', { title: 'Personal', objective: 'Personal' });
    const agency = await engine.createOperation('agency', { title: 'Agency', objective: 'Agency' });
    assert.equal((await engine.listOperations('personal')).length, 1);
    assert.equal((await engine.listOperations('agency')).length, 1);
    assert.equal(await engine.getOperation('agency', personal.id), null);
    assert.equal(await engine.getOperation('personal', agency.id), null);
  });
});

test('route layer keeps operation ids scoped to the selected business', async () => {
  await withEngine(async (engine) => {
    const app = express();
    app.use(express.json());
    registerOperationsRoutes(app, { engine, getBusinessKey: (req) => String(req.get('x-business-key') || 'personal') });
    const server = app.listen(0, '127.0.0.1');
    await new Promise((resolve) => server.once('listening', resolve));
    try {
      const address = server.address();
      const base = `http://127.0.0.1:${address.port}`;
      const createdResponse = await fetch(`${base}/api/operations`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'x-business-key': 'personal' },
        body: JSON.stringify({ originalRequest: 'Track an unresolved durable request', objective: 'Track it safely' }),
      });
      assert.equal(createdResponse.status, 201);
      const created = await createdResponse.json();
      const sameBusiness = await fetch(`${base}/api/operations/${created.operation.id}`, { headers: { 'x-business-key': 'personal' } });
      const otherBusiness = await fetch(`${base}/api/operations/${created.operation.id}`, { headers: { 'x-business-key': 'agency' } });
      assert.equal(sameBusiness.status, 200);
      assert.equal(otherBusiness.status, 404);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });
});

test('project resolver deterministically scores canonical names, aliases, repositories, and low confidence', async () => {
  await withEngine(async (engine) => {
    await engine.createProjectRegistryRecord('personal', {
      canonicalName: 'WARREN', aliases: ['Warren Creative Studio'], repo: { url: 'https://github.com/markgromer/warren-studio' },
    });
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Nexorda', aliases: ['Nex'] });
    const warren = await engine.resolveProject('personal', 'Fix the WARREN Creative Studio mobile layout');
    assert.equal(warren.confidence, 'high');
    assert.equal(warren.registryRecord.canonicalName, 'WARREN');
    const repo = await engine.resolveProject('personal', 'Inspect markgromer warren studio');
    assert.equal(repo.registryRecord.canonicalName, 'WARREN');
    const unknown = await engine.resolveProject('personal', 'work on a completely unknown moonbase');
    assert.equal(unknown.confidence, 'low');
    assert.equal(unknown.registryRecord, null);
  });
});

test('durable request classification excludes questions and the engine interprets objectives and mobile acceptance criteria', async () => {
  assert.equal(shouldCreateDurableOperationForRequest('How does the WARREN repository work?'), false);
  assert.equal(shouldCreateDurableOperationForRequest('Tell me what you retained. Do not audit the repository or start Codex.'), false);
  assert.equal(shouldCreateDurableOperationForRequest('Fix the WARREN mobile experience and get Codex working on the repository.'), true);
  assert.doesNotMatch(withoutProjectExecutionDeferrals('Do not audit or start Codex.'), /audit|Codex/i);
  assert.match(withoutProjectExecutionDeferrals('Keep auth intact. Do not deploy this change.'), /Do not deploy/i);
  await withEngine(async (engine) => {
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'WARREN', aliases: ['WARREN Creative Studio'] });
    const created = await engine.createFromRequest('personal', {
      originalRequest: 'The WARREN Creative Studio is still unusable on mobile. Own the problem and get Codex working on it.',
      autoPlan: true,
    });
    assert.match(created.operation.objective, /Make WARREN Creative Studio usable on mobile/i);
    assert.ok(created.operation.acceptanceCriteria.some((criterion) => /390px/.test(criterion)));
    assert.equal(created.operation.originalRequest, 'The WARREN Creative Studio is still unusable on mobile. Own the problem and get Codex working on it.');
  });
});

test('runtime policy overrides model risk and always gates high and critical actions', async () => {
  const policy = new ApprovalPolicy();
  const push = policy.classify({ provider: 'desktop', action: 'git_push', riskLevel: 'low', explicitRequest: true });
  assert.equal(push.riskLevel, 'high');
  assert.equal(push.approvalRequired, true);
  const destructive = policy.classify({ provider: 'internal', action: 'delete production data', riskLevel: 'low', configuredAutonomy: true });
  assert.equal(destructive.riskLevel, 'critical');
  assert.equal(destructive.approvalRequired, true);
  const checkpoint = policy.classify({ provider: 'approval', action: 'review checkpoint', explicitRequest: true });
  assert.equal(checkpoint.approvalRequired, true);
});

test('critical approvals require strong confirmation', async () => {
  await withEngine(async (engine) => {
    let operation = await engine.createOperation('personal', { title: 'Critical', objective: 'Critical action' });
    operation = await engine.planOperation('personal', operation.id, {
      steps: [{ title: 'Critical action', type: 'internal', provider: 'internal', toolName: 'delete production data', sequence: 1, riskLevel: 'low', input: { explicitRequest: true } }],
    });
    operation = await engine.startOperation('personal', operation.id, { runCycle: true });
    const approval = operation.approvals[0];
    assert.equal(approval.riskLevel, 'critical');
    const bypass = await executeMarcusOperationTool({
      name: 'approve_operation_step', args: { operationId: operation.id, approvalId: approval.id, strongConfirmation: true },
      engine, businessKey: 'personal', requestMessage: 'Show me the pending approval.',
    });
    assert.equal(bypass.ok, false);
    await assert.rejects(() => engine.approveOperationStep('personal', operation.id, approval.id, { message: 'approve', runCycle: false }), /strong confirmation/i);
    operation = await engine.approveOperationStep('personal', operation.id, approval.id, { message: 'I understand the irreversible risk', strongConfirmation: true, runCycle: false });
    assert.equal(operation.approvals[0].status, 'approved');
  });
});

test('runner enforces dependencies and runtime approval even when a step says approvalRequired false', async () => {
  await withEngine(async (engine) => {
    let operation = await engine.createOperation('personal', { title: 'Push', objective: 'Push changes' });
    operation = await engine.planOperation('personal', operation.id, {
      steps: [
        { title: 'Prepare', type: 'internal', provider: 'internal', toolName: 'prepare_context', sequence: 1, status: 'pending' },
        { title: 'Push', type: 'internal', provider: 'internal', toolName: 'git_push', sequence: 2, dependsOn: [], riskLevel: 'low', approvalRequired: false },
      ],
    });
    const firstId = operation.steps[0].id;
    await engine.store.update('personal', operation.id, (draft) => { draft.steps[1].dependsOn = [firstId]; return draft; });
    operation = await engine.startOperation('personal', operation.id, { runCycle: true });
    assert.equal(operation.steps[0].status, 'completed');
    assert.equal(operation.steps[1].status, 'waiting_for_approval');
    assert.equal(operation.status, 'waiting_for_approval');
    assert.equal(operation.approvals[0].riskLevel, 'high');
  });
});

test('recovery requires reconciliation for interrupted running steps without assuming completion', async () => {
  await withEngine(async (engine) => {
    const operation = await engine.store.create('personal', {
      title: 'Interrupted', objective: 'Recover', status: 'running',
      steps: [{ title: 'In flight', type: 'internal', status: 'running', sequence: 1 }],
    });
    const recovered = await engine.recovery.recoverBusiness('personal');
    assert.deepEqual(recovered, [operation.id]);
    const after = await engine.getOperation('personal', operation.id);
    assert.equal(after.status, 'recovery_required');
    assert.equal(after.steps[0].status, 'blocked');
    assert.equal(after.blockers[0].type, 'recovery_required');
    const resumed = await engine.resumeOperation('personal', operation.id, { reason: 'Provider state checked; retry is safe.', runCycle: false });
    assert.equal(resumed.status, 'queued');
    assert.equal(resumed.steps[0].status, 'ready');
    assert.equal(resumed.blockers[0].status, 'resolved');
  });
});

test('retry limits are enforced', async () => {
  await withEngine(async (engine) => {
    const operation = await engine.store.create('personal', {
      title: 'No retries', objective: 'Fail safely', status: 'failed',
      steps: [{ title: 'Failed', type: 'internal', status: 'failed', sequence: 1, attemptCount: 1, maxAttempts: 1 }],
    });
    await assert.rejects(() => engine.retryOperation('personal', operation.id, { runCycle: false }), /retry limit reached/i);
  });
});

test('completion is blocked by failed verification', async () => {
  await withEngine(async (engine) => {
    const operation = await engine.store.create('personal', {
      title: 'Verify', objective: 'Verify', status: 'verifying',
      steps: [{ title: 'Done', type: 'verification', status: 'completed', sequence: 1 }],
      verification: [{ type: 'test', status: 'failed', required: true }],
    });
    assert.equal(requiredVerificationPassed(operation), false);
    await assert.rejects(() => engine.completeOperation('personal', operation.id), /verification has not passed/i);
    const verificationId = operation.verification[0].id;
    const waived = await engine.waiveVerification('personal', operation.id, verificationId, { actor: 'mark', reason: 'Known CI outage; Mark manually reviewed the evidence.' });
    assert.equal(waived.verification[0].waived, true);
    assert.match(waived.approvals[0].action, /^waive_verification:/);
    const completed = await engine.completeOperation('personal', operation.id);
    assert.equal(completed.status, 'completed');
  });
});

test('external Codex handoff lifecycle is durable and only completes after registered verification', async () => {
  const legacyByBusiness = {
    personal: [{
      id: 'warren-project', name: 'WARREN', repoUrl: 'https://github.com/markgromer/warren-studio',
      workspacePath: 'C:\\Work\\WARREN', status: 'Active',
    }],
  };
  await withEngine(async (engine, { create }) => {
    const created = await engine.createFromRequest('personal', {
      originalRequest: 'The WARREN Creative Studio is still unusable on mobile. Own the problem and get Codex working on it.',
      objective: 'Make the WARREN Creative Studio usable on mobile.',
      autoPlan: true,
      autoStart: true,
    });
    let operation = created.operation;
    assert.equal(created.resolution.confidence, 'high');
    assert.equal(operation.status, 'blocked');
    assert.equal(operation.steps.find((step) => step.type === 'codex').status, 'blocked');
    assert.ok(operation.artifacts.some((artifact) => artifact.type === 'codex_handoff'));
    assert.match(operation.blockers[0].message, /no direct Codex launch API|external Codex/i);

    const reloaded = create();
    operation = await reloaded.getOperation('personal', operation.id);
    assert.equal(operation.status, 'blocked');
    assert.ok(operation.artifacts.some((artifact) => artifact.type === 'codex_handoff'));

    const verificationStep = operation.steps.find((step) => step.type === 'verification');
    operation = await reloaded.registerExternalCodexJob('personal', operation.id, {
      jobId: 'codex-run-123', status: 'completed', branch: 'codex/mobile-fix', commit: 'abc123',
      diffSummary: 'Responsive layout fixes.', result: 'Implementation finished.',
      verificationResults: [
        { type: 'artifact_present', status: 'passed', required: true, stepId: verificationStep.id, evidence: { commit: 'abc123' } },
        { type: 'diff_review', status: 'passed', required: true, stepId: verificationStep.id, evidence: { reviewedBy: 'Mark' } },
        { type: 'manual_review', status: 'passed', required: true, stepId: verificationStep.id, evidence: { reviewedBy: 'Mark' } },
      ],
    });
    assert.equal(operation.status, 'queued');
    operation = await reloaded.tick('personal', operation.id);
    assert.equal(operation.status, 'blocked');
    assert.ok(operation.artifacts.some((artifact) => artifact.type === 'untrusted_codex_verification_claim'));
    assert.equal(operation.verification.some((result) => ['diff_review', 'manual_review'].includes(result.type) && result.status === 'passed'), false);
    operation = await reloaded.registerManualVerificationEvidence('personal', operation.id, [
      { type: 'diff_review', status: 'passed', note: 'Mark reviewed the attached Codex diff and confirmed it matches the scoped request.' },
      { type: 'manual_review', status: 'passed', note: 'Mark manually reviewed the implementation evidence and acceptance criteria.' },
    ], { actor: 'authenticated_operator' });
    operation = await reloaded.tick('personal', operation.id);
    assert.equal(operation.status, 'completed');
    assert.ok(operation.completedAt);
    assert.equal(requiredVerificationPassed(operation), true);
  }, { legacyByBusiness });
});

test('operation persistence survives a fresh engine instance', async () => {
  await withEngine(async (engine, { create }) => {
    const operation = await engine.createOperation('personal', { title: 'Persist me', objective: 'Persist me', originalRequest: 'Persist' });
    const reloaded = create();
    const found = await reloaded.getOperation('personal', operation.id);
    assert.equal(found.id, operation.id);
    assert.equal(found.originalRequest, 'Persist');
  });
});

test('serialized concurrent writes preserve every operation', async () => {
  await withEngine(async (engine) => {
    await Promise.all(Array.from({ length: 12 }, (_, index) => engine.createOperation('personal', {
      title: `Concurrent ${index}`,
      objective: `Preserve write ${index}`,
    })));
    const operations = await engine.listOperations('personal');
    assert.equal(operations.length, 12);
    assert.equal(new Set(operations.map((operation) => operation.id)).size, 12);
  });
});

test('a corrupt primary operation file recovers from its last valid backup and preserves the corrupt file', async () => {
  await withEngine(async (engine, { dataDir }) => {
    const first = await engine.createOperation('personal', { title: 'First durable state', objective: 'Preserve me' });
    await engine.createOperation('personal', { title: 'Second durable state', objective: 'Create a backup of the first state' });
    const file = path.join(dataDir, 'businesses', 'personal', 'operations.json');
    await fs.writeFile(file, '{not-valid-json', 'utf8');
    const recovered = await engine.getOperation('personal', first.id);
    assert.equal(recovered.id, first.id);
    const files = await fs.readdir(path.dirname(file));
    assert.ok(files.some((name) => name.startsWith('operations.json.corrupt-')));
  });
});
