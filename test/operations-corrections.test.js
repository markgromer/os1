import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { createOperationsEngine } from '../marcus/operations/operation_engine.js';
import { discoverDurableBackupSources } from '../marcus/operations/operation_backups.js';
import { messageHasExplicitPublishApproval } from '../marcus/approvals/publish_safeguard.js';
import { validateAllowedWorkspaceRoots, validateTrustedWorkspace } from '../marcus/projects/workspace_trust.js';

async function withEngine(callback, options = {}) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-corrections-'));
  const create = () => createOperationsEngine({
    dataDir,
    getLegacyProjects: async (businessKey) => options.legacyByBusiness?.[businessKey] || [],
    queueDesktopAction: options.queueDesktopAction || null,
    directCodexAdapter: options.directCodexAdapter || null,
    githubReadAdapter: options.githubReadAdapter || null,
    providerTimeoutMs: options.providerTimeoutMs || 45_000,
  });
  try { return await callback(create(), { create, dataDir }); }
  finally { await fs.rm(dataDir, { recursive: true, force: true }); }
}

test('creation APIs issue identities and times while rehydration preserves stored records', async () => {
  await withEngine(async (engine) => {
    const forged = await engine.createOperation('personal', {
      id: 'op_forged', createdAt: '2000-01-01T00:00:00.000Z', updatedAt: '2000-01-01T00:00:00.000Z',
      title: 'Identity', objective: 'Identity', status: 'completed',
      steps: [{ id: 'step_forged' }], approvals: [{ id: 'approval_forged' }], artifacts: [{ id: 'artifact_forged' }],
    });
    assert.notEqual(forged.id, 'op_forged');
    assert.notEqual(forged.createdAt, '2000-01-01T00:00:00.000Z');
    assert.equal(forged.status, 'draft');
    assert.deepEqual(forged.steps, []);
    assert.deepEqual(forged.approvals, []);
    const project = await engine.createProjectRegistryRecord('personal', { id: 'registry_forged', createdAt: '2000-01-01T00:00:00.000Z', canonicalName: 'Identity Project' });
    assert.notEqual(project.id, 'registry_forged');
    assert.notEqual(project.createdAt, '2000-01-01T00:00:00.000Z');
    const reloaded = await engine.getOperation('personal', forged.id);
    assert.equal(reloaded.id, forged.id);
    assert.equal(reloaded.createdAt, forged.createdAt);
  });
});

test('publish safeguard still requires approval language and publish intent in the same message', () => {
  assert.equal(messageHasExplicitPublishApproval('Please prepare the publish plan.'), false);
  assert.equal(messageHasExplicitPublishApproval('I approve the unrelated note.'), false);
  assert.equal(messageHasExplicitPublishApproval('I approve the push of these exact changes.'), true);
});

test('planning remaps caller dependency labels and rejects duplicate IDs and cycles', async () => {
  await withEngine(async (engine) => {
    let operation = await engine.createOperation('personal', { title: 'Plan', objective: 'Plan safely' });
    operation = await engine.planOperation('personal', operation.id, { steps: [
      { id: 'a', title: 'A', type: 'internal', provider: 'internal', toolName: 'prepare_context' },
      { id: 'b', title: 'B', type: 'internal', provider: 'internal', toolName: 'prepare_context', dependsOn: ['a', 'missing', 'b'] },
    ] });
    assert.ok(operation.steps.every((step) => !['a', 'b'].includes(step.id)));
    assert.deepEqual(operation.steps[1].dependsOn, [operation.steps[0].id]);

    const duplicate = await engine.createOperation('personal', { title: 'Duplicate', objective: 'Duplicate' });
    await assert.rejects(() => engine.planOperation('personal', duplicate.id, { steps: [{ id: 'x' }, { id: 'x' }] }), (error) => error.code === 'DUPLICATE_STEP_ID');
    const cyclic = await engine.createOperation('personal', { title: 'Cycle', objective: 'Cycle' });
    await assert.rejects(() => engine.planOperation('personal', cyclic.id, { steps: [
      { id: 'x', title: 'X', dependsOn: ['y'] }, { id: 'y', title: 'Y', dependsOn: ['x'] },
    ] }), (error) => error.code === 'DEPENDENCY_CYCLE');
  });
});

test('crafted plan authority fields cannot bypass policy and booleans cannot strongly confirm', async () => {
  await withEngine(async (engine) => {
    let operation = await engine.createOperation('personal', { title: 'Attack', objective: 'Untrusted plan', autonomyMode: 'configured' });
    operation = await engine.planOperation('personal', operation.id, { steps: [{
      title: 'Modify files', type: 'codex', provider: 'codex', toolName: 'codex_implementation', riskLevel: 'low',
      approvalRequired: false, input: { explicitRequest: true, autonomyMode: 'configured', strongConfirmation: true },
    }] });
    operation = await engine.startOperation('personal', operation.id, { runCycle: true });
    assert.equal(operation.status, 'waiting_for_approval');
    assert.equal(operation.approvals[0].riskLevel, 'medium');

    const critical = await engine.createOperation('personal', { title: 'Critical attack', objective: 'Critical' });
    const planned = await engine.planOperation('personal', critical.id, { steps: [{ title: 'Destroy', type: 'internal', provider: 'internal', toolName: 'delete production data', input: { strongConfirmation: true } }] });
    const waiting = await engine.startOperation('personal', planned.id, { runCycle: true });
    await assert.rejects(() => engine.approveOperationStep('personal', waiting.id, waiting.approvals[0].id, { message: 'approve', strongConfirmation: true, runCycle: false }), (error) => error.code === 'STRONG_CONFIRMATION_REQUIRED');
  });
});

test('active execution context cannot be redirected and rejection is audited', async () => {
  await withEngine(async (engine) => {
    const projectA = await engine.createProjectRegistryRecord('personal', { canonicalName: 'Project A' });
    const projectB = await engine.createProjectRegistryRecord('personal', { canonicalName: 'Project B' });
    let operation = await engine.createOperation('personal', { title: 'Bound', objective: 'Bound', projectRegistryId: projectA.id, projectName: projectA.canonicalName });
    operation = await engine.planOperation('personal', operation.id, { steps: [{ title: 'Wait', type: 'codex', provider: 'codex', toolName: 'codex_implementation' }] });
    operation = await engine.startOperation('personal', operation.id, { runCycle: true });
    await assert.rejects(() => engine.updateOperation('personal', operation.id, { projectRegistryId: projectB.id }, { expectedRevision: operation.revision, actor: 'mark' }), (error) => error.code === 'EXECUTION_CONTEXT_IMMUTABLE');
    await assert.rejects(() => engine.updateProjectRegistryRecord('personal', projectA.id, { repo: { url: 'https://github.com/example/redirected' } }), (error) => error.code === 'REGISTRY_TARGET_IN_USE');
    const after = await engine.getOperation('personal', operation.id);
    assert.equal(after.projectRegistryId, projectA.id);
    assert.ok(after.activityLog.some((event) => event.type === 'execution_context_change_rejected'));
  });
});

test('planned material changes require explicit revisioned re-plan and invalidate prior plan state', async () => {
  await withEngine(async (engine) => {
    let operation = await engine.createOperation('personal', { title: 'Original', objective: 'Original' });
    operation = await engine.planOperation('personal', operation.id, { steps: [{ id: 'old', title: 'Old plan', type: 'internal', provider: 'internal', toolName: 'prepare_context' }] });
    await assert.rejects(() => engine.updateOperation('personal', operation.id, { objective: 'Redirected' }, { expectedRevision: operation.revision }), (error) => error.code === 'EXECUTION_CONTEXT_IMMUTABLE');
    const refreshed = await engine.getOperation('personal', operation.id);
    const replanned = await engine.replanOperation('personal', operation.id, {
      revision: refreshed.revision, patch: { objective: 'Revised explicitly' },
      plan: { steps: [{ id: 'new', title: 'New plan', type: 'internal', provider: 'internal', toolName: 'prepare_context' }] },
    });
    assert.equal(replanned.objective, 'Revised explicitly');
    assert.equal(replanned.steps.length, 1);
    assert.notEqual(replanned.steps[0].id, 'new');
    assert.ok(replanned.activityLog.some((event) => event.type === 'operation_plan_invalidated'));
  });
});

test('direct Codex adapter persists and polls queued/running/completed across restart without relaunch', async () => {
  let starts = 0;
  let polls = 0;
  let artifactReads = 0;
  let diffReads = 0;
  const adapter = {
    async startJob(job, { idempotencyKey }) { starts++; assert.ok(idempotencyKey); return { jobId: 'job-1', status: 'queued' }; },
    async getJobStatus(job) { polls++; return { ...job, status: polls === 1 ? 'queued' : polls === 2 ? 'running' : 'completed' }; },
    async getArtifacts() { artifactReads++; return [{ id: 'forged-artifact', createdAt: '2000-01-01T00:00:00.000Z', type: 'implementation_result', name: 'Implementation', content: 'done' }]; },
    async getDiff() { diffReads++; return { summary: 'one file changed' }; },
    async cancelJob(job) { return { ...job, status: 'cancelled' }; },
    async resumeJob(job) { return { ...job, status: 'running' }; },
    async sendFollowup(job) { return job; },
  };
  await withEngine(async (engine, { create }) => {
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Direct Project', aliases: ['Direct'] });
    let operation = (await engine.createFromRequest('personal', { originalRequest: 'Use Codex to implement the Direct Project fix.', autoPlan: true, autoStart: true })).operation;
    assert.equal(operation.status, 'awaiting_provider');
    const codexStep = operation.steps.find((step) => step.type === 'codex');
    assert.equal(operation.metadata.codexJobs[codexStep.id].jobId, 'job-1');
    assert.equal(starts, 1);

    const reloaded = create();
    operation = await reloaded.tick('personal', operation.id);
    assert.equal(operation.status, 'awaiting_provider');
    operation = await reloaded.tick('personal', operation.id);
    assert.equal(operation.status, 'awaiting_provider');
    operation = await reloaded.tick('personal', operation.id);
    assert.equal(operation.steps.find((step) => step.type === 'codex').status, 'completed');
    assert.ok(['blocked', 'verifying'].includes(operation.status));
    assert.equal(starts, 1);
    assert.equal(artifactReads, 1);
    assert.equal(diffReads, 1);
    const savedArtifact = operation.artifacts.find((artifact) => artifact.type === 'implementation_result');
    assert.notEqual(savedArtifact.id, 'forged-artifact');
    assert.notEqual(savedArtifact.createdAt, '2000-01-01T00:00:00.000Z');
  }, { directCodexAdapter: adapter });
});

test('direct Codex provider reports failure and cancellation honestly', async () => {
  for (const finalStatus of ['failed', 'cancelled']) {
    const adapter = {
      async startJob() { return { jobId: `job-${finalStatus}`, status: 'running' }; },
      async getJobStatus(job) { return { ...job, status: finalStatus, error: `${finalStatus} by provider` }; },
      async getArtifacts() { return []; }, async getDiff() { return {}; }, async cancelJob(job) { return { ...job, status: 'cancelled' }; },
      async resumeJob(job) { return job; }, async sendFollowup(job) { return job; },
    };
    await withEngine(async (engine) => {
      await engine.createProjectRegistryRecord('personal', { canonicalName: `Project ${finalStatus}` });
      let operation = (await engine.createFromRequest('personal', { originalRequest: `Use Codex to implement Project ${finalStatus}.`, autoPlan: true, autoStart: true })).operation;
      operation = await engine.tick('personal', operation.id);
      assert.equal(operation.status, finalStatus === 'failed' ? 'failed' : 'cancelled');
    }, { directCodexAdapter: adapter });
  }
});

test('a direct provider launch timeout becomes recovery-required and is not relaunched', async () => {
  let starts = 0;
  const adapter = {
    async startJob() { starts++; return new Promise((resolve) => setTimeout(() => resolve({ jobId: 'too-late', status: 'running' }), 1_500)); },
    async getJobStatus(job) { return job; }, async getArtifacts() { return []; }, async getDiff() { return {}; },
    async cancelJob(job) { return { ...job, status: 'cancelled' }; }, async resumeJob(job) { return job; }, async sendFollowup(job) { return job; },
  };
  await withEngine(async (engine) => {
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Timeout Project' });
    const operation = (await engine.createFromRequest('personal', { originalRequest: 'Use Codex to implement Timeout Project.', autoPlan: true, autoStart: true })).operation;
    assert.equal(operation.status, 'recovery_required');
    assert.equal(operation.providerActions.find((action) => action.provider === 'codex').status, 'unknown');
    await engine.tick('personal', operation.id);
    assert.equal(starts, 1);
  }, { directCodexAdapter: adapter, providerTimeoutMs: 1_000 });
});

test('desktop verification correlation survives restart and reconciles only matching results idempotently', async () => {
  const queued = [];
  const queueDesktopAction = async (action) => { queued.push(action); return { id: action.id }; };
  await withEngine(async (engine, { create, dataDir }) => {
    const workspace = path.join(dataDir, 'workspaces', 'project');
    await fs.mkdir(workspace, { recursive: true });
    await fs.writeFile(path.join(workspace, 'package.json'), JSON.stringify({ scripts: { test: 'node --test' } }));
    const project = await engine.createProjectRegistryRecord('personal', { canonicalName: 'Desktop Project', localWorkspace: { path: workspace }, commands: { test: 'node --test' } });
    await engine.approveProjectWorkspace('personal', project.id, { path: workspace, canonicalPath: workspace, desktopAgentId: 'agent-1' });
    let operation = await engine.store.create('personal', {
      title: 'Desktop verify', objective: 'Verify', projectRegistryId: project.id, projectName: project.canonicalName, status: 'queued',
      steps: [{ title: 'Test', type: 'verification', provider: 'verification', toolName: 'verify_operation', status: 'pending', input: { requirements: [{ type: 'test', required: true }] } }],
    });
    operation = await engine.tick('personal', operation.id);
    assert.equal(operation.status, 'blocked');
    assert.equal(queued.length, 1);
    const correlation = operation.desktopCorrelations[0];
    assert.equal(correlation.actionId, queued[0].id);

    const reloaded = create();
    await reloaded.recovery.recoverBusiness('personal');
    const recovered = await reloaded.getOperation('personal', operation.id);
    assert.equal(recovered.status, 'recovery_required');
    const result = {
      id: correlation.actionId, type: 'run-project-script', businessKey: 'personal', projectRegistryId: project.id,
      desktopAgentId: 'agent-1', ok: true, details: { stdout: 'ok', secret: 'must-redact' },
    };
    const reconciled = await reloaded.reconcileDesktopResult(result, { runCycle: true });
    assert.equal(reconciled.operation.status, 'completed');
    assert.equal(reconciled.operation.verification[0].status, 'passed');
    const duplicate = await reloaded.reconcileDesktopResult(result, { runCycle: false });
    assert.equal(duplicate.duplicate, true);
    await assert.rejects(() => reloaded.reconcileDesktopResult({ ...result, id: 'unknown' }), (error) => error.code === 'DESKTOP_ACTION_UNKNOWN');
    await assert.rejects(() => reloaded.reconcileDesktopResult({ ...result, businessKey: 'agency' }), (error) => error.code === 'DESKTOP_ACTION_UNKNOWN');
    await assert.rejects(() => reloaded.reconcileDesktopResult({ ...result, projectRegistryId: 'wrong' }), (error) => error.code === 'DESKTOP_RESULT_MISMATCH');
  }, { queueDesktopAction });
});

test('GitHub read provider allowlists actions and keeps registry access business-scoped', async () => {
  const calls = [];
  const adapter = async (input) => { calls.push(input); return { repository: input.repository, action: input.action }; };
  await withEngine(async (engine) => {
    const project = await engine.createProjectRegistryRecord('personal', { canonicalName: 'GitHub Project', repo: { url: 'https://github.com/markgromer/os1' } });
    let operation = await engine.store.create('personal', {
      title: 'Read', objective: 'Read', projectRegistryId: project.id, status: 'queued',
      steps: [{ title: 'Metadata', type: 'github_read', provider: 'github_read', toolName: 'repository_metadata', status: 'pending' }],
    });
    operation = await engine.tick('personal', operation.id);
    assert.equal(operation.steps[0].status, 'completed');
    assert.equal(calls[0].repository, 'markgromer/os1');
    const unsupported = await engine.providers.githubRead.execute({ operation, step: { toolName: 'arbitrary_endpoint', input: {} }, registryRecord: project });
    assert.equal(unsupported.status, 'failed');

    const crossBusiness = await engine.store.create('agency', {
      title: 'Cross business', objective: 'Cross', projectRegistryId: project.id, status: 'queued',
      steps: [{ title: 'Metadata', type: 'github_read', provider: 'github_read', toolName: 'repository_metadata', status: 'pending' }],
    });
    const blocked = await engine.tick('agency', crossBusiness.id);
    assert.equal(blocked.status, 'recovery_required');
    assert.equal(calls.length, 1);
  }, { githubReadAdapter: adapter });
});

test('workspace trust rejects broad roots, traversal, prefix confusion, nonexistent paths, and symlink escape', async (t) => {
  const base = await fs.mkdtemp(path.join(os.tmpdir(), 'workspace-trust-'));
  try {
    const root = path.join(base, 'allowed-root');
    const project = path.join(root, 'project');
    const sibling = path.join(base, 'allowed-root-evil');
    await fs.mkdir(project, { recursive: true });
    await fs.mkdir(sibling, { recursive: true });
    assert.equal(validateTrustedWorkspace({ workspacePath: project, allowedRoots: [root], registeredPath: project }), await fs.realpath(project));
    assert.throws(() => validateTrustedWorkspace({ workspacePath: sibling, allowedRoots: [root] }), (error) => error.code === 'WORKSPACE_OUTSIDE_ALLOWED_ROOT');
    assert.throws(() => validateTrustedWorkspace({ workspacePath: path.join(root, '..', 'allowed-root-evil'), allowedRoots: [root] }), (error) => error.code === 'WORKSPACE_OUTSIDE_ALLOWED_ROOT');
    assert.throws(() => validateTrustedWorkspace({ workspacePath: path.join(root, 'missing'), allowedRoots: [root] }), (error) => error.code === 'WORKSPACE_NOT_FOUND');
    assert.throws(() => validateAllowedWorkspaceRoots([os.homedir()]), (error) => error.code === 'WORKSPACE_ROOT_TOO_BROAD');
    const link = path.join(root, 'escape-link');
    try {
      await fs.symlink(sibling, link, process.platform === 'win32' ? 'junction' : 'dir');
      assert.throws(() => validateTrustedWorkspace({ workspacePath: link, allowedRoots: [root] }), (error) => error.code === 'WORKSPACE_OUTSIDE_ALLOWED_ROOT');
    } catch (error) {
      if (error?.code === 'EPERM') t.diagnostic('Symlink/junction creation is unavailable in this environment.');
      else throw error;
    }
  } finally { await fs.rm(base, { recursive: true, force: true }); }
});

test('medium-confidence resolution requires an audited explicit confirmation before planning', async () => {
  await withEngine(async (engine) => {
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Atlas North', aliases: ['Atlas'] });
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Atlas South', aliases: ['Atlas'] });
    const created = await engine.createFromRequest('personal', { originalRequest: 'Fix Atlas with Codex.', autoPlan: true });
    assert.equal(created.resolution.confidence, 'medium');
    assert.equal(created.operation.status, 'draft');
    await assert.rejects(() => engine.planOperation('personal', created.operation.id), (error) => error.code === 'PROJECT_CONFIRMATION_REQUIRED');
    const confirmed = await engine.confirmProject('personal', created.operation.id, { actor: 'mark', expectedRevision: created.operation.revision });
    assert.equal(confirmed.metadata.projectResolution.confirmed, true);
    assert.ok(confirmed.activityLog.some((event) => event.type === 'project_resolution_confirmed'));
    const planned = await engine.planOperation('personal', confirmed.id);
    assert.equal(planned.status, 'planned');
  });
});

test('recovery examines expired approvals and incomplete verification without assuming success', async () => {
  await withEngine(async (engine) => {
    const expired = await engine.store.create('personal', {
      title: 'Expired approval', objective: 'Recover approval', status: 'waiting_for_approval',
      steps: [{ id: 'step-expired', title: 'Approve', status: 'waiting_for_approval' }],
      approvals: [{ id: 'approval-expired', stepId: 'step-expired', status: 'pending', expiresAt: '2000-01-01T00:00:00.000Z' }],
    });
    const incomplete = await engine.store.create('personal', {
      title: 'Incomplete verification', objective: 'Verify', status: 'running',
      steps: [{ title: 'Implementation', type: 'codex', status: 'completed' }, { title: 'Verify', type: 'verification', status: 'pending' }],
    });
    const recovered = await engine.recovery.recoverBusiness('personal');
    assert.ok(recovered.includes(expired.id));
    assert.ok(recovered.includes(incomplete.id));
    assert.equal((await engine.getOperation('personal', expired.id)).approvals[0].status, 'expired');
    assert.equal((await engine.getOperation('personal', incomplete.id)).status, 'blocked');
  });
});

test('durable backup discovery includes operation and registry files for configured and discovered businesses', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'durable-backups-'));
  try {
    const businesses = path.join(dataDir, 'businesses');
    for (const key of ['personal', 'discovered']) {
      await fs.mkdir(path.join(businesses, key), { recursive: true });
      await fs.writeFile(path.join(businesses, key, 'operations.json'), '{}');
      await fs.writeFile(path.join(businesses, key, 'project-registry.json'), '{}');
    }
    const sources = await discoverDurableBackupSources({ businessDataDir: businesses, configuredBusinessKeys: ['personal', 'configured-without-files'] });
    assert.deepEqual(sources.map((item) => item.prefix), [
      'operations-discovered', 'project-registry-discovered', 'operations-personal', 'project-registry-personal',
    ]);
  } finally { await fs.rm(dataDir, { recursive: true, force: true }); }
});

test('project registry restores from its sibling backup without overwriting the corrupt file silently', async () => {
  await withEngine(async (engine, { dataDir }) => {
    const first = await engine.createProjectRegistryRecord('personal', { canonicalName: 'First Registry State' });
    await engine.createProjectRegistryRecord('personal', { canonicalName: 'Second Registry State' });
    const file = path.join(dataDir, 'businesses', 'personal', 'project-registry.json');
    await fs.writeFile(file, '{broken', 'utf8');
    const recovered = await engine.registry.get('personal', first.id);
    assert.equal(recovered.id, first.id);
    const files = await fs.readdir(path.dirname(file));
    assert.ok(files.some((name) => name.startsWith('project-registry.json.corrupt-')));
  });
});
