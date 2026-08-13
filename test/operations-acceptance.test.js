import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import express from 'express';

import { ApprovalPolicy } from '../marcus/approvals/approval_policy.js';
import { registerOperationsRoutes } from '../marcus/api/operations_routes.js';
import { createOperationsEngine } from '../marcus/operations/operation_engine.js';

const WARREN_REQUEST = 'The WARREN Creative Studio is still unusable on mobile. Own the problem and get Codex working on it.';

async function withApi(callback, options = {}) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-acceptance-'));
  const workspaceRoot = path.join(dataDir, 'workspaces');
  const warrenWorkspace = path.join(workspaceRoot, 'warren');
  await fs.mkdir(warrenWorkspace, { recursive: true });
  const createEngine = (overrides = {}) => createOperationsEngine({
    dataDir,
    getLegacyProjects: async (businessKey) => options.legacyByBusiness?.[businessKey] || [],
    queueDesktopAction: options.queueDesktopAction || null,
    allowedWorkspaceRoots: [workspaceRoot],
    ...overrides,
  });
  const startApi = async (engine = createEngine()) => {
    const app = express();
    app.use(express.json({ limit: '1mb' }));
    registerOperationsRoutes(app, { engine, getBusinessKey: (req) => String(req.get('x-business-key') || 'personal') });
    const server = app.listen(0, '127.0.0.1');
    await new Promise((resolve) => server.once('listening', resolve));
    const address = server.address();
    const base = `http://127.0.0.1:${address.port}`;
    const close = () => new Promise((resolve) => server.close(resolve));
    const request = async (method, url, body, businessKey = 'agency') => {
      const response = await fetch(`${base}${url}`, {
        method,
        headers: { 'content-type': 'application/json', 'x-business-key': businessKey },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const json = await response.json().catch(() => ({}));
      return { response, json };
    };
    return { engine, server, base, close, request };
  };
  try {
    return await callback({ dataDir, workspaceRoot, warrenWorkspace, createEngine, startApi });
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
}

async function createWarrenRecord(api, workspacePath) {
  const { response, json } = await api.request('POST', '/api/project-registry', {
    canonicalName: 'WARREN',
    aliases: ['Warren', 'Creative Studio', 'mobile creative studio', 'Gromore Admin'],
    description: 'WARREN Creative Studio and Gromore Admin web application.',
    repo: {
      provider: 'github',
      owner: 'markgromer',
      name: 'warren',
      fullName: 'markgromer/warren',
      url: 'https://github.com/markgromer/warren',
      defaultBranch: 'main',
      workingBranchPattern: 'codex/warren-mobile-{operationId}',
    },
    localWorkspace: { path: workspacePath, platform: process.platform },
    deployments: {
      productionUrl: 'https://warren.example.com',
      previewUrl: 'https://warren-preview.example.com',
      renderServiceId: 'srv-warren',
      cloudflareProject: 'warren-pages',
    },
    stack: ['node', 'express', 'static frontend'],
    commands: { build: 'npm run build', test: 'npm test', lint: 'npm run lint', typecheck: 'npm run typecheck' },
  });
  assert.equal(response.status, 201);
  return json.project;
}

test('WARREN durable operations acceptance scenario survives restart and requires verification', async () => {
  await withApi(async ({ warrenWorkspace, createEngine, startApi }) => {
    let api = await startApi();
    const warren = await createWarrenRecord(api, warrenWorkspace);

    const resolved = await api.request('POST', '/api/project-registry/resolve', { request: WARREN_REQUEST });
    assert.equal(resolved.response.status, 200);
    assert.equal(resolved.json.resolution.confidence, 'high');
    assert.equal(resolved.json.resolution.registryRecord.id, warren.id);

    const created = await api.request('POST', '/api/operations', {
      originalRequest: WARREN_REQUEST,
      requestedBy: 'mark',
      source: 'acceptance_test',
      autoPlan: true,
      autoStart: false,
    });
    assert.equal(created.response.status, 201);
    let operation = created.json.operation;
    assert.equal(operation.projectRegistryId, warren.id);
    assert.equal(operation.originalRequest, WARREN_REQUEST);
    assert.match(operation.objective, /Make WARREN Creative Studio usable on mobile/i);
    assert.notEqual(operation.objective, WARREN_REQUEST);
    assert.ok(operation.acceptanceCriteria.length >= 4);
    assert.deepEqual(operation.plan, operation.steps.map((step) => step.title));
    assert.ok(operation.steps.every((step, index) => step.sequence === index + 1));
    const codexStep = operation.steps.find((step) => step.type === 'codex');
    assert.ok(codexStep);

    const started = await api.request('POST', `/api/operations/${operation.id}/start`, {});
    assert.equal(started.response.status, 200);
    operation = started.json.operation;
    assert.equal(operation.status, 'blocked');
    assert.equal(operation.steps.find((step) => step.id === codexStep.id).status, 'blocked');
    assert.equal(operation.metadata.codexJobs?.[codexStep.id]?.provider, 'external_handoff');
    assert.equal(operation.metadata.codexJobs?.[codexStep.id]?.jobId, '');
    const handoff = operation.artifacts.find((artifact) => artifact.type === 'codex_handoff');
    assert.ok(handoff);
    for (const expected of ['WARREN', 'Repository', 'Suggested work branch', 'Objective', 'Constraints', 'Acceptance criteria', 'Verification requirements', 'Do not push']) {
      assert.match(handoff.content, new RegExp(expected, 'i'));
    }

    await api.close();
    api = await startApi(createEngine());
    const reloaded = await api.request('GET', `/api/operations/${operation.id}`);
    assert.equal(reloaded.response.status, 200);
    assert.equal(reloaded.json.operation.status, 'blocked');
    assert.equal(reloaded.json.operation.artifacts.find((artifact) => artifact.type === 'codex_handoff').content, handoff.content);

    const otherBusiness = await api.request('GET', `/api/operations/${operation.id}`, undefined, 'personal');
    assert.equal(otherBusiness.response.status, 404);

    const registered = await api.request('POST', `/api/operations/${operation.id}/external-job`, {
      jobId: 'codex-run-warren-1',
      status: 'completed',
      branch: 'codex/warren-mobile-fix',
      commit: 'abc123def456',
      diffSummary: 'Responsive Creative Studio layout changes for mobile.',
      result: 'Codex completed the mobile usability implementation.',
      registeredBy: 'mark',
    });
    assert.equal(registered.response.status, 200);
    operation = registered.json.operation;
    assert.equal(operation.status, 'queued');
    assert.equal(operation.steps.find((step) => step.type === 'codex').status, 'completed');
    assert.ok(operation.artifacts.some((artifact) => artifact.type === 'external_job'));
    assert.ok(operation.artifacts.some((artifact) => artifact.type === 'codex_diff'));

    const failedTick = await api.request('POST', `/api/operations/${operation.id}/tick`, {});
    assert.equal(failedTick.response.status, 200);
    operation = failedTick.json.operation;
    assert.equal(operation.status, 'blocked');
    assert.notEqual(operation.status, 'completed');
    assert.ok(operation.verification.some((item) => item.required && ['needs_manual_review', 'failed'].includes(item.status)));

    const failedEvidence = await api.request('POST', `/api/operations/${operation.id}/manual-verification-evidence`, {
      results: [{ type: 'build', status: 'failed', note: 'Build failed on the mobile Creative Studio route during acceptance testing.' }],
    });
    assert.equal(failedEvidence.response.status, 200);
    const failedAgain = await api.request('POST', `/api/operations/${operation.id}/tick`, {});
    assert.equal(failedAgain.response.status, 200);
    operation = failedAgain.json.operation;
    assert.equal(operation.status, 'blocked');
    assert.ok(operation.verification.some((item) => item.type === 'build' && item.status === 'failed'));

    const passedEvidence = await api.request('POST', `/api/operations/${operation.id}/manual-verification-evidence`, {
      results: [
        { type: 'build', status: 'passed', note: 'Build passed after the mobile Creative Studio fix.' },
        { type: 'test', status: 'passed', note: 'Relevant smoke tests passed for the mobile Creative Studio workflow.' },
        { type: 'lint', status: 'passed', note: 'Lint passed after the WARREN mobile fix.' },
        { type: 'typecheck', status: 'passed', note: 'Typecheck passed after the WARREN mobile fix.' },
        { type: 'diff_review', status: 'passed', note: 'Mark reviewed the responsive layout diff and accepted it for this operation.' },
      ],
    });
    assert.equal(passedEvidence.response.status, 200);
    const finalTick = await api.request('POST', `/api/operations/${operation.id}/tick`, {});
    assert.equal(finalTick.response.status, 200);
    operation = finalTick.json.operation;
    assert.equal(operation.status, 'completed');
    assert.ok(operation.activityLog.some((event) => event.type === 'operation_completed'));
    assert.ok(operation.activityLog.some((event) => event.type === 'external_codex_job_registered'));
    assert.ok(operation.activityLog.some((event) => event.type === 'manual_verification_evidence_registered'));
    await api.close();
  });
});

test('operation creation deduplicates recent active work for the same business, project, and objective', async () => {
  await withApi(async ({ warrenWorkspace, startApi }) => {
    const api = await startApi();
    try {
      await createWarrenRecord(api, warrenWorkspace);
      const first = await api.request('POST', '/api/operations', { originalRequest: WARREN_REQUEST, autoPlan: true });
      const second = await api.request('POST', '/api/operations', { originalRequest: 'Please own the WARREN Creative Studio mobile problem and get Codex working on it.', autoPlan: true });
      assert.equal(first.response.status, 201);
      assert.equal(second.response.status, 201);
      assert.equal(second.json.operation.id, first.json.operation.id);
      assert.equal(second.json.reused, true);
      assert.ok(second.json.operation.activityLog.some((event) => event.type === 'duplicate_operation_reused'));
    } finally {
      await api.close();
    }
  });
});

test('approval policy resists high and critical action bypass variations', () => {
  const policy = new ApprovalPolicy();
  for (const action of [
    'git push',
    'merge pull request',
    'deploy production',
    'change DNS record',
    'modify environment variables',
    'send client communication',
    'run database migration',
  ]) {
    const decision = policy.classify({ business: 'agency', projectRegistryId: 'registry_1', provider: 'desktop', action, riskLevel: 'low', approvalRequired: false });
    assert.equal(decision.riskLevel, 'high', action);
    assert.equal(decision.approvalRequired, true, action);
  }
  for (const action of ['production data deletion', 'delete production data', 'billing change', 'credential rotation', 'rotate credential', 'configure-pc-access:C:\\']) {
    const decision = policy.classify({ business: 'agency', projectRegistryId: 'registry_1', provider: 'internal', action, riskLevel: 'low', approvalRequired: false });
    assert.equal(decision.riskLevel, 'critical', action);
    assert.equal(decision.approvalRequired, true, action);
    assert.equal(decision.approvalRequirement, 'explicit_strong_confirmation', action);
  }
});

test('full-PC access is an exact critical operation with persisted desktop read-back', async () => {
  await withApi(async ({ createEngine }) => {
    const queued = [];
    const engine = createEngine({
      getDesktopContext: async () => ({
        desktopAuthorization: {
          agentId: 'mark-desktop', scope: 'workspace_roots', fullPcAccess: false,
          allowedRoots: ['C:\\Users\\markg\\Documents'], newProjectRoot: 'C:\\Users\\markg\\Documents\\Marcus Projects',
          pcAccessRoots: ['C:\\Users\\markg\\Documents'],
        },
      }),
      queueDesktopAction: async (action) => {
        queued.push(structuredClone(action));
        return action;
      },
    });
    const created = await engine.createPcAccessOperation('agency', { requestedBy: 'mark-mobile' });
    let operation = created.operation;
    assert.equal(created.reused, false);
    assert.deepEqual(created.pcAccessRoots, ['C:\\']);
    assert.equal(operation.status, 'waiting_for_approval');
    assert.equal(operation.riskLevel, 'critical');
    assert.equal(queued.length, 0);
    const approval = operation.approvals.find((item) => item.status === 'pending');
    assert.equal(approval.riskLevel, 'critical');
    assert.match(approval.action, /configure-pc-access/i);

    operation = await engine.approveOperationStep('agency', operation.id, approval.id, {
      approvedBy: 'mark-mobile', message: 'I understand and approve this critical PC access action.', runCycle: true,
    });
    assert.equal(operation.status, 'blocked', JSON.stringify(operation, null, 2));
    assert.equal(queued.length, 1);
    assert.equal(queued[0].type, 'configure-pc-access');
    assert.equal(queued[0].payload.desktopAgentId, 'mark-desktop');
    assert.deepEqual(queued[0].payload.pcAccessRoots, ['C:\\']);

    const desktopResult = (action, details) => ({
      id: action.id, type: action.type, businessKey: action.payload.businessKey,
      operationId: action.payload.operationId, stepId: action.payload.stepId,
      projectRegistryId: action.payload.projectRegistryId, desktopAgentId: action.payload.desktopAgentId,
      idempotencyKey: action.payload.idempotencyKey, attemptNumber: action.payload.attemptNumber,
      ok: true, details,
    });
    let reconciled = await engine.reconcileDesktopResult(desktopResult(queued[0], {
      persisted: true, runtimeApplied: true, fullPcAccess: true, pcAccessRoots: ['C:\\'], credentialContentBlocked: true,
    }), { runCycle: true });
    operation = reconciled.operation;
    assert.equal(queued.length, 2);
    assert.equal(queued[1].type, 'verify-pc-access');
    reconciled = await engine.reconcileDesktopResult(desktopResult(queued[1], {
      verified: true, persisted: true, runtimeApplied: true, fullPcAccess: true, pcAccessRoots: ['C:\\'], credentialContentBlocked: true,
    }), { runCycle: true });
    operation = reconciled.operation;
    assert.equal(operation.status, 'completed', JSON.stringify(operation, null, 2));
    assert.ok(operation.steps.every((step) => step.status === 'completed'));
  });
});

test('operation completion refuses incomplete steps, pending approvals, active blockers, and manual review', async () => {
  await withApi(async ({ createEngine }) => {
    const engine = createEngine();
    const operation = await engine.store.create('agency', {
      title: 'Unsafe completion',
      objective: 'Do not complete early',
      status: 'verifying',
      steps: [
        { title: 'Done', type: 'internal', status: 'completed', sequence: 1 },
        { title: 'Pending', type: 'verification', status: 'pending', sequence: 2 },
      ],
      approvals: [{ action: 'deploy production', status: 'pending', riskLevel: 'high' }],
      blockers: [{ type: 'manual_review', status: 'active', message: 'Manual review required.' }],
      verification: [{ type: 'manual_review', status: 'needs_manual_review', required: true }],
    });
    await assert.rejects(() => engine.completeOperation('agency', operation.id), /pending/i);

    await engine.store.update('agency', operation.id, (draft) => {
      draft.steps[1].status = 'completed';
      return draft;
    });
    await assert.rejects(() => engine.completeOperation('agency', operation.id), /approval/i);

    await engine.store.update('agency', operation.id, (draft) => {
      draft.approvals[0].status = 'approved';
      return draft;
    });
    await assert.rejects(() => engine.completeOperation('agency', operation.id), /blocker/i);

    await engine.store.update('agency', operation.id, (draft) => {
      draft.blockers[0].status = 'resolved';
      return draft;
    });
    await assert.rejects(() => engine.completeOperation('agency', operation.id), /verification/i);
  });
});

test('operation readiness reports non-sensitive durable engine health', async () => {
  await withApi(async ({ warrenWorkspace, startApi }) => {
    const api = await startApi();
    try {
      await createWarrenRecord(api, warrenWorkspace);
      const created = await api.request('POST', '/api/operations', { originalRequest: WARREN_REQUEST, autoPlan: true, autoStart: true });
      assert.equal(created.response.status, 201);
      const readiness = await api.request('GET', '/api/operations/readiness');
      assert.equal(readiness.response.status, 200);
      assert.equal(readiness.json.readiness.operationEngineInitialized, true);
      assert.equal(readiness.json.readiness.codex.mode, 'external_handoff');
      assert.equal(readiness.json.readiness.codex.directAdapterConfigured, false);
      assert.ok(Number.isInteger(readiness.json.readiness.pendingExternalCodexCount));
      assert.equal(JSON.stringify(readiness.json).includes('https://github.com/markgromer/warren'), false);
    } finally {
      await api.close();
    }
  });
});
