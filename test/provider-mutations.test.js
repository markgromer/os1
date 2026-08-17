import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { createOperationsEngine } from '../marcus/operations/operation_engine.js';
import { executeMarcusOperationTool, getMarcusOperationToolDefinitions } from '../marcus/operations/marcus_operation_tools.js';

const SHA = 'a'.repeat(40);
const ACCOUNT = 'a'.repeat(32);
const ZONE = 'b'.repeat(32);
const RECORD = 'c'.repeat(32);
const VERSION = '11111111-1111-4111-8111-111111111111';
const DEPLOYMENT = '22222222-2222-4222-8222-222222222222';

async function withEngine(callback, options = {}) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-provider-mutations-'));
  const engine = createOperationsEngine({
    dataDir,
    githubWriteAdapter: options.githubWriteAdapter,
    cloudflareWriteAdapter: options.cloudflareWriteAdapter,
  });
  try { return await callback(engine); }
  finally { await fs.rm(dataDir, { recursive: true, force: true }); }
}

async function addProject(engine, overrides = {}) {
  return engine.createProjectRegistryRecord('personal', {
    canonicalName: 'Marcus Provider Demo', aliases: ['provider demo'],
    repo: { url: 'https://github.com/markgromer/provider-demo' },
    deployments: {
      productionUrl: 'https://marcus-provider-demo.markgromer.workers.dev',
      cloudflareProject: 'marcus-provider-demo', cloudflareAccountId: ACCOUNT,
      cloudflareZoneId: ZONE, cloudflareZoneName: 'example.com',
    },
    ...overrides,
  });
}

test('provider mutation tools are explicit durable preparation tools', () => {
  const names = getMarcusOperationToolDefinitions().map((tool) => tool.function.name);
  assert.ok(names.includes('get_operation_evidence'));
  assert.ok(names.includes('prepare_github_merge'));
  assert.ok(names.includes('prepare_cloudflare_dns_change'));
  assert.ok(names.includes('prepare_cloudflare_worker_deployment'));
});

test('GitHub merge is project/SHA bound, explicitly approved, called once, and duplicate preparation is reused', async () => {
  const calls = [];
  await withEngine(async (engine) => {
    await addProject(engine);
    const args = {
      projectName: 'Marcus Provider Demo', repository: 'markgromer/provider-demo',
      pullNumber: 7, expectedHeadSha: SHA, mergeMethod: 'squash',
    };
    const requestMessage = 'Prepare to merge pull request 7 in markgromer/provider-demo at the exact reviewed head.';
    const prepared = await executeMarcusOperationTool({ name: 'prepare_github_merge', args, engine, businessKey: 'personal', requestMessage });
    assert.equal(prepared.ok, true);
    assert.equal(prepared.operation.status, 'waiting_for_approval');
    assert.equal(prepared.operation.approvals[0].riskLevel, 'high');
    assert.match(prepared.operation.approvals[0].action, /markgromer\/provider-demo#7/);
    assert.equal(calls.length, 0);

    const duplicate = await executeMarcusOperationTool({ name: 'prepare_github_merge', args, engine, businessKey: 'personal', requestMessage });
    assert.equal(duplicate.reused, true);
    assert.equal(duplicate.operation.id, prepared.operation.id);

    const unrelated = await executeMarcusOperationTool({
      name: 'approve_operation_step', args: { operationId: prepared.operation.id, approvalId: prepared.operation.approvals[0].id },
      engine, businessKey: 'personal', requestMessage: 'Show me the pending operation.',
    });
    assert.equal(unrelated.ok, false);
    assert.equal(calls.length, 0);

    const approved = await executeMarcusOperationTool({
      name: 'approve_operation_step', args: { operationId: prepared.operation.id, approvalId: prepared.operation.approvals[0].id },
      engine, businessKey: 'personal', requestMessage: `Approve ${prepared.operation.id}`,
    });
    assert.equal(approved.ok, true);
    assert.equal(approved.operation.status, 'completed');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].repository, 'markgromer/provider-demo');
    assert.equal(calls[0].input.expectedHeadSha, SHA);
    assert.equal(approved.operation.steps[1].status, 'completed');
    assert.match(approved.operation.steps[1].output, /mergeCommitSha/);
  }, {
    githubWriteAdapter: async (input) => {
      calls.push(input);
      return { verified: true, repository: input.repository, pullNumber: input.input.pullNumber, mergeCommitSha: 'd'.repeat(40) };
    },
  });
});

test('GitHub provider refuses a repository mismatch before creating an operation', async () => {
  await withEngine(async (engine) => {
    await addProject(engine);
    await assert.rejects(() => engine.createProviderActionFromRequest('personal', {
      originalRequest: 'Merge pull request 7 in another/repository.', projectName: 'Marcus Provider Demo',
      repository: 'another/repository', provider: 'github_write', action: 'merge_pull_request',
      input: { pullNumber: 7, expectedHeadSha: SHA },
    }), (error) => error.code === 'PROVIDER_TARGET_MISMATCH');
  });
});

test('Cloudflare DNS upsert is project-zone bound and cannot run before approval', async () => {
  const calls = [];
  await withEngine(async (engine) => {
    await addProject(engine);
    const prepared = await executeMarcusOperationTool({
      name: 'prepare_cloudflare_dns_change', businessKey: 'personal', engine,
      requestMessage: 'Update the DNS record api.example.com for Marcus Provider Demo.',
      args: {
        projectName: 'Marcus Provider Demo', action: 'upsert', zoneId: ZONE,
        recordId: RECORD, recordType: 'CNAME', name: 'api.example.com', content: 'target.example.net', ttl: 1, proxied: true,
      },
    });
    assert.equal(prepared.operation.status, 'waiting_for_approval');
    assert.equal(prepared.operation.approvals[0].riskLevel, 'high');
    assert.equal(calls.length, 0);
    const approved = await executeMarcusOperationTool({
      name: 'approve_operation_step', args: { operationId: prepared.operation.id, approvalId: prepared.operation.approvals[0].id },
      engine, businessKey: 'personal', requestMessage: `Approve ${prepared.operation.id}`,
    });
    assert.equal(approved.operation.status, 'completed');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].input.zoneId, ZONE);
    assert.equal(calls[0].input.name, 'api.example.com');
    assert.equal(calls[0].registryTarget.deployments.cloudflareProject, 'marcus-provider-demo');
  }, { cloudflareWriteAdapter: async (input) => { calls.push(input); return { verified: true, after: input.input }; } });
});

test('Cloudflare DNS deletion requires strong confirmation while Worker promotion retains exact binding', async () => {
  const calls = [];
  await withEngine(async (engine) => {
    await addProject(engine);
    const deletion = await engine.createProviderActionFromRequest('personal', {
      originalRequest: 'Delete the DNS record old.example.com for Marcus Provider Demo.', projectName: 'Marcus Provider Demo',
      provider: 'cloudflare_write', action: 'delete_dns_record',
      input: { zoneId: ZONE, recordId: RECORD, recordType: 'A', name: 'old.example.com', content: '192.0.2.2', ttl: 1, proxied: false },
    });
    assert.equal(deletion.operation.approvals[0].riskLevel, 'critical');
    await assert.rejects(() => engine.approveOperationStep('personal', deletion.operation.id, deletion.operation.approvals[0].id, {
      message: 'approve', runCycle: true,
    }), /strong confirmation/i);
    assert.equal(calls.length, 0);

    const deployment = await executeMarcusOperationTool({
      name: 'prepare_cloudflare_worker_deployment', businessKey: 'personal', engine,
      requestMessage: 'Deploy the approved Cloudflare Worker version for Marcus Provider Demo.',
      args: { projectName: 'Marcus Provider Demo', accountId: ACCOUNT, scriptName: 'marcus-provider-demo', versionId: VERSION, expectedCurrentDeploymentId: DEPLOYMENT },
    });
    assert.equal(deployment.operation.approvals[0].riskLevel, 'high');
    const approved = await executeMarcusOperationTool({
      name: 'approve_operation_step', args: { operationId: deployment.operation.id, approvalId: deployment.operation.approvals[0].id },
      engine, businessKey: 'personal', requestMessage: `Approve ${deployment.operation.id}`,
    });
    assert.equal(approved.operation.status, 'completed');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].input.versionId, VERSION);
    assert.equal(calls[0].input.expectedCurrentDeploymentId, DEPLOYMENT);

    await assert.rejects(() => engine.createProviderActionFromRequest('personal', {
      originalRequest: 'Deploy another-worker in Cloudflare for Marcus Provider Demo.', projectName: 'Marcus Provider Demo',
      provider: 'cloudflare_write', action: 'deploy_worker_version',
      input: { accountId: ACCOUNT, scriptName: 'another-worker', versionId: VERSION, expectedCurrentDeploymentId: DEPLOYMENT },
    }), /not bound/i);
  }, { cloudflareWriteAdapter: async (input) => { calls.push(input); return { verified: true, deploymentId: '3'.repeat(32), versionId: input.input.versionId }; } });
});

test('an indeterminate post-mutation read-back enters recovery instead of claiming failure or retrying', async () => {
  let calls = 0;
  await withEngine(async (engine) => {
    await addProject(engine);
    const prepared = await engine.createProviderActionFromRequest('personal', {
      originalRequest: 'Update the DNS record api.example.com for Marcus Provider Demo.', projectName: 'Marcus Provider Demo',
      provider: 'cloudflare_write', action: 'upsert_dns_record',
      input: { zoneId: ZONE, recordId: RECORD, recordType: 'CNAME', name: 'api.example.com', content: 'target.example.net', ttl: 1, proxied: true },
    });
    const approval = prepared.operation.approvals[0];
    const result = await engine.approveOperationStep('personal', prepared.operation.id, approval.id, { message: 'Approve the exact DNS change.', runCycle: true });
    assert.equal(result.status, 'recovery_required');
    assert.equal(result.steps[1].status, 'blocked');
    assert.equal(calls, 1);
    assert.ok(result.blockers.some((blocker) => blocker.type === 'recovery_required'));
  }, {
    cloudflareWriteAdapter: async () => {
      calls += 1;
      throw Object.assign(new Error('Mutation accepted; read-back unavailable.'), { code: 'PROVIDER_STATE_UNKNOWN' });
    },
  });
});
