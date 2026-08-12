import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(fileURLToPath(new URL('..', import.meta.url)));

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function spawnServer({ startupCheck = false, adminToken = 'test-admin-token', production = false, extraEnv = {} } = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-server-smoke-'));
  const workspaceRoot = path.join(root, 'workspaces');
  await fs.mkdir(workspaceRoot, { recursive: true });
  const port = await freePort();
  const child = spawn(process.execPath, ['server.js'], {
    cwd: repositoryRoot,
    windowsHide: true,
    env: {
      ...process.env,
      PORT: String(port),
      MARCUS_HOST: '127.0.0.1',
      TASK_TRACKER_DATA_DIR: path.join(root, 'data'),
      TASK_TRACKER_SETTINGS_DIR: path.join(root, 'settings'),
      TASK_TRACKER_BACKUP_DIR: path.join(root, 'backups'),
      MARCUS_ALLOW_UNAUTHENTICATED_LOCAL: adminToken ? 'false' : 'true',
      MARCUS_STARTUP_CHECK: startupCheck ? 'true' : 'false',
      NODE_ENV: production ? 'production' : 'test',
      ADMIN_TOKEN: adminToken,
      MARCUS_ALLOWED_WORKSPACE_ROOTS: workspaceRoot,
      RENDER: '', RENDER_SERVICE_ID: '', RENDER_EXTERNAL_URL: '',
      ...extraEnv,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += String(chunk); });
  child.stderr.on('data', (chunk) => { output += String(chunk); });
  const waitForExit = () => child.exitCode !== null
    ? Promise.resolve({ code: child.exitCode, signal: child.signalCode, output })
    : new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal, output })));
  const waitForReady = async () => {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      if (output.includes('M.A.R.C.U.S. running on')) return;
      if (child.exitCode !== null) throw new Error(`Server exited before ready (${child.exitCode}).\n${output}`);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(`Timed out waiting for server.\n${output}`);
  };
  const close = async () => {
    if (child.exitCode === null) child.kill();
    if (child.exitCode === null) await waitForExit();
    await fs.rm(root, { recursive: true, force: true });
  };
  return { child, port, root, workspaceRoot, get output() { return output; }, waitForExit, waitForReady, close };
}

async function withMockCodexAdapter(callback) {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ provider: 'mock_http_codex', jobId: 'mock-job-1', status: 'started' }));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    return await callback(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('isolated server startup succeeds only for explicit loopback development or configured admin auth', async () => {
  const local = await spawnServer({ startupCheck: true, adminToken: '' });
  try {
    const exited = await local.waitForExit();
    assert.equal(exited.code, 0, exited.output);
    assert.match(exited.output, /startup validation completed/i);
  } finally { await local.close(); }

  const hosted = await spawnServer({ startupCheck: true, adminToken: '', production: true });
  try {
    const exited = await hosted.waitForExit();
    assert.notEqual(exited.code, 0);
    assert.match(exited.output, /ADMIN_TOKEN is required/i);
  } finally { await hosted.close(); }
});

test('server auth, business scope, existing reads, Marcus routing, and Live operation summary regressions', async () => {
  const server = await spawnServer();
  const base = `http://127.0.0.1:${server.port}`;
  const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  try {
    await server.waitForReady();
    assert.equal((await fetch(`${base}/api/health`)).status, 200);
    const livePage = await fetch(`${base}/live.html`);
    const liveHtml = await livePage.text();
    assert.equal(livePage.status, 200);
    assert.match(liveHtml, /<body class="live-focus">/);
    assert.match(liveHtml, /class="live-focus-hero"/);
    assert.match(liveHtml, /body\.live-focus \.command-stage/);
    assert.equal((await fetch(`${base}/api/operations/summary`)).status, 401);
    assert.equal((await fetch(`${base}/api/tasks`, { headers: adminHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/projects`, { headers: adminHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/tasks`, { headers: { ...adminHeaders, 'x-business-key': 'not-configured' } })).status, 403);

    const switched = await fetch(`${base}/api/businesses/active`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ key: 'agency' }) });
    assert.equal(switched.status, 200);
    assert.equal((await fetch(`${base}/api/tasks`, { headers: { ...adminHeaders, 'x-business-key': 'agency' } })).status, 200);

    const command = await fetch(`${base}/api/marcus/command`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ message: 'Fix the Atlas mobile layout and verify it.' }) });
    const commandBody = await command.json();
    assert.equal(command.status, 200);
    assert.equal(commandBody.intent, 'durable_operation');
    const chat = await fetch(`${base}/api/chat`, { method: 'POST', headers: adminHeaders, body: JSON.stringify({ message: 'Implement the Atlas repository fix and verify it.' }) });
    assert.equal(chat.status, 200);
    assert.ok((await chat.json()).reply);

    const sessionResponse = await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders });
    const session = await sessionResponse.json();
    assert.ok(session.token);
    const liveHeaders = { authorization: `Bearer ${session.token}`, 'x-business-key': 'agency', 'content-type': 'application/json' };
    const operatorHealth = await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders });
    assert.equal(operatorHealth.status, 200);
    const operatorHealthBody = await operatorHealth.json();
    assert.equal(operatorHealthBody.capabilities.projectOperator.available, true);
    assert.equal(operatorHealthBody.capabilities.projectOperator.mode, 'codex_handoff');
    assert.equal(operatorHealthBody.capabilities.projectOperator.canStartCodexDirectly, false);
    assert.ok(operatorHealthBody.blockers.some((item) => /direct Codex launch adapter/i.test(item)));
    const savedProviderSettings = await fetch(`${base}/api/settings`, { method: 'PUT', headers: adminHeaders, body: JSON.stringify({
      githubToken: 'ghp_savedprovider1234567890',
      githubOwner: 'markgromer',
      cloudflareApiToken: 'cf_savedprovider1234567890',
      cloudflareAccountId: 'account_123',
      cloudflareDefaultZoneId: 'zone_123',
      renderApiKey: 'rnd_savedprovider1234567890',
    }) });
    assert.equal(savedProviderSettings.status, 200);
    const settingsAfterProviderSave = await (await fetch(`${base}/api/settings`, { headers: adminHeaders })).json();
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'githubToken'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'cloudflareApiToken'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'renderApiKey'), false);
    assert.equal(Object.hasOwn(settingsAfterProviderSave, 'externalActionDrafts'), false);
    assert.equal(settingsAfterProviderSave.githubConfigured, true);
    assert.equal(settingsAfterProviderSave.githubSource, 'settings');
    assert.equal(settingsAfterProviderSave.cloudflareConfigured, true);
    assert.equal(settingsAfterProviderSave.cloudflareSource, 'settings');
    const operatorHealthWithSavedProviders = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
    assert.equal(operatorHealthWithSavedProviders.capabilities.github.backendTokenConfigured, true);
    assert.equal(operatorHealthWithSavedProviders.capabilities.github.source, 'settings');
    assert.equal(operatorHealthWithSavedProviders.capabilities.cloudflare.backendTokenConfigured, true);
    assert.equal(operatorHealthWithSavedProviders.capabilities.cloudflare.source, 'settings');
    assert.equal(operatorHealthWithSavedProviders.blockers.some((item) => /GITHUB_TOKEN is not configured/i.test(item)), false);
    assert.equal(operatorHealthWithSavedProviders.blockers.some((item) => /CLOUDFLARE_API_TOKEN is not configured/i.test(item)), false);
    const draftExternalActionResponse = await fetch(`${base}/api/marcus/external-actions/draft`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({
        type: 'email',
        to: 'client@example.com',
        subject: 'Atlas status',
        body: 'Atlas audit is ready for review.',
        projectName: 'Atlas',
        reason: 'Marcus drafted this from the project conversation.',
      }),
    });
    assert.equal(draftExternalActionResponse.status, 201);
    const draftExternalAction = (await draftExternalActionResponse.json()).action;
    assert.equal(draftExternalAction.status, 'pending_approval');
    assert.equal(draftExternalAction.requiresApproval, true);
    assert.equal(draftExternalAction.createdBy, 'marcus');
    const listedExternalActions = await (await fetch(`${base}/api/marcus/external-actions`, { headers: liveHeaders })).json();
    assert.equal(listedExternalActions.ok, true);
    assert.ok(listedExternalActions.actions.some((item) => item.id === draftExternalAction.id));
    const vagueApproval = await fetch(`${base}/api/marcus/external-actions/${draftExternalAction.id}/approve`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({ message: 'looks fine' }),
    });
    assert.equal(vagueApproval.status, 409);
    assert.equal((await vagueApproval.json()).approvalRequired, true);
    const explicitApproval = await fetch(`${base}/api/marcus/external-actions/${draftExternalAction.id}/approve`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({ message: `approve ${draftExternalAction.id}` }),
    });
    assert.equal(explicitApproval.status, 200);
    const explicitApprovalBody = await explicitApproval.json();
    assert.equal(explicitApprovalBody.action.status, 'approved');
    assert.match(explicitApprovalBody.note, /separate explicit provider action/i);
    const draftTextResponse = await fetch(`${base}/api/marcus/external-actions/draft`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({ type: 'text', to: '+15555550123', body: 'Can you confirm the Atlas review window?' }),
    });
    assert.equal(draftTextResponse.status, 201);
    const draftTextAction = (await draftTextResponse.json()).action;
    const rejectedText = await fetch(`${base}/api/marcus/external-actions/${draftTextAction.id}/reject`, {
      method: 'POST',
      headers: liveHeaders,
      body: JSON.stringify({ message: 'do not send this version' }),
    });
    assert.equal(rejectedText.status, 200);
    assert.equal((await rejectedText.json()).action.status, 'rejected');
    const summary = await fetch(`${base}/api/operations/summary`, { headers: liveHeaders });
    assert.equal(summary.status, 200);
    const summaryBody = await summary.json();
    assert.ok(summaryBody.operations.every((operation) => !Object.hasOwn(operation, 'artifacts')));
    assert.equal((await fetch(`${base}/api/operations`, { headers: liveHeaders })).status, 401);
    assert.equal((await fetch(`${base}/api/operations`, { method: 'POST', headers: liveHeaders, body: JSON.stringify({ originalRequest: 'mutate' }) })).status, 401);

    const agencyHeaders = { ...adminHeaders, 'x-business-key': 'agency' };
    const workspace = path.join(server.workspaceRoot, 'desktop-smoke');
    await fs.mkdir(workspace, { recursive: true });
    await fs.writeFile(path.join(workspace, 'package.json'), JSON.stringify({ scripts: { test: 'node --test' } }));
    const registryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      canonicalName: 'Desktop Smoke', localWorkspace: { path: workspace }, commands: { test: 'node --test' },
    }) });
    const registry = (await registryResponse.json()).project;
    assert.equal(registryResponse.status, 201);
    const manualEvidence = await fetch(`${base}/api/project-evidence/ingest`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id,
      source: 'manual',
      type: 'manual_note',
      event: 'operator_note',
      summary: 'Desktop Smoke is ready for evidence API verification.',
      actor: 'mark',
      provenance: { method: 'authenticated_smoke_test' },
    }) });
    assert.equal(manualEvidence.status, 201);
    const forgedEvidence = await fetch(`${base}/api/project-evidence/ingest`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id, source: 'github', type: 'commit', actor: 'mark', provenance: { method: 'manual' },
    }) });
    assert.equal(forgedEvidence.status, 403);
    const browserEvidence = await fetch(`${base}/api/project-evidence/browser-verification`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      projectRegistryId: registry.id, actor: 'mark', url: 'https://example.com/desktop-smoke', status: 'passed',
      viewports: [{ width: 1440, height: 900 }], screenshots: ['external://desktop-smoke.png'],
    }) });
    assert.equal(browserEvidence.status, 201);
    assert.equal((await browserEvidence.json()).mode, 'external_manual');
    const recalculatedActivity = await fetch(`${base}/api/project-activity/recalculate`, { method: 'POST', headers: agencyHeaders, body: '{}' });
    assert.equal(recalculatedActivity.status, 200);
    const projectActivity = await fetch(`${base}/api/project-activity/${registry.id}`, { headers: agencyHeaders });
    assert.equal(projectActivity.status, 200);
    assert.equal((await projectActivity.json()).activity.projectRegistryId, registry.id);
    const activeBrief = await fetch(`${base}/api/marcus/active-brief`, { headers: agencyHeaders });
    const activeBriefBody = await activeBrief.json();
    assert.equal(activeBrief.status, 200);
    assert.ok(activeBriefBody.projectEvidenceActivity.snapshots.some((item) => item.projectRegistryId === registry.id));
    assert.ok(Array.isArray(activeBriefBody.projectActivity));
    assert.equal((await fetch(`${base}/api/project-activity/current-focus`, { headers: agencyHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/project-activity/stale`, { headers: agencyHeaders })).status, 200);
    assert.equal((await fetch(`${base}/api/project-activity/bottlenecks`, { headers: agencyHeaders })).status, 200);
    const approvedWorkspace = await fetch(`${base}/api/project-registry/${registry.id}/approve-workspace`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ desktopAgentId: 'agent-smoke' }) });
    assert.equal(approvedWorkspace.status, 200);
    const operatorRegistryResponse = await fetch(`${base}/api/project-registry`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      canonicalName: 'Operator Smoke', aliases: ['Operator Smoke repository'],
    }) });
    const operatorRegistry = (await operatorRegistryResponse.json()).project;
    assert.equal(operatorRegistryResponse.status, 201);
    const operatorResponse = await fetch(`${base}/api/marcus/project-operator`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      message: 'Audit the Operator Smoke repository and get Codex fixing it.',
    }) });
    assert.equal(operatorResponse.status, 201);
    const operatorBody = await operatorResponse.json();
    assert.equal(operatorBody.status, 'codex_prepared');
    assert.match(operatorBody.codexPrompt, /Goal for Codex/);
    assert.match(operatorBody.auditBrief, /Operator Smoke/);
    assert.equal(operatorBody.operation.status, 'blocked');
    const operationResponse = await fetch(`${base}/api/operations`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({
      originalRequest: 'Verify Desktop Smoke tests.', projectRegistryId: registry.id, autoPlan: false,
    }) });
    const operation = (await operationResponse.json()).operation;
    const plannedResponse = await fetch(`${base}/api/operations/${operation.id}/plan`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ steps: [{
      title: 'Run tests', type: 'verification', provider: 'verification', toolName: 'verify_operation', input: { requirements: [{ type: 'test', required: true }] },
    }] }) });
    assert.equal(plannedResponse.status, 200);
    const startedResponse = await fetch(`${base}/api/operations/${operation.id}/start`, { method: 'POST', headers: agencyHeaders, body: '{}' });
    assert.equal(startedResponse.status, 200);
    const wrongAgentActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=wrong-agent`, { headers: agencyHeaders })).json();
    assert.equal(wrongAgentActions.actions.some((item) => item.requestedBy === `operation:${operation.id}`), false);
    const actions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    const action = actions.actions.find((item) => item.requestedBy === `operation:${operation.id}`);
    assert.ok(action?.id);
    const resultResponse = await fetch(`${base}/api/desktop-context/action-results`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
      id: action.id, type: action.type, businessKey: 'agency', operationId: action.payload.operationId, stepId: action.payload.stepId,
      projectRegistryId: registry.id, desktopAgentId: 'agent-smoke', idempotencyKey: action.payload.idempotencyKey,
      attemptNumber: action.payload.attemptNumber, ok: true, details: { stdout: 'tests passed' },
    }] }) });
    const resultBody = await resultResponse.json();
    assert.equal(resultBody.received, 1);
    const acknowledgedActions = await (await fetch(`${base}/api/desktop-context/actions?agentId=agent-smoke`, { headers: agencyHeaders })).json();
    assert.equal(acknowledgedActions.actions.some((item) => item.id === action.id), false);
    const unknownResult = await fetch(`${base}/api/desktop-context/action-results`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ agentId: 'agent-smoke', results: [{
      id: 'unknown-action', type: 'run-project-script', businessKey: 'agency', projectRegistryId: registry.id, desktopAgentId: 'agent-smoke', ok: true,
    }] }) });
    assert.equal((await unknownResult.json()).rejected[0].code, 'DESKTOP_ACTION_UNKNOWN');
  } finally { await server.close(); }
});

test('server enables direct Codex mode when HTTP adapter URL is configured', async () => {
  await withMockCodexAdapter(async (adapterUrl) => {
    const server = await spawnServer({ extraEnv: { MARCUS_CODEX_ADAPTER_URL: adapterUrl, MARCUS_CODEX_ADAPTER_TOKEN: 'test-token' } });
    const base = `http://127.0.0.1:${server.port}`;
    const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
    try {
      await server.waitForReady();
      const session = await (await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders })).json();
      const liveHeaders = { authorization: `Bearer ${session.token}`, 'content-type': 'application/json' };
      const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
      assert.equal(health.capabilities.projectOperator.mode, 'direct_codex');
      assert.equal(health.capabilities.projectOperator.canStartCodexDirectly, true);
      assert.equal(health.capabilities.projectOperator.provider, 'http_codex');
      assert.equal(health.blockers.some((item) => /direct Codex launch adapter/i.test(item)), false);
    } finally { await server.close(); }
  });
});

test('server enables Reggie-style GitHub Actions Codex mode when configured', async () => {
  const server = await spawnServer({ extraEnv: {
    MARCUS_CODEX_GITHUB_ACTIONS_ENABLED: 'true',
    MARCUS_CODEX_GITHUB_TOKEN: 'ghp_testcodexactions1234567890',
    MARCUS_CODEX_RUNNER_REPO: 'markgromer/os1',
  } });
  const base = `http://127.0.0.1:${server.port}`;
  const adminHeaders = { authorization: 'Bearer test-admin-token', 'content-type': 'application/json' };
  try {
    await server.waitForReady();
    const session = await (await fetch(`${base}/api/marcus/live/session`, { headers: adminHeaders })).json();
    const liveHeaders = { authorization: `Bearer ${session.token}`, 'content-type': 'application/json' };
    const health = await (await fetch(`${base}/api/marcus/operator-health`, { headers: liveHeaders })).json();
    assert.equal(health.capabilities.projectOperator.mode, 'direct_codex');
    assert.equal(health.capabilities.projectOperator.canStartCodexDirectly, true);
    assert.equal(health.capabilities.projectOperator.provider, 'github_actions_codex');
    assert.equal(health.blockers.some((item) => /direct Codex launch adapter/i.test(item)), false);
  } finally { await server.close(); }
});
