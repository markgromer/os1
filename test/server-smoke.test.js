import assert from 'node:assert/strict';
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

async function spawnServer({ startupCheck = false, adminToken = 'test-admin-token', production = false } = {}) {
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
    const approvedWorkspace = await fetch(`${base}/api/project-registry/${registry.id}/approve-workspace`, { method: 'POST', headers: agencyHeaders, body: JSON.stringify({ desktopAgentId: 'agent-smoke' }) });
    assert.equal(approvedWorkspace.status, 200);
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
