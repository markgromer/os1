import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const SHA = 'a'.repeat(40);
const MERGE_SHA = 'd'.repeat(40);
const ACCOUNT = 'a'.repeat(32);
const ZONE = 'b'.repeat(32);
const RECORD = 'c'.repeat(32);
const VERSION = '11111111-1111-4111-8111-111111111111';
const OLD_DEPLOYMENT = '22222222-2222-4222-8222-222222222222';
const NEW_DEPLOYMENT = '33333333-3333-4333-8333-333333333333';

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function readJson(req) {
  let body = '';
  for await (const chunk of req) body += String(chunk);
  return body ? JSON.parse(body) : {};
}

function send(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

async function startProviderMock() {
  const state = { githubMerged: false, githubMergeCalls: 0, dns: null, dnsWrites: 0, deployment: OLD_DEPLOYMENT, workerDeploys: 0 };
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const pathname = url.pathname;
    if (pathname === '/gh/repos/markgromer/provider-demo/pulls/7' && req.method === 'GET') {
      return send(res, 200, {
        number: 7, title: 'Provider acceptance', state: state.githubMerged ? 'closed' : 'open', draft: false,
        merged: state.githubMerged, mergeable: true, mergeable_state: 'clean', merge_commit_sha: state.githubMerged ? MERGE_SHA : null,
        head: { sha: SHA, ref: 'provider-acceptance' }, base: { ref: 'main' }, html_url: 'https://github.com/markgromer/provider-demo/pull/7',
      });
    }
    if (pathname === `/gh/repos/markgromer/provider-demo/commits/${SHA}/check-runs` && req.method === 'GET') {
      return send(res, 200, { check_runs: [{ id: 1, name: 'verify', status: 'completed', conclusion: 'success' }] });
    }
    if (pathname === `/gh/repos/markgromer/provider-demo/commits/${SHA}/status` && req.method === 'GET') return send(res, 200, { statuses: [] });
    if (pathname === '/gh/repos/markgromer/provider-demo/pulls/7/merge' && req.method === 'PUT') {
      const body = await readJson(req);
      assert.equal(body.sha, SHA);
      state.githubMergeCalls += 1;
      state.githubMerged = true;
      return send(res, 200, { merged: true, sha: MERGE_SHA, message: 'merged' });
    }
    if (pathname === `/cf/zones/${ZONE}` && req.method === 'GET') {
      return send(res, 200, { success: true, result: { id: ZONE, name: 'example.com', account: { id: ACCOUNT } } });
    }
    if (pathname === `/cf/zones/${ZONE}/dns_records` && req.method === 'GET') {
      return send(res, 200, { success: true, result: state.dns ? [state.dns] : [] });
    }
    if (pathname === `/cf/zones/${ZONE}/dns_records` && req.method === 'POST') {
      const body = await readJson(req);
      state.dnsWrites += 1;
      state.dns = { id: RECORD, ...body, modified_on: new Date().toISOString() };
      return send(res, 200, { success: true, result: state.dns });
    }
    if (pathname === `/cf/zones/${ZONE}/dns_records/${RECORD}` && req.method === 'GET') return send(res, 200, { success: true, result: state.dns });
    if (pathname === `/cf/accounts/${ACCOUNT}/workers/scripts` && req.method === 'GET') {
      return send(res, 200, { success: true, result: [{ id: 'marcus-provider-demo', created_on: '2026-08-12T00:00:00Z', modified_on: '2026-08-12T01:00:00Z' }] });
    }
    const workerRoot = `/cf/accounts/${ACCOUNT}/workers/scripts/marcus-provider-demo`;
    if (pathname === `${workerRoot}/versions/${VERSION}` && req.method === 'GET') return send(res, 200, { success: true, result: { id: VERSION, number: 2 } });
    if (pathname === `${workerRoot}/versions` && req.method === 'GET') return send(res, 200, { success: true, result: [{ id: VERSION, number: 2, metadata: { source: 'wrangler' } }] });
    if (pathname === `${workerRoot}/deployments` && req.method === 'GET') {
      return send(res, 200, { success: true, result: { deployments: [{ id: state.deployment, versions: [{ version_id: state.deployment === OLD_DEPLOYMENT ? '00000000-0000-4000-8000-000000000000' : VERSION, percentage: 100 }] }] } });
    }
    if (pathname === `${workerRoot}/deployments` && req.method === 'POST') {
      const body = await readJson(req);
      assert.deepEqual(body.versions, [{ version_id: VERSION, percentage: 100 }]);
      state.workerDeploys += 1;
      state.deployment = NEW_DEPLOYMENT;
      return send(res, 200, { success: true, result: { id: NEW_DEPLOYMENT } });
    }
    if (pathname === `${workerRoot}/deployments/${NEW_DEPLOYMENT}` && req.method === 'GET') {
      return send(res, 200, { success: true, result: { id: NEW_DEPLOYMENT, versions: [{ version_id: VERSION, percentage: 100 }] } });
    }
    send(res, 404, { message: `Unhandled ${req.method} ${pathname}` });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { server, state, base: `http://127.0.0.1:${server.address().port}` };
}

async function startMarcus(providerBase, dataDir) {
  const port = await freePort();
  const child = spawn(process.execPath, ['server.js'], {
    cwd: root, windowsHide: true,
    env: {
      ...process.env, NODE_ENV: 'test', PORT: String(port), MARCUS_HOST: '127.0.0.1', ADMIN_TOKEN: 'provider-test-admin',
      MARCUS_ALLOW_UNAUTHENTICATED_LOCAL: 'false', TASK_TRACKER_DATA_DIR: dataDir,
      TASK_TRACKER_SETTINGS_DIR: path.join(dataDir, 'settings'), TASK_TRACKER_BACKUP_DIR: path.join(dataDir, 'backups'),
      GITHUB_TOKEN: 'github-test-token', GITHUB_OWNER: 'markgromer', CLOUDFLARE_API_TOKEN: 'cloudflare-test-token',
      CLOUDFLARE_ACCOUNT_ID: ACCOUNT, CLOUDFLARE_DEFAULT_ZONE_ID: ZONE,
      MARCUS_TEST_GITHUB_API_BASE_URL: `${providerBase}/gh`, MARCUS_TEST_CLOUDFLARE_API_BASE_URL: `${providerBase}/cf`,
      RENDER: '', RENDER_SERVICE_ID: '', RENDER_EXTERNAL_URL: '',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += String(chunk); });
  child.stderr.on('data', (chunk) => { output += String(chunk); });
  const deadline = Date.now() + 20_000;
  while (!output.includes('M.A.R.C.U.S. running on') && child.exitCode === null && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (!output.includes('M.A.R.C.U.S. running on')) throw new Error(`Marcus failed to start.\n${output}`);
  return { child, base: `http://127.0.0.1:${port}`, get output() { return output; } };
}

test('real server executes approved GitHub and Cloudflare operations with authoritative read-back', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-provider-server-'));
  const provider = await startProviderMock();
  let marcus;
  try {
    marcus = await startMarcus(provider.base, dataDir);
    const headers = { authorization: 'Bearer provider-test-admin', 'content-type': 'application/json' };
    const createProject = async (project) => {
      const response = await fetch(`${marcus.base}/api/project-registry`, { method: 'POST', headers, body: JSON.stringify({ project }) });
      assert.equal(response.status, 201);
      return (await response.json()).project;
    };
    const githubProject = await createProject({ canonicalName: 'Provider GitHub Demo', repo: { url: 'https://github.com/markgromer/provider-demo' } });
    const cloudflareProject = await createProject({
      canonicalName: 'Provider Cloudflare Demo',
      deployments: { productionUrl: 'https://marcus-provider-demo.markgromer.workers.dev', cloudflareProject: 'marcus-provider-demo', cloudflareAccountId: ACCOUNT, cloudflareZoneId: ZONE, cloudflareZoneName: 'example.com' },
    });

    const prInspection = await (await fetch(`${marcus.base}/api/integrations/github/pull-request?owner=markgromer&repo=provider-demo&pullNumber=7`, { headers })).json();
    assert.equal(prInspection.headSha, SHA);
    assert.equal(prInspection.checks.settled, true);

    const prepare = async (body) => {
      const response = await fetch(`${marcus.base}/api/operations/provider-action`, { method: 'POST', headers, body: JSON.stringify(body) });
      assert.equal(response.status, 201);
      return (await response.json()).operation;
    };
    const approve = async (operation, message = `Approve ${operation.id}`) => {
      const approval = operation.approvals.find((item) => item.status === 'pending');
      const response = await fetch(`${marcus.base}/api/operations/${operation.id}/approvals/${approval.id}/approve`, { method: 'POST', headers, body: JSON.stringify({ message }) });
      assert.equal(response.status, 200);
      return (await response.json()).operation;
    };

    let github = await prepare({
      originalRequest: 'Merge pull request 7 in markgromer/provider-demo.', projectRegistryId: githubProject.id,
      repository: 'markgromer/provider-demo', provider: 'github_write', action: 'merge_pull_request',
      input: { pullNumber: 7, expectedHeadSha: SHA, mergeMethod: 'squash' },
    });
    assert.equal(github.status, 'waiting_for_approval');
    assert.equal(provider.state.githubMergeCalls, 0);
    github = await approve(github);
    assert.equal(github.status, 'completed');
    assert.equal(provider.state.githubMergeCalls, 1);
    assert.ok(github.artifacts.some((artifact) => artifact.type === 'provider_mutation_evidence'));

    let dns = await prepare({
      originalRequest: 'Create the DNS record api.example.com for Provider Cloudflare Demo.', projectRegistryId: cloudflareProject.id,
      provider: 'cloudflare_write', action: 'upsert_dns_record',
      input: { zoneId: ZONE, recordType: 'CNAME', name: 'api.example.com', content: 'target.example.net', ttl: 1, proxied: true },
    });
    assert.equal(provider.state.dnsWrites, 0);
    dns = await approve(dns);
    assert.equal(dns.status, 'completed');
    assert.equal(provider.state.dnsWrites, 1);
    assert.equal(provider.state.dns.content, 'target.example.net');

    const workers = await (await fetch(`${marcus.base}/api/integrations/cloudflare/workers`, { headers })).json();
    const versions = await (await fetch(`${marcus.base}/api/integrations/cloudflare/worker-versions?scriptName=marcus-provider-demo`, { headers })).json();
    const deployments = await (await fetch(`${marcus.base}/api/integrations/cloudflare/worker-deployments?scriptName=marcus-provider-demo`, { headers })).json();
    assert.equal(workers.workers[0].id, 'marcus-provider-demo');
    assert.equal(versions.versions[0].id, VERSION);
    assert.equal(deployments.deployments[0].id, OLD_DEPLOYMENT);

    let worker = await prepare({
      originalRequest: 'Deploy the Cloudflare Worker version for Provider Cloudflare Demo.', projectRegistryId: cloudflareProject.id,
      provider: 'cloudflare_write', action: 'deploy_worker_version',
      input: { accountId: ACCOUNT, scriptName: 'marcus-provider-demo', versionId: VERSION, expectedCurrentDeploymentId: OLD_DEPLOYMENT },
    });
    assert.equal(provider.state.workerDeploys, 0);
    worker = await approve(worker);
    assert.equal(worker.status, 'completed');
    assert.equal(provider.state.workerDeploys, 1);
    assert.equal(provider.state.deployment, NEW_DEPLOYMENT);
    assert.ok(worker.verification.some((result) => result.type === 'provider_readback' && result.status === 'passed'));
  } finally {
    if (marcus?.child && marcus.child.exitCode === null) marcus.child.kill();
    if (marcus?.child && marcus.child.exitCode === null) await new Promise((resolve) => marcus.child.once('exit', resolve));
    await new Promise((resolve) => provider.server.close(resolve));
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});
