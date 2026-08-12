import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';

import { HttpCodexAdapter, createHttpCodexAdapterFromEnv } from '../marcus/providers/http_codex_adapter.js';

async function withHttpCodexServer(handler, callback) {
  const requests = [];
  const server = http.createServer(async (req, res) => {
    let raw = '';
    req.on('data', (chunk) => { raw += String(chunk); });
    req.on('end', async () => {
      const body = raw ? JSON.parse(raw) : {};
      requests.push({ method: req.method, url: req.url, authorization: req.headers.authorization || '', body });
      try {
        const data = await handler(req, body);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(data));
      } catch (err) {
        res.writeHead(500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: String(err?.message || err) }));
      }
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  try {
    return await callback({ baseUrl: `http://127.0.0.1:${port}`, requests });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('HTTP Codex adapter starts, polls, and cancels jobs through configured endpoints', async () => {
  await withHttpCodexServer((req, body) => {
    if (req.url === '/codex/start') {
      assert.equal(body.idempotencyKey, 'idem-1');
      assert.match(body.prompt, /Goal for Codex/);
      return { provider: 'http_codex', jobId: 'job-http-1', status: 'started', branch: body.branch };
    }
    if (req.url === '/codex/status') return { provider: 'http_codex', jobId: body.jobId, status: 'running' };
    if (req.url === '/codex/cancel') return { provider: 'http_codex', jobId: body.jobId, status: 'cancelled' };
    throw new Error(`Unexpected path ${req.url}`);
  }, async ({ baseUrl, requests }) => {
    const adapter = new HttpCodexAdapter({ baseUrl, token: 'secret-token' });
    const started = await adapter.startJob({
      operationId: 'op-1',
      stepId: 'step-1',
      businessKey: 'personal',
      projectRegistryId: 'project-1',
      repository: 'markgromer/demo',
      branch: 'codex/op-1',
      prompt: '# Goal for Codex',
    }, { idempotencyKey: 'idem-1' });
    assert.equal(started.jobId, 'job-http-1');
    assert.equal(started.status, 'started');

    const running = await adapter.getJobStatus(started);
    assert.equal(running.status, 'running');

    const cancelled = await adapter.cancelJob(started);
    assert.equal(cancelled.status, 'cancelled');
    assert.equal(requests.every((request) => request.authorization === 'Bearer secret-token'), true);
  });
});

test('HTTP Codex adapter is disabled when no URL is configured', () => {
  assert.equal(createHttpCodexAdapterFromEnv({}), null);
  const adapter = createHttpCodexAdapterFromEnv({ MARCUS_CODEX_ADAPTER_URL: 'https://codex.example.test' });
  assert.equal(adapter.providerName, 'http_codex');
});
