import assert from 'node:assert/strict';
import test from 'node:test';

import { GitHubActionsCodexAdapter, createGitHubActionsCodexAdapterFromEnv } from '../marcus/providers/github_actions_codex_adapter.js';

test('GitHub Actions Codex adapter dispatches Reggie-style repository_dispatch jobs', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).endsWith('/dispatches')) return new Response(null, { status: 204 });
    if (String(url).includes('/actions/workflows/marcus-codex-runner.yml/runs')) {
      return Response.json({ workflow_runs: [{
        id: 123,
        html_url: 'https://github.com/markgromer/os1/actions/runs/123',
        status: 'in_progress',
        conclusion: null,
        display_title: `Marcus Codex ${calls[0].bodyJobId}`,
      }] });
    }
    throw new Error(`Unexpected fetch ${url}`);
  };
  try {
    const adapter = new GitHubActionsCodexAdapter({ token: 'ghp_test', runnerRepo: 'markgromer/os1' });
    const started = await adapter.startJob({
      operationId: 'op-1',
      stepId: 'step-1',
      businessKey: 'personal',
      projectRegistryId: 'project-1',
      repository: 'markgromer/demo',
      branch: 'codex/op-1',
      prompt: '# Goal for Codex',
    }, { idempotencyKey: 'idem-1' });
    calls[0].bodyJobId = started.jobId;
    assert.equal(started.provider, 'github_actions_codex');
    assert.equal(started.status, 'queued');
    assert.match(started.jobId, /^ghdispatch_/);
    const body = JSON.parse(calls[0].init.body);
    assert.equal(body.event_type, 'marcus_codex_job');
    assert.equal(body.client_payload.repository, 'markgromer/demo');
    assert.equal(body.client_payload.prompt, '# Goal for Codex');

    const status = await adapter.getJobStatus(started);
    assert.equal(status.status, 'running');
    assert.equal(status.rawMetadata.workflowRunId, 123);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('GitHub Actions Codex adapter only enables from env when explicitly requested', () => {
  assert.equal(createGitHubActionsCodexAdapterFromEnv({ GITHUB_TOKEN: 'ghp_test' }), null);
  const adapter = createGitHubActionsCodexAdapterFromEnv({
    MARCUS_CODEX_GITHUB_ACTIONS_ENABLED: 'true',
    GITHUB_TOKEN: 'ghp_test',
    MARCUS_CODEX_RUNNER_REPO: 'markgromer/os1',
  });
  assert.equal(adapter.providerName, 'github_actions_codex');
});
