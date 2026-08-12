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
  });
  assert.equal(adapter.providerName, 'github_actions_codex');
  assert.equal(adapter.runnerRepo, 'markgromer/Reggie');
});

test('GitHub Actions Codex adapter collects authoritative target PR, diff, commit, and checks once', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  const headSha = 'a'.repeat(40);
  globalThis.fetch = async (url) => {
    const href = String(url);
    calls.push(href);
    if (href === 'https://api.github.com/repos/markgromer/demo') {
      return Response.json({ full_name: 'markgromer/demo', default_branch: 'main', html_url: 'https://github.com/markgromer/demo' });
    }
    if (href.includes('/branches/codex%2Fop-1')) return Response.json({ name: 'codex/op-1', commit: { sha: headSha } });
    if (href.includes('/pulls?')) {
      return Response.json([{ number: 12, title: 'Marcus result', state: 'open', draft: false, html_url: 'https://github.com/markgromer/demo/pull/12', head: { ref: 'codex/op-1', sha: headSha }, base: { ref: 'main' } }]);
    }
    if (href.endsWith('/pulls/12')) {
      return Response.json({ number: 12, title: 'Marcus result', state: 'open', draft: false, merged: false, html_url: 'https://github.com/markgromer/demo/pull/12', additions: 2, deletions: 1, changed_files: 1, head: { ref: 'codex/op-1', sha: headSha }, base: { ref: 'main' } });
    }
    if (href.includes('/pulls/12/files?')) {
      return Response.json([{ filename: 'src/app.js', status: 'modified', additions: 2, deletions: 1, changes: 3, patch: '@@ -1 +1 @@\n-const old = true;\n+const apiKey=sk_12345678901234567890;' }]);
    }
    if (href.includes('/compare/main...codex%2Fop-1')) {
      return Response.json({ status: 'ahead', ahead_by: 1, behind_by: 0, total_commits: 1, html_url: 'https://github.com/markgromer/demo/compare/main...codex/op-1' });
    }
    if (href.includes(`/commits/${headSha}/check-runs?`)) {
      return Response.json({ check_runs: [{ id: 90, name: 'test', status: 'completed', conclusion: 'success', html_url: 'https://github.com/check/90', app: { slug: 'github-actions' } }] });
    }
    if (href.includes(`/commits/${headSha}/status?`)) {
      return Response.json({ state: 'success', statuses: [{ id: 91, context: 'deploy-preview', state: 'success', description: 'Ready' }] });
    }
    throw new Error(`Unexpected fetch ${href}`);
  };
  try {
    const adapter = new GitHubActionsCodexAdapter({ token: 'ghp_test' });
    const job = { jobId: 'job-1', repository: 'https://github.com/markgromer/demo.git', branch: 'codex/op-1', rawMetadata: {} };
    const [artifacts, diff] = await Promise.all([adapter.getArtifacts(job), adapter.getDiff(job)]);
    assert.deepEqual(artifacts.map((item) => item.type), ['github_pull_request', 'commit', 'target_checks', 'github_result_evidence']);
    assert.equal(diff.source, 'github_api');
    assert.equal(diff.authoritative, true);
    assert.equal(diff.headSha, headSha);
    assert.equal(diff.pullRequest.number, 12);
    assert.equal(diff.files.length, 1);
    assert.match(diff.files[0].patch, /\[REDACTED\]/);
    assert.doesNotMatch(diff.files[0].patch, /sk_12345678901234567890/);
    assert.match(diff.evidenceDigest, /^[a-f0-9]{64}$/);
    assert.equal(diff.checks.checkRuns[0].conclusion, 'success');
    assert.equal(calls.filter((href) => href === 'https://api.github.com/repos/markgromer/demo').length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('GitHub Actions Codex adapter fails closed when the target commit cannot be resolved', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    const href = String(url);
    if (href === 'https://api.github.com/repos/markgromer/demo') return Response.json({ default_branch: 'main' });
    if (href.includes('/pulls?')) return Response.json([]);
    if (href.includes('/branches/')) return Response.json({ message: 'Not Found' }, { status: 404 });
    throw new Error(`Unexpected fetch ${href}`);
  };
  try {
    const adapter = new GitHubActionsCodexAdapter({ token: 'ghp_test' });
    await assert.rejects(
      adapter.getDiff({ jobId: 'job-missing', repository: 'markgromer/demo', branch: 'codex/missing' }),
      (error) => error?.code === 'CODEX_TARGET_COMMIT_UNRESOLVED',
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
