import test from 'node:test';
import assert from 'node:assert/strict';
import { summarizeRelease } from '../marcus/work/release_summary.js';
import { GitHubEvidenceIngestor } from '../marcus/evidence/github_evidence.js';
import { ProjectEvidenceService } from '../marcus/evidence/project_evidence_service.js';

const now = '2026-09-05T10:00:00Z';
const target = 'https://app.example.com/';
const deploy = (id, timestamp, created, status = 'success', commit = 'abc') => ({
  id, source: 'github', type: status === 'success' ? 'production_published' : 'deployment_failed', timestamp,
  deployment: { environment: 'production', status, commitSha: commit, url: target },
  metadata: { deploymentCreatedAt: created },
});
const check = (id, commitSha, conclusion = 'success', timestamp = now) => ({
  id, source: 'github', type: 'test_run', commitSha, timestamp,
  metadata: { url: 'https://github.com/owner/repo/actions/runs/' + id, status: 'completed', conclusion },
});

test('release matches CI by nonempty commit and latest observation of each run', () => {
  const result = summarizeRelease([deploy('one', now, now), check('run', 'abc', 'failure'),
    check('run', 'abc', 'success', '2026-09-04'), check('wrong', 'other'), check('missing', '')]);
  assert.equal(result.checks.count, 1); assert.equal(result.checks.passed, 0);
  assert.equal(result.checks.allRecordedPassed, false);
  assert.equal(summarizeRelease([check('missing', '')]).checks.count, 0);
  assert.equal(summarizeRelease([check('run', 'head')], { defaultBranchHead: 'head' }).checks.passed, 1);
  const missingDeploymentCommit = summarizeRelease([deploy('missing-sha', now, now, 'success', ''), check('head-run', 'head')], { defaultBranchHead: 'head' });
  assert.equal(missingDeploymentCommit.commit, '');
  assert.equal(missingDeploymentCommit.checks.count, 0);
});

test('new failed deployment wins; delayed inactive status on older deployment does not override new release', () => {
  const oldInactive = deploy('old', '2026-09-05T11:00:00Z', '2026-09-04', 'inactive');
  assert.equal(summarizeRelease([oldInactive, deploy('current', now, now)]).deployment.id, 'current');
  assert.equal(summarizeRelease([deploy('old', now, '2026-09-04'), deploy('failed', now, now, 'failure')]).deployment.status, 'failure');
});

test('merged PR observations are deduplicated and ordered; provider errors remain visible', () => {
  const pr = (id, number, timestamp) => ({ id, source: 'github', type: 'pull_request_merged', timestamp, pullRequest: { number, title: 'Change ' + number } });
  const result = summarizeRelease([pr('older', 1, '2026-09-01'), pr('new', 2, now), pr('updated', 1, '2026-09-03')],
    { lastRefreshedAt: now, errors: [{ endpoint: 'deployment_status', error: 'Unavailable' }] });
  assert.deepEqual(result.mergedChanges.map((row) => row.id), ['new', 'updated']);
  assert.equal(result.refreshedAt, now); assert.equal(result.refreshErrors.length, 1);
});

async function ingest(deployments, statuses, productionUrl = target) {
  const rows = []; const paths = []; let state;
  const store = {
    getSourceState: async () => ({}),
    append: async (_business, evidence, options) => { assert.equal(options.trusted, true); rows.push(...evidence); return { accepted: evidence, duplicateCount: 0 }; },
    setSourceState: async (_business, _key, value) => { state = value; },
  };
  const api = async (endpoint) => {
    paths.push(endpoint);
    if (endpoint.includes('/statuses?')) {
      const id = endpoint.match(/deployments\/(\d+)\//)[1];
      if (statuses[id] instanceof Error) throw statuses[id];
      return statuses[id] || [];
    }
    if (endpoint.includes('/deployments?')) return deployments;
    if (endpoint.includes('/actions/runs?')) return { workflow_runs: [] };
    if (endpoint.includes('?')) return [];
    return { default_branch: 'main' };
  };
  const ingestor = new GitHubEvidenceIngestor({ api, store });
  const result = await ingestor.collectProject({ businessKey: 'personal', project: { id: 'exact', projectId: 'legacy',
    repo: { provider: 'github', fullName: 'owner/repo', defaultBranch: 'main' }, deployments: { productionUrl } }, force: true });
  return { rows: rows.filter((row) => row.deployment), paths, state, result };
}
const ghDeployment = (id, extra = {}) => ({ id, production_environment: true, transient_environment: false, created_at: now, sha: 'abc', ref: 'main', environment: 'production',
  statuses_url: 'https://attacker.example/never-follow', ...extra });
const ghStatus = (id, state = 'success', environment_url = target) => ({ id, state, environment_url, created_at: now, log_url: 'https://provider.example/receipt' });

test('GitHub production evidence requires a provider status and exact registered target, never a payload API URL', async () => {
  const result = await ingest([ghDeployment(1), ghDeployment(2, { production_environment: false }), ghDeployment(3, { transient_environment: true }),
    ghDeployment(4), ghDeployment(5), ghDeployment('malicious/path')], {
    1: [ghStatus(11)], 2: [ghStatus(22)], 3: [ghStatus(33)], 4: [], 5: [ghStatus(55, 'success', 'https://different.example')],
  });
  assert.equal(result.rows.length, 1); assert.equal(result.rows[0].type, 'production_published');
  assert.equal(result.rows[0].metadata.targetMapping, 'exact_production_url');
  assert.match(result.rows[0].externalId, /deployment:exact:owner\/repo:1:11/);
  assert.ok(result.paths.every((path) => !path.includes('attacker') && !path.includes('malicious')));
  assert.equal((await ingest([ghDeployment(1)], { 1: [ghStatus(11)] }, '')).rows.length, 0);
});

test('latest failed status stays failed when its URL is inherited from an older status on the same deployment', async () => {
  const result = await ingest([ghDeployment(1)], { 1: [ghStatus(12, 'failure', ''), ghStatus(11)] });
  assert.equal(result.rows.length, 1); assert.equal(result.rows[0].type, 'deployment_failed');
  assert.equal(result.rows[0].deployment.status, 'failure');
  assert.equal(result.rows[0].deployment.url, target);
});

test('failed status reads and unknown production targets persist errors, never fabricated success', async () => {
  const result = await ingest([ghDeployment(1), ghDeployment(2)], { 1: new Error('Rate limited'), 2: [ghStatus(22, 'failure', '')] });
  assert.equal(result.rows.length, 0); assert.equal(result.result.errors.length, 2);
  assert.equal(result.state.errors.length, 2);
});

test('preview deployments do not consume the bounded production status lookups', async () => {
  const previews = Array.from({ length: 20 }, (_, id) => ghDeployment(id + 100, { production_environment: false }));
  const result = await ingest([...previews, ghDeployment(1)], { 1: [ghStatus(11)] });
  assert.equal(result.rows.length, 1);
  assert.equal(result.paths.filter((path) => path.includes('/statuses?')).length, 1);
  assert.ok(result.paths.some((path) => path.endsWith('/deployments?per_page=100')));
});

test('scoped refresh reads mapped providers without other projects, global reconciliation or lost ambiguity guards', async () => {
  const calls = []; const saved = []; const states = new Map();
  const projects = [{ id: 'selected', repo: {}, deployments: { renderServiceId: 'chosen', cloudflareProject: 'chosen-pages', cloudflareAccountId: 'account', productionUrl: target } },
    { id: 'other', deployments: { renderServiceId: 'other', cloudflareProject: 'other-pages', cloudflareAccountId: 'account' } }];
  const store = { append: async (_key, rows) => { saved.push(...rows); return { accepted: rows, duplicateCount: 0 }; }, setSourceState: async (_key, id, state) => states.set(id, state) };
  const service = new ProjectEvidenceService({ store, listProjects: async () => projects,
    renderApi: async (path) => { calls.push(path); return [{ id: 'render-one', status: 'live', createdAt: now, commit: { id: 'abc' } }]; },
    cloudflareApi: async (path) => { calls.push(path); return { result: [{ id: 'pages-one', environment: 'production', created_on: now }] }; },
    syncAirtableDerivedState: async () => { throw new Error('Must not synchronize Airtable'); },
  });
  service.recalculate = async () => { throw new Error('Must not reconcile globally'); };
  const result = await service.refreshProject('personal', 'selected');
  assert.equal(result.projectRegistryId, 'selected'); assert.equal(calls.length, 2);
  assert.ok(calls.every((path) => !path.includes('other')));
  assert.ok(saved.every((row) => row.projectRegistryId === 'selected'));
  assert.equal(saved.find((row) => row.source === 'cloudflare').type, 'deployment_started');
  assert.ok(states.has('deployments:selected'));
  projects.push({ id: 'ambiguous', deployments: { renderServiceId: 'chosen', cloudflareProject: 'chosen-pages' } });
  calls.length = 0;
  const blocked = await service.refreshProject('personal', 'selected');
  assert.equal(calls.length, 0);
  assert.ok(blocked.deployments.results.every((row) => row.skipped === 'low_confidence_mapping'));
});
