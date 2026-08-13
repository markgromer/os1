import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  calculateBusinessActivity,
  calculateProjectActivitySnapshot,
  decayForEvidence,
} from '../marcus/evidence/activity_engine.js';
import { ProjectEvidenceStore } from '../marcus/evidence/evidence_store.js';
import { normalizeEvidence, normalizeManualEvidence } from '../marcus/evidence/evidence_types.js';
import { GitHubEvidenceIngestor } from '../marcus/evidence/github_evidence.js';
import {
  executeMarcusProjectActivityTool,
  getMarcusProjectActivityToolDefinitions,
} from '../marcus/evidence/marcus_project_activity_tools.js';
import { ProjectEvidenceService } from '../marcus/evidence/project_evidence_service.js';
import { BrowserVerificationProvider } from '../marcus/providers/browser_verification_provider.js';
import { CodexProvider } from '../marcus/providers/codex_provider.js';

const DAY_MS = 24 * 60 * 60 * 1_000;
const NOW = Date.parse('2026-08-05T20:00:00.000Z');

async function temporaryDataDir() {
  return fs.mkdtemp(path.join(os.tmpdir(), 'marcus-project-evidence-'));
}

function project(id, name, options = {}) {
  return {
    id,
    projectId: options.projectId || `legacy-${id}`,
    businessKey: options.businessKey || 'personal',
    canonicalName: name,
    aliases: options.aliases || [],
    status: options.status || 'active',
    createdAt: options.createdAt || new Date(NOW - 120 * DAY_MS).toISOString(),
    repo: { provider: 'github', fullName: options.repository || `markgromer/${id}`, defaultBranch: 'main' },
    localWorkspace: options.localWorkspace || {},
    deployments: options.deployments || {},
  };
}

function evidence(projectId, type, daysAgo = 0, extra = {}) {
  const timestamp = new Date(NOW - daysAgo * DAY_MS).toISOString();
  return normalizeEvidence({
    businessKey: 'personal',
    projectRegistryId: projectId,
    projectId: `legacy-${projectId}`,
    source: extra.source || (type === 'task_updated' ? 'airtable' : type.startsWith('codex_') ? 'codex' : type.startsWith('workspace_') ? 'desktop' : 'github'),
    type,
    event: extra.event || type,
    summary: extra.summary || `${projectId} ${type}`,
    timestamp,
    observedAt: new Date(NOW).toISOString(),
    actor: extra.actor || 'test-collector',
    externalId: extra.externalId || `${projectId}:${type}:${daysAgo}:${extra.index || 0}`,
    codexJobId: extra.codexJobId,
    confidence: extra.confidence ?? 1,
    branch: extra.branch,
    commitSha: extra.commitSha,
    deployment: extra.deployment,
    workspace: extra.workspace,
    pullRequest: extra.pullRequest,
    metadata: extra.metadata,
    provenance: { method: extra.method || 'test_fixture' },
  }, { assignedSource: extra.source || undefined, trusted: extra.trusted !== false, provenanceMethod: 'test_fixture' });
}

test('evidence normalization bounds data, redacts secrets, and blocks trusted-source impersonation', () => {
  const normalized = normalizeEvidence({
    businessKey: 'Agency', projectRegistryId: 'registry-1', source: 'manual', type: 'manual_note', event: 'operator_note',
    summary: 'Observed manually', timestamp: '2026-08-05T19:00:00Z', actor: 'mark', externalId: 'note-1',
    metadata: { apiToken: 'secret-value', note: 'Authorization: Bearer ghp_abcdefghijklmnopqrstuvwxyz' },
    provenance: { method: 'authenticated_note' },
  }, { assignedSource: 'manual', trusted: false });
  assert.equal(normalized.businessKey, 'agency');
  assert.equal(normalized.metadata.apiToken, '[REDACTED]');
  assert.match(normalized.metadata.note, /REDACTED/);
  assert.equal(normalized.provenance.trusted, false);
  assert.throws(() => normalizeManualEvidence({
    projectRegistryId: 'registry-1', source: 'github', type: 'commit', actor: 'mark', provenance: { method: 'manual' },
  }), { code: 'EVIDENCE_SOURCE_IMPERSONATION' });
  assert.throws(() => normalizeManualEvidence({
    projectRegistryId: 'registry-1', source: 'manual', type: 'deployment_completed', actor: 'mark', provenance: { method: 'manual' },
  }), { code: 'EVIDENCE_TYPE_IMPERSONATION' });
});

test('browser verification contract reports external/manual mode without inventing results', async () => {
  const provider = new BrowserVerificationProvider();
  for (const method of [
    'startVerification', 'getVerificationStatus', 'getScreenshots', 'getConsoleErrors',
    'getNetworkErrors', 'getAccessibilityResults', 'getInteractionResults', 'cancelVerification',
  ]) {
    const result = await provider[method]({ url: 'https://example.com' });
    assert.equal(result.status, 'external_required');
    assert.equal(result.mode, 'external_manual');
    assert.equal(result.verified, false);
  }
});

test('evidence store deduplicates, isolates businesses, persists restarts, serializes writes, and recovers backup', async () => {
  const root = await temporaryDataDir();
  try {
    const store = new ProjectEvidenceStore({ dataDir: root, maxHistory: 500 });
    const first = evidence('project-a', 'commit', 2, { commitSha: 'a'.repeat(40), externalId: 'commit:a' });
    const [one, two] = await Promise.all([
      store.append('personal', first, { assignedSource: 'github', trusted: true, provenanceMethod: 'test' }),
      store.append('personal', first, { assignedSource: 'github', trusted: true, provenanceMethod: 'test' }),
    ]);
    assert.equal(one.accepted.length + two.accepted.length, 1);
    await store.append('agency', { ...first, businessKey: 'agency', id: '', externalId: 'commit:agency' }, { assignedSource: 'github', trusted: true, provenanceMethod: 'test' });
    assert.equal((await store.list('personal')).length, 1);
    assert.equal((await store.list('agency')).length, 1);

    await store.append('personal', evidence('project-a', 'branch_updated', 1, { branch: 'main', externalId: 'branch:a' }), { assignedSource: 'github', trusted: true, provenanceMethod: 'test' });
    const reloaded = new ProjectEvidenceStore({ dataDir: root, maxHistory: 500 });
    assert.equal((await reloaded.list('personal')).length, 2);

    const file = reloaded.fileForBusiness('personal');
    await fs.writeFile(file, '{not valid json', 'utf8');
    const recovered = await reloaded.list('personal');
    assert.ok(recovered.length >= 1);
    const entries = await fs.readdir(path.dirname(file));
    assert.ok(entries.some((name) => name.startsWith('project-evidence.json.corrupt-')));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('GitHub ingestion collects repositories, commits, branches, pull requests, issues, and workflow evidence incrementally', async () => {
  const root = await temporaryDataDir();
  try {
    const store = new ProjectEvidenceStore({ dataDir: root });
    const githubProject = project('os1', 'M.A.R.C.U.S.', { repository: 'markgromer/os1' });
    const api = async (endpoint) => {
      if (endpoint.endsWith('/repos/markgromer/os1')) return { id: 10, default_branch: 'main', pushed_at: '2026-08-05T19:00:00Z', updated_at: '2026-08-05T19:00:00Z' };
      if (endpoint.includes('/commits?')) return [{ sha: 'a'.repeat(40), html_url: 'https://github.com/markgromer/os1/commit/a', commit: { message: 'Build evidence engine', author: { name: 'Mark', date: '2026-08-05T18:00:00Z' }, committer: { name: 'Mark', date: '2026-08-05T18:00:00Z' } }, author: { login: 'markgromer' } }];
      if (endpoint.includes('/branches?')) return [{ name: 'main', protected: true, commit: { sha: 'a'.repeat(40) } }];
      if (endpoint.includes('/pulls?')) return [{ number: 7, title: 'Evidence engine', state: 'open', draft: false, created_at: '2026-08-04T10:00:00Z', updated_at: '2026-08-05T17:00:00Z', user: { login: 'markgromer' }, head: { ref: 'evidence', sha: 'a'.repeat(40) }, base: { ref: 'main' }, html_url: 'https://github.com/markgromer/os1/pull/7' }];
      if (endpoint.includes('/issues?')) return [{ number: 9, title: 'Track focus shifts', state: 'open', created_at: '2026-08-01T10:00:00Z', updated_at: '2026-08-05T16:00:00Z', user: { login: 'markgromer' }, html_url: 'https://github.com/markgromer/os1/issues/9' }];
      if (endpoint.includes('/actions/runs?')) return { workflow_runs: [{ id: 100, name: 'Tests', status: 'completed', conclusion: 'success', head_branch: 'main', head_sha: 'a'.repeat(40), created_at: '2026-08-05T18:05:00Z', updated_at: '2026-08-05T18:10:00Z', run_attempt: 1, actor: { login: 'github-actions' } }] };
      throw new Error(`Unexpected endpoint ${endpoint}`);
    };
    const ingestor = new GitHubEvidenceIngestor({ api, store, minRefreshMs: 60_000 });
    const first = await ingestor.collectProject({ businessKey: 'personal', project: githubProject, force: true, nowMs: NOW });
    assert.equal(first.errors.length, 0);
    const types = new Set((await store.list('personal', { projectRegistryId: 'os1', limit: 100 })).map((item) => item.type));
    for (const type of ['repository_read', 'commit', 'branch_created', 'pull_request_opened', 'issue_updated', 'test_run']) assert.ok(types.has(type), type);
    const cached = await ingestor.collectProject({ businessKey: 'personal', project: githubProject, nowMs: NOW + 30_000 });
    assert.equal(cached.skipped, 'refresh_cache');
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('decay, focus shift, stale states, bottlenecks, and Airtable contradictions are deterministic and evidence-backed', () => {
  const marcus = project('os1', 'M.A.R.C.U.S.');
  const warren = project('warren', 'WARREN');
  const stale = project('stale', 'FlowKey', { createdAt: new Date(NOW - 42 * DAY_MS).toISOString() });
  const abandoned = project('abandoned', 'Old Build', { createdAt: new Date(NOW - 61 * DAY_MS).toISOString() });
  const bottleneck = project('bottleneck', 'Deploy Gap');
  const verifyGap = project('verify-gap', 'Verify Gap');
  const drift = project('drift', 'Codex Drift');
  const oneJob = project('one-job', 'One Codex Job');
  const contradiction = project('contradiction', 'Airtable Conflict');
  const projects = [marcus, warren, stale, abandoned, bottleneck, verifyGap, drift, oneJob, contradiction];
  const records = [];
  for (let i = 0; i < 11; i++) records.push(evidence('os1', 'commit', i / 24, { index: i, commitSha: String(i).padStart(40, '0') }));
  records.push(evidence('os1', 'workspace_active', 0, { source: 'desktop', workspace: { path: 'C:/os1', activeMinutes: 90 } }));
  records.push(evidence('os1', 'codex_job_started', 0, { source: 'codex', event: 'job_running', index: 1 }));
  records.push(evidence('warren', 'commit', 10, { commitSha: 'b'.repeat(40) }));
  for (let i = 0; i < 20; i++) records.push(evidence('bottleneck', 'commit', i / 2, { index: i, commitSha: `c${String(i).padStart(39, '0')}` }));
  records.push(evidence('bottleneck', 'codex_job_started', 1, { source: 'codex', event: 'job_running' }));
  records.push(evidence('verify-gap', 'production_published', 2, { source: 'render', deployment: { id: 'deploy-1', provider: 'render', environment: 'production', status: 'live' } }));
  records.push(evidence('drift', 'codex_job_started', 1, { source: 'codex', event: 'job_running', index: 1, codexJobId: 'job-a' }));
  records.push(evidence('drift', 'codex_job_updated', 2, { source: 'codex', event: 'job_registered', index: 2, codexJobId: 'job-b' }));
  records.push(evidence('one-job', 'codex_job_started', 1, { source: 'codex', event: 'job_running', index: 1, codexJobId: 'job-one' }));
  records.push(evidence('one-job', 'codex_job_updated', 2, { source: 'codex', event: 'job_registered', index: 2, codexJobId: 'job-one' }));
  records.push(evidence('contradiction', 'task_updated', 0, { source: 'airtable', metadata: { projectStatus: 'Active' } }));
  records.push(evidence('contradiction', 'commit', 46, { commitSha: 'd'.repeat(40) }));

  const currentCommit = decayForEvidence(records.find((item) => item.projectRegistryId === 'os1' && item.type === 'commit'), { nowMs: NOW });
  const oldCommit = decayForEvidence(evidence('warren', 'commit', 21, { commitSha: 'e'.repeat(40) }), { nowMs: NOW });
  assert.ok(currentCommit.contribution > oldCommit.contribution);
  assert.equal(oldCommit.halfLifeDays, 21);

  const focusActivity = calculateBusinessActivity({
    businessKey: 'personal', projects: [marcus, warren], evidence: records.filter((item) => ['os1', 'warren'].includes(item.projectRegistryId)), operations: [], nowMs: NOW,
    previousFocus: { currentFocusProject: { projectRegistryId: 'warren', projectName: 'WARREN' } },
  });
  assert.equal(focusActivity.currentFocus.currentFocusProject.projectRegistryId, 'os1');
  assert.equal(focusActivity.currentFocus.previousFocusProject.projectRegistryId, 'warren');
  assert.ok(focusActivity.currentFocus.focusShiftDetectedAt);
  assert.ok(focusActivity.currentFocus.evidence.length > 0);
  const activity = calculateBusinessActivity({ businessKey: 'personal', projects, evidence: records, operations: [], nowMs: NOW });
  assert.equal(activity.snapshots.find((item) => item.projectRegistryId === 'stale').state, 'stale');
  assert.equal(activity.snapshots.find((item) => item.projectRegistryId === 'abandoned').state, 'abandoned_candidate');
  assert.ok(activity.snapshots.find((item) => item.projectRegistryId === 'bottleneck').risks.some((item) => item.code === 'deployment_bottleneck'));
  assert.ok(activity.snapshots.find((item) => item.projectRegistryId === 'verify-gap').risks.some((item) => item.code === 'verification_gap'));
  assert.ok(activity.snapshots.find((item) => item.projectRegistryId === 'drift').risks.some((item) => item.code === 'codex_only_drift'));
  assert.equal(activity.snapshots.find((item) => item.projectRegistryId === 'one-job').risks.some((item) => item.code === 'codex_only_drift'), false);
  assert.ok(activity.snapshots.find((item) => item.projectRegistryId === 'contradiction').risks.some((item) => item.code === 'airtable_contradiction'));
  const bottleneckSnapshot = activity.snapshots.find((item) => item.projectRegistryId === 'bottleneck');
  assert.ok(bottleneckSnapshot.risks.find((item) => item.code === 'deployment_bottleneck').threshold.commitCount);
  assert.ok(bottleneckSnapshot.weightedContributions.length);
  assert.ok(bottleneckSnapshot.missingExpectedSignals.includes('deployment_completed'));
});

test('Codex lifecycle is reconstructed without counting a handoff as implementation or mutating terminal operations', async () => {
  const root = await temporaryDataDir();
  try {
    const registryProject = project('os1', 'M.A.R.C.U.S.');
    const operation = {
      id: 'op-1', projectRegistryId: 'os1', projectId: registryProject.projectId, title: 'Evidence work', status: 'completed',
      updatedAt: '2026-08-05T19:30:00Z', completedAt: '2026-08-05T19:30:00Z',
      steps: [{ id: 'codex-step', type: 'codex' }],
      activityLog: [
        { id: 'evt-1', type: 'external_codex_handoff_ready', stepId: 'codex-step', actor: 'codex-provider', timestamp: '2026-08-05T18:00:00Z', message: 'Handoff ready' },
        { id: 'evt-2', type: 'external_codex_job_registered', stepId: 'codex-step', actor: 'mark', timestamp: '2026-08-05T18:10:00Z', message: 'Job registered', data: { hasCommit: true, hasDiff: true } },
        { id: 'evt-3', type: 'operation_completed', actor: 'system', timestamp: '2026-08-05T19:30:00Z', message: 'Verified and completed' },
      ],
      metadata: { codexJobs: { 'codex-step': { recordId: 'record-1', jobId: 'job-1', provider: 'external_handoff', status: 'completed', branch: 'codex/evidence', startedAt: '2026-08-05T18:10:00Z', updatedAt: '2026-08-05T19:00:00Z', completedAt: '2026-08-05T19:00:00Z', diffSummary: 'Changed evidence files' } } },
      artifacts: [{ id: 'artifact-1', stepId: 'codex-step', type: 'commit', name: 'Codex commit', content: 'f'.repeat(40), createdAt: '2026-08-05T19:00:00Z' }],
      verification: [],
    };
    const before = structuredClone(operation);
    const service = new ProjectEvidenceService({ dataDir: root, listProjects: async () => [registryProject], listOperations: async () => [operation] });
    const refreshed = await service.refresh('personal', { sources: ['operations'], nowMs: NOW });
    assert.deepEqual(operation, before);
    const items = await service.listEvidence('personal', { limit: 100 });
    assert.ok(items.some((item) => item.event === 'handoff_created'));
    assert.ok(items.some((item) => item.event === 'job_registered'));
    assert.ok(items.some((item) => item.event === 'job_completed'));
    assert.ok(items.some((item) => item.event === 'result_verified'));
    const handoff = items.find((item) => item.event === 'handoff_created');
    assert.equal(decayForEvidence(handoff, { nowMs: NOW }).contribution, 0);
    const snapshot = refreshed.activity.snapshots[0];
    assert.equal(snapshot.codexJobs7d, 1);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('archived projects remain queryable but are excluded from activity and collection', async () => {
  const root = await temporaryDataDir();
  try {
    const active = project('active', 'Active Project');
    const archived = project('archived', 'Archived Project', { status: 'archived' });
    const service = new ProjectEvidenceService({
      dataDir: root,
      listProjects: async () => [active, archived],
      listOperations: async () => [{
        id: 'op-archived', projectRegistryId: archived.id, projectId: archived.projectId,
        title: 'Historical operation', status: 'completed', updatedAt: '2026-08-05T19:30:00Z',
        activityLog: [{ id: 'evt-archived', type: 'operation_completed', timestamp: '2026-08-05T19:30:00Z' }],
      }],
    });

    assert.equal((await service.assertProject('personal', archived.id)).id, archived.id);
    const refreshed = await service.refresh('personal', { sources: ['operations'], nowMs: NOW });
    assert.deepEqual(refreshed.activity.snapshots.map((item) => item.projectRegistryId), [active.id]);
    assert.equal((await service.listEvidence('personal', { projectRegistryId: archived.id })).length, 0);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('desktop activity aggregates bounded sessions and deployment ingestion requires exact mappings', async () => {
  const root = await temporaryDataDir();
  try {
    const trustedProject = project('desktop', 'Desktop Project', {
      localWorkspace: { path: 'C:\\work\\desktop', canonicalPath: 'C:\\work\\desktop', trustStatus: 'approved', desktopAgentId: 'agent-1' },
      deployments: { renderServiceId: 'srv-exact', productionUrl: 'https://desktop.example.com' },
    });
    const duplicateA = project('duplicate-a', 'Duplicate A', { deployments: { renderServiceId: 'srv-duplicate' } });
    const duplicateB = project('duplicate-b', 'Duplicate B', { deployments: { renderServiceId: 'srv-duplicate' } });
    let renderCalls = 0;
    const service = new ProjectEvidenceService({
      dataDir: root,
      listProjects: async () => [trustedProject, duplicateA, duplicateB],
      listOperations: async () => [],
      renderApi: async (endpoint) => {
        renderCalls++;
        assert.match(endpoint, /srv-exact/);
        return [{ deploy: { id: 'deploy-1', status: 'live', createdAt: '2026-08-05T17:00:00Z', updatedAt: '2026-08-05T18:00:00Z', branch: 'main', commit: { id: 'a'.repeat(40) } } }];
      },
    });
    const context = { processName: 'code', idleSeconds: 0, workspace: { workspacePath: 'C:\\work\\desktop', gitBranch: 'main', activeFile: 'server.js', recentFiles: ['README.md'] } };
    assert.equal((await service.recordDesktopContext('personal', { agentId: 'agent-1', context, nowMs: NOW - 10 * 60_000 })).recorded, true);
    assert.equal((await service.recordDesktopContext('personal', { agentId: 'agent-1', context, nowMs: NOW })).reconciled, true);
    const sessions = await service.listEvidence('personal', { projectRegistryId: 'desktop', source: 'desktop' });
    assert.equal(sessions.length, 1);
    assert.ok(sessions[0].workspace.activeMinutes >= 10);
    assert.deepEqual(new Set(sessions[0].workspace.filesObserved), new Set(['server.js', 'README.md']));
    const desktopActivity = await service.getActivity('personal', { nowMs: NOW });
    assert.ok(desktopActivity.snapshots.find((item) => item.projectRegistryId === 'desktop').desktopActiveMinutes7d >= 10);

    const deployments = await service.collectDeployments('personal', [trustedProject, duplicateA, duplicateB]);
    assert.equal(renderCalls, 1);
    assert.ok(deployments.results.some((item) => item.externalId === 'srv-duplicate' && item.skipped === 'low_confidence_mapping'));
    const deployEvidence = await service.listEvidence('personal', { projectRegistryId: 'desktop', source: 'render' });
    assert.equal(deployEvidence[0].type, 'production_published');
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('Codex provider lifecycle records follow-ups without persisting message content in evidence', async () => {
  const root = await temporaryDataDir();
  try {
    const registryProject = project('os1', 'M.A.R.C.U.S.');
    const service = new ProjectEvidenceService({
      dataDir: root,
      listProjects: async () => [registryProject],
      listOperations: async () => [],
    });
    const provider = new CodexProvider({
      onLifecycleEvent: (event) => service.recordCodexLifecycle(event),
    });
    const message = 'Inspect the private customer details before continuing.';
    await provider.sendFollowup({
      businessKey: 'personal', projectRegistryId: 'os1', operationId: 'op-1', stepId: 'step-1',
      recordId: 'record-1', jobId: 'job-1', provider: 'external_handoff', status: 'waiting_external',
    }, message);
    const items = await service.listEvidence('personal', { projectRegistryId: 'os1', source: 'codex' });
    assert.equal(items.length, 1);
    assert.equal(items[0].event, 'follow_up_sent');
    assert.equal(items[0].metadata.messageLength, message.length);
    assert.equal(JSON.stringify(items[0]).includes(message), false);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('activity chat tools return calculated evidence, thresholds, comparisons, and refresh results', async () => {
  const marcus = calculateProjectActivitySnapshot({
    businessKey: 'personal', project: project('os1', 'M.A.R.C.U.S.'), evidence: [evidence('os1', 'commit', 0, { commitSha: 'a'.repeat(40) })], operations: [], nowMs: NOW,
  });
  const stale = calculateProjectActivitySnapshot({
    businessKey: 'personal', project: project('old', 'Old Project', { createdAt: new Date(NOW - 42 * DAY_MS).toISOString() }), evidence: [], operations: [], nowMs: NOW,
  });
  const analysis = {
    snapshots: [marcus, stale],
    currentFocus: { currentFocusProject: { projectRegistryId: 'os1', projectName: 'M.A.R.C.U.S.' }, evidence: [{ id: 'e1', summary: 'Recent commit' }] },
    stale: [stale], bottlenecks: [], rules: { staleDays: 21 },
  };
  const service = {
    getActivity: async () => analysis,
    getProjectActivity: async (_business, id) => analysis.snapshots.find((item) => item.projectRegistryId === id),
    refresh: async () => ({ activity: analysis }),
  };
  assert.equal(getMarcusProjectActivityToolDefinitions().length, 8);
  const focus = await executeMarcusProjectActivityTool({ name: 'get_current_focus', args: {}, service, businessKey: 'personal' });
  assert.equal(focus.currentFocus.currentFocusProject.projectRegistryId, 'os1');
  assert.ok(focus.currentFocus.evidence.length);
  const staleResult = await executeMarcusProjectActivityTool({ name: 'list_stale_projects', args: {}, service, businessKey: 'personal' });
  assert.equal(staleResult.projects[0].state, 'stale');
  assert.equal(staleResult.thresholds.staleDays, 21);
  const compared = await executeMarcusProjectActivityTool({ name: 'compare_project_activity', args: { projectRegistryIds: ['os1', 'old'] }, service, businessKey: 'personal' });
  assert.equal(compared.projects.length, 2);
  const refreshed = await executeMarcusProjectActivityTool({ name: 'refresh_project_evidence', args: { sources: ['operations'] }, service, businessKey: 'personal' });
  assert.equal(refreshed.activity.snapshots.length, 2);
});
