import test from 'node:test';
import assert from 'node:assert/strict';
import { workFixture } from './helpers/work-fixture.js';
import { readWorkOverview, workOverviewReply } from '../marcus/work/work_overview.js';

test('overview projects existing domains without dispatch, keeps scope, and does not trust deployment prose', async () => {
  const f = await workFixture();
  const task = await f.create('Verify dedicated display');
  await f.create('Other secret work', { projectId: f.other.id });
  const old = '2026-09-04T00:00:00Z'; const recent = '2026-09-05T00:00:00Z';
  const evidence = { store: { readDocument: async () => ({ evidence: [
    { id: 'fake', projectRegistryId: f.project.id, provenance: { trusted: false }, source: 'manual', type: 'production_published', timestamp: recent, deployment: { environment: 'production', status: 'live' } },
    { id: 'old', projectRegistryId: f.project.id, provenance: { trusted: true }, source: 'render', type: 'production_published', timestamp: old, deployment: { environment: 'production', status: 'live' } },
    { id: 'latest', projectRegistryId: f.project.id, provenance: { trusted: true }, source: 'render', type: 'deployment_failed', timestamp: recent, deployment: { environment: 'production', status: 'failed' } },
    { id: 'other', projectRegistryId: f.other.id, provenance: { trusted: true }, source: 'render', type: 'production_published', timestamp: recent, deployment: { environment: 'production', status: 'live' } },
  ] }) } };
  const overview = await readWorkOverview('personal', { ...f, evidence });
  const project = overview.projects.find((row) => row.id === f.project.id);
  assert.equal(project.workCount, 1); assert.equal(project.items[0].id, task.id);
  assert.equal(project.deployment.id, 'latest'); assert.equal(project.deployment.status, 'failed');
  assert.equal(project.engineering.autoAdvance, false); assert.equal(project.engineering.granted, false);
  assert.equal((await f.graph.operations('personal')).size, 0);
  assert.match(workOverviewReply(project), /not automatically authorized/);
  assert.doesNotMatch(workOverviewReply(project), /Other secret/);
  assert.deepEqual((await readWorkOverview('agency', { ...f, evidence })).projects, []);
});

test('overview explicitly marks unavailable evidence and only verifies executions with required checks', async () => {
  const f = await workFixture();
  f.graph.operations = async () => new Map([
    ['reported', { id: 'reported', projectRegistryId: f.project.id, status: 'completed', updatedAt: '2026-09-05', verification: [] }],
    ['checked', { id: 'checked', projectRegistryId: f.project.id, status: 'completed', updatedAt: '2026-09-04', verification: [{ type: 'test', status: 'passed', required: true }] }],
    ['approval', { id: 'approval', projectRegistryId: f.project.id, title: 'Approve the exact execution', status: 'waiting_for_approval', updatedAt: '2026-09-03', verification: [] }],
  ]);
  const overview = await readWorkOverview('personal', { ...f, evidence: { store: { readDocument: async () => { throw new Error('unavailable'); } } } });
  assert.equal(overview.evidenceAvailable, false);
  const project = overview.projects.find((row) => row.id === f.project.id);
  assert.equal(project.deployment, null); assert.deepEqual(project.operations.map((row) => row.verified), [false, true, false]);
  assert.equal(project.needsYouCount, 1); assert.equal(project.attention[0].id, 'approval');
  assert.match(workOverviewReply(project), /requires an exact owner approval/);
});

test('work saves are idempotent per business and project, and conflicting retries fail closed', async () => {
  const f = await workFixture();
  const input = { projectId: f.project.id, objective: 'Real follow-up', acceptanceCriteria: ['Visible evidence'], clientRequestId: 'save-one' };
  const first = await f.graph.create('personal', input);
  const retry = await f.graph.create('personal', input);
  assert.equal(first.id, retry.id);
  assert.equal((await f.graph.snapshot('personal')).items.length, 1);
  await assert.rejects(f.graph.create('personal', { ...input, objective: 'Changed objective' }), /different work/);
  const separate = await f.graph.create('personal', { ...input, projectId: f.other.id });
  assert.notEqual(first.id, separate.id);
  assert.equal((await f.graph.operations('personal')).size, 0);
});

test('overview indexes project-owned records once instead of scanning all records for each project', async () => {
  const f = await workFixture(); let projectKeyReads = 0;
  const projects = Array.from({ length: 100 }, (_, index) => ({ id: 'project-' + index, canonicalName: 'Project ' + index }));
  const items = projects.map((project, index) => ({ id: 'item-' + index, get projectId() { projectKeyReads += 1; return project.id; },
    objective: project.canonicalName, acceptanceCriteria: [], readiness: { needsMark: false, runnable: true, status: 'ready', blockers: [] } }));
  f.graph.engine.registry.list = async () => projects;
  f.graph.snapshot = async () => ({ items, dependencies: [{ itemId: 'item-1', prerequisiteId: 'item-0' }] });
  f.graph.operations = async () => new Map();
  const result = await readWorkOverview('personal', { ...f, evidence: { store: { readDocument: async () => ({ evidence: [] }) } } });
  assert.equal(projectKeyReads, 100); assert.equal(result.projects.length, 100);
  assert.ok(result.projects.every((project) => project.workCount === 1 && project.readyCount === 1));
  assert.equal(result.projects[1].dependencies.length, 1); assert.equal(result.projects[2].dependencies.length, 0);
});
