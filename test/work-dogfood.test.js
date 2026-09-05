import test from 'node:test';
import assert from 'node:assert/strict';
import { workReadiness } from '../marcus/work/work_graph.js';
import { workFixture } from './helpers/work-fixture.js';

test('missing or cross-project bound operation is recovery-required, never runnable', () => {
  const item = { id: 'work_local', projectId: 'project_local', kind: 'task', status: 'running', operationId: 'op_absent', blockers: [], invalidatedBy: [] };
  const doc = { items: [item], dependencies: [] };
  for (const operations of [new Map(), new Map([['op_absent', { id: 'op_absent', projectRegistryId: 'another_project', status: 'running' }]])]) {
    const readiness = workReadiness(doc, item, operations);
    assert.equal(readiness.runnable, false);
    assert.equal(readiness.status, 'blocked');
    assert.ok(readiness.blockers.some((row) => row.type === 'recovery' && row.message.includes('reconcile evidence')));
  }
});

test('valid running bindings and unbound ready work preserve existing readiness', () => {
  const item = { id: 'work_local', projectId: 'project_local', kind: 'task', status: 'running', operationId: 'op_present', blockers: [], invalidatedBy: [] };
  const doc = { items: [item], dependencies: [] };
  const present = new Map([['op_present', { id: 'op_present', projectRegistryId: 'project_local', status: 'running' }]]);
  assert.equal(workReadiness(doc, item, present).status, 'running');
  assert.deepEqual(workReadiness(doc, item, present).blockers, []);
  assert.equal(workReadiness(doc, { ...item, status: 'ready', operationId: '' }, new Map()).runnable, true);
});

test('proactive operator does not promise that missing-operation work can continue', async () => {
  const fixture = await workFixture();
  const work = await fixture.create('Missing execution evidence must be reconciled');
  await fixture.graph.store.mutate('personal', (doc) => {
    const item = doc.items.find((row) => row.id === work.id);
    item.operationId = 'op_no_receipt'; item.status = 'running'; item.launchState = 'bound'; return null;
  });
  const summary = await fixture.operator.summary('personal');
  assert.equal(summary.canContinue.some((row) => row.id === work.id), false);
  assert.ok(summary.anomalies.some((row) => row.id === work.id && row.evidence.some((entry) => entry.type === 'work')));
  await assert.rejects(fixture.graph.assertOperationReady('personal', { id: 'op_no_receipt', projectRegistryId: fixture.project.id, metadata: { extra: { workItemId: work.id } } }), /not|blocked|stale/i);
});
