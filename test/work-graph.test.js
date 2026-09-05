import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { WorkGraph } from '../marcus/work/work_graph.js';
import { DomainStore } from '../marcus/operations/domain_store.js';
import { createOperationsEngine } from '../marcus/operations/operation_engine.js';

async function fixture() {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-graph-'));
  const engine = createOperationsEngine({ dataDir });
  const project = await engine.registry.create('personal', { canonicalName: 'MARCUS graph test', projectId: 'marcus-local' });
  const graph = new WorkGraph({ dataDir, engine });
  engine.setWorkGuard((key, operation) => graph.assertOperationReady(key, operation));
  return { dataDir, engine, project, graph, create: (objective, extra = {}) => graph.create('personal', { projectId: project.id, objective, acceptanceCriteria: [objective], ...extra }) };
}

test('graph rejects cycles and cross-scope links, explains blockers, and survives restart', async () => {
  const { graph, create, dataDir, engine } = await fixture();
  const parent = await create('Build graph feature', { kind: 'objective' });
  const first = await create('Implement readiness', { parentId: parent.id });
  const next = await create('Verify readiness', { parentId: parent.id });
  await graph.addDependency('personal', { itemId: next.id, prerequisiteId: first.id, type: 'work' });
  await assert.rejects(graph.addDependency('personal', { itemId: first.id, prerequisiteId: next.id, type: 'work' }), /cycle/i);
  await assert.rejects(graph.addDependency('personal', { itemId: first.id, prerequisiteId: parent.id, type: 'work' }), /cycle/i);
  await assert.rejects(graph.launch('personal', next.id), /Waiting for work/);
  const restored = new WorkGraph({ dataDir, engine });
  const state = await restored.snapshot('personal');
  assert.equal(state.items.find((item) => item.id === next.id).readiness.runnable, false);
  assert.deepEqual(new Set(await restored.impact('personal', first.id)), new Set([first.id, next.id, parent.id]));
  assert.equal((await restored.snapshot('agency')).items.length, 0);
  await assert.rejects(restored.snapshot('../personal'), /exact business/);
});

test('one work item binds one operation; verified completion unblocks downstream exactly once', async () => {
  const { graph, create, engine } = await fixture();
  const first = await create('Implement verification gate');
  const second = await create('Document verification gate');
  await graph.addDependency('personal', { itemId: second.id, prerequisiteId: first.id, type: 'work' });
  const launches = await Promise.allSettled([graph.launch('personal', first.id), graph.launch('personal', first.id)]);
  assert.ok(launches.some((result) => result.status === 'fulfilled'));
  const operations = await engine.store.listAll('personal');
  assert.equal(operations.length, 1);
  const operation = operations[0];
  // Unit fixture only: drive the existing operation record through both outcomes.
  await engine.store.update('personal', operation.id, (draft) => { draft.status = 'completed'; draft.verification = []; return draft; });
  assert.deepEqual(await graph.reconcile('personal'), []);
  await engine.store.update('personal', operation.id, (draft) => { draft.verification = [{ id: 'verify_1', type: 'test', required: true, status: 'passed' }]; return draft; });
  assert.deepEqual(await graph.reconcile('personal'), [first.id]);
  assert.deepEqual(await graph.reconcile('personal'), []);
  assert.equal((await graph.snapshot('personal')).items.find((item) => item.id === second.id).readiness.runnable, true);
  assert.equal((await graph.store.read('personal')).outbox.filter((event) => event.type === 'work.completed').length, 1);
});

test('approval and verification dependencies use exact durable records', async () => {
  const { graph, create, engine, project } = await fixture();
  const operation = await engine.createOperation('personal', { objective: 'Gate source', projectRegistryId: project.id });
  const item = await create('Wait for approval');
  await graph.addDependency('personal', { itemId: item.id, operationId: operation.id, requirementId: 'approval_exact', type: 'approval' });
  assert.equal((await graph.snapshot('personal')).items[0].readiness.needsMark, true);
  await engine.store.update('personal', operation.id, (draft) => { draft.approvals = [{ id: 'approval_wrong', status: 'approved' }]; return draft; });
  assert.equal((await graph.snapshot('personal')).items[0].readiness.runnable, false);
  await engine.store.update('personal', operation.id, (draft) => { draft.approvals = [{ id: 'approval_exact', status: 'approved', expiresAt: '2099-01-01T00:00:00Z' }]; return draft; });
  assert.equal((await graph.snapshot('personal')).items[0].readiness.runnable, true);
});

test('an indeterminate operation-creation attempt is not blindly relaunched', async () => {
  const { graph, create, engine } = await fixture();
  const item = await create('Create a durable operation');
  engine.createFromRequest = async () => { throw new Error('interrupted'); };
  await assert.rejects(graph.launch('personal', item.id), /interrupted/);
  await graph.reconcile('personal');
  await assert.rejects(graph.launch('personal', item.id), /reconcile/i);
});

test('domain persistence serializes instances, guards revisions, and preserves corrupt primary', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-domain-'));
  const options = { dataDir, name: 'test-domain', empty: () => ({ items: [] }), validate: (doc) => Array.isArray(doc.items) };
  const a = new DomainStore(options); const b = new DomainStore(options);
  await Promise.all(Array.from({ length: 12 }, (_, i) => (i % 2 ? a : b).mutate('personal', (doc) => { doc.items.push(i); return i; })));
  assert.equal((await a.read('personal')).items.length, 12);
  await assert.rejects(a.mutate('personal', () => null, { revision: 0 }), /revision/i);
  await fs.writeFile(a.file('personal'), 'broken fixture');
  assert.equal((await b.read('personal')).items.length, 11);
  await b.mutate('personal', (doc) => { doc.items.push('recovered'); return null; });
  assert.equal((await a.read('personal')).items.length, 12);
  assert.ok((await fs.readdir(path.dirname(a.file('personal')))).some((name) => name.includes('.corrupt-')));
});
