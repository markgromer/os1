import test from 'node:test';
import assert from 'node:assert/strict';
import { workFixture } from './helpers/work-fixture.js';

test('operation metadata survives normalization and fences direct provider/start paths', async () => {
  const f = await workFixture(); const work = await f.create('Implement policy-bound work');
  const bound = await f.graph.launch('personal', work.id);
  const operation = await f.engine.getOperation('personal', bound.operationId);
  assert.equal(operation.metadata.extra.workItemId, work.id);
  assert.equal(operation.metadata.extra.workAttemptId, bound.launchToken);
  await f.memory.add('personal', { kind: 'decision', projectId: f.project.id, content: 'Require a new owner decision before this execution.', sourceRefs: ['owner:test'] });
  await assert.rejects(f.engine.startOperation('personal', operation.id), /decisions changed/);
  await assert.rejects(f.engine.runner.invokeProvider({ operation, step: operation.steps[0], registryRecord: f.project, idempotencyKey: 'fixture' }), /decisions changed/);
  assert.equal((await f.engine.getOperation('personal', operation.id)).providerActions.length, 0);
});

test('reconciliation recovers the exact operation created before a binding interruption', async () => {
  const f = await workFixture(); const work = await f.create('Recover reserved local launch');
  const bound = await f.graph.launch('personal', work.id);
  await f.graph.store.mutate('personal', (doc) => { const item = doc.items.find((row) => row.id === work.id); item.operationId = ''; item.launchState = 'creating'; item.status = 'ready'; return null; });
  await f.graph.reconcile('personal'); const recovered = (await f.graph.snapshot('personal')).items.find((row) => row.id === work.id);
  assert.equal(recovered.operationId, bound.operationId); assert.equal(recovered.launchState, 'bound');
  assert.equal((await f.engine.store.listAll('personal')).length, 1);
});

test('cancelled ready work cannot be launched', async () => {
  const f = await workFixture(); const work = await f.create('Do not execute cancelled work');
  await f.graph.cancel('personal', work.id, 'Owner withdrew this task.');
  await assert.rejects(f.graph.launch('personal', work.id), /cancelled/);
  assert.equal((await f.engine.store.listAll('personal')).length, 0);
});
