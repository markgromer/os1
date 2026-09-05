import test from 'node:test';
import assert from 'node:assert/strict';
import { workFixture } from './helpers/work-fixture.js';
import { DurableExecution } from '../marcus/work/durable_execution.js';

test('durable outbox is retryable and queues newly unblocked work once across restart', async () => {
  const f = await workFixture(); const first = await f.create('First unit work'); const next = await f.create('Downstream unit work');
  await f.graph.addDependency('personal', { itemId: next.id, prerequisiteId: first.id, type: 'work' });
  let failOnce = true;
  f.bus.register({ name: 'test-flaky-observer', handle: () => { if (failOnce) { failOnce = false; throw new Error('injected observer failure'); } } });
  await f.execution.drainOutbox('personal'); await f.execution.drainOutbox('personal');
  assert.equal((await f.execution.store.read('personal')).runs.length, 1);
  await f.graph.store.mutate('personal', (doc) => { doc.items.find((row) => row.id === first.id).status = 'completed'; return null; });
  const event = { id: 'unit-completion-event', version: 1, type: 'work.completed', businessKey: 'personal', projectId: f.project.id, subjectId: first.id, correlationId: first.id, causationId: 'unit-receipt' };
  await f.execution.receive('personal', event);
  const restored = new DurableExecution({ dataDir: f.dataDir, graph: f.graph, director: f.director, bus: f.bus });
  await restored.receive('personal', event); await restored.receive('personal', { ...event, id: 'second-delivery-same-logical-work' });
  const doc = await restored.store.read('personal');
  assert.equal(doc.runs.filter((row) => row.workId === next.id).length, 1);
  assert.equal(doc.receipts.filter((row) => row.eventId === event.id).length, 1);
  assert.equal(await restored.claim('personal', 'worker'), null, 'automatic advancement defaults off');
});

test('leases fence stale workers; checkpoints, retry budget, dead letters and schedules survive restart', async () => {
  let time = Date.now(); const f = await workFixture({ now: () => time, leaseMs: 100 });
  const schedule = await f.execution.schedule('personal', { projectId: f.project.id, everyMs: 60000, firstDueAt: new Date(time).toISOString() });
  await Promise.all([f.execution.queueSchedules('personal'), f.execution.queueSchedules('personal')]);
  assert.equal((await f.execution.store.read('personal')).runs.length, 1);
  const first = await f.execution.claim('personal', 'worker-a'); time += 101;
  const restored = new DurableExecution({ dataDir: f.dataDir, graph: f.graph, director: f.director, now: () => time, leaseMs: 100 });
  const second = await restored.claim('personal', 'worker-b');
  assert.equal(second.id, first.id); assert.notEqual(second.lease.token, first.lease.token);
  await assert.rejects(restored.settle('personal', first.id, first.lease.token, {}), /no longer owns/);
  await restored.checkpoint('personal', second.id, second.lease.token, 'reconciled', { operationId: 'unit-receipt' });
  await restored.settle('personal', second.id, second.lease.token, { error: { code: 'INJECTED' } }); time += 10000;
  const third = await restored.claim('personal', 'worker-c'); await restored.settle('personal', third.id, third.lease.token, { error: { code: 'INJECTED' } });
  const doc = await restored.store.read('personal'); assert.equal(doc.runs[0].status, 'dead_letter'); assert.equal(doc.runs[0].attempts, 3);
  assert.ok(doc.runs[0].checkpoints.some((row) => row.phase === 'lease_expired'));
  time += 10 * 60000; await restored.queueSchedules('personal');
  assert.equal((await restored.store.read('personal')).runs.length, 2, 'missed periods coalesce');
  assert.equal((await restored.store.read('personal')).schedules[0].id, schedule.id);
});

test('proactive summaries retain evidence, respect busy/away and hourly budgets, and resolve routine alerts', async () => {
  let time = Date.now(); const f = await workFixture({ now: () => time });
  for (let i = 0; i < 3; i++) {
    const work = await f.create(`Owner decision ${i}`);
    await f.graph.store.mutate('personal', (doc) => { doc.items.find((row) => row.id === work.id).blockers.push({ type: 'owner', message: `Owner choice ${i} is required.` }); return null; });
  }
  await f.operator.setPresence('personal', { presence: 'busy', maxInterruptionsPerHour: 2 });
  const busy = await f.operator.pass('personal'); assert.equal(busy.needsMark.length, 3); assert.equal((await f.attention.list('personal')).length, 0);
  await f.operator.setPresence('personal', { presence: 'available', maxInterruptionsPerHour: 2 });
  await f.operator.pass('personal'); await f.operator.pass('personal');
  assert.equal((await f.attention.list('personal')).length, 2); assert.equal((await f.operator.store.read('personal')).digests.length, 1);
  time += 3600001; await f.operator.pass('personal'); assert.equal((await f.attention.list('personal')).length, 3);
  await f.graph.store.mutate('personal', (doc) => { for (const item of doc.items) item.blockers = []; return null; });
  const summary = await f.operator.pass('personal'); assert.equal(summary.needsMark.length, 0); assert.equal(summary.opportunities.length, 3); assert.equal(summary.canContinue.length, 0);
  assert.equal((await f.attention.list('personal', { status: 'resolved' })).length, 3); assert.ok(summary.away.changes.every((row) => row.id));
});

test('queued advancement waits for an active matching director grant without spending retry attempts', async () => {
  const f = await workFixture(); const work = await f.create('Wait for director authority');
  await f.execution.setPolicy('personal', f.project.id, true);
  await f.execution.drainOutbox('personal');
  for (const configuration of [null, { lifecycle: 'paused', projectIds: [f.project.id] }, { lifecycle: 'active', projectIds: [f.other.id] }]) {
    if (configuration) await f.director.configure('personal', configuration);
    for (let attempt = 0; attempt < 4; attempt++) assert.deepEqual(await f.execution.pass('personal'), []);
    const pending = (await f.execution.store.read('personal')).runs.find((run) => run.workId === work.id);
    assert.equal(pending.status, 'queued'); assert.equal(pending.attempts, 0);
  }
  await f.director.configure('personal', { lifecycle: 'active', projectIds: [f.project.id] });
  const claimed = await f.execution.claim('personal', 'resumed-worker');
  assert.equal(claimed.workId, work.id); assert.equal(claimed.attempts, 1);
});
