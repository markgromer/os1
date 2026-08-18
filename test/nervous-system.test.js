import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { runOperatingLoopPass, startOperatingLoop } from '../marcus/nervous_system/operating_loop.js';
import { AttentionStore } from '../marcus/nervous_system/attention_store.js';
import { OutcomeLedger } from '../marcus/nervous_system/outcome_ledger.js';
import { ReflexEngine } from '../marcus/nervous_system/reflex_engine.js';
import { createSignal, signalMatches } from '../marcus/nervous_system/signal.js';
import { SignalBus } from '../marcus/nervous_system/signal_bus.js';
import { SignalJournal } from '../marcus/nervous_system/signal_journal.js';

test('signals normalize provenance and support exact and namespace pathway matching', () => {
  const signal = createSignal({ type: 'Operation.Status Changed', source: 'Operation Monitor', businessKey: 'My Business' });
  assert.equal(signal.type, 'operation.status_changed');
  assert.equal(signal.source, 'operation_monitor');
  assert.equal(signal.businessKey, 'my-business');
  assert.equal(signalMatches('operation.*', signal.type), true);
  assert.equal(signalMatches('evidence.*', signal.type), false);
  assert.ok(signal.id.startsWith('sig_'));
  assert.ok(signal.traceId);
});

test('signal bus routes in priority order, isolates pathway failure, and journals delivery', async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-nerves-'));
  const failures = [];
  const order = [];
  const journal = new SignalJournal({ dataDir: temp });
  const bus = new SignalBus({ journal, onError: (error) => failures.push(error.message) });
  bus.register({ name: 'late', accepts: ['operation.*'], priority: 20, handle: () => order.push('late') });
  bus.register({ name: 'broken', accepts: ['*'], priority: 10, handle: () => { throw new Error('reflex failed'); } });
  bus.register({ name: 'early', accepts: ['operation.status.changed'], priority: 5, handle: () => order.push('early') });

  const result = await bus.publish({ type: 'operation.status.changed', source: 'test', businessKey: 'personal' });
  assert.deepEqual(order, ['early', 'late']);
  assert.deepEqual(failures, ['reflex failed']);
  assert.equal(result.delivery.handled.length, 2);
  assert.equal(result.delivery.failed.length, 1);
  const entries = await journal.recent('personal');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].signal.type, 'operation.status.changed');
});

test('journal failure degrades delivery evidence without cancelling completed pathways', async () => {
  const errors = [];
  const bus = new SignalBus({ journal: { append: async () => { throw new Error('disk unavailable'); } }, onError: (error) => errors.push(error.message) });
  let handled = false;
  bus.register({ name: 'reflex', handle: () => { handled = true; } });
  const result = await bus.publish({ type: 'system.test', source: 'test' });
  assert.equal(handled, true);
  assert.equal(result.delivery.failed[0].pathway, 'signal-journal');
  assert.deepEqual(errors, ['disk unavailable']);
});

test('operating loop senses, routes, maintains homeostasis, and prevents overlapping cycles', async () => {
  const received = [];
  const bus = new SignalBus();
  bus.register({ name: 'receiver', accepts: ['system.*'], handle: (signal) => received.push(signal.type) });
  let maintained = 0;
  const pass = await runOperatingLoopPass({
    bus,
    cycle: 4,
    sensors: [{ name: 'proprioception', sense: () => ({ type: 'system.health.observed', businessKey: 'personal' }) }],
    homeostasis: [async () => { maintained += 1; }],
  });
  assert.deepEqual(received, ['system.health.observed']);
  assert.equal(maintained, 1);
  assert.deepEqual(pass, { cycle: 4, sensors: 1, signals: 1, handled: 1, failed: 0, maintenance: 1 });

  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  const timers = { setTimeout: () => 1, setInterval: () => 2, clearTimeout() {}, clearInterval() {} };
  const loop = startOperatingLoop({ bus, sensors: [{ name: 'slow', sense: async () => { await pending; } }], timers });
  const first = loop.tick();
  assert.equal(await loop.tick(), null);
  release();
  await first;
  assert.equal(loop.cycle, 1);
  assert.equal(loop.health().status, 'healthy');
  assert.equal(loop.health().skippedOverlaps, 1);
  loop.stop();
  assert.equal(loop.health().status, 'stopped');
});

test('cadence skips slower sensors until their scheduled cycle', async () => {
  const received = [];
  const bus = new SignalBus();
  bus.register({ name: 'receiver', accepts: ['*'], handle: (signal) => received.push(signal.type) });
  const sensors = [
    { name: 'fast', everyCycles: 1, sense: () => ({ type: 'fast.observed', businessKey: 'personal' }) },
    { name: 'slow', everyCycles: 3, sense: () => ({ type: 'slow.observed', businessKey: 'personal' }) },
  ];
  await runOperatingLoopPass({ bus, sensors, cycle: 1 });
  await runOperatingLoopPass({ bus, sensors, cycle: 2 });
  await runOperatingLoopPass({ bus, sensors, cycle: 3 });
  await runOperatingLoopPass({ bus, sensors, cycle: 4 });
  assert.deepEqual(received, ['fast.observed', 'slow.observed', 'fast.observed', 'fast.observed', 'fast.observed', 'slow.observed']);
});

test('attention deduplicates recurring signals and preserves ownership transitions', async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-attention-'));
  const store = new AttentionStore({ dataDir: temp });
  const first = await store.raise('personal', { fingerprint: 'provider:uncertain:one', signalType: 'provider.uncertain', title: 'Provider uncertain', owner: 'mark' });
  const repeated = await store.raise('personal', { fingerprint: 'provider:uncertain:one', signalType: 'provider.uncertain', title: 'Provider still uncertain', owner: 'mark' });
  assert.equal(repeated.id, first.id);
  assert.equal(repeated.occurrences, 2);
  const resolved = await store.transition('personal', first.id, 'resolved', { resolution: 'Authoritative read-back passed.' });
  assert.equal(resolved.status, 'resolved');
  assert.equal((await store.list('personal', { status: 'open' })).length, 0);
  const deferred = await store.raise('personal', { fingerprint: 'later', signalType: 'project.decaying', title: 'Revisit later' });
  await store.transition('personal', deferred.id, 'deferred', { deferUntil: '2026-01-01T00:00:00.000Z' });
  assert.equal((await store.reopenDue('personal', new Date('2026-01-02T00:00:00.000Z')))[0].status, 'open');
});

test('reflex failures become durable outcomes and attention instead of stopping the arc', async () => {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-reflex-'));
  const attentionStore = new AttentionStore({ dataDir: temp });
  const outcomeLedger = new OutcomeLedger({ dataDir: temp });
  const engine = new ReflexEngine({ attentionStore, outcomeLedger });
  engine.register({ name: 'broken-recovery', when: () => true, act: async () => { throw new Error('read-back unavailable'); }, owner: 'mark' });
  engine.register({ name: 'safe-followup', priority: 200, when: () => true, act: async () => ({ response: 'Retry remained inhibited.', status: 'succeeded' }) });
  const signal = createSignal({ type: 'provider.state.uncertain', source: 'test', businessKey: 'personal', subject: { type: 'operation', id: 'op1' }, severity: 'warning' });
  const results = await engine.handle(signal);
  assert.deepEqual(results.map((item) => item.status), ['failed', 'succeeded']);
  assert.equal((await attentionStore.list('personal', { status: 'open' }))[0].owner, 'mark');
  const outcomes = await outcomeLedger.list('personal');
  assert.deepEqual(new Set(outcomes.map((item) => item.status)), new Set(['failed', 'succeeded']));
  const corrected = await outcomeLedger.correct('personal', outcomes[0].id, 'Mark supplied a verified recovery method.', { reusable: true });
  assert.equal(corrected.reusable, true);
});
