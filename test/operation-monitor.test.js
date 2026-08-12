import assert from 'node:assert/strict';
import test from 'node:test';

import { runOperationMonitorPass, startOperationMonitor } from '../marcus/operations/operation_monitor.js';

test('operation monitor advances only execution-safe nonterminal states', async () => {
  const operations = [
    { id: 'queued', status: 'queued' },
    { id: 'running', status: 'running' },
    { id: 'provider', status: 'awaiting_provider' },
    { id: 'verifying', status: 'verifying' },
    { id: 'approval', status: 'waiting_for_approval' },
    { id: 'blocked', status: 'blocked' },
    { id: 'paused', status: 'paused' },
    { id: 'recovery', status: 'recovery_required' },
    { id: 'completed', status: 'completed' },
  ];
  const ticks = [];
  const engine = {
    async listOperations(_businessKey, filters) {
      assert.deepEqual(filters.status, ['queued', 'running', 'awaiting_provider', 'verifying']);
      return operations.filter((operation) => filters.status.includes(operation.status));
    },
    async tick(businessKey, operationId) { ticks.push(`${businessKey}:${operationId}`); },
  };
  const result = await runOperationMonitorPass({ engine, businessKeys: ['personal'] });
  assert.deepEqual(ticks.sort(), ['personal:provider', 'personal:queued', 'personal:running', 'personal:verifying']);
  assert.deepEqual(result, { businesses: 1, inspected: 4, ticked: 4, failed: 0 });
});

test('operation monitor serializes passes and can be stopped cleanly', async () => {
  let release;
  let tickCount = 0;
  const pending = new Promise((resolve) => { release = resolve; });
  const scheduled = [];
  const cleared = [];
  const timers = {
    setTimeout(fn) { scheduled.push(fn); return 1; },
    setInterval(fn) { scheduled.push(fn); return 2; },
    clearTimeout(id) { cleared.push(id); },
    clearInterval(id) { cleared.push(id); },
  };
  const monitor = startOperationMonitor({
    engine: {
      async listOperations() { return [{ id: 'op1', status: 'running' }]; },
      async tick() { tickCount += 1; await pending; },
    },
    listBusinessKeys: async () => ['personal'],
    timers,
  });
  const first = monitor.tick();
  const overlapping = await monitor.tick();
  assert.equal(overlapping, null);
  for (let attempt = 0; tickCount === 0 && attempt < 10; attempt += 1) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(tickCount, 1);
  release();
  await first;
  monitor.stop();
  assert.deepEqual(cleared, [1, 2]);
});
