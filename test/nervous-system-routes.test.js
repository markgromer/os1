import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import express from 'express';

import { registerNervousSystemRoutes } from '../marcus/api/nervous_system_routes.js';
import { AttentionStore } from '../marcus/nervous_system/attention_store.js';
import { OutcomeLedger } from '../marcus/nervous_system/outcome_ledger.js';
import { SignalJournal } from '../marcus/nervous_system/signal_journal.js';

test('nervous-system routes expose health and support explicit attention and correction transitions', async (t) => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-nerve-routes-'));
  const attentionStore = new AttentionStore({ dataDir });
  const outcomeLedger = new OutcomeLedger({ dataDir });
  const signalJournal = new SignalJournal({ dataDir });
  const attention = await attentionStore.raise('personal', { signalType: 'test.warning', title: 'Review this', owner: 'mark' });
  const outcome = await outcomeLedger.record('personal', { pathway: 'test-reflex', response: 'Observed.', status: 'succeeded' });
  await signalJournal.append({ id: 'sig_test', type: 'test.warning', source: 'test', businessKey: 'personal', severity: 'warning' }, { handled: [], failed: [] });
  let ticks = 0;
  const app = express(); app.use(express.json());
  registerNervousSystemRoutes(app, { attentionStore, outcomeLedger, signalJournal, getBusinessKey: () => 'personal', getLoopHealth: () => ({ status: 'healthy', cycle: 7 }), triggerLoop: async () => { ticks += 1; return { cycle: 8 }; } });
  const server = app.listen(0, '127.0.0.1'); t.after(() => server.close());
  await new Promise((resolve) => server.once('listening', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;

  const status = await (await fetch(`${base}/api/marcus/nervous-system/status`)).json();
  assert.equal(status.loop.status, 'healthy'); assert.equal(status.attention.length, 1); assert.equal(status.signals.length, 1); assert.equal(status.outcomes.length, 1);
  const resolved = await (await fetch(`${base}/api/marcus/attention/${attention.id}`, { method: 'PATCH', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ status: 'resolved', resolution: 'Handled.' }) })).json();
  assert.equal(resolved.item.status, 'resolved');
  const corrected = await (await fetch(`${base}/api/marcus/outcomes/${outcome.id}/correction`, { method: 'PATCH', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ correction: 'Use the verified method.', reusable: true }) })).json();
  assert.equal(corrected.outcome.reusable, true);
  const ticked = await (await fetch(`${base}/api/marcus/nervous-system/tick`, { method: 'POST' })).json();
  assert.equal(ticked.result.cycle, 8); assert.equal(ticks, 1);
});
