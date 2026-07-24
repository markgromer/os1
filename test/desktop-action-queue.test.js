import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { DesktopActionQueue } from '../marcus/operations/desktop_action_queue.js';

async function temporaryQueue(options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-desktop-queue-'));
  return {
    root,
    queue: new DesktopActionQueue({ dataDir: root, ...options }),
    async close() { await fs.rm(root, { recursive: true, force: true }); },
  };
}

test('desktop actions and idempotency survive a queue reload', async () => {
  const fixture = await temporaryQueue();
  try {
    const created = await fixture.queue.enqueue({
      id: 'action-1',
      idempotencyKey: 'operation:step:attempt-1',
      type: 'run-project-script',
      payload: { scriptName: 'test', desktopAgentId: 'agent-a' },
      requestedBy: 'operation:operation-1',
    });
    const reloaded = new DesktopActionQueue({ dataDir: fixture.root });
    assert.deepEqual((await reloaded.list()).map((action) => action.id), ['action-1']);

    const duplicate = await reloaded.enqueue({
      id: 'different-id',
      idempotencyKey: 'operation:step:attempt-1',
      type: 'run-project-script',
      payload: { scriptName: 'build' },
    });
    assert.equal(duplicate.id, created.id);
    assert.equal((await reloaded.list()).length, 1);
  } finally { await fixture.close(); }
});

test('claimed actions use durable leases and remain available until valid acknowledgement', async () => {
  const fixture = await temporaryQueue({ leaseMs: 30_000 });
  try {
    await fixture.queue.enqueue({
      id: 'action-lease',
      idempotencyKey: 'idem-lease',
      type: 'validate-workspace',
      payload: { desktopAgentId: 'agent-a' },
    });
    assert.equal(await fixture.queue.acknowledge({
      id: 'action-lease', agentId: 'agent-a', type: 'validate-workspace', idempotencyKey: 'idem-lease',
    }), false);
    assert.deepEqual(await fixture.queue.claim('agent-b', { now: 1_000 }), []);
    const claimed = await fixture.queue.claim('agent-a', { now: 1_000 });
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].deliveryAttempts, 1);
    assert.deepEqual(await fixture.queue.claim('agent-a', { now: 1_001 }), []);

    const restarted = new DesktopActionQueue({ dataDir: fixture.root, leaseMs: 30_000 });
    assert.deepEqual(await restarted.claim('agent-a', { now: 30_999 }), []);
    const redelivered = await restarted.claim('agent-a', { now: 31_000 });
    assert.equal(redelivered[0].id, 'action-lease');
    assert.equal(redelivered[0].deliveryAttempts, 2);

    assert.equal(await restarted.acknowledge({
      id: 'action-lease', agentId: 'agent-b', type: 'validate-workspace', idempotencyKey: 'idem-lease',
    }), false);
    assert.equal((await restarted.list()).length, 1);
    assert.equal(await restarted.acknowledge({
      id: 'action-lease', agentId: 'agent-a', type: 'validate-workspace', idempotencyKey: 'idem-lease',
    }), true);
    assert.deepEqual(await new DesktopActionQueue({ dataDir: fixture.root }).list(), []);
  } finally { await fixture.close(); }
});

test('desktop action queue recovers the last valid backup and preserves corruption evidence', async () => {
  const fixture = await temporaryQueue();
  try {
    await fixture.queue.enqueue({ id: 'action-1', type: 'open-vscode', payload: { path: 'one' } });
    await fixture.queue.enqueue({ id: 'action-2', type: 'open-vscode', payload: { path: 'two' } });
    await fs.writeFile(fixture.queue.file, '{broken', 'utf8');

    const recovered = new DesktopActionQueue({ dataDir: fixture.root });
    assert.deepEqual((await recovered.list()).map((action) => action.id), ['action-1']);
    const entries = await fs.readdir(fixture.root);
    assert.ok(entries.some((name) => name.startsWith('desktop-actions.json.corrupt-')));
  } finally { await fixture.close(); }
});

test('desktop action queue rejects unbounded or unserializable payloads without dropping pending work', async () => {
  const fixture = await temporaryQueue({ maxActions: 1 });
  try {
    const circular = {};
    circular.self = circular;
    await assert.rejects(
      fixture.queue.enqueue({ id: 'circular', type: 'open-vscode', payload: circular }),
      { code: 'DESKTOP_ACTION_PAYLOAD_INVALID' },
    );
    await assert.rejects(
      fixture.queue.enqueue({ id: 'large', type: 'open-vscode', payload: { value: 'x'.repeat(33_000) } }),
      { code: 'DESKTOP_ACTION_PAYLOAD_TOO_LARGE' },
    );
    await fixture.queue.enqueue({ id: 'kept', type: 'open-vscode', payload: {} });
    await assert.rejects(
      fixture.queue.enqueue({ id: 'overflow', type: 'open-vscode', payload: {} }),
      { code: 'DESKTOP_ACTION_QUEUE_FULL' },
    );
    assert.deepEqual((await fixture.queue.list()).map((action) => action.id), ['kept']);
  } finally { await fixture.close(); }
});
