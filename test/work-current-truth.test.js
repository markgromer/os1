import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { workReadiness } from '../marcus/work/work_graph.js';
import { DomainStore } from '../marcus/operations/domain_store.js';

test('historical completion with missing or cross-project evidence cannot release dependent work', () => {
  const previous = { id: 'previous', projectId: 'project', kind: 'task', status: 'completed', operationId: 'op', blockers: [], invalidatedBy: [] };
  const next = { ...previous, id: 'next', status: 'ready', operationId: '' };
  const doc = { items: [previous, next], dependencies: [{ id: 'dep', itemId: 'next', type: 'work', prerequisiteId: 'previous' }] };
  for (const operations of [new Map(), new Map([['op', { id: 'op', projectRegistryId: 'other', status: 'completed' }]])]) {
    assert.equal(workReadiness(doc, previous, operations).status, 'blocked');
    assert.equal(workReadiness(doc, next, operations).runnable, false);
  }
  const operations = new Map([['op', { id: 'op', projectRegistryId: 'project', status: 'completed' }]]);
  assert.equal(workReadiness(doc, previous, operations).status, 'completed');
  assert.equal(workReadiness(doc, next, operations).runnable, true);
});

test('a future schema never silently restores an older backup', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'constitution-version-'));
  const store = new DomainStore({ dataDir: root, name: 'version-test', empty: () => ({ items: [] }) });
  try {
    await store.mutate('personal', (doc) => { doc.items.push('old'); });
    const file = store.file('personal');
    await fs.copyFile(file, file + '.bak');
    const future = { ...(await store.read('personal')), schemaVersion: 2, items: ['new-version'] };
    await fs.writeFile(file, JSON.stringify(future));
    await assert.rejects(store.read('personal'), { code: 'DOMAIN_VERSION_UNSUPPORTED' });
    assert.deepEqual(JSON.parse(await fs.readFile(file, 'utf8')), future);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
