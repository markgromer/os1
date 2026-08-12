import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { formatMissionMemoryForPrompt, MissionMemoryStore } from '../marcus/memory/mission_memory_store.js';

async function withStore(callback) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-mission-memory-'));
  try {
    return await callback({ dataDir, store: new MissionMemoryStore({ dataDir }) });
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
}

test('mission memory seeds the durable operator mission and persists explicit memory across restart', async () => {
  await withStore(async ({ dataDir, store }) => {
    const defaults = await store.list('personal', { status: 'active' });
    assert.ok(defaults.memories.some((item) => item.kind === 'mission' && /durable, trusted operator/i.test(item.content)));
    assert.ok(defaults.memories.some((item) => item.kind === 'standing_instruction' && /investigate before answering/i.test(item.content)));
    assert.ok(defaults.memories.some((item) => item.kind === 'preference' && /prebuilt voice/i.test(item.content)));

    const added = await store.add('personal', {
      kind: 'preference',
      title: 'Project communication',
      content: 'Explain the operational result before technical implementation details.',
      priority: 5,
    }, { actor: 'mark', source: 'test' });
    assert.equal(added.created, true);
    const duplicate = await store.add('personal', {
      kind: 'preference',
      content: 'Explain the operational result before technical implementation details.',
      priority: 4,
    }, { actor: 'mark', source: 'test' });
    assert.equal(duplicate.created, false);
    assert.equal(duplicate.memory.id, added.memory.id);

    const restarted = new MissionMemoryStore({ dataDir });
    const relevant = await restarted.relevant('personal', 'How should project results be explained?');
    assert.equal(relevant.some((item) => item.id === added.memory.id), true);
    assert.match(formatMissionMemoryForPrompt(relevant), /operational result before technical implementation/i);

    const agency = await restarted.list('agency', { status: 'active' });
    assert.equal(agency.memories.some((item) => item.id === added.memory.id), false);
    assert.equal(agency.memories.some((item) => item.kind === 'mission'), true);

    const archived = await restarted.update('personal', added.memory.id, { status: 'archived' });
    assert.equal(archived.status, 'archived');
    assert.equal((await restarted.relevant('personal', 'project results')).some((item) => item.id === added.memory.id), false);
  });
});

test('mission memory rejects credentials and recovers the last valid backup without discarding corruption evidence', async () => {
  await withStore(async ({ dataDir, store }) => {
    await assert.rejects(() => store.add('personal', {
      kind: 'fact',
      content: 'The API key is api_key="sk-do-not-store-this-value".',
    }), /cannot store credentials/i);

    const first = await store.add('personal', { kind: 'fact', content: 'First durable fact.' });
    await store.add('personal', { kind: 'fact', content: 'Second durable fact.' });
    const file = store.fileForBusiness('personal');
    await fs.writeFile(file, '{not-json', 'utf8');

    const recovered = new MissionMemoryStore({ dataDir });
    const result = await recovered.list('personal', { status: 'active' });
    assert.equal(result.memories.some((item) => item.id === first.memory.id), true);
    const files = await fs.readdir(path.dirname(file));
    assert.equal(files.some((name) => /^marcus-mission-memory\.json\.corrupt-/.test(name)), true);
  });
});
