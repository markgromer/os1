import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { BrowserPublicationStore } from '../marcus/browser_publication_store.js';

function normalizePublications(value) {
  return (Array.isArray(value) ? value : [])
    .filter((item) => item && typeof item === 'object' && item.id && item.text)
    .map((item) => ({ id: String(item.id), text: String(item.text) }));
}

test('browser publication approvals migrate once and survive a fresh store instance', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-browser-publications-'));
  const first = new BrowserPublicationStore({ dataDir, normalize: normalizePublications });

  assert.deepEqual(await first.list({
    legacyLoader: async () => [{ id: 'intro', text: 'Pending introduction' }],
  }), [{ id: 'intro', text: 'Pending introduction' }]);

  const restarted = new BrowserPublicationStore({ dataDir, normalize: normalizePublications });
  assert.deepEqual(await restarted.list({ legacyLoader: async () => [] }), [
    { id: 'intro', text: 'Pending introduction' },
  ]);
});

test('browser publication approvals recover the last valid backup without discarding corruption', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-browser-publications-'));
  const store = new BrowserPublicationStore({ dataDir, normalize: normalizePublications });
  await store.list({ legacyLoader: async () => [{ id: 'intro', text: 'Approved text' }] });
  await store.replace([
    { id: 'intro', text: 'Approved text' },
    { id: 'followup', text: 'Second draft' },
  ]);
  await fs.writeFile(store.file, '{broken', 'utf8');

  assert.deepEqual(await store.list(), [{ id: 'intro', text: 'Approved text' }]);
  const files = await fs.readdir(dataDir);
  assert.equal(files.some((name) => name.startsWith('marcus-browser-publications.json.corrupt-')), true);
});
