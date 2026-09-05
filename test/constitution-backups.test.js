import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { discoverDurableBackupSources } from '../marcus/operations/operation_backups.js';

test('scheduled backup discovery includes every constitution domain for configured and recovered businesses', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'constitution-backups-'));
  const names = ['operations', 'project-registry', 'project-evidence', 'marcus-mission-memory',
    'work-graph', 'work-context', 'engineering-director', 'human-identities', 'execution-runs', 'operator-digests'];
  try {
    for (const key of ['personal', 'agency']) {
      await fs.mkdir(path.join(root, key));
      for (const name of names) await fs.writeFile(path.join(root, key, `${name}.json`), '{}');
    }
    const sources = await discoverDurableBackupSources({ businessDataDir: root, configuredBusinessKeys: ['personal', 'absent'] });
    assert.equal(sources.length, names.length * 2);
    assert.equal(new Set(sources.map((row) => row.prefix)).size, sources.length);
    for (const key of ['personal', 'agency']) {
      assert.deepEqual(sources.filter((row) => row.businessKey === key).map((row) => row.fileName).sort(), names.map((name) => `${name}.json`).sort());
    }
    assert.ok(sources.every((row) => path.relative(root, row.sourceFile).startsWith(row.businessKey + path.sep)));
    assert.equal(sources.some((row) => row.businessKey === 'absent'), false);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
