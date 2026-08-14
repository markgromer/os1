import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  checkObsidianLinks,
  extractWikiLinks,
  normalizeWikiTarget,
} from '../scripts/check-obsidian-links.mjs';

test('Obsidian wiki target normalization handles aliases, anchors, paths, and extensions', () => {
  assert.equal(normalizeWikiTarget('project-index'), 'project-index');
  assert.equal(normalizeWikiTarget('projects/marcus|Marcus'), 'marcus');
  assert.equal(normalizeWikiTarget('daily/2026-08-13#Pull Requests'), '2026-08-13');
  assert.equal(normalizeWikiTarget('templates/project-note-template.md'), 'project-note-template');
});

test('Obsidian wiki link extraction returns normalized targets', () => {
  const links = extractWikiLinks('See [[projects/marcus|Marcus]], [[current-status#Active]], and [[daily-index]].');
  assert.deepEqual(links.map((link) => link.target), ['marcus', 'current-status', 'daily-index']);
});

test('Obsidian wiki link check reports unresolved links and honors explicit allowlist', async () => {
  const vaultRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-obsidian-links-'));
  try {
    await fs.writeFile(path.join(vaultRoot, 'index.md'), 'See [[existing-note]] and [[future-note]].', 'utf8');
    await fs.writeFile(path.join(vaultRoot, 'existing-note.md'), '# Existing\n', 'utf8');

    const failed = await checkObsidianLinks({ vaultRoot, allowlist: new Map() });
    assert.equal(failed.unresolved.length, 1);
    assert.equal(failed.unresolved[0].target, 'future-note');

    const allowed = await checkObsidianLinks({
      vaultRoot,
      allowlist: new Map([['future-note', 'Intentional future placeholder for test.']]),
    });
    assert.equal(allowed.unresolved.length, 0);
  } finally {
    await fs.rm(vaultRoot, { recursive: true, force: true });
  }
});

test('Marcus Obsidian vault has no accidental broken wiki links', async () => {
  const result = await checkObsidianLinks();
  assert.deepEqual(result.unresolved, []);
});
