import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { writeMarcusCommunityProfile } = require('../desktop-community-profiles.cjs');

test('desktop community profile writer confines bounded notes to the Obsidian people folder', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'marcus-community-note-'));
  try {
    const filename = 'community-pat-example-abc123.md';
    const content = '# Pat Example\n\nStatus: active\nTags: #person #community-profile\n';
    const result = writeMarcusCommunityProfile({ filename, content }, { root });
    assert.equal(result.ok, true);
    assert.equal(fs.readFileSync(path.join(root, 'docs', 'marcus', 'people', filename), 'utf8'), content);
    assert.equal(writeMarcusCommunityProfile({ filename: '../escape.md', content }, { root }).ok, false);
    assert.equal(writeMarcusCommunityProfile({ filename, content: 'x'.repeat(121_000) }, { root }).ok, false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
