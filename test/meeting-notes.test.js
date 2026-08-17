import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { writeMarcusMeetingNote } = require('../desktop-meeting-notes.cjs');

test('desktop meeting-note writer confines bounded summaries to the Obsidian conversation folder', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'marcus-meeting-note-'));
  try {
    const filename = '2026-08-17-call-abc123.md';
    const content = '# Call\n\nStatus: active\nTags: #conversation #meeting\n';
    const result = writeMarcusMeetingNote({ filename, content }, { root });
    assert.equal(result.ok, true);
    assert.equal(fs.readFileSync(path.join(root, 'docs', 'marcus', 'conversations', filename), 'utf8'), content);
    const updated = `${content}\n## Summary\n\nUpdated checkpoint.\n`;
    assert.equal(writeMarcusMeetingNote({ filename, content: updated }, { root }).ok, true);
    assert.equal(fs.readFileSync(path.join(root, 'docs', 'marcus', 'conversations', filename), 'utf8'), updated);
    assert.equal(writeMarcusMeetingNote({ filename: '../escape.md', content }, { root }).ok, false);
    assert.equal(writeMarcusMeetingNote({ filename, content: 'x'.repeat(81_000) }, { root }).ok, false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
