import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { discoverRecentCodexWorkspaces, parseGitStatus } = require('../desktop-codex-sessions.cjs');

test('git status parsing preserves leading-column status and dot-prefixed paths', () => {
  const result = parseGitStatus(' D .github/workflows/deploy.yml\n M src/app/page.tsx\n?? output/', 30);
  assert.equal(result.count, 3);
  assert.deepEqual(result.entries[0], { status: 'D', file: '.github/workflows/deploy.yml' });
  assert.deepEqual(result.entries[1], { status: 'M', file: 'src/app/page.tsx' });
  assert.deepEqual(result.entries[2], { status: '??', file: 'output/' });
});

test('Codex workspace discovery reads only bounded session metadata and ignores subagents', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-codex-sessions-'));
  const codexHome = path.join(root, '.codex');
  const workspace = path.join(root, 'scoopFairies');
  const otherWorkspace = path.join(root, 'Task Tracker');
  const now = new Date(2026, 7, 12, 12, 0, 0);
  const day = path.join(codexHome, 'sessions', '2026', '08', '12');
  await fs.mkdir(day, { recursive: true });
  await fs.mkdir(workspace, { recursive: true });
  await fs.mkdir(otherWorkspace, { recursive: true });

  const writeSession = async (name, payload, modifiedAt) => {
    const file = path.join(day, `${name}.jsonl`);
    await fs.writeFile(file, `${JSON.stringify({ type: 'session_meta', payload })}\n${JSON.stringify({ type: 'response_item', payload: { text: 'SECRET TRANSCRIPT CONTENT' } })}\n`);
    await fs.utimes(file, modifiedAt, modifiedAt);
  };
  await writeSession('older', { id: 'session-old', cwd: workspace, source: 'vscode', originator: 'codex_vscode' }, new Date(now.getTime() - 120_000));
  await writeSession('newer', { id: 'session-new', cwd: workspace, source: 'vscode', originator: 'codex_vscode' }, new Date(now.getTime() - 30_000));
  await writeSession('other', { id: 'session-other', cwd: otherWorkspace, source: 'cli', originator: 'codex_cli_rs' }, new Date(now.getTime() - 60_000));
  await writeSession('subagent', { id: 'session-subagent', cwd: workspace, source: { subagent: 'guardian' }, originator: 'codex' }, new Date(now.getTime() - 10_000));

  try {
    const result = discoverRecentCodexWorkspaces({ codexHome, nowMs: now.getTime(), maxResults: 5 });
    assert.equal(result.length, 2);
    assert.equal(result[0].sessionId, 'session-new');
    assert.equal(result[0].projectName, 'Scoop Fairies');
    assert.equal(result.some((item) => item.sessionId === 'session-subagent'), false);
    assert.doesNotMatch(JSON.stringify(result), /SECRET TRANSCRIPT CONTENT/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
