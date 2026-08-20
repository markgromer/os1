import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { discoverRecentCodexWorkspaces, parseGitStatus, readSessionHandoffSummary, readSessionOriginalUserRequest, readSessionRollingContext } = require('../desktop-codex-sessions.cjs');

test('git status parsing preserves leading-column status and dot-prefixed paths', () => {
  const result = parseGitStatus(' D .github/workflows/deploy.yml\n M src/app/page.tsx\n?? output/', 30);
  assert.equal(result.count, 3);
  assert.deepEqual(result.entries[0], { status: 'D', file: '.github/workflows/deploy.yml' });
  assert.deepEqual(result.entries[1], { status: 'M', file: 'src/app/page.tsx' });
  assert.deepEqual(result.entries[2], { status: '??', file: 'output/' });
});

test('Codex workspace discovery reads bounded metadata plus assistant handoff and ignores raw transcript', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-codex-sessions-'));
  const codexHome = path.join(root, '.codex');
  const workspace = path.join(root, 'scoopFairies');
  const otherWorkspace = path.join(root, 'Task Tracker');
  const now = new Date(2026, 7, 12, 12, 0, 0);
  const day = path.join(codexHome, 'sessions', '2026', '08', '12');
  await fs.mkdir(day, { recursive: true });
  await fs.mkdir(workspace, { recursive: true });
  await fs.mkdir(otherWorkspace, { recursive: true });

  const writeSession = async (name, payload, modifiedAt, extraEvents = []) => {
    const file = path.join(day, `${name}.jsonl`);
    await fs.writeFile(file, [
      JSON.stringify({ type: 'session_meta', payload }),
      JSON.stringify({ type: 'response_item', payload: { role: 'user', text: 'SECRET TRANSCRIPT CONTENT' } }),
      ...extraEvents.map((event) => JSON.stringify(event)),
    ].join('\n') + '\n');
    await fs.utimes(file, modifiedAt, modifiedAt);
    return file;
  };
  await writeSession('older', { id: 'session-old', cwd: workspace, source: 'vscode', originator: 'codex_vscode' }, new Date(now.getTime() - 120_000));
  const newerFile = await writeSession('newer', { id: 'session-new', cwd: workspace, source: 'vscode', originator: 'codex_vscode' }, new Date(now.getTime() - 30_000), [
    { type: 'response_item', timestamp: '2026-08-12T11:58:00.000Z', payload: { role: 'user', text: 'Make the portrait decision check compact and preserve workspace context.' } },
    { type: 'response_item', payload: { role: 'assistant', text: 'Fixed the live blocker. Refresh https://poopsites.com/admin/reggie and confirm 15 sites load.' } },
    { type: 'response_item', timestamp: '2026-08-12T11:59:00.000Z', payload: { role: 'user', text: 'merge, commit and deploy' } },
  ]);
  await writeSession('other', { id: 'session-other', cwd: otherWorkspace, source: 'cli', originator: 'codex_cli_rs' }, new Date(now.getTime() - 60_000));
  await writeSession('subagent', { id: 'session-subagent', cwd: workspace, source: { subagent: 'guardian' }, originator: 'codex' }, new Date(now.getTime() - 10_000));

  try {
    const result = discoverRecentCodexWorkspaces({ codexHome, nowMs: now.getTime(), maxResults: 5 });
    assert.equal(result.length, 2);
    assert.equal(result[0].sessionId, 'session-new');
    assert.equal(result[0].projectName, 'Scoop Fairies');
    assert.match(result[0].handoffSummary, /Fixed the live blocker/);
    assert.equal(result[0].handoffStatus, 'ready_for_mark');
    assert.match(result[0].originalUserRequest, /portrait decision check/);
    assert.equal(result[0].latestUserRequest, 'merge, commit and deploy');
    assert.equal(result.some((item) => item.sessionId === 'session-subagent'), false);
    assert.doesNotMatch(JSON.stringify(result), /SECRET TRANSCRIPT CONTENT/);
    assert.deepEqual(result[0].rollingContext.map((item) => item.role), ['user', 'user', 'assistant', 'user']);
    assert.match(result[0].rollingContext[0].content, /redacted sensitive content/);
    assert.match(readSessionRollingContext(newerFile)[2].content, /Fixed the live blocker/);
    assert.match(readSessionOriginalUserRequest(newerFile).request, /portrait decision check/);
    assert.equal(readSessionHandoffSummary(newerFile).status, 'ready_for_mark');
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
