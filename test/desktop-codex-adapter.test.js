import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { DesktopCodexAdapter } from '../marcus/providers/desktop_codex_adapter.js';

test('desktop Codex adapter durably queues one local job and exposes token-scoped monitor events', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-desktop-codex-'));
  const actions = [];
  try {
    const adapter = new DesktopCodexAdapter({
      dataDir,
      monitorBaseUrl: 'https://marcus.example.test',
      queueAction: async (action) => { actions.push(action); return action; },
    });
    const input = {
      operationId: 'op-1',
      stepId: 'step-1',
      businessKey: 'personal',
      projectRegistryId: 'project-1',
      projectName: 'Visible Project',
      repository: '',
      branch: 'main',
      workspacePath: 'C:\\work\\visible-project',
      desktopAgentId: 'desktop-1',
      prompt: 'Build the requested project.',
    };
    const started = await adapter.startJob(input, { idempotencyKey: 'idem-1' });
    assert.equal(started.status, 'queued');
    assert.equal(actions.length, 1);
    assert.equal(actions[0].type, 'start-local-codex-job');
    assert.equal(actions[0].payload.path, input.workspacePath);
    assert.equal(actions[0].payload.prompt, input.prompt);
    const monitor = new URL(actions[0].payload.monitorUrl);
    const monitorToken = monitor.searchParams.get('monitorToken');
    assert.ok(monitorToken);

    const running = await adapter.ingestUpdate({
      jobId: started.jobId,
      desktopAgentId: 'desktop-1',
      status: 'running',
      threadId: 'thread-1',
      events: [{ type: 'thread.started', timestamp: '2026-08-12T12:00:00Z', data: { threadId: 'thread-1' } }],
    });
    assert.equal(running.status, 'running');
    assert.equal((await adapter.getPublicJob(started.jobId, 'wrong-token')), null);
    const visible = await adapter.getPublicJob(started.jobId, monitorToken, { after: 0 });
    assert.equal(visible.events.length, 1);
    assert.equal(visible.threadId, 'thread-1');

    await adapter.ingestUpdate({
      jobId: started.jobId,
      desktopAgentId: 'desktop-1',
      status: 'completed',
      finalOutput: 'The project is complete.',
      diffSummary: '2 files changed',
      changedFiles: ['A package.json', 'A src/index.js'],
      events: [{ type: 'turn.completed', data: { status: 'completed' } }],
    });
    const completed = await adapter.getJobStatus(started);
    assert.equal(completed.status, 'completed');
    assert.equal((await adapter.getArtifacts(started))[0].type, 'local_codex_result');
    assert.equal((await adapter.getDiff(started)).files.length, 2);
    await adapter.ingestUpdate({
      jobId: started.jobId,
      desktopAgentId: 'desktop-1',
      status: 'running',
      events: [{ type: 'late.running.event' }],
    });
    assert.equal((await adapter.getJobStatus(started)).status, 'completed');

    const reloaded = new DesktopCodexAdapter({
      dataDir,
      monitorBaseUrl: 'https://marcus.example.test',
      queueAction: async (action) => { actions.push(action); return action; },
    });
    const replay = await reloaded.startJob(input, { idempotencyKey: 'idem-1' });
    assert.equal(replay.status, 'completed');
    assert.equal(actions.length, 1);
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});

test('desktop Codex adapter rejects updates from a different machine', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-desktop-codex-agent-'));
  try {
    const adapter = new DesktopCodexAdapter({ dataDir, queueAction: async (action) => action });
    const started = await adapter.startJob({
      operationId: 'op-2', stepId: 'step-2', businessKey: 'personal', projectRegistryId: 'project-2',
      workspacePath: 'C:\\work\\project-2', desktopAgentId: 'desktop-1', prompt: 'Build it.',
    }, { idempotencyKey: 'idem-2' });
    await assert.rejects(
      adapter.ingestUpdate({ jobId: started.jobId, desktopAgentId: 'desktop-2', status: 'completed' }),
      (error) => error.code === 'CODEX_AGENT_MISMATCH',
    );
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});
