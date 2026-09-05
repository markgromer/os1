import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { CodexComposeState, composeKey, exactCodexJob } from '../public/codex-compose.js';
import { DesktopCodexAdapter } from '../marcus/providers/desktop_codex_adapter.js';

const native = { id: 'view-1', name: 'Scoop Systems', source: 'codex', workspacePath: 'C:\\work\\scoop', raw: { sessionId: 'thread-1' } };
const job = { jobId: 'job-1', businessKey: 'personal', workspacePath: 'c:/work/scoop/', threadId: 'thread-1' };
const key = composeKey(native, 'personal');
function state() {
  const values = new Map();
  const storage = { getItem: (k) => values.get(k), setItem: (k, v) => values.set(k, v) };
  return { storage, compose: new CodexComposeState(storage, () => 'request-000000001') };
}
const receipt = (request) => ({ ok: true, receipt: { requestId: request.requestId, jobId: request.jobId, actionId: 'action-1', phase: 'queued' } });

test('Codex routing requires the real jobId, business, exact workspace and exact thread; never a similar project name', () => {
  assert.equal(exactCodexJob(native, [job], 'personal'), job);
  for (const changed of [{ threadId: 'other-thread' }, { businessKey: 'other' }, { workspacePath: 'c:/other' }, { jobId: '' }]) assert.equal(exactCodexJob(native, [{ ...job, ...changed }], 'personal'), null);
  assert.equal(exactCodexJob(native, [job, { ...job, jobId: 'job-2' }], 'personal'), null);
  assert.equal(exactCodexJob({ ...native, raw: {} }, [job], 'personal'), null);
  assert.equal(exactCodexJob({ source: 'job', raw: job }, [job], 'personal'), job);
  assert.equal(composeKey({ ...native, id: 'view-after-reorder' }, 'personal'), key);
  assert.notEqual(composeKey(native, 'agency'), key);
});

test('drafts survive reload and blocked native-session sends never call Marcus or Codex', async () => {
  const { compose, storage } = state(); compose.draft(key, 'Keep this exact draft.');
  await compose.send(key, null, () => assert.fail('must not dispatch'));
  assert.equal(compose.get(key).phase, 'blocked');
  assert.equal(new CodexComposeState(storage).get(key).draft, 'Keep this exact draft.');
});

test('pending sends resist double clicks and polling; edits while waiting are retained', async () => {
  const { compose } = state(); compose.draft(key, 'Original');
  let release, count = 0;
  const dispatch = async (request) => { count++; await new Promise((resolve) => { release = resolve; }); return receipt(request); };
  const first = compose.send(key, job, dispatch);
  await compose.send(key, job, dispatch);
  assert.equal(count, 1); assert.equal(compose.get(key).phase, 'sending');
  compose.draft(key, 'New draft while sending'); release(); await first;
  assert.equal(compose.get(key).draft, 'New draft while sending');
  assert.equal(compose.get(key).phase, 'queued');
  assert.match(compose.get(key).notice, /not yet proof/);
});

test('only matching receipts clear drafts; uncertain sends reuse the exact request after reload', async () => {
  const { compose, storage } = state(); compose.draft(key, 'Original');
  let sent;
  await compose.send(key, job, async (request) => { sent = request; return { ok: true, reply: 'I recorded that.' }; });
  assert.equal(compose.get(key).phase, 'uncertain'); assert.equal(compose.get(key).draft, 'Original');
  const reloaded = new CodexComposeState(storage);
  reloaded.draft(key, 'Next draft');
  await reloaded.send(key, job, async (request) => { assert.deepEqual(request, sent); return receipt(request); });
  assert.equal(reloaded.get(key).draft, 'Next draft'); assert.equal(reloaded.get(key).request, null);
});

test('definitive errors retain drafts and do not claim delivery; unavailable storage is harmless', async () => {
  const compose = new CodexComposeState({ getItem() { throw new Error('disabled'); }, setItem() { throw new Error('disabled'); } }, () => 'request-000000001');
  compose.draft(key, 'Keep me');
  await compose.send(key, job, async () => { throw Object.assign(new Error('No permission'), { definitive: true }); });
  assert.equal(compose.get(key).draft, 'Keep me'); assert.equal(compose.get(key).phase, 'failed');
});

test('desktop follow-up receipt is durable, concurrent/restart idempotent and business-scoped', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'codex-receipt-'));
  const actions = [];
  try {
    const adapter = new DesktopCodexAdapter({ dataDir: root, queueAction: async (action) => { actions.push(action); return action; } });
    const started = await adapter.startJob({ operationId: 'o', stepId: 's', businessKey: 'personal', workspacePath: 'C:\\work\\scoop', desktopAgentId: 'desktop', prompt: 'Fixture' });
    const options = { businessKey: 'personal', requestId: 'request-000000001' };
    await assert.rejects(adapter.queueFollowup(started.jobId, 'Follow up', options), /no resumable/);
    await adapter.ingestUpdate({ jobId: started.jobId, desktopAgentId: 'desktop', threadId: 'thread', status: 'completed' });
    await assert.rejects(adapter.queueFollowup(started.jobId, 'Follow up', { ...options, businessKey: 'agency' }), /not found/);
    const [a, b] = await Promise.all([adapter.queueFollowup(started.jobId, 'Follow up', options), adapter.queueFollowup(started.jobId, 'Follow up', options)]);
    assert.deepEqual(a.receipt, b.receipt); assert.equal(actions.length, 2);
    assert.equal(actions[1].payload.message, 'Follow up'); assert.equal(actions[1].payload.threadId, 'thread');
    assert.equal(actions[1].id, a.receipt.actionId);
    await assert.rejects(adapter.queueFollowup(started.jobId, 'Different', options), /different message/);
    await assert.rejects(adapter.queueFollowup(started.jobId, 'Second', { ...options, requestId: 'request-000000002' }), /already active/);
    const reloaded = new DesktopCodexAdapter({ dataDir: root, queueAction: () => assert.fail('do not resend') });
    assert.deepEqual((await reloaded.queueFollowup(started.jobId, 'Follow up', options)).receipt, a.receipt);
    assert.equal((await reloaded.listJobs({ businessKey: 'agency' })).length, 0);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

test('uncertain desktop persistence cannot be re-enqueued after a restart', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'codex-uncertain-'));
  try {
    const adapter = new DesktopCodexAdapter({ dataDir: root, queueAction: async (action) => action });
    const job = await adapter.startJob({ operationId: 'o', stepId: 's', businessKey: 'personal', workspacePath: 'C:\\test', desktopAgentId: 'desktop', prompt: 'Fixture' });
    await adapter.ingestUpdate({ jobId: job.jobId, desktopAgentId: 'desktop', threadId: 'thread', status: 'completed' });
    adapter.queueAction = async () => { throw new Error('IO error'); };
    const options = { businessKey: 'personal', requestId: 'request-000000001' };
    await assert.rejects(adapter.queueFollowup(job.jobId, 'Follow up', options), (error) => error.definite === false);
    const reloaded = new DesktopCodexAdapter({ dataDir: root, queueAction: () => assert.fail('do not resend') });
    await assert.rejects(reloaded.queueFollowup(job.jobId, 'Follow up', options), /uncertain/);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
