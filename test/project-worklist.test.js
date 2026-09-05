import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { groupProjects, isSetAside, taskKey, projectContextHtml, projectRowHtml, visibleWorklistGroups } from '../public/project-worklist.js';
import { AwarenessStore } from '../marcus/awareness/awareness_store.js';
const started = '2026-09-05T10:00:00.000Z';
const now = Date.parse('2026-09-05T12:00:00Z');
const session = (id, workspace = 'C:/work/reggie') => ({ id, name: 'REGGIE', source: 'codex', workspacePath: workspace, latestRequest: 'Fix the real website alerts', response: 'Three alerts were stale records.', updatedAt: started, raw: { sessionId: id, latestUserRequestAt: started } });

test('default list is seven days and task summaries, agent state and next steps are visible before opening context', () => {
  const now = Date.parse('2026-09-05T12:00:00Z');
  const groups = groupProjects([session('one'), { ...session('old', 'C:/old'), updatedAt: '2026-08-20T12:00:00Z' }], null);
  assert.equal(visibleWorklistGroups(groups, 'active', '', now).length, 1);
  assert.equal(visibleWorklistGroups(groups, 'all', '', now).length, 2);
  const html = projectRowHtml(groups[0]);
  for (const needed of ['Three alerts were stale records.', 'Agent:', 'Next:', 'Done for now', 'Open context']) assert.ok(html.includes(needed), needed);
  assert.ok(!html.includes('<details'));
});

test('worklist groups exact workspaces, not similar names; multiple conversations stay under one project', () => {
  const groups = groupProjects([session('one'), session('two'), session('other', 'C:/work/other-reggie')], null);
  assert.equal(groups.length, 2); assert.equal(groups[0].tasks.length, 2);
  assert.equal(groups[0].status, 'No live run confirmed');
  assert.equal(groups[0].summary, 'Fix the real website alerts');
});

test('native transcript and desktop job for the exact thread do not become duplicate tasks', () => {
  const groups = groupProjects([session('one'), { ...session('provider'), source: 'job', raw: { jobId: 'j1', threadId: 'one', status: 'running' } }], null);
  assert.equal(groups[0].tasks.length, 1); assert.equal(groups[0].status, 'In progress');
});

test('old tasks stay in history inside a recently active project; pending decisions remain visible', () => {
  const old = { ...session('old'), latestRequest: 'Old request', updatedAt: '2026-08-20T10:00:00Z' };
  const current = session('current');
  const groups = groupProjects([old, current], null, [], now);
  assert.deepEqual(groups[0].activeTasks.map((task) => task.id), ['current']);
  assert.equal(groups[0].tasks.length, 2);
  assert.ok(!projectRowHtml(groups[0]).includes('Old request'));
  assert.ok(projectContextHtml(groups[0], { showHandled: true }).includes('Old request'));
  assert.ok(projectContextHtml(groups[0]).includes('Show history (1)'));
  assert.equal(visibleWorklistGroups(groupProjects([old], null, [], now), 'aside', '', now).length, 0);
  const pending = { ...old, source: 'operation', raw: { status: 'waiting_for_approval' } };
  assert.equal(visibleWorklistGroups(groupProjects([pending], null, [], now), 'active', '', now).length, 1);
  assert.equal(projectRowHtml(groups[0]).split(current.latestRequest).length - 1, 1, 'single task request is not repeated');
});

test('handled tasks and projects stay hidden through repeated reports but new requests bring them back', () => {
  const task = session('one');
  const key = groupProjects([task], null)[0].key;
  const preferences = [{ key, hidden: true, resumeAfter: started }, { key: taskKey(task), hidden: true, resumeAfter: started }];
  const repeated = groupProjects([{ ...task, updatedAt: '2026-09-06T10:00:00Z' }], null, preferences)[0];
  assert.equal(repeated.hidden, true); assert.equal(repeated.activeTasks.length, 0);
  const resumed = groupProjects([{ ...task, raw: { ...task.raw, latestUserRequestAt: '2026-09-06T10:00:00Z' } }], null, preferences)[0];
  assert.equal(resumed.hidden, false); assert.equal(resumed.activeTasks.length, 1);
  assert.equal(isSetAside(key, started, [{ ...preferences[0], hidden: false }]), false);
});

test('context shows actionable tasks without fake pipeline, automatic acceptance, or unsafe HTML', () => {
  const group = groupProjects([{ ...session('one'), latestRequest: '<script>bad()</script>' }], null)[0];
  const html = projectContextHtml(group);
  assert.ok(html.includes('Done for now')); assert.ok(html.includes('Set project aside')); assert.ok(html.includes('Conversation'));
  assert.ok(html.includes('&lt;script&gt;')); assert.ok(!html.includes('<script>'));
  assert.ok(!html.includes('Accept & archive')); assert.ok(!html.includes('Current pipeline'));
});

test('worklist preferences persist in the existing awareness store without changing lifecycle or leaking across businesses', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'worklist-preferences-'));
  try {
    const store = new AwarenessStore({ dataDir: root });
    await store.synchronize('personal', [{ id: 'r1', canonicalName: 'REGGIE', status: 'Active' }]);
    const before = (await store.read('personal')).projects;
    await store.setWorklistPreference('personal', { key: 'registry:r1', hidden: true, resumeAfter: started });
    const restarted = new AwarenessStore({ dataDir: root });
    assert.equal((await restarted.read('personal')).worklistPreferences[0].hidden, true);
    assert.deepEqual((await restarted.read('personal')).projects, before);
    assert.equal(((await restarted.read('agency')).worklistPreferences || []).length, 0);
    await restarted.synchronize('personal', [{ id: 'r1', canonicalName: 'REGGIE', status: 'Active' }]);
    assert.equal((await restarted.read('personal')).worklistPreferences[0].hidden, true);
    await restarted.setWorklistPreference('personal', { key: 'registry:r1', hidden: false });
    assert.equal((await restarted.read('personal')).worklistPreferences[0].hidden, false);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
