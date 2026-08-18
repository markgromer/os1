import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { BrowserMissionStore } from '../marcus/skills/browser_mission_store.js';

test('browser mission survives multi-turn instructions and records verified skill evidence', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-browser-mission-'));
  try {
    const store = new BrowserMissionStore({ dataDir });
    const started = await store.startOrResume({
      businessKey: 'personal', platform: 'skool', instruction: 'Browse the ScoopOS community and learn the tone.', skill: 'marcus_browser_read',
    });
    const resumed = await store.startOrResume({
      businessKey: 'personal', platform: 'skool', instruction: 'Draft your first standalone post.', skill: 'marcus_browser_prepare_post',
    });
    assert.equal(resumed.id, started.id);
    assert.equal(resumed.objective, 'Browse the ScoopOS community and learn the tone.');
    assert.deepEqual(resumed.instructions, [
      'Browse the ScoopOS community and learn the tone.',
      'Draft your first standalone post.',
    ]);

    const waiting = await store.recordResult(resumed.id, {
      skill: 'marcus_browser_prepare_post', ok: true, waitingForApproval: true,
      evidence: { type: 'skool.prepare-standalone-post', summary: 'Verified main feed composer and exact text.' },
    });
    assert.equal(waiting.status, 'waiting_for_approval');
    assert.equal(waiting.evidence.at(-1).type, 'skool.prepare-standalone-post');
    assert.equal((await store.active('personal')).id, started.id);

    const completed = await store.recordResult(resumed.id, {
      skill: 'marcus_browser_submit', ok: true, completed: true,
      evidence: { type: 'browser.publish-approved-draft', summary: 'Published object verified.' },
    });
    assert.equal(completed.status, 'completed');
    assert.ok(completed.completedAt);
    assert.equal(await store.active('personal'), null);
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});

test('browser mission failure remains durable as a recovery step instead of false completion', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-browser-mission-'));
  try {
    const store = new BrowserMissionStore({ dataDir });
    const mission = await store.startOrResume({
      businessKey: 'personal', platform: 'skool', instruction: 'Create a standalone post.', skill: 'marcus_browser_prepare_post',
    });
    const failed = await store.recordResult(mission.id, {
      skill: 'marcus_browser_prepare_post', ok: false, error: 'Thread comment editor was rejected.',
    });
    assert.equal(failed.status, 'recovering');
    assert.equal(failed.currentStep, 'recover last skill');
    assert.match(failed.error, /thread comment editor/i);

    const reloaded = new BrowserMissionStore({ dataDir });
    assert.equal((await reloaded.active('personal')).status, 'recovering');
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});
