import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import express from 'express';

import { registerCommunityIntelligenceRoutes } from '../marcus/api/community_intelligence_routes.js';
import { CommunityIntelligenceStore } from '../marcus/memory/community_intelligence_store.js';

test('community intelligence routes ingest profiles, queue Obsidian projection, and enforce notification evidence', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-community-routes-'));
  const projections = [];
  const app = express();
  app.use(express.json());
  const store = new CommunityIntelligenceStore({ dataDir });
  registerCommunityIntelligenceRoutes(app, {
    store,
    getBusinessKey: () => 'personal',
    queueProfileProjection: async (_businessKey, memberId) => {
      projections.push(memberId);
      return { actionId: 'action_123', memberId };
    },
  });
  const server = http.createServer(app);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  try {
    const observationResponse = await fetch(`${baseUrl}/api/marcus/community/observations`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ observations: [{
        platform: 'skool', community: 'ScoopOS', member: { displayName: 'Route Tester' },
        kind: 'post', contentSummary: 'Shared a route automation checklist.',
      }] }),
    });
    assert.equal(observationResponse.status, 201);
    const observationBody = await observationResponse.json();
    assert.equal(observationBody.created, 1);
    assert.equal(observationBody.projections.length, 1);
    assert.equal(projections.length, 1);

    const members = await (await fetch(`${baseUrl}/api/marcus/community/members?q=route`)).json();
    assert.equal(members.ok, true);
    assert.equal(members.members.length, 1);

    const notificationBody = await (await fetch(`${baseUrl}/api/marcus/community/notifications`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ notifications: [{
        platform: 'skool', community: 'ScoopOS', kind: 'reply',
        summary: 'Route Tester asked a follow-up question?',
      }] }),
    })).json();
    const notificationId = notificationBody.notifications[0].id;
    const rejected = await fetch(`${baseUrl}/api/marcus/community/notifications/${notificationId}/transition`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ state: 'responded' }),
    });
    assert.equal(rejected.status, 400);
    const accepted = await fetch(`${baseUrl}/api/marcus/community/notifications/${notificationId}/transition`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ state: 'cleared', evidence: 'Visible notification read-back confirmed it was cleared.' }),
    });
    assert.equal(accepted.status, 200);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});
