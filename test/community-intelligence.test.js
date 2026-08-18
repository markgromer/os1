import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  buildCommunityProfileMarkdown,
  CommunityIntelligenceStore,
  recommendCommunityNotificationAction,
} from '../marcus/memory/community_intelligence_store.js';

async function withStore(callback) {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-community-'));
  try {
    return await callback({ dataDir, store: new CommunityIntelligenceStore({ dataDir }) });
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
}

test('community observations deduplicate and build one durable member profile', async () => {
  await withStore(async ({ dataDir, store }) => {
    const observation = {
      platform: 'skool',
      community: 'ScoopOS',
      member: { displayName: 'Pat Example', profileUrl: 'https://www.skool.com/@pat-example' },
      kind: 'comment',
      contentSummary: 'Asked how route automation handles weather delays.',
      sourceUrl: 'https://www.skool.com/localgiants/weather-routing',
      topics: ['route automation', 'weather'],
      engagement: { reactions: 3 },
      observedAt: '2026-08-17T20:00:00.000Z',
    };
    const first = await store.ingestObservations('personal', [observation]);
    const duplicate = await store.ingestObservations('personal', [observation]);
    assert.equal(first.created, 1);
    assert.equal(duplicate.created, 0);
    const listed = await store.listMembers('personal');
    assert.equal(listed.members.length, 1);
    assert.equal(listed.members[0].displayName, 'Pat Example');
    assert.equal(listed.members[0].observationCount, 1);
    assert.deepEqual(listed.members[0].topics[0], { name: 'route automation', count: 1 });

    const restarted = new CommunityIntelligenceStore({ dataDir });
    const detail = await restarted.getMember('personal', listed.members[0].id);
    assert.equal(detail.observations.length, 1);
    assert.match(buildCommunityProfileMarkdown(detail.member, detail.observations), /## Recent Activity/);
    assert.match(buildCommunityProfileMarkdown(detail.member, detail.observations), /weather-routing/);
  });
});

test('community profile inference rejects sensitive traits', async () => {
  await withStore(async ({ store }) => {
    await store.ingestObservations('personal', [{
      platform: 'skool', community: 'ScoopOS', member: { displayName: 'Alex Member' },
      kind: 'post', contentSummary: 'Shared an operations checklist.',
      inferences: [
        { label: 'content preference', summary: 'Often discusses operational checklists.', confidence: 0.8 },
        { label: 'political affiliation', summary: 'Unverified political guess.', confidence: 0.9 },
      ],
    }]);
    const result = await store.listMembers('personal');
    assert.equal(result.members[0].inferences.length, 1);
    assert.equal(result.members[0].inferences[0].label, 'content preference');
  });
});

test('community identity reconciliation enriches an earlier name-only profile without splitting it', async () => {
  await withStore(async ({ store }) => {
    await store.ingestObservations('personal', [{
      platform: 'skool', community: 'ScoopOS', member: { displayName: 'Jamie Member' },
      kind: 'comment', contentSummary: 'Asked about scheduling.',
    }]);
    await store.ingestObservations('personal', [{
      platform: 'skool', community: 'ScoopOS',
      member: { displayName: 'Jamie Member', profileUrl: 'https://www.skool.com/@jamie-member' },
      kind: 'post', contentSummary: 'Shared a scheduling workflow.',
    }]);
    const result = await store.listMembers('personal');
    assert.equal(result.members.length, 1);
    assert.equal(result.members[0].observationCount, 2);
    assert.equal(result.members[0].profileUrl, 'https://www.skool.com/@jamie-member');
  });
});

test('community identity reconciliation keeps conflicting stable profiles separate', async () => {
  await withStore(async ({ store }) => {
    for (const suffix of ['one', 'two']) {
      await store.ingestObservations('personal', [{
        platform: 'skool', community: 'ScoopOS',
        member: { displayName: 'Shared Name', profileUrl: `https://www.skool.com/@shared-${suffix}` },
        kind: 'post', contentSummary: `Source ${suffix}.`,
      }]);
    }
    const result = await store.listMembers('personal');
    assert.equal(result.members.length, 2);
  });
});

test('notification triage recommends response drafts but requires evidence to record external completion', async () => {
  await withStore(async ({ store }) => {
    const recommendation = recommendCommunityNotificationAction({ kind: 'reply' }, 'Pat asked whether the checklist is available?');
    assert.equal(recommendation.action, 'draft_response');
    const ingested = await store.ingestNotifications('personal', [{
      platform: 'skool', community: 'ScoopOS', kind: 'reply',
      actor: { displayName: 'Pat Example' },
      summary: 'Pat asked whether the checklist is available?',
      sourceUrl: 'https://www.skool.com/localgiants/checklist',
    }]);
    const id = ingested.notifications[0].id;
    await assert.rejects(
      () => store.transitionNotification('personal', id, { state: 'responded' }),
      /requires browser or publication evidence/i,
    );
    const drafted = await store.transitionNotification('personal', id, { state: 'drafted', draftId: 'pub_12345678' });
    assert.equal(drafted.state, 'drafted');
    const responded = await store.transitionNotification('personal', id, {
      state: 'responded', evidence: 'Exact approved reply published and read back from the thread.',
      sourceUrl: 'https://www.skool.com/localgiants/checklist',
    });
    assert.equal(responded.state, 'responded');
    assert.equal(responded.transitionEvidence.length, 2);
  });
});

test('community intelligence store recovers from its backup without overwriting the corrupt source silently', async () => {
  await withStore(async ({ dataDir, store }) => {
    await store.ingestObservations('personal', [{
      platform: 'skool', community: 'ScoopOS', member: { displayName: 'Backup Person' },
      kind: 'post', contentSummary: 'First observation.',
    }]);
    await store.ingestObservations('personal', [{
      platform: 'skool', community: 'ScoopOS', member: { displayName: 'Second Person' },
      kind: 'comment', contentSummary: 'Second observation.',
    }]);
    const file = path.join(dataDir, 'businesses', 'personal', 'marcus-community-intelligence.json');
    await fs.writeFile(file, '{corrupt', 'utf8');
    const recovered = new CommunityIntelligenceStore({ dataDir });
    const listed = await recovered.listMembers('personal');
    assert.equal(listed.members.length, 1);
    const files = await fs.readdir(path.dirname(file));
    assert.equal(files.some((name) => name.startsWith('marcus-community-intelligence.json.corrupt-')), true);
  });
});
