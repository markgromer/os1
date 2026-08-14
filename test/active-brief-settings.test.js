import test from 'node:test';
import assert from 'node:assert/strict';

import { buildActiveBrief } from '../marcus/intelligence/active_brief.js';

test('connected AI and Google integrations do not create Mark credential decisions', () => {
  const brief = buildActiveBrief({
    settings: {
      aiEnabled: true,
      openrouterConfigured: true,
      openrouterKeyHint: '****1234',
      googleConfigured: true,
      googleConnected: true,
    },
    nowMs: Date.UTC(2026, 7, 14),
  });

  const priorityTitles = (brief.topPriorities || []).map((item) => item.title);
  assert.equal(priorityTitles.some((title) => /needs credentials|needs provider credentials|connection needs refresh/i.test(title)), false);
  assert.equal((brief.waitingOnMark || []).some((item) => /OpenAI routing|Google integrations/i.test(item.title)), false);
});

test('Google OAuth setup is distinct from account connection refresh', () => {
  const brief = buildActiveBrief({
    settings: {
      aiEnabled: true,
      openaiKeyHint: '****5678',
      googleConfigured: true,
      googleConnected: false,
    },
    nowMs: Date.UTC(2026, 7, 14),
  });

  const googleItem = (brief.waitingOnMark || []).find((item) => /Google/i.test(item.title));
  assert.ok(googleItem);
  assert.match(googleItem.title, /connection needs refresh/i);
  assert.match(googleItem.recommendedAction, /Reconnect Google account/i);
  assert.doesNotMatch(googleItem.title, /credentials/i);
});
