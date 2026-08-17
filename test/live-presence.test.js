import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildLivePresenceStatus,
  patchLivePresenceSetup,
} from '../marcus/live/live_presence.js';

test('live presence status reports Mark-owned blockers before setup is complete', () => {
  const status = buildLivePresenceStatus({
    settings: {},
    voice: { configured: false, telemetryReady: true },
    desktop: null,
  });

  assert.equal(status.model, 'local_browser_presence');
  assert.equal(status.readyForPublicVoice, false);
  assert.equal(status.desktopOnline, false);
  assert.ok(status.nextHumanSteps.some((item) => item.id === 'browser_profile'));
  assert.ok(status.blockedReasons.some((reason) => reason.includes('Desktop agent')));
});

test('live presence setup patch saves checklist and enables public voice when required gates pass', () => {
  let settings = {};
  settings = patchLivePresenceSetup(settings, {
    defaultMode: 'public_auto_reply',
    completed: {
      browser_profile: true,
      google_account: true,
      gmail_access: true,
      assistant_identity: true,
      platform_login: true,
      audio_router: true,
      marcus_mic: true,
      mark_mic: true,
      emergency_controls: true,
    },
    notes: {
      audio_router: 'VB-CABLE installed and selected.',
    },
  }, '2026-08-15T12:00:00.000Z');

  const status = buildLivePresenceStatus({
    settings,
    voice: { configured: true, telemetryReady: true },
    desktop: { agentId: 'Marks_PC', observedAt: '2026-08-15T12:01:00.000Z' },
  });

  assert.equal(status.defaultMode, 'public_auto_reply');
  assert.equal(status.readyForPublicVoice, true);
  assert.equal(status.requiredCompleted, status.requiredTotal);
  assert.equal(status.items.find((item) => item.id === 'audio_router').note, 'VB-CABLE installed and selected.');
});
