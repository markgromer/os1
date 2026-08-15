import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  normalizeRealtimeTelemetryEvent,
  RealtimeTelemetryStore,
  summarizeRealtimeTelemetry,
} from '../marcus/voice/realtime_telemetry.js';

function completeSession(sessionId = 'voice-session-1') {
  const base = { sessionId, occurredAt: '2026-08-12T07:00:00.000Z' };
  return [
    { ...base, eventId: 'e1', type: 'client_context', platform: 'android', browser: 'chromium', displayMode: 'standalone', installed: true, online: true },
    { ...base, eventId: 'e2', type: 'session_started' },
    { ...base, eventId: 'e3', type: 'voice_state', state: 'listening' },
    { ...base, eventId: 'e4', type: 'speech_started' },
    { ...base, eventId: 'e5', type: 'speech_stopped' },
    { ...base, eventId: 'e6', type: 'user_transcript', length: 42, text: 'This text must never be persisted.' },
    { ...base, eventId: 'e7', type: 'operator_started' },
    { ...base, eventId: 'e8', type: 'operator_completed', outcome: 'success', operationId: 'op_voice_acceptance' },
    { ...base, eventId: 'e9', type: 'audio_started' },
    { ...base, eventId: 'e10', type: 'assistant_transcript', length: 80, text: 'Nor should assistant text be persisted.' },
    { ...base, eventId: 'e11', type: 'audio_interrupted' },
    { ...base, eventId: 'e12', type: 'network_offline' },
    { ...base, eventId: 'e13', type: 'network_online' },
    { ...base, eventId: 'e14', type: 'voice_state', state: 'listening' },
    { ...base, eventId: 'e15', type: 'background_suspended' },
    { ...base, eventId: 'e16', type: 'background_resumed' },
    { ...base, eventId: 'e17', type: 'voice_state', state: 'listening' },
    { ...base, eventId: 'e18', type: 'physical_review_confirmed', confirmed: true, note: 'must not persist' },
  ];
}

test('realtime telemetry strips content and summarizes every acceptance gate', () => {
  const nowMs = Date.parse('2026-08-12T07:00:01.000Z');
  const events = completeSession().map((event) => normalizeRealtimeTelemetryEvent(event, { nowMs }));
  assert.equal(events.every(Boolean), true);
  assert.doesNotMatch(JSON.stringify(events), /text must never|assistant text/i);
  const summary = summarizeRealtimeTelemetry(events, 'voice-session-1');
  assert.equal(summary.readyForPhysicalReview, true);
  assert.equal(summary.physicalReviewConfirmed, true);
  assert.equal(summary.acceptedOnPhysicalDevice, true);
  assert.deepEqual(summary.gates, {
    signalingConnected: true,
    userSpeechRecognized: true,
    assistantAudioStreamed: true,
    interruptionObserved: true,
    operatorBridgeCompleted: true,
    networkRecovery: true,
    backgroundRecovery: true,
    installedAndroidContext: true,
  });
  assert.equal(summary.operationId, 'op_voice_acceptance');
});

test('realtime telemetry accepts bounded personality mode changes without content', () => {
  const event = normalizeRealtimeTelemetryEvent({
    eventId: 'mode-change',
    sessionId: 'voice-session-1',
    type: 'personality_mode_changed',
    mode: 'demo',
    source: 'mobile_ui',
    changed: true,
    text: 'do not store this',
  }, { nowMs: Date.parse('2026-08-12T07:00:01.000Z') });

  assert.equal(event.type, 'personality_mode_changed');
  assert.equal(event.mode, 'demo');
  assert.equal(event.source, 'mobile_ui');
  assert.equal(event.changed, true);
  assert.equal(Object.prototype.hasOwnProperty.call(event, 'text'), false);
});

test('realtime telemetry persists bounded, idempotent, content-free events', async () => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'marcus-realtime-telemetry-'));
  try {
    const store = new RealtimeTelemetryStore({ dataDir, maxEvents: 100 });
    const first = await store.append('personal', completeSession());
    assert.equal(first.accepted, 18);
    const duplicate = await store.append('personal', completeSession());
    assert.equal(duplicate.duplicates, 18);
    const result = await store.acceptance('personal', { sessionId: 'voice-session-1' });
    assert.equal(result.latest.readyForPhysicalReview, true);
    assert.equal(result.latest.acceptedOnPhysicalDevice, true);
    assert.equal(result.privacy.transcriptTextStored, false);
    const raw = await fs.readFile(path.join(dataDir, 'businesses', 'personal', 'marcus-realtime-telemetry.json'), 'utf8');
    assert.doesNotMatch(raw, /text must never|assistant text|must not persist/i);
  } finally {
    await fs.rm(dataDir, { recursive: true, force: true });
  }
});
