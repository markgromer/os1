import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const TELEMETRY_VERSION = 1;
const DEFAULT_MAX_EVENTS = 1_000;
const MAX_BATCH_SIZE = 50;
const MAX_SESSION_ID_LENGTH = 80;
const MAX_OPERATION_ID_LENGTH = 120;
const TRANSIENT_RENAME_CODES = new Set(['EACCES', 'EBUSY', 'EPERM']);

export const REALTIME_TELEMETRY_EVENT_TYPES = new Set([
  'client_context',
  'session_started',
  'session_stopped',
  'voice_state',
  'speech_started',
  'speech_stopped',
  'user_transcript',
  'assistant_transcript',
  'audio_started',
  'audio_stopped',
  'audio_interrupted',
  'operator_started',
  'operator_completed',
  'network_offline',
  'network_online',
  'background_suspended',
  'background_resumed',
  'voice_error',
]);

const VOICE_STATES = new Set(['offline', 'connecting', 'listening', 'thinking', 'speaking', 'reconnecting', 'paused', 'error']);
const OUTCOMES = new Set(['success', 'failure']);
const DISPLAY_MODES = new Set(['browser', 'standalone', 'minimal-ui', 'fullscreen', 'unknown']);
const PLATFORMS = new Set(['android', 'ios', 'desktop', 'unknown']);
const BROWSERS = new Set(['chromium', 'firefox', 'safari', 'other', 'unknown']);

function boundedToken(value, maxLength) {
  const token = typeof value === 'string' ? value.trim() : '';
  if (!token || token.length > maxLength || !/^[a-zA-Z0-9._:-]+$/.test(token)) return '';
  return token;
}

function enumValue(value, allowed, fallback = '') {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return allowed.has(normalized) ? normalized : fallback;
}

function boundedInteger(value, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.min(max, Math.max(min, Math.round(parsed)));
}

function normalizeOccurredAt(value, nowMs) {
  const parsed = typeof value === 'number' ? value : Date.parse(String(value || ''));
  const earliest = nowMs - (30 * 24 * 60 * 60_000);
  const latest = nowMs + (24 * 60 * 60_000);
  const timestamp = Number.isFinite(parsed) && parsed >= earliest && parsed <= latest ? parsed : nowMs;
  return new Date(timestamp).toISOString();
}

function normalizeBusinessKey(value) {
  return String(value || 'personal').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64) || 'personal';
}

export function normalizeRealtimeTelemetryEvent(input, { nowMs = Date.now() } = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const sessionId = boundedToken(input.sessionId, MAX_SESSION_ID_LENGTH);
  const type = enumValue(input.type, REALTIME_TELEMETRY_EVENT_TYPES);
  if (!sessionId || !type) return null;

  const event = {
    eventId: boundedToken(input.eventId, MAX_SESSION_ID_LENGTH) || crypto.randomUUID(),
    sessionId,
    type,
    occurredAt: normalizeOccurredAt(input.occurredAt, nowMs),
    receivedAt: new Date(nowMs).toISOString(),
  };

  if (type === 'voice_state') {
    const state = enumValue(input.state, VOICE_STATES);
    if (!state) return null;
    event.state = state;
    const reconnectAttempt = boundedInteger(input.reconnectAttempt, 0, 100);
    if (reconnectAttempt !== null) event.reconnectAttempt = reconnectAttempt;
  }
  if (type === 'user_transcript' || type === 'assistant_transcript') {
    const length = boundedInteger(input.length, 1, 12_000);
    if (length === null) return null;
    event.length = length;
  }
  if (type === 'operator_completed') {
    event.outcome = enumValue(input.outcome, OUTCOMES, 'failure');
    const operationId = boundedToken(input.operationId, MAX_OPERATION_ID_LENGTH);
    if (operationId) event.operationId = operationId;
  }
  if (type === 'client_context') {
    event.displayMode = enumValue(input.displayMode, DISPLAY_MODES, 'unknown');
    event.platform = enumValue(input.platform, PLATFORMS, 'unknown');
    event.browser = enumValue(input.browser, BROWSERS, 'unknown');
    event.installed = input.installed === true;
    event.online = input.online !== false;
  }
  return event;
}

function eventIndexAfter(events, type, afterIndex = -1, predicate = () => true) {
  for (let index = afterIndex + 1; index < events.length; index += 1) {
    if (events[index].type === type && predicate(events[index])) return index;
  }
  return -1;
}

function listeningIndexAfter(events, afterIndex) {
  return eventIndexAfter(events, 'voice_state', afterIndex, (event) => event.state === 'listening');
}

export function summarizeRealtimeTelemetry(events, sessionId) {
  const sessionEvents = (Array.isArray(events) ? events : []).filter((event) => event?.sessionId === sessionId);
  if (!sessionEvents.length) return null;
  const context = [...sessionEvents].reverse().find((event) => event.type === 'client_context') || {};
  const networkOfflineIndex = eventIndexAfter(sessionEvents, 'network_offline');
  const networkOnlineIndex = networkOfflineIndex >= 0 ? eventIndexAfter(sessionEvents, 'network_online', networkOfflineIndex) : -1;
  const backgroundSuspendIndex = eventIndexAfter(sessionEvents, 'background_suspended');
  const backgroundResumeIndex = backgroundSuspendIndex >= 0 ? eventIndexAfter(sessionEvents, 'background_resumed', backgroundSuspendIndex) : -1;
  const successfulOperator = sessionEvents.find((event) => event.type === 'operator_completed' && event.outcome === 'success');
  const androidStandalone = context.platform === 'android' && context.displayMode === 'standalone' && context.installed === true;
  const gates = {
    signalingConnected: listeningIndexAfter(sessionEvents, -1) >= 0,
    userSpeechRecognized: sessionEvents.some((event) => event.type === 'user_transcript' && event.length > 0),
    assistantAudioStreamed: sessionEvents.some((event) => event.type === 'audio_started'),
    interruptionObserved: sessionEvents.some((event) => event.type === 'audio_interrupted'),
    operatorBridgeCompleted: Boolean(successfulOperator),
    networkRecovery: networkOnlineIndex >= 0 && listeningIndexAfter(sessionEvents, networkOnlineIndex) >= 0,
    backgroundRecovery: backgroundResumeIndex >= 0 && listeningIndexAfter(sessionEvents, backgroundResumeIndex) >= 0,
    installedAndroidContext: androidStandalone,
  };
  return {
    sessionId,
    startedAt: sessionEvents[0].occurredAt,
    lastEventAt: sessionEvents.at(-1).occurredAt,
    eventCount: sessionEvents.length,
    client: {
      displayMode: context.displayMode || 'unknown',
      platform: context.platform || 'unknown',
      browser: context.browser || 'unknown',
      installed: context.installed === true,
    },
    gates,
    readyForPhysicalReview: Object.values(gates).every(Boolean),
    operationId: successfulOperator?.operationId || '',
  };
}

async function replaceFileAtomically(tmpFile, destinationFile) {
  const maxAttempts = process.platform === 'win32' ? 6 : 1;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await fs.rename(tmpFile, destinationFile);
      return;
    } catch (error) {
      if (attempt === maxAttempts || !TRANSIENT_RENAME_CODES.has(error?.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 20 * attempt));
    }
  }
}

export class RealtimeTelemetryStore {
  constructor({ dataDir, maxEvents = DEFAULT_MAX_EVENTS } = {}) {
    this.dataDir = path.resolve(String(dataDir || path.join(process.cwd(), 'data')));
    this.maxEvents = Math.max(100, boundedInteger(maxEvents, 100, 10_000) || DEFAULT_MAX_EVENTS);
    this.writeQueue = Promise.resolve();
  }

  fileForBusiness(businessKey) {
    return path.join(this.dataDir, 'businesses', normalizeBusinessKey(businessKey), 'marcus-realtime-telemetry.json');
  }

  async read(businessKey) {
    const file = this.fileForBusiness(businessKey);
    try {
      const parsed = JSON.parse(await fs.readFile(file, 'utf8'));
      return {
        version: TELEMETRY_VERSION,
        events: Array.isArray(parsed?.events) ? parsed.events.slice(-this.maxEvents) : [],
      };
    } catch (error) {
      if (error?.code === 'ENOENT') return { version: TELEMETRY_VERSION, events: [] };
      throw error;
    }
  }

  async append(businessKey, rawEvents) {
    const batch = (Array.isArray(rawEvents) ? rawEvents : [rawEvents]).slice(0, MAX_BATCH_SIZE);
    const normalized = batch.map((event) => normalizeRealtimeTelemetryEvent(event)).filter(Boolean);
    const run = async () => {
      const current = await this.read(businessKey);
      const existingIds = new Set(current.events.map((event) => `${event.sessionId}:${event.eventId}`));
      const accepted = [];
      for (const event of normalized) {
        const key = `${event.sessionId}:${event.eventId}`;
        if (existingIds.has(key)) continue;
        existingIds.add(key);
        accepted.push(event);
      }
      const events = [...current.events, ...accepted].slice(-this.maxEvents);
      const file = this.fileForBusiness(businessKey);
      await fs.mkdir(path.dirname(file), { recursive: true });
      const tmpFile = `${file}.tmp-${crypto.randomBytes(6).toString('hex')}`;
      await fs.writeFile(tmpFile, `${JSON.stringify({ version: TELEMETRY_VERSION, events }, null, 2)}\n`, 'utf8');
      await replaceFileAtomically(tmpFile, file);
      return { accepted: accepted.length, rejected: batch.length - normalized.length, duplicates: normalized.length - accepted.length };
    };
    const pending = this.writeQueue.then(run, run);
    this.writeQueue = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async acceptance(businessKey, { sessionId = '', limit = 20 } = {}) {
    await this.writeQueue;
    const { events } = await this.read(businessKey);
    const requestedSessionId = boundedToken(sessionId, MAX_SESSION_ID_LENGTH);
    const sessionIds = [];
    const seen = new Set();
    for (let index = events.length - 1; index >= 0; index -= 1) {
      const candidate = events[index]?.sessionId;
      if (!candidate || seen.has(candidate) || (requestedSessionId && candidate !== requestedSessionId)) continue;
      seen.add(candidate);
      sessionIds.push(candidate);
      if (sessionIds.length >= Math.min(50, Math.max(1, Number(limit) || 20))) break;
    }
    const sessions = sessionIds.map((id) => summarizeRealtimeTelemetry(events, id)).filter(Boolean);
    return {
      version: TELEMETRY_VERSION,
      privacy: {
        transcriptTextStored: false,
        requestTextStored: false,
        credentialsStored: false,
        retentionEvents: this.maxEvents,
      },
      sessions,
      latest: sessions[0] || null,
    };
  }
}
