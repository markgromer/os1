import assert from 'node:assert/strict';
import test from 'node:test';
import { createMarcusRealtimeVoice } from '../client/marcus-realtime.js';

class Emitter {
  constructor() { this.handlers = new Map(); }
  on(name, handler) {
    const handlers = this.handlers.get(name) || [];
    handlers.push(handler);
    this.handlers.set(name, handlers);
    return this;
  }
  emit(name, ...args) {
    for (const handler of this.handlers.get(name) || []) handler(...args);
  }
}

class FakeSession extends Emitter {
  constructor() {
    super();
    this.transport = new Emitter();
    this.connectCalls = [];
    this.closeCalls = 0;
    this.interruptCalls = 0;
  }
  async connect(options) {
    this.connectCalls.push(options);
    this.transport.emit('connection_change', 'connected');
  }
  close() {
    this.closeCalls += 1;
    this.transport.emit('connection_change', 'disconnected');
  }
  interrupt() { this.interruptCalls += 1; }
}

function secretResponse() {
  return {
    ok: true,
    status: 200,
    json: async () => ({ value: 'ek_test', session: { model: 'gpt-realtime-2.1', voice: 'marin' } }),
  };
}

test('Marcus browser voice uses the SDK tool bridge and reports interruption lifecycle', async () => {
  const sessions = [];
  const configs = [];
  const statuses = [];
  const transcripts = [];
  const assistantText = [];
  const telemetry = [];
  const requests = [];
  const voice = createMarcusRealtimeVoice({
    fetchFn: async () => secretResponse(),
    getAuthToken: async () => 'live-token',
    invokeMarcus: async (message) => { requests.push(message); return { ok: true, reply: 'Durable result' }; },
    sessionFactory: (config) => {
      configs.push(config);
      const session = new FakeSession();
      sessions.push(session);
      return session;
    },
    onStatus: (status) => statuses.push(status),
    onTranscript: (text) => transcripts.push(text),
    onAssistantText: (text) => assistantText.push(text),
    onEvent: (event) => telemetry.push(event),
    sessionRefreshMs: 60_000,
  });

  await voice.start();
  assert.equal(voice.getState(), 'listening');
  assert.equal(voice.isActive(), true);
  assert.deepEqual(sessions[0].connectCalls, [{ apiKey: 'ek_test' }]);
  assert.equal(configs[0].model, 'gpt-realtime-2.1');
  assert.equal(configs[0].voice, 'marin');
  assert.deepEqual(await configs[0].executeOperator('Audit Reggie'), { ok: true, reply: 'Durable result' });
  assert.deepEqual(requests, ['Audit Reggie']);

  sessions[0].emit('transport_event', { type: 'conversation.item.input_audio_transcription.completed', transcript: 'Audit Reggie' });
  sessions[0].emit('transport_event', { type: 'response.output_audio_transcript.done', transcript: 'I audited Reggie.' });
  sessions[0].emit('audio_start');
  assert.equal(voice.getState(), 'speaking');
  sessions[0].emit('audio_interrupted');
  assert.equal(voice.getState(), 'listening');
  assert.deepEqual(transcripts, ['Audit Reggie']);
  assert.deepEqual(assistantText, ['I audited Reggie.']);
  assert.ok(statuses.some((status) => status.detail === 'Interrupted; listening'));
  assert.ok(telemetry.some((event) => event.type === 'user_transcript' && event.length === 12));
  assert.ok(telemetry.some((event) => event.type === 'assistant_transcript' && event.length === 17));
  assert.ok(telemetry.some((event) => event.type === 'audio_started'));
  assert.ok(telemetry.some((event) => event.type === 'audio_interrupted'));
  assert.ok(telemetry.some((event) => event.type === 'operator_completed' && event.outcome === 'success'));
  assert.doesNotMatch(JSON.stringify(telemetry), /Audit Reggie|I audited Reggie|Durable result/);

  voice.interrupt();
  assert.equal(sessions[0].interruptCalls, 1);
  voice.stop();
  assert.equal(voice.getState(), 'offline');
  assert.equal(voice.isActive(), false);
});

test('Marcus browser voice reconnects after background and network recovery with fresh credentials', async () => {
  const sessions = [];
  let secretRequests = 0;
  let online = true;
  const voice = createMarcusRealtimeVoice({
    fetchFn: async () => { secretRequests += 1; return secretResponse(); },
    sessionFactory: () => {
      const session = new FakeSession();
      sessions.push(session);
      return session;
    },
    reconnectDelaysMs: [0, 0, 0],
    sessionRefreshMs: 60_000,
    isOnline: () => online,
  });

  await voice.start();
  voice.suspend();
  assert.equal(voice.getState(), 'paused');
  assert.equal(voice.isActive(), true);
  assert.equal(sessions[0].closeCalls, 1);
  await voice.resume();
  assert.equal(voice.getState(), 'listening');
  assert.equal(secretRequests, 2);

  online = false;
  await voice.networkChanged(false);
  assert.equal(voice.getState(), 'reconnecting');
  online = true;
  await voice.networkChanged(true);
  assert.equal(voice.getState(), 'listening');
  assert.equal(secretRequests, 3);

  sessions.at(-1).transport.emit('connection_change', 'disconnected');
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(voice.getState(), 'listening');
  assert.equal(secretRequests, 4);
  voice.stop();
});

test('Marcus browser voice retries an expired ephemeral credential with a fresh one', async () => {
  let secretRequests = 0;
  const sessions = [];
  const voice = createMarcusRealtimeVoice({
    fetchFn: async () => { secretRequests += 1; return secretResponse(); },
    sessionFactory: () => {
      const session = new FakeSession();
      if (sessions.length === 0) session.connect = async () => { throw new Error('ephemeral token expired'); };
      sessions.push(session);
      return session;
    },
    reconnectDelaysMs: [0, 0],
    sessionRefreshMs: 60_000,
  });

  await assert.rejects(voice.start(), /expired/i);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(secretRequests, 2);
  assert.equal(voice.getState(), 'listening');
  assert.equal(voice.isActive(), true);
  voice.stop();
});

test('a stopped stale connection cannot replace a newer Marcus voice session', async () => {
  let resolveFirstAuth;
  const firstAuth = new Promise((resolve) => { resolveFirstAuth = resolve; });
  let authCalls = 0;
  let secretRequests = 0;
  const sessions = [];
  const voice = createMarcusRealtimeVoice({
    getAuthToken: async () => {
      authCalls += 1;
      if (authCalls === 1) return firstAuth;
      return 'fresh-live-token';
    },
    fetchFn: async () => { secretRequests += 1; return secretResponse(); },
    sessionFactory: () => {
      const session = new FakeSession();
      sessions.push(session);
      return session;
    },
    sessionRefreshMs: 60_000,
  });

  const staleStart = voice.start();
  voice.stop();
  const freshStart = voice.start();
  resolveFirstAuth('stale-live-token');
  await Promise.all([staleStart, freshStart]);

  assert.equal(secretRequests, 1);
  assert.equal(sessions.length, 1);
  assert.equal(voice.getState(), 'listening');
  voice.stop();
});
