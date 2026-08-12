import { RealtimeAgent, RealtimeSession, tool } from '@openai/agents-realtime';
import { z } from 'zod';

const DEFAULT_MODEL = 'gpt-realtime-2.1';
const DEFAULT_VOICE = 'marin';
const DEFAULT_RECONNECT_DELAYS_MS = [500, 1_000, 2_000, 4_000, 8_000, 15_000];
const DEFAULT_SESSION_REFRESH_MS = 55 * 60_000;

function errorMessage(error, fallback = 'Marcus voice failed.') {
  const candidate = error?.error?.message || error?.message || error;
  return String(candidate || fallback).trim() || fallback;
}

function isRetryableConnectionError(error) {
  const status = Number(error?.status || error?.statusCode || 0);
  const name = String(error?.name || '').toLowerCase();
  const message = errorMessage(error).toLowerCase();
  if ([400, 401, 403].includes(status)) return false;
  if (['notallowederror', 'securityerror'].includes(name)) return false;
  return !/(microphone|permission).*(denied|blocked)|unauthorized|invalid admin token|openai is not configured/.test(message);
}

function createSdkSession({ model, voice, executeOperator }) {
  const operatorTool = tool({
    name: 'marcus_operator',
    description: 'Send Mark\'s complete spoken request to the durable Marcus operator. Use this for every substantive question, project discussion, Codex request, status check, decision, and approval.',
    parameters: z.object({
      message: z.string().min(1).max(12_000).describe('Mark\'s complete request or follow-up, preserving project names, constraints, and approval language.'),
    }),
    timeoutMs: 120_000,
    async execute({ message }) {
      const output = await executeOperator(String(message || '').trim().slice(0, 12_000));
      return JSON.stringify(output);
    },
  });

  const agent = new RealtimeAgent({
    name: 'Marcus Voice',
    instructions: [
      "You are the realtime voice interface for Marcus, Mark's trusted project operator.",
      'Keep the conversation natural, calm, concise, and spoken-friendly.',
      'For every substantive question, project discussion, request, decision, status check, or follow-up, call marcus_operator exactly once with the complete user intent. Preserve project names and important details. Short approval follow-ups such as "do it" must also go through the tool.',
      'Do not answer substantive requests from your own knowledge and do not claim that work was executed independently. Marcus is the authority for project context, durable operations, Codex work, approvals, and completion evidence.',
      'After marcus_operator returns, speak its result faithfully. Preserve operation identifiers, approval requests, blockers, and uncertainty when they matter.',
      'Never bypass Marcus approval requirements for external messages, publishing, deployment, DNS, merges, billing, or other consequential actions.',
      'You may respond without a tool only to a brief greeting, a request to repeat yourself, or a voice-session control question.',
    ].join('\n'),
    tools: [operatorTool],
  });

  return new RealtimeSession(agent, {
    model,
    transport: 'webrtc',
    historyStoreAudio: false,
    tracingDisabled: true,
    config: {
      outputModalities: ['audio'],
      parallelToolCalls: false,
      reasoning: { effort: 'low' },
      audio: {
        input: {
          noiseReduction: { type: 'near_field' },
          transcription: {
            model: 'gpt-live-transcribe',
            delay: 'low',
            prompt: 'A project operations conversation with Marcus. Preserve project, repository, domain, company, and product names exactly.',
            keywords: ['Marcus', 'Codex', 'GitHub', 'Cloudflare', 'Reggie', 'Sweep and Go'],
            languages: ['en'],
          },
          turnDetection: {
            type: 'semantic_vad',
            eagerness: 'medium',
            createResponse: true,
            interruptResponse: true,
          },
        },
        output: { voice },
      },
    },
  });
}

export function createMarcusRealtimeVoice(options = {}) {
  const onStatus = typeof options.onStatus === 'function' ? options.onStatus : () => {};
  const onTranscript = typeof options.onTranscript === 'function' ? options.onTranscript : () => {};
  const onAssistantText = typeof options.onAssistantText === 'function' ? options.onAssistantText : () => {};
  const onError = typeof options.onError === 'function' ? options.onError : () => {};
  const invokeMarcus = typeof options.invokeMarcus === 'function'
    ? options.invokeMarcus
    : async () => ({ ok: false, error: 'Marcus operator bridge is unavailable.' });
  const getAuthToken = typeof options.getAuthToken === 'function' ? options.getAuthToken : async () => '';
  const fetchFn = typeof options.fetchFn === 'function' ? options.fetchFn : globalThis.fetch.bind(globalThis);
  const sessionFactory = typeof options.sessionFactory === 'function' ? options.sessionFactory : createSdkSession;
  const setTimer = typeof options.setTimer === 'function' ? options.setTimer : globalThis.setTimeout.bind(globalThis);
  const clearTimer = typeof options.clearTimer === 'function' ? options.clearTimer : globalThis.clearTimeout.bind(globalThis);
  const isOnline = typeof options.isOnline === 'function'
    ? options.isOnline
    : () => typeof navigator === 'undefined' || navigator.onLine !== false;
  const reconnectDelays = Array.isArray(options.reconnectDelaysMs) && options.reconnectDelaysMs.length
    ? options.reconnectDelaysMs.map((value) => Math.max(0, Number(value) || 0))
    : DEFAULT_RECONNECT_DELAYS_MS;
  const sessionRefreshMs = Math.max(1_000, Number(options.sessionRefreshMs) || DEFAULT_SESSION_REFRESH_MS);

  let session = null;
  let state = 'offline';
  let desiredActive = false;
  let suspended = false;
  let reconnectAttempt = 0;
  let reconnectTimer = null;
  let refreshTimer = null;
  let setupAbort = null;
  let connectPromise = null;
  let connectionVersion = 0;

  function setState(next, detail = '') {
    state = next;
    onStatus({ state, detail, active: desiredActive, reconnectAttempt });
  }

  function clearReconnectTimer() {
    if (reconnectTimer !== null) clearTimer(reconnectTimer);
    reconnectTimer = null;
  }

  function clearRefreshTimer() {
    if (refreshTimer !== null) clearTimer(refreshTimer);
    refreshTimer = null;
  }

  function closeCurrentSession() {
    connectionVersion += 1;
    setupAbort?.abort();
    setupAbort = null;
    clearRefreshTimer();
    const current = session;
    session = null;
    try { current?.close(); } catch {}
  }

  function exhaustReconnects(message) {
    desiredActive = false;
    const error = new Error(message || 'Marcus voice could not reconnect. Tap Retry voice.');
    setState('error', error.message);
    onError(error);
  }

  function scheduleReconnect(detail = 'Voice connection lost') {
    if (!desiredActive || suspended || reconnectTimer !== null || connectPromise) return;
    if (!isOnline()) {
      setState('reconnecting', 'Waiting for network');
      return;
    }
    if (reconnectAttempt >= reconnectDelays.length) {
      exhaustReconnects('Marcus voice could not reconnect. Tap Retry voice.');
      return;
    }
    const delay = reconnectDelays[reconnectAttempt];
    reconnectAttempt += 1;
    setState('reconnecting', delay ? `${detail}. Retrying shortly.` : `${detail}. Reconnecting now.`);
    reconnectTimer = setTimer(() => {
      reconnectTimer = null;
      connectNow({ automatic: true }).catch(() => {});
    }, delay);
  }

  function bindSessionEvents(current, version) {
    const isCurrent = () => session === current && connectionVersion === version;
    current.transport.on('connection_change', (status) => {
      if (!isCurrent()) return;
      if (status === 'connected') {
        reconnectAttempt = 0;
        setState('listening', 'Listening');
      } else if (status === 'disconnected') {
        session = null;
        clearRefreshTimer();
        try { current.close(); } catch {}
        if (desiredActive && !suspended) scheduleReconnect('Voice connection lost');
        else setState(suspended ? 'paused' : 'offline', suspended ? 'Voice paused while the app is in the background' : 'Voice off');
      }
    });
    current.on('agent_start', () => { if (isCurrent()) setState('thinking', 'Thinking'); });
    current.on('agent_tool_start', () => { if (isCurrent()) setState('thinking', 'Marcus is working on it'); });
    current.on('audio_start', () => { if (isCurrent()) setState('speaking', 'Marcus is speaking'); });
    current.on('audio_stopped', () => { if (isCurrent()) setState('listening', 'Listening'); });
    current.on('audio_interrupted', () => { if (isCurrent()) setState('listening', 'Interrupted; listening'); });
    current.on('agent_end', () => {
      if (isCurrent() && state !== 'speaking') setState('listening', 'Listening');
    });
    current.on('transport_event', (event) => {
      if (!isCurrent()) return;
      if (event?.type === 'input_audio_buffer.speech_started') setState('listening', 'Listening');
      if (event?.type === 'input_audio_buffer.speech_stopped') setState('thinking', 'Thinking');
      if (event?.type === 'conversation.item.input_audio_transcription.completed') {
        const transcript = String(event.transcript || '').trim();
        if (transcript) onTranscript(transcript);
      }
      if (event?.type === 'response.output_audio_transcript.done') {
        const transcript = String(event.transcript || '').trim();
        if (transcript) onAssistantText(transcript);
      }
    });
    current.on('error', (event) => {
      if (!isCurrent()) return;
      const error = event?.error instanceof Error ? event.error : new Error(errorMessage(event));
      onError(error);
    });
  }

  async function executeOperator(message) {
    const normalized = String(message || '').trim().slice(0, 12_000);
    setState('thinking', normalized ? 'Marcus is working on it' : 'Marcus is checking the request');
    if (!normalized) return { ok: false, error: 'The voice request did not contain a usable message.' };
    try {
      return await invokeMarcus(normalized);
    } catch (error) {
      return { ok: false, error: errorMessage(error, 'Marcus could not process the voice request.') };
    }
  }

  async function connectAttempt(version, abortController) {
    if (!isOnline()) throw Object.assign(new Error('Waiting for network'), { retryable: true });
    const token = String(await getAuthToken() || '').trim();
    if (version !== connectionVersion || !desiredActive || suspended) return false;
    const secretResponse = await fetchFn('/api/marcus/realtime/client-secret', {
      method: 'POST',
      credentials: 'include',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      signal: abortController.signal,
    });
    const secret = await secretResponse.json().catch(() => ({}));
    if (!secretResponse.ok || !secret.value) {
      throw Object.assign(new Error(secret.error || `Voice session setup failed (${secretResponse.status}).`), { status: secretResponse.status });
    }
    if (version !== connectionVersion || !desiredActive || suspended) return false;

    const current = sessionFactory({
      model: String(secret.session?.model || DEFAULT_MODEL),
      voice: String(secret.session?.voice || DEFAULT_VOICE),
      executeOperator,
    });
    session = current;
    bindSessionEvents(current, version);
    await current.connect({ apiKey: secret.value });
    if (version !== connectionVersion || session !== current || !desiredActive || suspended) {
      if (session === current) session = null;
      try { current.close(); } catch {}
      return false;
    }

    reconnectAttempt = 0;
    setState('listening', 'Listening');
    clearRefreshTimer();
    refreshTimer = setTimer(() => {
      if (!desiredActive || suspended || session !== current) return;
      closeCurrentSession();
      reconnectAttempt = 0;
      scheduleReconnect('Refreshing voice session');
    }, sessionRefreshMs);
    return true;
  }

  async function connectNow({ automatic = false } = {}) {
    if (!desiredActive || suspended) return false;
    if (session) return true;
    if (connectPromise) return connectPromise;
    clearReconnectTimer();
    const version = ++connectionVersion;
    const abortController = new AbortController();
    setupAbort = abortController;
    setState(automatic ? 'reconnecting' : 'connecting', automatic ? 'Reconnecting voice' : 'Connecting voice');
    const pending = connectAttempt(version, abortController);
    connectPromise = pending;
    let retry = null;
    try {
      return await pending;
    } catch (error) {
      if (version !== connectionVersion) return false;
      const current = session;
      session = null;
      try { current?.close(); } catch {}
      if (desiredActive && !suspended && (error?.retryable === true || isRetryableConnectionError(error))) {
        retry = errorMessage(error, 'Voice connection failed');
      } else {
        desiredActive = false;
        setState('error', errorMessage(error, 'Voice connection failed.'));
        onError(error instanceof Error ? error : new Error(errorMessage(error)));
      }
      throw error;
    } finally {
      if (connectPromise === pending) connectPromise = null;
      if (setupAbort === abortController) setupAbort = null;
      if (retry) scheduleReconnect(retry);
    }
  }

  async function start() {
    desiredActive = true;
    suspended = false;
    reconnectAttempt = 0;
    return connectNow({ automatic: false });
  }

  function stop() {
    desiredActive = false;
    suspended = false;
    reconnectAttempt = 0;
    clearReconnectTimer();
    closeCurrentSession();
    connectPromise = null;
    setState('offline', 'Voice off');
  }

  function suspend(detail = 'Voice paused while the app is in the background') {
    if (!desiredActive) return;
    suspended = true;
    clearReconnectTimer();
    closeCurrentSession();
    connectPromise = null;
    setState('paused', detail);
  }

  async function resume() {
    if (!desiredActive) return false;
    suspended = false;
    reconnectAttempt = 0;
    if (!isOnline()) {
      setState('reconnecting', 'Waiting for network');
      return false;
    }
    return connectNow({ automatic: true });
  }

  async function networkChanged(online) {
    if (!desiredActive) return false;
    if (!online) {
      clearReconnectTimer();
      closeCurrentSession();
      connectPromise = null;
      setState('reconnecting', 'Waiting for network');
      return false;
    }
    if (suspended) return false;
    return resume();
  }

  function interrupt() {
    try { session?.interrupt(); } catch {}
  }

  return {
    start,
    stop,
    suspend,
    resume,
    networkChanged,
    interrupt,
    getState: () => state,
    isActive: () => desiredActive,
  };
}

if (typeof window !== 'undefined') {
  window.createMarcusRealtimeVoice = createMarcusRealtimeVoice;
}
