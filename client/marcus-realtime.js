import { RealtimeAgent, RealtimeSession, tool } from '@openai/agents-realtime';
import { z } from 'zod';
import {
  buildMarcusRealtimeInstructions,
  MARCUS_PERSONALITY_MODE_IDS,
  normalizeMarcusPersonalityMode,
} from '../marcus/voice/personality_modes.js';
import { sanitizeOperatorResultForSpeech } from '../marcus/voice/spoken_reference.js';

const DEFAULT_MODEL = 'gpt-realtime-2.1';
const DEFAULT_VOICE = 'cedar';
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

function createSdkSession({ model, voice, personalityMode, continuityBrief, executeOperator, setPersonalityMode, mediaStream, audioElement }) {
  const operatorTool = tool({
    name: 'marcus_operator',
    description: 'Send Mark\'s complete spoken request to the durable Marcus operator for project work, live status, Codex work, audits, approvals, consequential actions, durable memory, and verified completion evidence.',
    parameters: z.object({
      message: z.string().min(1).max(12_000).describe('Mark\'s complete request or follow-up, preserving project names, constraints, and approval language.'),
    }),
    timeoutMs: 120_000,
    async execute({ message }) {
      const output = await executeOperator(String(message || '').trim().slice(0, 12_000));
      return JSON.stringify(output);
    },
  });
  const personalityTool = tool({
    name: 'set_marcus_personality_mode',
    description: 'Change Marcus voice personality mode for this Realtime voice session when Mark explicitly asks for a mode change.',
    parameters: z.object({
      mode: z.enum(MARCUS_PERSONALITY_MODE_IDS).describe('The requested Marcus personality mode.'),
    }),
    async execute({ mode }) {
      const output = await setPersonalityMode(mode, { source: 'spoken_command' });
      return JSON.stringify(output);
    },
  });

  const agent = new RealtimeAgent({
    name: 'Marcus Voice',
    instructions: [
      buildMarcusRealtimeInstructions({ personalityMode, continuityBrief }),
      'Browser work is durable operator work. For any request to inspect, navigate, write, reply, comment, or post in MARCUS Chrome, Skool, Gmail, Zoom, or the visible thread or page, call marcus_operator exactly once. Never claim that you lack a browser before the operator reports the live browser status.',
      'A request to write or reply on a site should prepare the draft through MARCUS Chrome and place it in Mark\'s publication approval queue. Do not claim it was posted until the operator confirms the approved browser publication.',
    ].join('\n'),
    tools: [operatorTool, personalityTool],
  });

  return new RealtimeSession(agent, {
    model,
    transport: 'webrtc',
    historyStoreAudio: false,
    tracingDisabled: true,
    ...(mediaStream ? { mediaStream } : {}),
    ...(audioElement ? { audioElement } : {}),
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
  const onEvent = typeof options.onEvent === 'function' ? options.onEvent : () => {};
  const invokeMarcus = typeof options.invokeMarcus === 'function'
    ? options.invokeMarcus
    : async () => ({ ok: false, error: 'Marcus operator bridge is unavailable.' });
  const getAuthToken = typeof options.getAuthToken === 'function' ? options.getAuthToken : async () => '';
  const getMediaStream = typeof options.getMediaStream === 'function' ? options.getMediaStream : async () => options.mediaStream || null;
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
  let mutedIdleTimer = null;
  let modeSwitchTimer = null;
  let setupAbort = null;
  let connectPromise = null;
  let connectionVersion = 0;
  let inputMuted = false;
  let activePersonalityMode = normalizeMarcusPersonalityMode(options.personalityMode);

  function emitEvent(type, metadata = {}) {
    try {
      onEvent({ type, occurredAt: new Date().toISOString(), ...metadata });
    } catch {}
  }

  function setState(next, detail = '') {
    state = next;
    onStatus({ state, detail, active: desiredActive, reconnectAttempt });
    emitEvent('voice_state', { state, reconnectAttempt });
  }

  function clearReconnectTimer() {
    if (reconnectTimer !== null) clearTimer(reconnectTimer);
    reconnectTimer = null;
  }

  function clearRefreshTimer() {
    if (refreshTimer !== null) clearTimer(refreshTimer);
    refreshTimer = null;
  }

  function clearMutedIdleTimer() {
    if (mutedIdleTimer !== null) clearTimer(mutedIdleTimer);
    mutedIdleTimer = null;
  }

  function clearModeSwitchTimer() {
    if (modeSwitchTimer !== null) clearTimer(modeSwitchTimer);
    modeSwitchTimer = null;
  }

  function setReadyState(detail = '') {
    setState(inputMuted ? 'idle' : 'listening', detail || (inputMuted ? 'Hold Space to talk' : 'Listening'));
  }

  function closeCurrentSession() {
    connectionVersion += 1;
    setupAbort?.abort();
    setupAbort = null;
    clearRefreshTimer();
    clearMutedIdleTimer();
    clearModeSwitchTimer();
    const current = session;
    session = null;
    try { current?.close(); } catch {}
  }

  async function setPersonalityMode(mode, { source = 'manual' } = {}) {
    const next = normalizeMarcusPersonalityMode(mode);
    const changed = next !== activePersonalityMode;
    activePersonalityMode = next;
    emitEvent('personality_mode_changed', { mode: activePersonalityMode, source, changed });

    if (changed && desiredActive && !suspended) {
      const wasMuted = inputMuted;
      closeCurrentSession();
      reconnectAttempt = 0;
      setState('reconnecting', `Switching to ${activePersonalityMode.replace(/_/g, ' ')} mode`);
      modeSwitchTimer = setTimer(() => {
        modeSwitchTimer = null;
        if (!desiredActive || suspended) return;
        connectNow({ automatic: true }).then(() => {
          try { session?.mute(wasMuted); } catch {}
        }).catch(() => {});
      }, 0);
    }

    return { ok: true, mode: activePersonalityMode, changed };
  }

  function exhaustReconnects(message) {
    desiredActive = false;
    const error = new Error(message || 'Marcus voice could not reconnect. Tap Retry voice.');
    setState('error', error.message);
    emitEvent('voice_error');
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
    let assistantAudioActive = false;
    const markAssistantAudioStarted = () => {
      if (!isCurrent() || assistantAudioActive) return;
      assistantAudioActive = true;
      emitEvent('audio_started');
      setState('speaking', 'Marcus is speaking');
    };
    const markAssistantAudioStopped = () => {
      if (!isCurrent()) return;
      if (assistantAudioActive) emitEvent('audio_stopped');
      assistantAudioActive = false;
      setReadyState();
    };
    const markAssistantAudioInterrupted = () => {
      if (!isCurrent()) return;
      if (assistantAudioActive) emitEvent('audio_interrupted');
      assistantAudioActive = false;
      setReadyState(inputMuted ? 'Hold Space to talk' : 'Interrupted; listening');
    };
    current.transport.on('connection_change', (status) => {
      if (!isCurrent()) return;
      if (status === 'connected') {
        reconnectAttempt = 0;
        setReadyState();
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
    current.on('audio_start', markAssistantAudioStarted);
    current.on('audio_stopped', markAssistantAudioStopped);
    current.on('audio_interrupted', markAssistantAudioInterrupted);
    current.on('agent_end', () => {
      if (isCurrent() && state !== 'speaking') setReadyState();
    });
    current.on('transport_event', (event) => {
      if (!isCurrent()) return;
      if (event?.type === 'input_audio_buffer.speech_started') {
        emitEvent('speech_started');
        if (assistantAudioActive) markAssistantAudioInterrupted();
        else setReadyState('Listening');
      }
      if (event?.type === 'input_audio_buffer.speech_stopped') {
        emitEvent('speech_stopped');
        setState('thinking', 'Thinking');
      }
      if (event?.type === 'conversation.item.input_audio_transcription.completed') {
        const transcript = String(event.transcript || '').trim();
        if (transcript) {
          emitEvent('user_transcript', { length: transcript.length });
          onTranscript(transcript);
        }
      }
      if (event?.type === 'response.output_audio_transcript.done') {
        const transcript = String(event.transcript || '').trim();
        if (transcript) {
          emitEvent('assistant_transcript', { length: transcript.length });
          onAssistantText(transcript);
        }
      }
      if (event?.type === 'response.output_audio_transcript.delta') markAssistantAudioStarted();
      if (event?.type === 'response.output_audio.done') markAssistantAudioStopped();
    });
    current.on('error', (event) => {
      if (!isCurrent()) return;
      const error = event?.error instanceof Error ? event.error : new Error(errorMessage(event));
      emitEvent('voice_error');
      onError(error);
    });
  }

  async function executeOperator(message) {
    const normalized = String(message || '').trim().slice(0, 12_000);
    setState('thinking', normalized ? 'Marcus is working on it' : 'Marcus is checking the request');
    if (!normalized) return { ok: false, error: 'The voice request did not contain a usable message.' };
    emitEvent('operator_started');
    try {
      const result = await invokeMarcus(normalized);
      emitEvent('operator_completed', {
        outcome: result?.ok === false ? 'failure' : 'success',
        operationId: String(result?.operationId || '').trim().slice(0, 120),
      });
      return sanitizeOperatorResultForSpeech(result);
    } catch (error) {
      emitEvent('operator_completed', { outcome: 'failure' });
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
      headers: token
        ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
        : { 'Content-Type': 'application/json' },
      body: JSON.stringify({ personalityMode: activePersonalityMode }),
      signal: abortController.signal,
    });
    const secret = await secretResponse.json().catch(() => ({}));
    if (!secretResponse.ok || !secret.value) {
      throw Object.assign(new Error(secret.error || `Voice session setup failed (${secretResponse.status}).`), { status: secretResponse.status });
    }
    if (version !== connectionVersion || !desiredActive || suspended) return false;

    const mediaStream = await getMediaStream();
    const current = sessionFactory({
      model: String(secret.session?.model || DEFAULT_MODEL),
      voice: String(secret.session?.voice || DEFAULT_VOICE),
      personalityMode: normalizeMarcusPersonalityMode(secret.session?.personalityMode || secret.session?.personality_mode),
      continuityBrief: String(secret.session?.continuityBrief || '').slice(0, 3_500),
      executeOperator,
      setPersonalityMode,
      mediaStream,
      audioElement: options.audioElement || null,
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
    try { current.mute(inputMuted); } catch {}
    setReadyState();
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
        emitEvent('voice_error');
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
    inputMuted = false;
    clearMutedIdleTimer();
    reconnectAttempt = 0;
    emitEvent('session_started');
    const connected = await connectNow({ automatic: false });
    try { session?.mute(false); } catch {}
    return connected;
  }

  function stop() {
    if (desiredActive) emitEvent('session_stopped');
    desiredActive = false;
    suspended = false;
    inputMuted = false;
    reconnectAttempt = 0;
    clearReconnectTimer();
    clearMutedIdleTimer();
    clearModeSwitchTimer();
    closeCurrentSession();
    connectPromise = null;
    setState('offline', 'Voice off');
  }

  function mute(muted) {
    inputMuted = Boolean(muted);
    if (!desiredActive) return false;
    clearMutedIdleTimer();
    try { session?.mute(inputMuted); } catch {}
    if (inputMuted) {
      setState('thinking', 'Processing voice');
      mutedIdleTimer = setTimer(() => {
        if (desiredActive && inputMuted && state === 'thinking') setReadyState();
      }, 12_000);
    } else {
      setReadyState('Listening');
    }
    return true;
  }

  function suspend(detail = 'Voice paused while the app is in the background') {
    if (!desiredActive) return;
    if (!suspended) emitEvent('background_suspended');
    suspended = true;
    clearReconnectTimer();
    closeCurrentSession();
    connectPromise = null;
    setState('paused', detail);
  }

  async function resume() {
    if (!desiredActive) return false;
    if (suspended) emitEvent('background_resumed');
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
    emitEvent(online ? 'network_online' : 'network_offline');
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

  function announce(message) {
    const update = String(message || '').trim().slice(0, 1_500);
    if (!update || !desiredActive || suspended || !session || typeof session.sendMessage !== 'function') return false;
    session.sendMessage(`Durable Marcus status update: ${update}\nSpeak this update concisely and faithfully. Do not call a tool or imply any additional action.`);
    return true;
  }

  function sendContext(message, { speak = true } = {}) {
    const update = String(message || '').trim().slice(0, 6_000);
    if (!update || !desiredActive || suspended || !session || typeof session.sendMessage !== 'function') return false;
    session.sendMessage(`Live call context update:\n${update}\n\nUse this as meeting context. ${speak ? 'Respond only if a short spoken note would help Mark right now.' : 'Do not speak unless Mark asks for a response.'}`);
    return true;
  }

  return {
    start,
    stop,
    suspend,
    resume,
    networkChanged,
    interrupt,
    announce,
    sendContext,
    mute,
    getPersonalityMode: () => activePersonalityMode,
    setPersonalityMode,
    getState: () => state,
    isActive: () => desiredActive,
  };
}

if (typeof window !== 'undefined') {
  window.createMarcusRealtimeVoice = createMarcusRealtimeVoice;
}
