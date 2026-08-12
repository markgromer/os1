(function attachMarcusRealtimeVoice(global) {
  const REALTIME_CALLS_URL = 'https://api.openai.com/v1/realtime/calls';

  function parseJson(value, fallback = {}) {
    try {
      return JSON.parse(String(value || ''));
    } catch {
      return fallback;
    }
  }

  function createMarcusRealtimeVoice(options = {}) {
    const onStatus = typeof options.onStatus === 'function' ? options.onStatus : () => {};
    const onTranscript = typeof options.onTranscript === 'function' ? options.onTranscript : () => {};
    const onAssistantText = typeof options.onAssistantText === 'function' ? options.onAssistantText : () => {};
    const onError = typeof options.onError === 'function' ? options.onError : () => {};
    const invokeMarcus = typeof options.invokeMarcus === 'function'
      ? options.invokeMarcus
      : async () => ({ ok: false, error: 'Marcus operator bridge is unavailable.' });
    const getAuthToken = typeof options.getAuthToken === 'function' ? options.getAuthToken : () => '';

    let peer = null;
    let dataChannel = null;
    let microphone = null;
    let remoteAudio = null;
    let state = 'offline';
    let starting = false;
    const handledCalls = new Set();

    function setState(next, detail = '') {
      state = next;
      onStatus({ state, detail, active: state !== 'offline' && state !== 'error' });
    }

    function sendEvent(event) {
      if (!dataChannel || dataChannel.readyState !== 'open') {
        throw new Error('Marcus voice session is not ready.');
      }
      dataChannel.send(JSON.stringify(event));
    }

    async function handleMarcusToolCall(call) {
      const callId = String(call?.call_id || call?.callId || '').trim();
      const name = String(call?.name || '').trim();
      if (!callId || name !== 'marcus_operator' || handledCalls.has(callId)) return;
      handledCalls.add(callId);

      const args = typeof call.arguments === 'string' ? parseJson(call.arguments) : (call.arguments || {});
      const message = String(args?.message || '').trim().slice(0, 12_000);
      setState('thinking', message ? 'Marcus is working on it.' : 'Marcus is checking the request.');

      let output;
      try {
        output = message
          ? await invokeMarcus(message)
          : { ok: false, error: 'The voice request did not contain a usable message.' };
      } catch (error) {
        output = { ok: false, error: error?.message || 'Marcus could not process the voice request.' };
      }

      sendEvent({
        type: 'conversation.item.create',
        item: {
          type: 'function_call_output',
          call_id: callId,
          output: JSON.stringify(output),
        },
      });
      sendEvent({
        type: 'response.create',
        response: {
          instructions: 'Speak the Marcus operator result faithfully and concisely. Preserve approval requirements, operation identifiers, blockers, and uncertainty. Do not invent completed work.',
        },
      });
    }

    function handleEvent(event) {
      switch (event?.type) {
        case 'session.created':
        case 'session.updated':
          setState('listening', 'Listening');
          break;
        case 'input_audio_buffer.speech_started':
          setState('listening', 'Listening');
          break;
        case 'input_audio_buffer.speech_stopped':
        case 'response.created':
          setState('thinking', 'Thinking');
          break;
        case 'response.output_audio.delta':
        case 'response.output_audio_transcript.delta':
          setState('speaking', 'Marcus is speaking');
          break;
        case 'conversation.item.input_audio_transcription.completed': {
          const transcript = String(event.transcript || '').trim();
          if (transcript) onTranscript(transcript);
          break;
        }
        case 'response.output_audio_transcript.done': {
          const transcript = String(event.transcript || '').trim();
          if (transcript) onAssistantText(transcript);
          break;
        }
        case 'response.function_call_arguments.done':
          handleMarcusToolCall(event).catch(onError);
          break;
        case 'response.output_item.done':
          if (event.item?.type === 'function_call') {
            handleMarcusToolCall(event.item).catch(onError);
          }
          break;
        case 'response.done':
          setState('listening', 'Listening');
          break;
        case 'error': {
          const message = event.error?.message || event.message || 'Realtime voice failed.';
          setState('error', message);
          onError(new Error(message));
          break;
        }
        default:
          break;
      }
    }

    async function start() {
      if (starting || (peer && peer.connectionState !== 'closed')) return;
      starting = true;
      setState('connecting', 'Connecting voice');
      try {
        const token = String(await getAuthToken() || '').trim();
        const secretResponse = await fetch('/api/marcus/realtime/client-secret', {
          method: 'POST',
          credentials: 'include',
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        const secret = await secretResponse.json().catch(() => ({}));
        if (!secretResponse.ok || !secret.value) {
          throw new Error(secret.error || `Voice session setup failed (${secretResponse.status}).`);
        }

        microphone = await navigator.mediaDevices.getUserMedia({
          audio: {
            echoCancellation: true,
            noiseSuppression: true,
            autoGainControl: true,
          },
        });

        peer = new RTCPeerConnection();
        remoteAudio = document.createElement('audio');
        remoteAudio.autoplay = true;
        remoteAudio.setAttribute('playsinline', '');
        peer.ontrack = (event) => {
          remoteAudio.srcObject = event.streams[0];
          remoteAudio.play().catch(() => {});
        };
        peer.onconnectionstatechange = () => {
          if (!peer) return;
          if (peer.connectionState === 'connected') setState('listening', 'Listening');
          if (['failed', 'disconnected', 'closed'].includes(peer.connectionState)) stop();
        };
        for (const track of microphone.getTracks()) peer.addTrack(track, microphone);

        dataChannel = peer.createDataChannel('oai-events');
        dataChannel.addEventListener('open', () => setState('listening', 'Listening'));
        dataChannel.addEventListener('message', (event) => handleEvent(parseJson(event.data)));
        dataChannel.addEventListener('error', () => onError(new Error('Marcus voice data channel failed.')));

        const offer = await peer.createOffer();
        await peer.setLocalDescription(offer);
        const answerResponse = await fetch(REALTIME_CALLS_URL, {
          method: 'POST',
          body: offer.sdp,
          headers: {
            Authorization: `Bearer ${secret.value}`,
            'Content-Type': 'application/sdp',
          },
        });
        if (!answerResponse.ok) {
          const detail = await answerResponse.text().catch(() => '');
          throw new Error(detail || `Realtime connection failed (${answerResponse.status}).`);
        }
        await peer.setRemoteDescription({ type: 'answer', sdp: await answerResponse.text() });
      } catch (error) {
        stop();
        setState('error', error?.message || 'Voice connection failed.');
        onError(error);
        throw error;
      } finally {
        starting = false;
      }
    }

    function stop() {
      try { dataChannel?.close(); } catch {}
      try { peer?.close(); } catch {}
      try { microphone?.getTracks().forEach((track) => track.stop()); } catch {}
      try {
        if (remoteAudio) {
          remoteAudio.pause();
          remoteAudio.srcObject = null;
        }
      } catch {}
      dataChannel = null;
      peer = null;
      microphone = null;
      remoteAudio = null;
      handledCalls.clear();
      setState('offline', 'Voice off');
    }

    return {
      start,
      stop,
      getState: () => state,
      isActive: () => state !== 'offline' && state !== 'error',
    };
  }

  global.createMarcusRealtimeVoice = createMarcusRealtimeVoice;
})(window);
