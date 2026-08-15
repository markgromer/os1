(function attachMarcusRealtimeClient(global) {
    const STATE_EVENT = 'statechange';

    function safeJson(value) {
        try { return JSON.parse(value); } catch { return null; }
    }

    function contentFromRealtimeItem(item) {
        const content = Array.isArray(item?.content) ? item.content : [];
        return content.map((part) => part?.transcript || part?.text || '').filter(Boolean).join(' ').trim();
    }

    class MarcusRealtimeClient extends EventTarget {
        constructor(options = {}) {
            super();
            this.fetcher = options.fetcher || global.fetch.bind(global);
            this.getContext = typeof options.getContext === 'function' ? options.getContext : () => ({});
            this.onToolCall = typeof options.onToolCall === 'function' ? options.onToolCall : async () => ({ ok: false, error: 'No tool bridge configured' });
            this.peer = null;
            this.channel = null;
            this.stream = null;
            this.audio = null;
            this.state = 'idle';
            this.sessionId = '';
            this.muted = false;
            this.assistantTranscript = '';
            this.clientId = crypto.randomUUID();
            this.ownershipHeld = false;
            this.ownershipTimer = null;
        }

        setState(next, detail = {}) {
            if (!next || (next === this.state && !Object.keys(detail).length)) return;
            const previous = this.state;
            this.state = next;
            this.dispatchEvent(new CustomEvent(STATE_EVENT, { detail: { state: next, previous, ...detail } }));
            this.reportState(next, { previous, ...detail }).catch(() => {});
        }

        emit(name, detail = {}) {
            this.dispatchEvent(new CustomEvent(name, { detail }));
        }

        async reportState(state, detail = {}) {
            if (!this.sessionId) return;
            const eventByState = {
                connecting: 'connect_requested', armed: detail?.previous === 'thinking' || detail?.previous === 'speaking' ? 'response_done' : 'connected', listening: 'speech_started',
                thinking: 'speech_stopped', speaking: 'audio_started', interrupted: 'interrupted',
                muted: 'muted', error: 'failed', idle: 'disconnected',
            };
            await this.fetcher('/api/marcus/live/voice/events', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sessionId: this.sessionId,
                    event: eventByState[state] || state,
                    metadata: detail,
                    error: detail?.message || '',
                }),
            });
        }

        send(event) {
            if (this.channel?.readyState !== 'open') return false;
            this.channel.send(JSON.stringify(event));
            return true;
        }

        async start() {
            if (this.peer) return;
            this.setState('connecting');
            try {
                const statusResponse = await this.fetcher('/api/marcus/live/voice/status');
                const status = await statusResponse.json().catch(() => ({}));
                if (!statusResponse.ok || !status?.realtimeConfigured) {
                    throw new Error(status?.error || 'OpenAI Realtime is not configured. Add an OpenAI API key in Settings.');
                }

                await this.updateOwnership('acquire');
                this.ownershipHeld = true;
                this.ownershipTimer = setInterval(() => this.updateOwnership('renew').catch(() => {}), 20_000);

                this.stream = await navigator.mediaDevices.getUserMedia({
                    audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
                });
                this.peer = new RTCPeerConnection();
                this.audio = document.createElement('audio');
                this.audio.autoplay = true;
                this.audio.setAttribute('aria-hidden', 'true');
                this.peer.ontrack = (event) => {
                    this.audio.srcObject = event.streams[0];
                    this.audio.play().catch(() => {});
                };
                for (const track of this.stream.getTracks()) this.peer.addTrack(track, this.stream);

                this.channel = this.peer.createDataChannel('oai-events');
                this.channel.addEventListener('open', () => {
                    this.setState('armed');
                    this.emit('ready', { sessionId: this.sessionId });
                });
                this.channel.addEventListener('message', (event) => this.handleEvent(safeJson(event.data)));
                this.channel.addEventListener('close', () => {
                    if (this.state !== 'idle') this.setState('idle');
                });

                const offer = await this.peer.createOffer();
                await this.peer.setLocalDescription(offer);
                const context = this.getContext() || {};
                const response = await this.fetcher('/api/marcus/live/voice/realtime', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/sdp',
                        'X-Marcus-Context': encodeURIComponent(JSON.stringify(context).slice(0, 6000)),
                    },
                    body: offer.sdp,
                });
                const answer = await response.text();
                if (!response.ok) throw new Error(safeJson(answer)?.error || answer || `Realtime connection failed (${response.status})`);
                this.sessionId = response.headers.get('x-marcus-voice-session-id') || crypto.randomUUID();
                await this.peer.setRemoteDescription({ type: 'answer', sdp: answer });
            } catch (error) {
                this.setState('error', { message: error?.message || 'Voice connection failed' });
                this.closeMedia();
                throw error;
            }
        }

        async handleToolCall(event) {
            const callId = event.call_id || event.item_id || '';
            const args = safeJson(event.arguments || '{}') || {};
            let output;
            try {
                output = await this.onToolCall({ name: event.name || 'marcus_command', arguments: args, callId });
            } catch (error) {
                output = { ok: false, error: error?.message || 'MARCUS tool failed' };
            }
            this.send({
                type: 'conversation.item.create',
                item: { type: 'function_call_output', call_id: callId, output: JSON.stringify(output) },
            });
            this.send({ type: 'response.create' });
            this.emit('toolresult', { callId, output });
        }

        handleEvent(event) {
            if (!event?.type) return;
            const type = event.type;
            this.emit('realtimeevent', { event });

            if (type === 'input_audio_buffer.speech_started') {
                if (this.state === 'speaking') this.setState('interrupted');
                this.setState('listening');
            } else if (type === 'input_audio_buffer.speech_stopped') {
                this.setState('thinking');
            } else if (type === 'response.audio.delta' || type === 'response.output_audio.delta') {
                this.setState('speaking');
            } else if (type === 'response.audio_transcript.delta' || type === 'response.output_audio_transcript.delta') {
                this.assistantTranscript += event.delta || '';
                this.emit('transcriptdelta', { role: 'ai', content: this.assistantTranscript });
            } else if (type === 'response.audio_transcript.done' || type === 'response.output_audio_transcript.done') {
                const content = (event.transcript || this.assistantTranscript).trim();
                this.assistantTranscript = '';
                if (content) this.emit('transcript', { role: 'ai', content, spoken: true, unheard: !document.hasFocus() });
            } else if (type === 'conversation.item.input_audio_transcription.completed') {
                const content = (event.transcript || contentFromRealtimeItem(event.item)).trim();
                if (content) this.emit('transcript', { role: 'user', content, spoken: true, unheard: false });
            } else if (type === 'response.function_call_arguments.done') {
                this.handleToolCall(event).catch(() => {});
            } else if (type === 'response.done') {
                this.setState('armed');
            } else if (type === 'error') {
                const message = event.error?.message || 'Realtime voice error';
                this.setState('error', { message });
                this.emit('error', { message, event });
            }
        }

        interrupt() {
            this.send({ type: 'response.cancel' });
            this.send({ type: 'output_audio_buffer.clear' });
            this.setState('interrupted');
        }

        toggleMute(force) {
            this.muted = typeof force === 'boolean' ? force : !this.muted;
            for (const track of this.stream?.getAudioTracks?.() || []) track.enabled = !this.muted;
            this.setState(this.muted ? 'muted' : 'armed');
            return this.muted;
        }

        async updateOwnership(action) {
            const response = await this.fetcher('/api/marcus/live/voice/ownership', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action, owner: 'realtime-browser', clientId: this.clientId, ttlMs: 45_000 }),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(data?.error || 'The microphone is in use by another MARCUS voice process.');
            return data.ownership;
        }

        closeMedia() {
            if (this.ownershipTimer) clearInterval(this.ownershipTimer);
            this.ownershipTimer = null;
            try { this.channel?.close(); } catch { /* ignore */ }
            try { this.peer?.close(); } catch { /* ignore */ }
            for (const track of this.stream?.getTracks?.() || []) track.stop();
            if (this.audio) this.audio.srcObject = null;
            this.channel = null;
            this.peer = null;
            this.stream = null;
            this.audio = null;
            if (this.ownershipHeld) {
                this.updateOwnership('release').catch(() => {});
                this.ownershipHeld = false;
            }
        }

        stop() {
            this.closeMedia();
            this.sessionId = '';
            this.muted = false;
            this.setState('idle');
        }
    }

    global.MarcusRealtimeClient = MarcusRealtimeClient;
})(globalThis);

