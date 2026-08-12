# Voice Interface

Status: primary architecture selected and deployed. The official SDK signaling and recovery paths are verified in production Chromium; installed-Android speech, interruption, and physical phone-lock/network-handoff verification remain open.

## Objective

Voice should be the fastest way for Mark to use the same Marcus operator. It should not create a second project memory, tool catalog, approval model, or assistant personality.

## Selected Stack

- Conversation model: OpenAI Realtime `gpt-realtime-2.1`.
- Browser session library: `@openai/agents-realtime`.
- Browser/mobile transport: WebRTC.
- Default voice: `marin`.
- Operational bridge: `marcus_operator` -> `POST /api/marcus/live/chat`.
- Acceptance evidence: redacted lifecycle events -> `POST /api/marcus/realtime/telemetry` -> `GET /api/marcus/realtime/acceptance`.
- Existing fallback: OpenAI file transcription plus ElevenLabs or browser speech synthesis.

OpenAI's current voice-agent guidance recommends Realtime speech-to-speech for low first-audio latency, barge-in, natural turn-taking, and realtime tool use. It recommends WebRTC for browser and mobile clients. Sources: [Voice agents](https://developers.openai.com/api/docs/guides/voice-agents), [Realtime WebRTC](https://developers.openai.com/api/docs/guides/realtime-webrtc), and [Realtime tools](https://developers.openai.com/api/docs/guides/realtime-mcp).

## Why This Fits Marcus

- The required OpenAI credential already exists in Marcus settings.
- The Android experience is already a web PWA, so WebRTC works without a native app rewrite.
- Speech, interruption, and turn-taking are supplied as a maintained service instead of a custom STT/VAD/TTS stack.
- The Realtime model can call a single narrow function while Marcus retains durable operations and approval enforcement.
- There is no second vendor-hosted project memory or tool authority to reconcile.

## Alternatives Considered

ElevenLabs Agents has excellent voice selection, hosted agent configuration, tools, and web/mobile clients. It remains useful when a branded voice or telephony becomes the dominant requirement. It would add a separate agent configuration and, for full Marcus control, a custom LLM or webhook bridge.

LiveKit Agents is the strongest provider-neutral media framework considered. It provides turn detection, interruption handling, observability, mobile networking, and provider choice. It also adds an agent server and deployment surface that Marcus does not currently need.

## Authority Boundary

The Realtime voice layer may:

- Listen and speak.
- Manage conversational turns and interruption.
- Send the complete user intent to `marcus_operator`.
- Speak Marcus's returned result.

The Realtime voice layer may not independently:

- Read or mutate GitHub, Cloudflare, Render, or local workspaces.
- Start or report Codex work outside a durable Marcus operation.
- Send texts or emails.
- Publish, deploy, change DNS, merge, bill, or contact clients.
- Mark work complete without Marcus verification evidence.

## Configuration

- `OPENAI_API_KEY`: standard server-side API key or the equivalent saved Marcus setting.
- `MARCUS_REALTIME_MODEL`: optional model override.
- `MARCUS_REALTIME_VOICE`: optional voice override.

The browser receives a short-lived client secret from `POST /api/marcus/realtime/client-secret`. The standard API key is never returned to the browser.

## Session Lifecycle

- Semantic VAD creates turns and interrupts Marcus when Mark starts speaking.
- The PWA closes the media session while hidden or locked and reconnects when visible.
- Offline, online, and unexpected WebRTC disconnect events enter a bounded reconnect loop.
- Every reconnect mints a new ephemeral credential.
- A connected session renews at 55 minutes before the Realtime session limit.
- A connection version prevents a stale asynchronous setup from replacing a newer session.
- WebRTC speaking state is derived from output-audio transcript deltas because the SDK's generic audio callback is not emitted when WebRTC owns playback; `response.output_audio.done` ends playback, the later final transcript does not reopen it, and speech detected during active playback records barge-in.

Local acceptance covers these lifecycle rules with an SDK-session test double. Production Chromium covers live signaling plus synthetic network and page lifecycle recovery. Installed-Android behavior remains an explicit completion check.

## Acceptance Telemetry

The PWA assigns a random in-memory acceptance session ID and batches allowlisted events after authentication. Offline events remain in a bounded in-memory queue and flush after network recovery; no telemetry queue is persisted in browser storage.

Stored evidence includes event type, timestamps, voice state, transcript length, coarse platform/browser/display mode, operator outcome, and optional durable operation ID. It excludes transcript text, request/reply text, credentials, IP addresses, and raw user-agent strings. Server retention is capped at 1,000 events per business.

`GET /api/marcus/realtime/acceptance` derives these gates: signaling connected, user speech recognized, assistant audio streamed, interruption observed, operator bridge completed, network recovery, background recovery, and installed Android context. All gates passing marks the session ready for physical review; it does not independently prove the device was physical.

Local normalization, persistence, redaction, deduplication, auth, business-scope, WebRTC playback inference, and gate-derivation tests passed on 2026-08-12 in the `94/94` suite.

## Production Evidence

On 2026-08-12 the canonical Render PWA loaded service worker `marcus-mobile-v6`, authenticated through one-time pairing, obtained a short-lived Realtime client secret, created an OpenAI WebRTC call with HTTP 201, and reached `Voice on` / `Listening`. The SDK bundle was served with gzip.

The same production browser went offline and entered `Reconnecting` / `Waiting for network`. Restoring network minted a fresh credential, created a second HTTP 201 Realtime call, and returned to `Listening`. Synthetic `pagehide` / `pageshow` events moved the UI through `Paused` and created a third fresh credential and HTTP 201 call. The run had no browser warnings or errors and no durable token in local storage.

This proves production authentication, PWA assets, ephemeral-key minting, SDK WebRTC signaling, and browser lifecycle recovery. It does not prove actual Android microphone quality, spoken tool invocation, barge-in, physical phone-lock recovery, or cellular/Wi-Fi handoff; those remain in the completion gate.

Android authentication uses the one-time pairing flow in [[access-model]], so voice setup does not require copying the durable server credential to the device. The production pairing-to-voice path passed with no durable token in browser storage.

Production telemetry acceptance on 2026-08-12 used service worker `marcus-mobile-v9` and acceptance session `1ad47863-7d44-4a2b-9363-0aa50f67e16c`. A clean paired mobile Chromium run had zero browser errors or warnings, established Realtime, recognized a synthetic spoken read-only status request, called the real `marcus_operator` bridge, produced assistant audio, recorded a timed barge-in, and recovered after network and page-background cycles. The resulting gates were all true except `installedAndroidContext`, which remained false because this was deliberately a browser-emulated test rather than a physical installed PWA.

A separate one-shot project-continuity run used acceptance session `b1af050e-d971-470d-8632-749329fe0c8d`. It transcribed the Reggie request, called the real operator once, returned the exact saved setup-button, API-token, slug, verification, and blocking requirements, streamed assistant audio, created no operation, and stored no transcript/request text or credentials. That run exposed final-transcript event ordering that could leave the UI on `Speaking`; service worker `marcus-mobile-v10` applies and tests the corrected ordering.

The status request explicitly prohibited audits, Codex, and operation creation. Production still had four operations afterward, with the latest created at `2026-08-12T07:13:48.308Z`, before this acceptance run. The acceptance response reported that transcript text, request text, and credentials are not stored.

## Completion Gate

This capability is not complete until every acceptance test in [[implementation-roadmap]] passes on the installed Android PWA against the durable production host.
