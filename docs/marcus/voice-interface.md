# Voice Interface

Status: primary architecture selected and initial mobile integration implemented. Local WebRTC setup is verified; production Android speech verification remains open.

## Objective

Voice should be the fastest way for Mark to use the same Marcus operator. It should not create a second project memory, tool catalog, approval model, or assistant personality.

## Selected Stack

- Conversation model: OpenAI Realtime `gpt-realtime-2.1`.
- Browser/mobile transport: WebRTC.
- Default voice: `marin`.
- Operational bridge: `marcus_operator` -> `POST /api/marcus/live/chat`.
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

## Completion Gate

This capability is not complete until every acceptance test in [[implementation-roadmap]] passes on the installed Android PWA against the durable production host.
