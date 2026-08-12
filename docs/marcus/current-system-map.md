# Current System Map

## Server

`server.js` is the main Express app.

It owns:

- Auth.
- Settings.
- Business routing.
- Legacy task/project/inbox APIs.
- Marcus chat and live endpoints.
- Integrations.
- Scheduler startup.
- Static UI serving.

Production runtime:

- Render service: `task-tracker`
- Canonical host: `https://task-tracker-5wsa.onrender.com`
- Mobile PWA: `https://task-tracker-5wsa.onrender.com/mobile.html`
- Persistent disk: `/var/data/task-tracker`

## Data

Legacy data:

- `data/tasks.json`
- `data/businesses/<business>/tasks.json`

Durable operation data:

- `data/businesses/<business>/operations.json`
- `data/businesses/<business>/project-registry.json`

Session/control data:

- `data/marcus-operational-controls.json`
- `data/marcus-session-state.json`
- `data/mobile-pairing.json` for the active hash-only, short-lived Android pairing challenge

Settings:

- Current default: `%APPDATA%/M.A.R.C.U.S./settings.json`
- Legacy: `%APPDATA%/Task Tracker/settings.json`

The legacy settings file currently contains richer integration/business configuration than the newer settings path.

Marcus Mobile conversation state is stored under `marcusLiveConversation` in settings. It retains a bounded recent transcript and one active project binding. Recent context is limited to 45 minutes when requirements are assembled for a project operation; stored messages are capped at 80.

## Marcus Operations

`marcus/operations` contains the durable operation engine.

Important concepts:

- Operation creation from request.
- Project resolution.
- Planning.
- Runtime approval policy.
- Provider execution.
- Verification.
- Recovery.
- Reconciliation.

## Project Operator

`marcus/operators/project_operator_service.js` is the conversation-to-Codex operator layer.

It owns:

- Detecting project/audit/Codex work requests.
- Resolving the project through the durable operations engine.
- Gathering legacy store, project evidence, desktop, and repository sample context.
- Writing a Marcus Project Execution Brief.
- Composing a Codex-ready prompt.
- Creating a durable operation and either starting a direct Codex job through the configured adapter or creating an external Codex handoff.
- Auto-registering an authenticated user's explicit GitHub `owner/repository` target when it is not already in the project registry.
- Reusing the active mobile project and recent requirements when a follow-up says "check the repo", "do it", or otherwise omits the project name.
- Requiring a positive work action before auditing or creating an operation; repository, site, and Codex mentions alone remain conversation context.
- Respecting "do not audit" as context-only and "audit/prepare, but do not start Codex" as a planned operation with no provider start.

The production project registry includes `Reggie` at `markgromer/Reggie`, with `connect.scooper.site` and `Sweep and Go` aliases.

Current routes:

- `GET /api/marcus/operator-health`
- `POST /api/marcus/project-operator`
- `POST /api/marcus/live/chat` for project operator requests
- `POST /api/chat` for project operator requests

`/api/marcus/operator-health` is the honest capability readout. It reports whether Marcus can audit, prepare Codex handoffs, start Codex directly, read GitHub/Cloudflare through server credentials, see the desktop agent, and handle email/text capabilities with approval gates.

External communication routes:

- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/send`
- `POST /api/marcus/external-actions/:id/reject`

Email uses SMTP. Text uses Quo's message API. Drafting and approval are durable; provider acceptance moves the action through `sending` to `sent` and records the provider receipt. Production credentials are still required before either provider can send.

## Providers

`marcus/providers` contains execution/read providers:

- Codex provider.
- HTTP Codex adapter.
- GitHub Actions Codex adapter.
- Desktop provider.
- GitHub read provider.
- Browser verification provider.

The HTTP Codex adapter is enabled only when `MARCUS_CODEX_ADAPTER_URL` or `CODEX_ADAPTER_URL` is configured. It calls start/status/follow-up/artifact/diff/cancel endpoints and keeps the durable operation lifecycle in the existing provider runner.

The GitHub Actions Codex adapter borrows Reggie's runner pattern. It is enabled only when `MARCUS_CODEX_GITHUB_ACTIONS_ENABLED=true` and a GitHub token is available through `MARCUS_CODEX_GITHUB_TOKEN`, `CODEX_GITHUB_TOKEN`, or `GITHUB_TOKEN`. It dispatches `repository_dispatch` event `marcus_codex_job` to `MARCUS_CODEX_RUNNER_REPO` or `markgromer/Reggie`, where `.github/workflows/marcus-codex-runner.yml` runs `openai/codex-action@v1` with Reggie's existing runner secrets.

Production currently uses this GitHub Actions adapter. GitHub repository reads, repository dispatch, runner reconciliation, pull-request creation, and durable verification completion were exercised end to end on the Marcus demo project.

Cloudflare production access uses a dedicated account token named `Marcus Production Operator`. It covers the Developer Services policy plus DNS write and zone read. It excludes billing, membership, and API-token administration. Production reads of all zones and the configured zone's DNS records have passed.

## Evidence

`marcus/evidence` collects activity signals from:

- Operations.
- Airtable-derived legacy state.
- GitHub.
- Render.
- Cloudflare.
- Browser verification.
- Desktop workspace activity.

## Intelligence

`marcus/intelligence` builds the active brief.

It turns stores, settings, desktop context, tasks, projects, and messages into scored operational signals.

## Live UI

`public/live.html` is the Marcus Live HUD.

It uses:

- Server-sent events from `/api/marcus/live`.
- Active brief from `/api/marcus/active-brief`.
- Live chat from `/api/marcus/live/chat`.
- Voice status, transcription, and speech endpoints.

The existing Live voice path is a chained fallback: browser or recorded speech input, OpenAI transcription, Marcus text chat, and ElevenLabs or browser speech output.

## Mobile UI

`public/mobile.html` is the Android-friendly Marcus mobile shell.

It uses:

- `public/manifest.webmanifest` for Android home-screen installation.
- `public/sw.js` for a small app-shell cache.
- `GET /api/auth/status` and `POST /api/auth/login` for the existing admin-cookie auth flow.
- `POST /api/auth/pairing-code` and `POST /api/auth/pair` for short-lived, one-time Android pairing without copying the durable admin secret to the phone.
- `GET /api/marcus/live/session` to create a short-lived Live token after auth.
- `GET /api/marcus/operator-health` to show Codex, GitHub, Cloudflare, and desktop capability status.
- `POST /api/marcus/live/chat` for conversation-first project operator chat.
- `public/marcus-realtime.js` for a WebRTC speech-to-speech session.
- `POST /api/marcus/realtime/client-secret` for a short-lived OpenAI Realtime client secret; the standard OpenAI key remains on the server.
- `POST /api/marcus/realtime/telemetry` for authenticated, redacted voice acceptance events.
- `GET /api/marcus/realtime/acceptance` for derived acceptance gates by page session.

`client/marcus-realtime.js` builds the browser client with `@openai/agents-realtime`; `scripts/build-mobile.mjs` bundles it into `public/marcus-realtime.js`. Realtime voice uses `gpt-realtime-2.1` by default with voice `marin`, semantic VAD, interruption, near-field noise reduction, and live transcription. Its only operational function is `marcus_operator`, which sends substantive spoken requests back through `/api/marcus/live/chat`. The voice model does not own GitHub, Cloudflare, Codex, external communication, or approval authority.

The browser lifecycle closes the media session while the PWA is hidden, resumes with a fresh ephemeral credential, reconnects after network or WebRTC loss with bounded backoff, and refreshes at 55 minutes before the Realtime session limit. Service worker cache `marcus-mobile-v9` carries the acceptance telemetry client and keeps its initial event queue silent until authentication succeeds.

The SDK's generic `audio_start` callback is not emitted when WebRTC owns audio playback. Marcus therefore also derives speaking state from Realtime output-audio transcript events, derives playback completion from `response.output_audio.done`, and records barge-in when input speech begins while assistant playback is active. Guarded state prevents duplicate events if another transport emits the generic callbacks.

`marcus/voice/realtime_telemetry.js` accepts only allowlisted event types and bounded metadata. It stores no transcript, request, reply, credential, IP address, or raw user agent. Events are capped at 1,000 per business in `data/businesses/<business>/marcus-realtime-telemetry.json`. The acceptance view derives signaling, recognized-speech, assistant-audio-stream, interruption, operator-bridge, network-recovery, background-recovery, and installed-Android-context gates.

`/api/marcus/live/chat` keeps recent conversation turns, an active project, and bounded durable requirement memory for up to 40 projects on the server. Each project retains at most 12 deduplicated requirement sentences. Both sides of each exchange carry project metadata. When a message explicitly names a project, Marcus resolves that project before building the request and includes only matching user requirements; the explicit target overrides the previously active project. Older conversations remain compatible because a user turn can inherit the project metadata from its paired assistant reply. Context-only replies summarize at most three substantive requirements instead of replaying raw history.

The project operator receives the accumulated project-scoped user requirements rather than only the latest short follow-up. If rolling chat and project memory do not contain them, Marcus reconstructs requirements from matching durable operation requests and writes the recovered summary back to project memory. Read-only and acceptance commands are excluded as requirements. Explicit audit or Codex commands use the same resolved project for repository inspection, prompt composition, operation binding, and provider launch.

The mobile app is a PWA first. Pairing sets the existing secure HttpOnly authentication cookie; the six-digit code is not retained. It does not add a separate Android credential store or native notification channel yet.

Pairing state is persisted on the Render data volume under an exclusive file lock. A code minted before a Render process replacement can be consumed once by the replacement process; a replay is rejected.

## Desktop Agent

`desktop-agent.cjs` relays local context and executes queued desktop actions.

The Windows scheduled task `MARCUS-DesktopAgent` targets the canonical Render host. It reads the admin credential from `%APPDATA%/M.A.R.C.U.S/mobile-live-admin-token.txt`, so the secret is not present in Task Scheduler arguments or the `node.exe` command line. Polling is serialized and transient process-spawn failures do not terminate the relay.

It can:

- Report active window/workspace/git context.
- Open VS Code.
- Run allowlisted project scripts.
- Prepare publish checks.
- Publish project changes only when authorized.
- Validate workspace trust challenges.
