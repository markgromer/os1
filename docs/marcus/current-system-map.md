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

Settings:

- Current default: `%APPDATA%/M.A.R.C.U.S./settings.json`
- Legacy: `%APPDATA%/Task Tracker/settings.json`

The legacy settings file currently contains richer integration/business configuration than the newer settings path.

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

Current routes:

- `GET /api/marcus/operator-health`
- `POST /api/marcus/project-operator`
- `POST /api/marcus/live/chat` for project operator requests
- `POST /api/chat` for project operator requests

`/api/marcus/operator-health` is the honest capability readout. It reports whether Marcus can audit, prepare Codex handoffs, start Codex directly, read GitHub/Cloudflare through server credentials, see the desktop agent, and handle email/text capabilities with approval gates.

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

Realtime voice uses `gpt-realtime-2.1` by default with voice `marin`. Its only operational function is `marcus_operator`, which sends substantive spoken requests back through `/api/marcus/live/chat`. The voice model does not own GitHub, Cloudflare, Codex, external communication, or approval authority.

The mobile app is a PWA first. Pairing sets the existing secure HttpOnly authentication cookie; the six-digit code is not retained. It does not add a separate Android credential store or native notification channel yet.

## Desktop Agent

`desktop-agent.cjs` relays local context and executes queued desktop actions.

It can:

- Report active window/workspace/git context.
- Open VS Code.
- Run allowlisted project scripts.
- Prepare publish checks.
- Publish project changes only when authorized.
- Validate workspace trust challenges.
