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
- `data/businesses/<business>/marcus-mission-memory.json`

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
- Gathering legacy store, project evidence, desktop, and deep GitHub repository context.
- Writing a Marcus Project Execution Brief.
- Composing a Codex-ready prompt.
- Creating a durable operation and either starting a direct Codex job through the configured adapter or creating an external Codex handoff.
- Auto-registering an authenticated user's explicit GitHub `owner/repository` target when it is not already in the project registry.
- Reusing the active mobile project and recent requirements when a follow-up says "check the repo", "do it", or otherwise omits the project name.
- Requiring a positive work action before auditing or creating an operation; repository, site, and Codex mentions alone remain conversation context.
- Respecting "do not audit" as context-only and "audit/prepare, but do not start Codex" as a planned operation with no provider start.

Repository auditing uses each discovered repository's recursive Git tree, default branch, head commit, recent commits, open pull requests, and request-ranked source/configuration/test files. It excludes generated trees, oversized or binary files, and obvious secret-file paths before reads; evidence is redacted again before persistence. Marcus records coverage, API-call count, failures, selected paths, and elapsed time. The execution brief and real direct-Codex handoff may retain up to 30,000 characters so source evidence is not reduced to filename previews.

## Mission Memory

`marcus/memory/mission_memory_store.js` owns durable cross-project operator memory. It stores bounded typed records for missions, standing instructions, preferences, decisions, and facts with status, priority, source, actor, timestamps, and store revision. Records are business-scoped, atomically written, backed up, recovered from the last valid sibling backup, and archived rather than physically deleted.

The initial durable records encode Mark's trusted-operator mission, evidence-first assistance standard, and preference for a maintained prebuilt voice interface. Explicit `remember`, mission, preference, and `from now on` messages write through deterministic server handling rather than model inference. Credential-like content is rejected. Relevant active records enter normal chat context, project execution briefs, and the real Codex handoff.

Production acceptance on 2026-08-12 confirmed four active records, conversational create and recall, exact memory-record provenance in a real deep Reggie execution brief, and zero provider jobs during the staged read-only audit. The staged operation was cancelled after inspection. A Render process replacement then reset uptime and a fresh Live session recalled the same record; the memory and cancelled operation state survived on the persistent volume.

Mission memory routes require durable admin authentication; a short-lived Live token cannot call them directly:

- `GET /api/marcus/memory`
- `GET /api/marcus/memory/relevant`
- `POST /api/marcus/memory`
- `PATCH /api/marcus/memory/:id`

Voice and mobile users can still issue an explicit `remember` command through `POST /api/marcus/live/chat`, which records Mark as the source and applies the same validation. Operator health and combined acceptance report whether an active mission and standing instruction exist.

The production project registry includes `Reggie` at `markgromer/Reggie`, with `connect.scooper.site` and `Sweep and Go` aliases.

Current routes:

- `GET /api/marcus/operator-health`
- `GET /api/marcus/acceptance`
- `POST /api/marcus/project-operator`
- `POST /api/marcus/live/chat` for project operator requests
- `POST /api/chat` for project operator requests

`/api/marcus/operator-health` is the honest capability readout. It reports whether Marcus can audit, prepare Codex handoffs, start Codex directly, read GitHub/Cloudflare through server credentials, see the desktop agent, and handle email/text capabilities with approval gates.

External communication routes:

- `GET /api/marcus/providers/config`
- `PUT /api/marcus/providers/config`
- `POST /api/marcus/providers/verify`
- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/send`
- `POST /api/marcus/external-actions/:id/reject`

Email uses SMTP. Text uses Quo's message API. Drafting and approval are durable; provider acceptance moves the action through `sending` to `sent` and records the provider receipt. Marcus Mobile's admin-only `Integrations` dialog writes Quo/SMTP settings without returning secrets, verifies each provider without sending, and records a bounded timestamped verification result. Quo verification retains an unambiguous resolved sender. A later settings change invalidates that provider's verification. Production credentials are still required before either provider can send.

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

After a successful runner workflow, `marcus/providers/github_actions_codex_adapter.js` independently queries the target repository for the work branch, pull request, head commit, changed files, bounded patches, check runs, and commit statuses. The evidence is redacted, hashed with an evidence digest, and stored separately from Codex's own claims. `marcus/operations/codex_result_reviewer.js` compares only complete authoritative GitHub diff evidence with the original request and acceptance criteria. The semantic reviewer receives strict JSON-schema output and may cite only exact ids from Marcus's validated evidence catalog. Marcus-generated audit, handoff, and completion-control criteria are instead bound deterministically to durable operation records. Missing commits, missing/truncated patches, pending or failed target checks, malformed or uncited model output, incomplete criterion coverage, unsupported execution claims, and digest mismatches fail closed to `needs_manual_review` or `failed`.

An automated result review can satisfy only `diff_review`. It cannot satisfy build, test, lint, typecheck, browser, deployment, merge, or communication evidence. Provider-supplied `codex_result_review` artifacts are renamed and retained as untrusted claims before the independent reviewer runs.

Production acceptance operation `op_NfHu37cdF1aSjQ` exercised this path against demo PR #4. The first target-token request failed closed because the repository was not enrolled in Reggie Hub. After authenticated enrollment, the same durable operation completed Reggie run `31584535255`, collected two complete patches at head `4ee4135eb98be5bc57385be0ff128ee78fa42729`, and bound them to evidence digest `99dc7a16679924e30285fff2b3fb1baaff9b24379e73d5f6d35332c4104c8d1b`. An initial uncited model pass was not sufficient to complete the operation; independent build, test, lint, and URL-health evidence plus the corrected strict review were required. PR #4 remained open and live `/version` remained HTTP 404, proving no merge or deployment occurred.

Production currently uses this GitHub Actions adapter. GitHub repository reads, repository dispatch, runner reconciliation, pull-request creation, and durable verification completion were exercised end to end on the Marcus demo project. Before deployment of the independent review change, the new collector was also exercised read-only against demo PR #3 and resolved its exact head commit, two complete changed-file patches, and GitHub check/status state without a write, merge, or launch action.

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
- `GET /api/marcus/acceptance` for the combined voice, provider, Codex, GitHub, Cloudflare, OpenAI, and desktop acceptance report.

`client/marcus-realtime.js` builds the browser client with `@openai/agents-realtime`; `scripts/build-mobile.mjs` bundles it into `public/marcus-realtime.js`. Realtime voice uses `gpt-realtime-2.1` by default with voice `marin`, semantic VAD, interruption, near-field noise reduction, and live transcription. Its only operational function is `marcus_operator`, which sends substantive spoken requests back through `/api/marcus/live/chat`. The voice model does not own GitHub, Cloudflare, Codex, external communication, or approval authority.

`client/mobile-operation-tracker.js` builds `public/marcus-operation-tracker.js`. Marcus Mobile records only the active operation id and bounded status signature in local storage, then polls the Live-token-safe `GET /api/operations/summary`. The summary exposes title, project, status, current step, step progress, verification counts, blocker count, approval/recovery flags, and update time. It does not expose prompts, audit excerpts, artifacts, source patches, provider metadata, or credentials.

The server's durable operation monitor independently ticks only `queued`, `running`, `awaiting_provider`, and `verifying` operations. It does not touch waiting-approval, blocked, paused, recovery-required, failed, cancelled, or completed states. Mobile polling is therefore observational rather than the mechanism that keeps Codex work alive.

The browser lifecycle closes the media session while the PWA is hidden, resumes with a fresh ephemeral credential, reconnects after network or WebRTC loss with bounded backoff, and refreshes at 55 minutes before the Realtime session limit. A valid durable pairing cookie takes over immediately if a Render restart invalidates a process-bound Live token, even when the stale token is still present in the Authorization header. Service worker cache `marcus-mobile-v14` adds the active-work tracker to the existing mobile `Verify` dashboard for a fresh voice run, provider verification, and approved-send evidence. Selective Realtime announcements speak only persisted completion, failure, cancellation, approval, blocked, or recovery transitions; ordinary progress polling stays visual.

The SDK's generic `audio_start` callback is not emitted when WebRTC owns audio playback. Marcus therefore derives speaking state from output-audio transcript deltas, derives playback completion from `response.output_audio.done`, treats the later final transcript as text-only, and records barge-in when input speech begins while assistant playback is active. Guarded state prevents duplicate events if another transport emits the generic callbacks.

`marcus/voice/realtime_telemetry.js` accepts only allowlisted event types and bounded metadata. It stores no transcript, request, reply, credential, IP address, or raw user agent. Events are capped at 1,000 per business in `data/businesses/<business>/marcus-realtime-telemetry.json`. The acceptance view derives signaling, recognized-speech, assistant-audio-stream, interruption, operator-bridge, network-recovery, background-recovery, and installed-Android-context gates. A session becomes physical-device evidence only when every derived gate passes in installed Android standalone context and Mark explicitly confirms the run from that same session.

`/api/marcus/live/chat` keeps recent conversation turns, an active project, and bounded durable requirement memory for up to 40 projects on the server. Each project retains at most 12 deduplicated requirement sentences. Both sides of each exchange carry project metadata. When a message explicitly names a project, Marcus resolves that project before building the request and includes only matching user requirements; the explicit target overrides the previously active project. Older conversations remain compatible because a user turn can inherit the project metadata from its paired assistant reply. Context-only replies summarize at most three substantive requirements instead of replaying raw history.

The project operator receives the accumulated project-scoped user requirements rather than only the latest short follow-up. If rolling chat and project memory do not contain them, Marcus reconstructs requirements from matching durable operation requests and writes the recovered summary back to project memory. Read-only and acceptance commands are excluded as requirements. Explicit audit or Codex commands use the same resolved project for repository inspection, prompt composition, operation binding, and provider launch.

The mobile app is a PWA first. Pairing sets the existing secure HttpOnly authentication cookie; the six-digit code is not retained. It does not add a separate Android credential store or native notification channel yet.

Pairing state is persisted on the Render data volume under an exclusive file lock. A code minted before a Render process replacement can be consumed once by the replacement process; a replay is rejected.

## External Communication Providers

Provider administration uses paired durable-admin routes:

- `GET` and `PUT /api/marcus/providers/config` return or update redacted server-side settings.
- `POST /api/marcus/providers/verify` authenticates and resolves the selected provider without sending.
- `POST /api/marcus/external-actions/draft` creates an unsent draft.
- Separate approve and send routes retain the draft -> approval -> provider-receipt boundary.

Quo is configured in production with a dedicated existing `os1` credential and the Operations line. The no-send verifier resolved the canonical sender, phone-number id, and user id on 2026-08-12. Credentials are never returned by the configuration API.

SMTP is not configured yet. The selected implementation path uses Resend's SMTP relay and the verified `gromore.media` domain after creation of a dedicated sending-only credential. The approved text and email acceptance sends remain pending, so Marcus does not yet report either send gate as passed.

## Desktop Agent

`desktop-agent.cjs` relays local context and executes queued desktop actions.

The Windows scheduled task `MARCUS-DesktopAgent` targets the canonical Render host. It reads the admin credential from `%APPDATA%/M.A.R.C.U.S/mobile-live-admin-token.txt`, so the secret is not present in Task Scheduler arguments or the `node.exe` command line. Polling is serialized and transient process-spawn failures do not terminate the relay.

The matching VS Code task also targets the canonical Render host and relies on the same protected token file. Production operator health confirmed the relay online after the 2026-08-12 deployment restart.

It can:

- Report active window/workspace/git context.
- Open VS Code.
- Run allowlisted project scripts.
- Prepare publish checks.
- Publish project changes only when authorized.
- Validate workspace trust challenges.
