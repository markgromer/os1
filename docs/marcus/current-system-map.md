# Current System Map

Status: active

The [[operator-intelligence-layer]] adds bounded voice continuity, speech-safe human references, request-specific job priming, semantic interruption alignment checks, recovered-method memory, and locked-decision enforcement. Raw identifiers remain in durable records while voice receives a reduced human-readable projection.

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
- Typed `github_write` and `cloudflare_write` steps.
- Immutable project/provider execution targets.
- Drift checks, idempotency, authoritative provider read-back, and recovery-required handling for uncertain post-mutation state.

## Project Operator

`marcus/operators/project_operator_service.js` is the conversation-to-Codex operator layer.

It owns:

- Detecting project/audit/Codex work requests.
- Resolving the project through the durable operations engine.
- Gathering legacy store, project evidence, desktop, and deep GitHub repository context.
- Writing a Marcus Project Execution Brief.
- Composing a Codex-ready prompt.
- Creating a durable operation and either starting a direct Codex job through the configured adapter or creating an external Codex handoff.
- Auto-registering an authenticated user's strongly explicit GitHub target when it is not already in the project registry. Accepted forms are a GitHub URL, a `.git` target, a whole-message `owner/repository`, or `owner/repository` adjacent to an explicit `GitHub`, `repo`, or `repository` label. Incidental conversational paths such as `workflow/operation` are not project declarations.
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

The broader context-memory target is documented in [[context-memory]]. Today, durable context is split across mission memory, project-scoped Live requirements, operation records, client/task/inbox stores, active brief intelligence, desktop context, and the Obsidian-compatible `docs/marcus/` vault. Automatic daily/project/person/client/money note writing and vault indexing are planned work, not yet a verified runtime capability.

The production project registry includes `Reggie` at `markgromer/Reggie`, with `connect.scooper.site` and `Sweep and Go` aliases.

Current routes:

- `GET /api/marcus/operator-health`
- `GET /api/marcus/acceptance`
- `POST /api/marcus/project-operator`
- `POST /api/marcus/live/chat` for project operator requests
- `GET /api/marcus/live-presence/status` for local browser-presence readiness
- `PUT /api/marcus/live-presence/setup` for Mark-maintained live-presence setup state
- `POST /api/chat` for project operator requests
- `POST /api/operations/provider-action` to prepare a durable provider mutation without approving it
- `GET /api/integrations/github/pull-request`
- `GET /api/integrations/cloudflare/workers`
- `GET /api/integrations/cloudflare/worker-versions`
- `GET /api/integrations/cloudflare/worker-deployments`

`/api/marcus/operator-health` is the honest capability readout. It reports whether Marcus can audit, prepare Codex handoffs, start Codex directly, read GitHub/Cloudflare through server credentials, prepare approved provider mutations, see the desktop agent, and handle email/text capabilities with approval gates.

External communication routes:

- `GET /api/marcus/providers/config`
- `PUT /api/marcus/providers/config`
- `POST /api/marcus/providers/verify`
- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/send`
- `POST /api/marcus/external-actions/:id/reject`

Email uses SMTP. Text uses Quo's message API. Every route in the external-action list requires the durable admin token or paired HttpOnly cookie; a Live token alone receives 401. Conversational approval through `/api/marcus/live/chat` also checks durable authentication on the request before it can approve or send. Drafting and approval are durable; provider acceptance moves the action through `sending` to `sent` and records the provider receipt. Marcus Mobile's admin-only `Integrations` dialog writes Quo/SMTP settings without returning secrets, verifies each provider without sending, and records a bounded timestamped verification result. The `Verify` dashboard can open an exact-draft review containing recipient, subject, project, reason, body, and draft id; its send command remains disabled until Mark explicitly authorizes that exact draft. Quo verification retains an unambiguous resolved sender. A later settings change invalidates that provider's verification. Production credentials are still required before either provider can send.

Inbound email uses IMAP primitives in `server.js`: `GET /api/integrations/email/status`, `POST /api/integrations/email/test`, `POST /api/integrations/email/sync`, and `POST /api/integrations/email/archive-to-qdrant`. These routes can fetch and normalize mailbox content, import unique inbox items, and optionally upsert bounded email knowledge to Qdrant. A deployed scheduled watcher and automatic reply-draft promotion are planned rather than verified.

Skool and Zoom are not provider-integrated API bots in this repo. The operating model in [[external-presence]] and [[live-presence]] uses local browser presence on Mark's PC: a dedicated Marcus browser profile, visible assistant identity, virtual audio routing, Realtime voice, browser/chat observation, and durable note/action capture. Skool and future social/community channels remain watch-and-draft unless a compliant posting path is explicitly approved. Zoom live participation is allowed only through the local visible assistant model after identity, audio, and consent gates are met.

## Providers

`marcus/providers` contains execution/read providers:

- Codex provider.
- HTTP Codex adapter.
- GitHub Actions Codex adapter.
- Desktop provider.
- GitHub read provider.
- GitHub write provider for exact-head pull-request merges.
- Cloudflare write provider for project-bound DNS changes and Worker-version deployments.
- Browser verification provider.

The HTTP Codex adapter is enabled only when `MARCUS_CODEX_ADAPTER_URL` or `CODEX_ADAPTER_URL` is configured. It calls start/status/follow-up/artifact/diff/cancel endpoints and keeps the durable operation lifecycle in the existing provider runner.

The GitHub Actions Codex adapter borrows Reggie's runner pattern. It is enabled only when `MARCUS_CODEX_GITHUB_ACTIONS_ENABLED=true` and a GitHub token is available through `MARCUS_CODEX_GITHUB_TOKEN`, `CODEX_GITHUB_TOKEN`, or `GITHUB_TOKEN`. It dispatches `repository_dispatch` event `marcus_codex_job` to `MARCUS_CODEX_RUNNER_REPO` or `markgromer/Reggie`, where `.github/workflows/marcus-codex-runner.yml` runs `openai/codex-action@v1` with Reggie's existing runner secrets.

`marcus/providers/codex_provider.js` defines the branch authority in the handoff. The authenticated implementation request permits scoped commits and pushes only to the suggested nonproduction operation branch plus creation/update of its review PR. Protected/default/production branch pushes, merge, deploy, DNS, credentials, and external communication remain separate approved actions.

After a successful runner workflow, `marcus/providers/github_actions_codex_adapter.js` independently queries the target repository for the work branch, pull request, head commit, changed files, bounded patches, check runs, and commit statuses. The evidence is redacted, hashed with an evidence digest, and stored separately from Codex's own claims. `marcus/operations/codex_result_reviewer.js` compares only complete authoritative GitHub diff evidence with the original request and acceptance criteria. The semantic reviewer receives strict JSON-schema output and may cite only exact ids from Marcus's validated evidence catalog. Marcus-generated audit, handoff, and completion-control criteria are instead bound deterministically to durable operation records. Missing commits, missing/truncated patches, pending or failed target checks, malformed or uncited model output, incomplete criterion coverage, unsupported execution claims, and digest mismatches fail closed to `needs_manual_review` or `failed`.

An automated result review can satisfy only `diff_review`. It cannot satisfy build, test, lint, typecheck, browser, deployment, merge, or communication evidence. Provider-supplied `codex_result_review` artifacts are renamed and retained as untrusted claims before the independent reviewer runs.

Production acceptance operation `op_NfHu37cdF1aSjQ` exercised this path against demo PR #4. The first target-token request failed closed because the repository was not enrolled in Reggie Hub. After authenticated enrollment, the same durable operation completed Reggie run `31584535255`, collected two complete patches at head `4ee4135eb98be5bc57385be0ff128ee78fa42729`, and bound them to evidence digest `99dc7a16679924e30285fff2b3fb1baaff9b24379e73d5f6d35332c4104c8d1b`. An initial uncited model pass was not sufficient to complete the operation; independent build, test, lint, and URL-health evidence plus the corrected strict review were required. PR #4 remained open and live `/version` remained HTTP 404, proving no merge or deployment occurred.

Production currently uses this GitHub Actions adapter. GitHub repository reads, repository dispatch, runner reconciliation, pull-request creation, and durable verification completion were exercised end to end on the Marcus demo project. Before deployment of the independent review change, the new collector was also exercised read-only against demo PR #3 and resolved its exact head commit, two complete changed-file patches, and GitHub check/status state without a write, merge, or launch action.

Production operation `op_-qcwlO85nndNkw` exercised the revised one-instruction path. Marcus audited one repository, indexed six paths, read six files in 2,493 ms, and started Reggie run `31616694759` with zero pending approvals. The run opened PR #5. Independent review caught an overbroad approval sentence, the review branch was corrected to head `4b49e5cd580b238402b07ff776cb82899206f34c`, and refreshed authoritative evidence passed `diff_review`. Three tests, Wrangler dry-run, JavaScript syntax, and `git diff --check` then passed; the operation completed. PR #5 stayed open and the live Worker stayed on its prior deployment/version.

Cloudflare production access uses a dedicated account token named `Marcus Production Operator`. It covers the Developer Services policy plus DNS write and zone read. It excludes billing, membership, and API-token administration. Production reads of all zones and the configured zone's DNS records have passed. Worker script, version, and deployment inspection also passed against `marcus-operator-demo-worker`; the current deployment pins version `a51aa87d-a3e8-4dc3-ab81-2b9577a5a17c` at 100 percent.

Provider mutations run through `marcus/providers/github_provider.js`, `marcus/providers/cloudflare_provider.js`, and the durable operation runner. The project registry is the authority boundary. Successful completion requires a trusted `provider_readback` verification result and `provider_mutation_evidence` artifact. If a provider accepts a mutation but Marcus cannot prove final state, the operation enters recovery instead of retrying. The production demo merge and Worker deployment operations are prepared and waiting for explicit approval; no live provider mutation has been executed through these paths yet.

## Project Awareness And Memory Index

`marcus/awareness/awareness_store.js` persists business-scoped awareness projects in `data/businesses/<business>/marcus-awareness.json`. Each record has a stable awareness id, registry binding, lifecycle, objective belief, confidence, lifecycle history, and indexed-memory metadata. Writes are serialized, atomic, backed up, and recover from the last valid sibling backup.

`marcus/awareness/project_memory_index.js` creates a missing root `marcus.txt` only for the current workspace or an approved local workspace. It reads bounded non-secret excerpts from `marcus.txt`, README/package metadata, and a matching `docs/marcus/projects/<project>.md` note. It also builds a bounded repository manifest while excluding dependencies, build output, Git internals, symlinks, environment files, and secret-like files.

The durable operation monitor and periodic evidence pass record terminal operation outcomes into awareness work history. When the bound local workspace is readable, the same idempotent event is appended to `marcus.txt` with status, verification count, and unresolved blockers, then the project memory index is refreshed.

`marcus/awareness/awareness_service.js` reconciles registry identity, lifecycle, project activity, operation summaries, and indexed project memory. Recent exact Codex workspaces reported by the desktop relay are additively registered as pending-trust projects when no path or repository match exists. Archived and completed projects remain searchable, while active evidence collection excludes both statuses.

Authenticated routes in `marcus/api/awareness_routes.js` expose the compact feed, historical search, project detail, lifecycle changes, memory refresh, and bounded project context. `POST /api/marcus/live/chat` accepts an `awarenessProjectId`; when present, the exact awareness context is loaded before normal Marcus intent, operation, and approval routing.

## Evidence

`marcus/evidence` collects activity signals from:

- Operations.
- Airtable-derived legacy state.
- GitHub.
- Render.
- Cloudflare.
- Browser verification.
- Desktop workspace activity.

Archived and completed project-registry records remain available for historical lookup but are excluded from provider collection, activity snapshots, current-focus selection, and bottleneck scoring. This prevents retired or finished records from remaining operationally active.

Project activity now exposes an evidence-backed operating layer in addition to the older compatibility `state`:

- `operationalState` maps the project into Marcus-facing states such as active, verifying, at risk, decaying, and dormant.
- `health` combines objective clarity, definition-of-done presence, recent meaningful movement, blockers, verification gaps, and deterministic risk rules.
- `momentum` counts meaningful movement rather than raw activity. Codex handoffs and repository reads do not count as implementation progress.
- `decay` reports quiet-but-healthy, attention-slipping, at-risk, decaying, or dormant-candidate stages with cadence, evidence timestamps, severity, and a reason.
- `lastMeaningfulMovementAt`, `lastVerifiedEvidenceAt`, and `nextExpectedEvent` are derived from observed evidence and operation state.

The project registry also preserves canonical operating fields: business area, current objective, definition of done, success evidence, objective cadence, durable project memory categories, and archive history. These fields are file-backed and survive restart. They are not yet a full multi-objective/task graph; objective and granular task lifecycle expansion remains planned.

Capability audit for the Marcus/OS1 operating loop:

- Real: project registry, alias-aware project resolution, operation-backed Codex handoffs/direct adapters, evidence ingestion/deduplication, activity snapshots, deterministic health/decay/momentum, mission memory, approval-gated provider mutation paths, desktop context/action evidence, mobile voice bridge, and operator health reporting.
- Partial: objective state is canonicalized on project records but only the current objective is modeled; derived project summaries exist but are not yet a full timeline/projection service; brief generation consumes older active-brief signals and project-activity data separately; notifications are consequence-oriented in selected flows but not a complete notification engine.
- Simulated/development-only: browser verification can accept authenticated external/manual results when no direct browser adapter is configured.
- Unavailable/not complete: automatic specialist-agent creation/evaluation/retirement, full natural-language global command search over every domain, complete offline mobile action reconciliation, and a complete multi-objective decision execution graph.

## Decisions And Authority

Durable operation approvals are now Marcus decision records, not simple boolean flags. Each pending approval includes:

- decision statement
- project and objective
- why the decision is needed
- Marcus recommendation
- supporting operation, step, and policy evidence
- alternatives considered
- benefit, cost, risk, and consequence of waiting
- reversibility and rollback method
- authority level and available actions

Approval, approval with conditions, and decline outcomes are persisted back onto the same decision package with Mark's reasoning, conditions where supplied, decision actor, and timestamp. This still uses the existing operation approval lifecycle, so high/critical actions remain stopped until explicit authorization. Full decision follow-through across every domain is partial: operation approvals execute and verify through the durable runner, but broader multi-project decisions, discuss/defer workflows, and automatic post-decision notification routing are not yet a complete standalone decision engine.

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

`public/live-presence.html` is the local browser-presence setup console. It reads `/api/marcus/live-presence/status`, saves manual setup progress through `/api/marcus/live-presence/setup`, and shows readiness for private copilot and public voice modes. The model lives in `marcus/live/live_presence.js`.

The existing Live voice path is a chained fallback: browser or recorded speech input, OpenAI transcription, Marcus text chat, and ElevenLabs or browser speech output.

## Visualizer UI

`public/visualizer.html` is connected to live operations, Codex jobs, desktop context, ActiveBrief, the project registry, and `GET /api/marcus/awareness?includeArchived=true`. Canonical awareness supplies lifecycle, archive state, indexed-memory freshness, and stable project context. Archive and restore use authenticated server lifecycle writes; browser-local dismissal is no longer the project authority.

The default ledger suppresses archived projects, intentionally dormant projects, and completed projects older than fourteen days. Search can reveal non-archived historical projects, while the archived view remains separate. Project conversation sends the stable awareness id through the existing Live chat so durable work still follows the normal operation and approval boundaries. The richer evidence reconciliation, explicit correction model, and first-class Codex packet described in [[visualizer-operational-awareness]] remain partial.

## Mobile UI

`public/mobile.html` is the Android-friendly Marcus mobile shell.

It uses:

- `public/manifest.webmanifest` for Android home-screen installation with stable app ID `/mobile.html` and explicit 192x192/512x512 `any` and `maskable` PNG icons.
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

`client/marcus-realtime.js` builds the browser client with `@openai/agents-realtime`; `scripts/build-mobile.mjs` bundles it into `public/marcus-realtime.js`. Realtime voice uses `gpt-realtime-2.1` by default with voice `cedar`, semantic VAD, interruption, near-field noise reduction, and live transcription. It is instructed to speak as Marcus directly, with concise natural answers, variable human tone, smart dry humor when appropriate, and no unnecessary spoken IDs. Its only operational function is `marcus_operator`, which sends durable work, live status, approvals, and verified context requests back through `/api/marcus/live/chat`. The voice model does not own GitHub, Cloudflare, Codex, external communication, or approval authority.

Selectable personality prompt modes are defined in [[personality-modes]] and selected by `MARCUS_REALTIME_PERSONALITY_MODE` or the per-session `personalityMode` sent from Marcus Mobile and the OBS console. The deployed default remains Operator mode. Meeting Shadow, Public Assistant, Demo, and Roast Light have prompt fragments, status/client-secret reporting, a mobile selector, an OBS/demo sidecar, and a spoken-command Realtime tool. They still require production verification before they should be represented as complete live-call capabilities.

`client/mobile-operation-tracker.js` builds `public/marcus-operation-tracker.js`. Marcus Mobile records only the active operation id and bounded status signature in local storage, then polls the Live-token-safe `GET /api/operations/summary`. The summary exposes title, project, status, current step, step progress, verification counts, blocker count, approval/recovery flags, update time, and at most one redacted pending-approval descriptor containing `id`, `action`, `riskLevel`, `reason`, and `expiresAt`. The `Verify` view separately renders all currently returned pending approvals in critical-to-low risk order and binds each review button to its exact summarized operation. It does not expose prompts, audit excerpts, artifacts, source patches, provider metadata, credentials, or provider mutation input.

The server's durable operation monitor independently ticks only `queued`, `running`, `awaiting_provider`, and `verifying` operations. It does not touch waiting-approval, blocked, paused, recovery-required, failed, cancelled, or completed states. Mobile polling is therefore observational rather than the mechanism that keeps Codex work alive.

Startup recovery reconciles a completed Codex provider job only when its bound operation step is not already complete. A stable blocked operation with completed implementation and an active verification blocker is left unchanged across restart. Runner and recovery blocker creation is idempotent by active blocker type and step, preventing repeated process replacements from growing duplicate blockers or recovery events.

The browser lifecycle closes the media session while the PWA is hidden, resumes with a fresh ephemeral credential, reconnects after network or WebRTC loss with bounded backoff, and refreshes at 55 minutes before the Realtime session limit. A valid durable pairing cookie takes over immediately if a Render restart invalidates a process-bound Live token, even when the stale token is still present in the Authorization header. Service worker cache `marcus-mobile-v23` includes explicit raster install assets, the acceptance controls, a risk-ordered required-approval queue, an exact-target operation approval dialog, exact-draft failed-delivery retry, the OBS sidecar shell, and a completed phone-confirmation label. Operation approval remains disabled until Mark checks the authorization statement, and critical actions also require typed strong confirmation. Message delivery remains disabled until Mark checks a separate statement authorizing the exact displayed draft. Both paths use paired admin authentication; direct external-action routes and conversational approval attempts reject a Live token without durable authentication. The acceptance session ID, start time, and coarse platform/display context persist locally for up to two hours; context changes invalidate the ID, and the telemetry queue remains memory-only. Selective Realtime announcements speak only persisted completion, failure, cancellation, approval, blocked, or recovery transitions; ordinary progress polling stays visual. Production acceptance now has receipt-backed approved Quo and SMTP sends, all eight installed-Android voice lifecycle gates, explicit phone confirmation, and a 13/13 combined result.

The Mobile voice mode selector stores only the selected mode id in local storage and sends it with the next Realtime client-secret request. A mode change while voice is active closes the current disposable WebRTC session and reconnects with the new prompt. The Realtime voice model can also call `set_marcus_personality_mode` for explicit spoken mode commands. Mode changes do not alter provider authority, operation approval, external-message approval, or credential access.

`public/obs-marcus.html` is a browser/OBS sidecar for demos and live-call support. It can request microphone capture or browser-supported display/system-audio capture and pass the resulting `MediaStream` into `client/marcus-realtime.js`. It can also send pasted Zoom chat or meeting notes into the active voice session through `sendContext()`. It does not directly connect to Zoom APIs, read Zoom chat automatically, or change meeting consent requirements.

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
- All external-action routes and conversational approval execution require durable admin authentication; a short-lived Live token is insufficient.

Quo is configured in production with a dedicated existing `os1` credential and the Operations line. The no-send verifier resolved the canonical sender, phone-number id, and user id on 2026-08-12. Credentials are never returned by the configuration API.

SMTP is configured through Resend's relay with `Marcus <marcus@gromore.media>`, and Resend shows `gromore.media` as verified. A dedicated sending credential is now verified and the combined production report contains receipt-backed approved-send evidence for both SMTP and Quo. Provider-form writes invalidate verification only when that provider's effective configuration fingerprint changes, so an email-key rotation cannot erase unchanged Quo verification. Draft approval and provider acceptance remain separate durable facts.

## Desktop Agent

`desktop-agent.cjs` relays local context and executes queued desktop actions.

The same desktop agent owns the MARCUS browser viewport loop. `desktop-marcus-browser.cjs` launches or attaches only to the isolated `%LOCALAPPDATA%\M.A.R.C.U.S\MarcusBrowserProfile` Chrome instance through `127.0.0.1:9333`, captures bounded JPEG page frames, blocks password-field frames and typing, and executes the narrow `marcus-browser-open` and `marcus-browser-command` desktop actions. `/visualizer.html` renders the frame and maintains explicit Mark/MARCUS control ownership. See [[live-presence]] and [[access-model]].

Browser bridge routes:

- `POST /api/marcus/browser/relay`
- `GET /api/marcus/browser/status`
- `GET /api/marcus/browser/frame`
- `POST /api/marcus/browser/control`
- `POST /api/marcus/browser/actions`

The Windows scheduled task `MARCUS-DesktopAgent` targets the canonical Render host. It reads the admin credential from `%APPDATA%/M.A.R.C.U.S/mobile-live-admin-token.txt`, so the secret is not present in Task Scheduler arguments or the `node.exe` command line. Polling is serialized and transient process-spawn failures do not terminate the relay.

The matching VS Code task also targets the canonical Render host and relies on the same protected token file. Production operator health confirmed the relay online after the 2026-08-12 deployment restart.

It can:

- Report active window/workspace/git context.
- Open VS Code.
- Run allowlisted project scripts.
- Prepare publish checks.
- Publish project changes only when authorized.
- Validate workspace trust challenges.
- Declare Mark's explicit broad-PC authorization and the exact permitted roots.
- Report a typed PC-operator capability manifest through `GET /api/marcus/pc/capabilities`.
- Prepare an exact-agent, all-fixed-drive critical operation through `POST /api/marcus/pc/access-request` without changing local policy.
- Persist approved `configure-pc-access` scope and independently read it back through `verify-pc-access` before operation completion.
- Search filenames, list folders and installed applications, and read bounded non-secret text files within declared roots.
- Visibly open an exact file/folder/HTTP(S) URL or launch an already-installed Start-menu application only from Mark's direct authenticated request.
- Discover and switch between recent Codex workspaces instead of binding the conversation to whichever project was most recently active.
- Create an exact new project folder below `MARCUS_NEW_PROJECT_ROOT`, initialize Git, and open it in VS Code.
- Run local Codex through `codex exec --json --sandbox workspace-write` and stream lifecycle events to the server.
- Open `/codex-run.html` in Chrome kiosk or app mode so Mark can watch the job in real time.
- Connect a verified GitHub repository as `origin` and run the existing approved publish path.
- Run an approval-gated Wrangler Worker deployment and return the verified `workers.dev` or `pages.dev` URL.

`marcus/providers/desktop_codex_adapter.js` is the durable server-side local-job adapter. It stores job status independently from the desktop action queue, issues a per-job monitor capability whose hash is retained in the job store, validates that updates come from the bound desktop agent, and exposes bounded events, final output, changed files, diff summary, follow-up, and cancellation. A follow-up rotates the monitor capability before dispatch and reopens the kiosk on the PC while resuming the same Codex thread. The public monitor API accepts only the unguessable per-job capability; it does not accept or reveal the Marcus admin token.

Production verification on 2026-08-13 UTC passed the full phone-to-PC project route. The scheduled relay advertises `C:\` full-PC scope, eight PC capabilities, and kiosk mode; hosted Marcus routes exact attested local workspaces through `desktop_codex_with_fallback`, and Scoop Fairies project switching opened the correct VS Code workspace. Critical operation `op_1HYqnishgglGZQ` persisted and read back the exact policy, and production inventory, filename search, directory listing, and bounded non-secret read returned real demo-project evidence. The `Marcus PC Bridge Demo` operation created and attested a blank Git workspace, launched Codex, resumed the same thread for a correction, opened a fresh kiosk monitor, created and pushed a private GitHub repository, and deployed its Worker through the shell-free Windows launcher. Independent project checks pass: 5/5 tests, lint, typecheck, Wrangler dry-run, root HTTP 200, and health HTTP 200. The complete Marcus suite passes `147/147`, and syntax lint passes for 72 JavaScript files. `MARCUS_DESKTOP_CODEX_ENABLED=false` disables the desktop Codex path.
