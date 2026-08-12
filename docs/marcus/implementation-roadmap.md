# Implementation Roadmap

## Phase 1: Project Operator Service

Create a dedicated `ProjectOperatorService`.

Responsibilities:

- Accept conversational requests.
- Decide whether the request is question, preparation, execution, or external communication.
- Resolve the project.
- Gather context.
- Produce an execution brief.
- Produce a Codex prompt.
- Create or reuse a durable operation.

Primary integration point:

- `/api/marcus/live/chat`
- `/api/chat`
- `/api/marcus/project-operator`

Status: initial implementation exists in `marcus/operators/project_operator_service.js`. It deterministically resolves a project, gathers current context, writes an execution brief, composes a Codex prompt, and creates a durable Codex handoff operation.

Deep-audit status: implemented and verified locally and in production. Marcus indexes recursive GitHub trees for up to six request-related repositories, captures branch/commit/pull-request state, reads request-ranked source/configuration/test evidence, excludes obvious secret paths, redacts retained excerpts, and records coverage/failures/timing. The real Codex handoff carries up to 30,000 characters of audit evidence and requires Codex to reopen callers, dependents, and tests.

Production read-only deep-audit acceptance on 2026-08-12 resolved `markgromer/Reggie`, indexed 180 paths, read ten request-ranked files with no failed checks, inspected the current head and two open pull requests, and generated a 15,566-character prompt containing the Sweep and Go popup, API-token, slug, and verification requirements. It took 4,060 ms, created planned operation `op_iezXfygvErao0w`, started zero provider jobs, and was cancelled after inspection. GitHub account enumeration confirmed Hub is the `hub/` subtree in the private Reggie repository, not a separate current repository.

Mobile continuity status: implemented and covered by regression tests. Marcus stores a bounded recent transcript and active project, recognizes explicit GitHub `owner/repository` references, and carries earlier requirements into a later audit/Codex request. `Reggie` is registered in production as `markgromer/Reggie` with `connect.scooper.site` and `Sweep and Go` aliases.

Execution-language status: explicit phrases such as install, replace, migrate, start Codex, get Codex fixing, and get it going in Codex authorize the medium-risk Codex implementation step from the authenticated request. They do not create a redundant approval loop before Codex starts. Read-only questions and explicit do-not-start instructions remain non-authorizing. High/critical merge, deployment, DNS, publish, and communication actions keep their independent approval boundary. Marcus's immediate reply now keeps a concise audit summary instead of dumping operation internals.

Production acceptance on 2026-08-12 passed this path with operation `op_-qcwlO85nndNkw`: Marcus audited six files in 2,493 ms, started one Reggie job with zero pending approvals, opened demo PR #5, caught and corrected an approval-boundary defect during independent review, refreshed the exact-head evidence, and completed after three tests, Wrangler dry-run, syntax, and diff checks passed. The PR remained open and the Worker remained unchanged.

## Phase 1A: Durable Mission Memory

Status: implemented and verified locally and in production. `MissionMemoryStore` persists the overall operator mission, standing instructions, preferences, decisions, and facts independently from the rolling chat and per-project requirements. It is business-scoped, revisioned, atomic, backup-aware, secret-rejecting, and available to Live voice/mobile chat, main chat, project audits, Codex handoffs, operator health, and combined acceptance.

Regression evidence covers default mission seeding, explicit conversational memory and recall, admin-only routes, Live-token rejection, deduplication, archival, restart persistence, business isolation, secret rejection, corrupt-primary recovery, backup discovery, and mission text in the real generated Codex handoff. The complete local suite passes `101/101`, and GitHub CI run `31582100170` passed for implementation commit `71dc510`.

Production mission-memory acceptance on 2026-08-12 created standing instruction `mem_1aUnq0Ll2OzFeg` through Marcus Live: before starting Codex, deeply audit relevant repositories and carry inspected evidence into the prompt. Marcus recalled it in a separate conversation turn. Read-only Reggie operation `op_wrI7kf12uhE9ig` attached that exact memory ID plus three seeded records, indexed 180 paths, read ten files with zero failed checks, preserved the Sweep and Go/API-token/slug requirements, remained `planned` with zero Codex jobs, and was cancelled.

Render then replaced the process: uptime reset from 103 seconds to 14.5 seconds after an observed outage. The memory store remained at revision 3 with all four active records, a newly minted Live session recalled the standing instruction, and the staged operation remained cancelled with zero jobs. Operator health and combined acceptance both reported mission memory ready. GitHub CI run `31582262882` passed for the acceptance-record commit.

## Phase 2: Context Gathering

Add a reusable context gatherer that can pull:

- Project registry.
- Legacy project data.
- Relevant tasks.
- Inbox/client communication.
- Existing operations.
- Project evidence.
- GitHub repo metadata and important files.
- Cloudflare deployment/DNS metadata.
- Desktop context.

The output should be structured and stored as an operation artifact.

Status: GitHub deep gathering is implemented in the project operator and persisted as bounded operation metadata plus the execution brief. Cloudflare/Render evidence still comes from the existing project-evidence layer rather than the same recursive repository audit transaction.

## Phase 3: Codex Session Launch

Current Codex support can create strong handoffs. The next upgrade is launch orchestration.

Requirements:

- Generate prompt from execution brief.
- Start Codex when a direct adapter is available.
- Otherwise produce a clean external handoff.
- Store Codex job id, branch, artifacts, and status.
- Poll or reconcile status.

Status: handoff mode and direct launch are implemented. Production uses the Reggie-style GitHub Actions adapter. The first complete production acceptance operation dispatched `openai/codex-action@v1`, recovered from a failed runner attempt, opened a pull request, and completed after independent verification evidence was registered.

Direct adapter environment:

- `MARCUS_CODEX_ADAPTER_URL` or `CODEX_ADAPTER_URL`
- `MARCUS_CODEX_ADAPTER_TOKEN` or `CODEX_ADAPTER_TOKEN`
- Optional path overrides: `MARCUS_CODEX_ADAPTER_START_PATH`, `MARCUS_CODEX_ADAPTER_STATUS_PATH`, `MARCUS_CODEX_ADAPTER_FOLLOWUP_PATH`, `MARCUS_CODEX_ADAPTER_ARTIFACTS_PATH`, `MARCUS_CODEX_ADAPTER_DIFF_PATH`, `MARCUS_CODEX_ADAPTER_CANCEL_PATH`
- Optional timeout: `MARCUS_CODEX_ADAPTER_TIMEOUT_MS`

When configured, `/api/marcus/operator-health` reports `mode: direct_codex` and the active provider. When no direct adapter is configured, Marcus stays in `codex_handoff` mode and does not claim a real session started.

Reggie-style GitHub Actions adapter environment:

- `MARCUS_CODEX_GITHUB_ACTIONS_ENABLED=true`
- `MARCUS_CODEX_GITHUB_TOKEN` or `CODEX_GITHUB_TOKEN` or `GITHUB_TOKEN`
- Optional runner repo: `MARCUS_CODEX_RUNNER_REPO` or `CODEX_RUNNER_REPO`; default is `markgromer/Reggie`
- Optional runner event: `MARCUS_CODEX_RUNNER_EVENT_TYPE` or `CODEX_RUNNER_EVENT_TYPE`
- Optional workflow file: `MARCUS_CODEX_RUNNER_WORKFLOW` or `CODEX_RUNNER_WORKFLOW`

When this adapter is configured, `/api/marcus/operator-health` reports provider `github_actions_codex`. The default Reggie runner uses `REGGIE_OPENAI_API_KEY` and `REGGIE_GITHUB_TOKEN`, which were verified through a successful production run on 2026-08-12.

## Phase 4: Result Review

Marcus should audit Codex output before reporting success.

Status: implemented and accepted in production. Marcus now separates semantic code review from deterministic control-plane evidence binding, so a model cannot invent that an audit, handoff, verification, merge boundary, or deployment boundary occurred.

Checks:

- Did the result address the original request?
- Were expected files changed?
- Were tests/build/lint run?
- Is browser verification needed?
- Is deployment or client communication still pending approval?

Implemented path:

- Collect target PR, head commit, changed files, bounded patches, check runs, and commit statuses directly from GitHub after the Reggie runner completes.
- Bind the stored diff and independent review with a SHA-256 evidence digest.
- Treat repository patches as untrusted input to a separate AI reviewer.
- Use OpenAI strict structured output for semantic implementation criteria, with one schema-required coverage entry per criterion and evidence references restricted to exact catalog ids.
- Bind Marcus-generated audit, Codex-handoff, and completion-control criteria deterministically to durable operation metadata, provider job identity, GitHub artifacts, and authenticated verification evidence.
- Exclude prior `diff_review` results from the evidence catalog so result review cannot prove itself.
- Require explicit coverage of every acceptance criterion with validated evidence-catalog references, no high/blocker finding, no unsupported execution claim, and at least 0.8 confidence before passing `diff_review`.
- Fail closed on missing/truncated evidence, pending/failed target checks, invalid review output, or provenance mismatch.
- Refresh GitHub evidence and the independent review on verification retry without relaunching Codex.
- Keep build, test, lint, typecheck, browser, deployment, merge, and communication gates independent.

Production acceptance on 2026-08-12:

- Durable operation `op_NfHu37cdF1aSjQ` audited one repository, indexed six paths, read six files through eleven GitHub API calls, and carried all four mission-memory records into an 11,281-character Codex prompt.
- The first Reggie dispatch, run `31584286489`, failed before Codex when Reggie Hub correctly refused to mint a target-repository token for an unenrolled site. The demo repository was then enrolled through the authenticated Hub provisioning path; no credential was printed or stored in documentation.
- Existing operation attempt two dispatched Reggie run `31584535255`, completed `openai/codex-action`, and opened PR #4 at exact head `4ee4135eb98be5bc57385be0ff128ee78fa42729` without creating a second operation.
- GitHub evidence resolved two complete patches, the open PR, commit, checks, and SHA-256 digest `99dc7a16679924e30285fff2b3fb1baaff9b24379e73d5f6d35332c4104c8d1b`.
- An early reviewer output claimed success with blank citations. Marcus's independent build/test/lint gates kept the operation blocked. The corrected reviewer then rejected two uncited structured responses instead of accepting them.
- Independent verification at the exact PR head passed five of five tests, `wrangler deploy --dry-run`, JavaScript syntax, and clean-worktree checks. A separate production observation confirmed PR #4 remained open and unmerged while live `/version` returned HTTP 404.
- The final strict review cited both changed files plus build, test, lint, and URL-health evidence; all four criteria were grounded, no unsupported claims remained, and the operation completed.
- No extra Reggie job was dispatched during evidence refresh. PR #4 remains unmerged and the live Cloudflare Worker remains unchanged.
- Local regression passed `114/114`; GitHub CI run `31586173120` passed for implementation commit `4a98ffc` before the final production review.

## Phase 4A: Approved GitHub And Cloudflare Mutations

Status: implemented, locally mutation-tested, deployed, and production-prepared. Live consequential execution remains pending explicit approval.

Implemented actions:

- Merge one pull request in the exact registered GitHub repository at an exact expected head SHA.
- Create or update one project-bound Cloudflare DNS record.
- Delete one exact DNS record only when its expected state still matches and strong confirmation is present.
- Promote one exact version of one registered Cloudflare Worker while pinning the expected current deployment.

Control path:

`authenticated request -> provider inspection -> high-confidence registry binding -> immutable durable operation -> runtime approval -> drift revalidation -> one provider call -> authoritative read-back -> evidence-backed completion`

The action allowlist is deliberately narrower than the credentials. Marcus does not expose an arbitrary GitHub or Cloudflare request tool. Provider calls are idempotent where possible; accepted writes with uncertain read-back enter `recovery_required` and are not retried automatically.

Verification on 2026-08-12:

- The real-server integration harness performed a GitHub merge, DNS create, and Worker deployment exactly once against stateful mock providers, then verified provider read-back and evidence persistence.
- Focused provider tests passed `7/7`; the complete local suite passed `128/128`; JavaScript syntax passed for 73 files.
- GitHub CI runs `31615296935` and `31615747675` passed for commits `0409400` and `17769b0`.
- Render deployed both commits. Production operator health reports GitHub merge plus Cloudflare DNS/Worker mutation paths available.
- Live read-only inspection resolved demo PR #4 at exact head `4ee4135eb98be5bc57385be0ff128ee78fa42729`, with settled checks and no failures.
- Live Cloudflare inspection resolved Worker version `a51aa87d-a3e8-4dc3-ab81-2b9577a5a17c` and deployment `d8eb7206-6d65-434b-aaab-04cd51f62823` at 100 percent.
- `Marcus Operator Demo` is bound to the exact GitHub repository, Cloudflare account, Worker, and production URL.
- Merge operation `op_wSMm8zWz7DGGiA` and Worker operation `op_nA9c9c_bZYsMjg` are both `waiting_for_approval`. Re-inspection proved the PR and deployment remained unchanged.

Remaining acceptance: Mark must explicitly approve the exact demo PR merge and Worker deployment operations before Marcus may execute them. The Worker action targets the already-active version and should exercise the idempotent no-duplicate-write path. A separate approved future version is needed to prove a real Cloudflare production deployment mutation without intentionally rolling the demo backward.

## Phase 5: External Communication

Marcus can draft text/email actions and execute an approved provider send. Approval remains mandatory and distinct from provider acceptance.

Flows:

- Draft reply.
- Show recipient, channel, subject/body.
- Ask for approval.
- Send through configured provider as a separate approved provider action.
- Attach sent-message evidence to the project.

Implemented and tested locally on 2026-08-11:

- Text drafts use Quo after explicit approval.
- Email drafts use SMTP after explicit approval.
- Send claims use `sending` to prevent concurrent duplicate provider calls.
- Successful replay is idempotent and returns the stored receipt.
- Operator health distinguishes inbound text webhooks from outbound text capability.

Added and tested locally on 2026-08-12:

- Marcus Mobile provides one Quo/SMTP provider dialog.
- Configuration and no-send verification require durable admin-cookie/token authority; a Live-session token alone receives 401.
- Secret values are never returned, and blank secret inputs preserve existing values.
- Quo sender resolution and SMTP authentication persist bounded verification evidence and issue no message request or SMTP `DATA`.
- The complete local suite passes `98/98` with provider setup, redaction, authorization, no-send verification, approved-send/idempotency checks, deep GitHub audits, and mobile acceptance aggregation.

Production status: Quo is configured and verified against the Operations line. Approved acceptance action `rv1v4_RKB38v` reached `sent` with provider acceptance on 2026-08-12, so both Quo gates pass. SMTP is not configured. The selected SMTP path is a dedicated Resend sending-only credential for `Marcus <marcus@gromore.media>`, followed by one explicitly approved email acceptance send.

Production provider-onboarding acceptance on 2026-08-12:

- Render served service worker `marcus-mobile-v12` with the provider dialog at the canonical mobile URL.
- A fresh one-time pairing code authenticated a 390 x 844 Chromium session; the dialog opened with zero browser errors or warnings and no local/session storage entries.
- Unauthenticated and Live-session-only provider config requests returned 401; the paired durable-admin context succeeded.
- Config responses expose no API key or SMTP password. A dedicated Quo `os1` credential was later saved and verified without sending.
- Quo verification resolved the Operations line, phone-number id, and user id and retained `sent: false`. SMTP remains unconfigured.

Production project-continuity acceptance on 2026-08-11:

- A first mobile API turn set `Reggie` / `markgromer/Reggie` as the active project with the Sweep and Go API-token/slug popup requirement.
- A read-only follow-up named the correct active project and repeated the exact requirements without creating an operation.
- A separate audit request inspected one repository and three files before producing a 4,563-character Codex prompt containing the popup, API-token, and slug requirements.
- Acceptance operation `op_AxhGdUBf5tlB6g` stopped at `waiting_for_approval` and was cancelled without launching Codex or modifying the repository.

Production conversation/intent acceptance on 2026-08-12:

- A read-only Reggie turn retained `markgromer/Reggie` plus the Sweep and Go API-token, slug, verification gate, and setup-button requirements; the production operation count did not change.
- A separate "audit and prepare, do not start Codex" turn inspected one repository and three files and produced a 5,230-character prompt containing the retained API-token and slug requirements.
- Staged acceptance operation `op_4py4qYDAyb7J1A` remained `planned`, contained zero provider jobs, and was cancelled after verification without starting Codex or modifying the repository.

## Phase 6: Documentation Automation

Keep this Obsidian-compatible folder current.

Every meaningful Marcus architecture change should update:

- [[current-system-map]]
- [[execution-loop]]
- [[access-model]]
- [[implementation-roadmap]]

## Phase 7: Production Voice Interface

Decision: use OpenAI Realtime speech-to-speech over WebRTC as the primary mobile conversation layer. Keep ElevenLabs/browser speech as narration fallback and do not build a custom STT/TTS tuning stack.

Implemented slice:

- Server-minted short-lived Realtime client secrets.
- `gpt-realtime-2.1` and `marin` defaults with environment overrides.
- Official `@openai/agents-realtime` browser session rather than a hand-written Realtime transport.
- Semantic VAD, barge-in, near-field noise reduction, and `gpt-live-transcribe` input transcription.
- Android PWA start/stop voice control.
- Background/phone-lock pause and foreground resume with a fresh ephemeral credential.
- Network and WebRTC disconnect recovery with bounded backoff.
- Scheduled renewal at 55 minutes before the Realtime session limit.
- A single `marcus_operator` bridge back to the durable Live chat and approval flow.
- A persistent active-work strip that follows the same durable operation across refresh and shows current step, step progress, verification counts, blockers, and approval/recovery state through the redacted operation-summary API.
- A server-side monitor that keeps execution-safe durable jobs moving when the phone is closed, without ticking approval-gated, blocked, paused, recovery-required, or terminal operations.
- Selective Realtime announcements for persisted completion, failure, cancellation, approval, blocker, and recovery transitions.
- Project-scoped conversation memory that resolves the current explicit repository before selecting retained requirements, with bounded per-project requirement storage and durable-operation recovery.
- Bounded context replies and direct project-switch regression coverage to prevent stale requirements from entering Codex prompts.
- Authenticated, business-scoped acceptance telemetry with strict field allowlisting and 1,000-event retention.
- Derived gates for speech recognition, assistant audio, interruption, operator completion, network/background recovery, and installed Android context.
- Unit and smoke coverage for session policy, auth, static assets, interruption state, network/background recovery, expired credentials, and stale-connection races.
- Unit coverage for monitor state allowlisting/serialization, summary redaction, tracker persistence/deduplication, terminal handoff, and voice announcements.

Acceptance tests still required before this phase is complete:

- Start and stop a voice session from the installed Android PWA.
- Hold a multi-turn project conversation without reselecting the project.
- Interrupt Marcus while it is speaking and continue naturally.
- Create a project audit/Codex operation by voice.
- Approve a waiting operation by voice and confirm the same durable operation advances.
- Confirm external communication and production mutations still pause for explicit approval.
- Verify recovery after phone lock, network interruption, and an expired Live token.
- Inspect the resulting acceptance session and confirm every physical-device gate is true without relying on transcript storage.

The production paired-admin `Verify` dashboard starts a fresh acceptance session, persists only its ID/start time/coarse platform-display context for up to two hours, shows each voice/provider gate, and enables physical confirmation only after all eight Android voice gates pass in installed standalone mode. Context matching prevents browser-tab evidence from being reused after a standalone launch. `GET /api/marcus/acceptance` combines that evidence with provider, approved-send, Codex, GitHub, Cloudflare, OpenAI, and desktop readiness. Service worker `marcus-mobile-v15` and the dashboard passed Render deployment and phone-size Chromium acceptance; the physical-phone run remains pending.

Status: the official SDK, recovery, acceptance telemetry, durable-work tracking, PWA install assets, and mobile deployment contract remain covered in the current local `128/128` suite and 66-file syntax lint. At 390x844, Playwright confirmed the acceptance dialog is scrollable, its install/new-test/voice controls remain visible without overlap, standalone Android context is recorded, the same session survives reload, and a browser/standalone context mismatch invalidates the saved ID. Production service-worker cache `marcus-mobile-v15` and all four required 192x192/512x512 `any`/`maskable` PNG icons are live. The live combined report passes 9/12 gates; only SMTP verification, the approved SMTP acceptance send, and physical installed-Android voice acceptance remain. A real installed-Android speech, barge-in, and recovery conversation remains pending.

Verified locally on 2026-08-12:

- The configured OpenAI account minted a short-lived `gpt-realtime-2.1` client secret.
- A Playwright mobile browser authenticated to Marcus, started the voice control, established the OpenAI WebRTC call with HTTP 201, and reached `Voice on` / `Listening` with no browser warnings or errors.
- This used a synthetic microphone track; it does not replace the installed-Android speech and interruption tests above.

Verified against production on 2026-08-12:

- The durable mobile admin credential authenticated on the canonical Render host.
- The PWA loaded under service worker cache `marcus-mobile-v6`; the official SDK bundle was served with gzip.
- Production minted a short-lived `gpt-realtime-2.1` / `marin` client secret.
- A paired mobile Chromium session started voice, reached `Voice on` / `Listening`, and established the OpenAI WebRTC call with HTTP 201.
- Simulated network loss moved the UI to `Reconnecting` / `Waiting for network`; restoring network minted a fresh credential, opened a new HTTP 201 Realtime call, and returned to `Listening`.
- A browser `pagehide` / `pageshow` acceptance moved the UI through `Paused` and reconnected with a third fresh credential and HTTP 201 Realtime call.
- The browser reported no warnings or errors and stored no durable token in local storage.
- Production operator health reported `direct_codex`, GitHub ready, and Cloudflare ready.
- One-time six-digit mobile pairing was exercised against production. Cookie authentication succeeded, code reuse returned 401, no durable token was stored in browser storage, and the paired session established the Realtime call.
- A separate code minted before deployment `858a0ba` survived Render process replacement, authenticated with HTTP 200 on the replacement process, and returned 401 on replay from a fresh session.
- Service worker `marcus-mobile-v9` acceptance session `1ad47863-7d44-4a2b-9363-0aa50f67e16c` recorded every browser/synthetic gate as true: signaling, recognized speech, assistant audio, interruption, operator completion, network recovery, and background recovery.
- The same session correctly left `installedAndroidContext` false. It used browser-emulated Android plus deterministic synthetic speech and does not replace the physical-phone completion checks.
- The spoken request was read-only and explicitly prohibited audit, Codex, and operation creation. Production retained four operations, with no operation created during the acceptance run.

## Demo Deployment

GitHub demo repo:

- `https://github.com/markgromer/marcus-operator-demo-worker`

Live Cloudflare Worker:

- `https://marcus-operator-demo-worker.markgromer.workers.dev`

The deployed Worker demonstrates the audit and handoff contract. Its `/codex/start` endpoint is intentionally simulated and should not be treated as proof that a real Codex implementation session exists. The real execution proof is the durable Marcus operation and Reggie runner evidence below.

Verified endpoints:

- `/health`
- `/readiness`
- `/demo`
- `/audit`
- `/codex/start`

The readiness contract was verified live on 2026-08-12 from GitHub commit `4827421` and Cloudflare Worker version `a51aa87d-a3e8-4dc3-ab81-2b9577a5a17c`. It reports the runtime, audit path, Codex handoff, and external-action approval boundary ready. A live Reggie request returned an audited project, a structured Codex goal, `handoff_ready`, and `approvalRequiredForExternalActions: true`.

Production Codex acceptance evidence:

- Durable operation: `op_N_PUttVpm72mWw`
- Resolved project: `Marcus Operator Demo`
- Audit scope: one repository and three important files
- Codex runner: `https://github.com/markgromer/Reggie/actions/runs/31566699387`
- Review pull request: `https://github.com/markgromer/marcus-operator-demo-worker/pull/3`
- Result: operation completed with build, test, syntax/lint substitute, artifact, and diff-review evidence passed
- Verified branch: five tests passed and `wrangler deploy --dry-run` passed
- The pull request remains unmerged and undeployed, preserving the review and production-approval boundary.

Independent result-review acceptance evidence:

- Durable operation: `op_NfHu37cdF1aSjQ`
- Codex runner: `https://github.com/markgromer/Reggie/actions/runs/31584535255`
- Review pull request: `https://github.com/markgromer/marcus-operator-demo-worker/pull/4`
- Exact head: `4ee4135eb98be5bc57385be0ff128ee78fa42729`
- Evidence digest: `99dc7a16679924e30285fff2b3fb1baaff9b24379e73d5f6d35332c4104c8d1b`
- Result: four of four acceptance criteria grounded; five of five tests, Wrangler dry-run, syntax, artifact, diff-review, and unchanged-production evidence passed
- Boundary proof: PR #4 is open and unmerged; live `/version` returns HTTP 404 because the acceptance change was not deployed

One-instruction conversation-to-Codex acceptance evidence:

- Durable operation: `op_-qcwlO85nndNkw`
- Audit: one repository, six paths indexed, six files read, 2,493 ms
- Codex runner: `https://github.com/markgromer/Reggie/actions/runs/31616694759`
- Review pull request: `https://github.com/markgromer/marcus-operator-demo-worker/pull/5`
- Exact verified head: `4b49e5cd580b238402b07ff776cb82899206f34c`
- Result: no redundant medium approval; the operation blocked until independent diff review plus three tests, Wrangler dry-run, JavaScript syntax, and diff checks passed
- Boundary proof: PR #5 is open and unmerged, live `/operator-controls` returns HTTP 404, and the Worker deployment/version did not change

Production Reggie project-operator acceptance evidence:

- Durable operation: `op_f6XKmXTWILGvpQ`
- Resolved project: `Reggie` / `markgromer/Reggie`, without falling back to the earlier Marcus demo context
- Audit scope: one repository, 180 paths indexed, 10 files read, 2,548 ms
- Codex prompt: 18,731 characters, including the Sweep and Go Setup button, modal, API-token/slug, verification gate, secure-storage, preservation, and focused-test requirements
- Codex runner: `https://github.com/markgromer/Reggie/actions/runs/31588016385`
- Review pull request: `https://github.com/markgromer/Reggie/pull/16`
- Exact head: `36166678b23ec7f2a382d8c51a8d024c95715ffa`
- GitHub source-quality `verify` check passed; an isolated exact-head `npm run check` validated 55 managed files and standard, Pages, Vinext, and Render fixtures
- `git diff --check` passed. The repository has no lockfile, so `npm ci` was not applicable and is not recorded as passing
- Independent criterion review, artifact presence, and authenticated manual review passed; the operation completed with no active blockers
- Boundary proof: PR #16 remains open and unmerged. Marcus did not deploy it

Provider acceptance status on 2026-08-12:

- The existing dedicated Quo credential named `os1` is saved only in Marcus's redacted server settings.
- Quo authenticated sender lookup verified the Operations line `+18886107667`, resolved its phone-number and user ids, and reported `sent: false`.
- Production text acceptance action `rv1v4_RKB38v` was explicitly approved and provider-accepted at `2026-08-12T15:56:49.822Z`.
- SMTP remains unconfigured. The selected path is a dedicated Resend sending-only credential with `smtp.resend.com` and `Marcus <marcus@gromore.media>` after explicit approval.
- The remaining combined acceptance gates are SMTP verification, approved email send, and physical installed-Android voice acceptance.
