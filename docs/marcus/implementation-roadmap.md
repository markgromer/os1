# Implementation Roadmap

## 2026-08-17: Browser Presence Slice

Status: implemented and locally verified

- Dedicated Chrome profile launcher with localhost-only debugging.
- Continuous browser viewport relay into `/visualizer.html`.
- Mark/MARCUS control ownership and bounded remote input.
- Password-field frame and typing suppression.
- Direct-request Marcus tools for browser status and exact URL opening.
- Authenticated server routes and durable desktop action delivery tests.

Next: visible chat observation, Zoom/Skool page adapters, audio-device verification, meeting memory capture, and first consented live-call acceptance.

## 2026-08-17: Operator Intelligence Layer

Status: implemented locally; automated validation and demo required before production claim

- Human spoken-reference projection
- Bounded relationship continuity with public-mode privacy filtering
- Recurring-job priming manifests in project execution briefs
- Semantic post-interruption alignment audit
- Verified recovered-method memory
- Locked-decision conflict enforcement
- Runnable `npm run demo:marcus` evidence harness

## 2026-08-15: Local Live Presence Foundation

Status: implemented locally and covered by focused tests. Production deployment and live PC setup still require Mark to complete the browser profile, platform logins, audio routing, and emergency-control checklist.

- Added `marcus/live/live_presence.js` with live modes, platform targets, setup-item normalization, readiness scoring, and Mark-owned next-step reporting.
- Added `GET /api/marcus/live-presence/status` and `PUT /api/marcus/live-presence/setup`.
- Added `/live-presence.html` as the operator setup console for browser profile, visible identity, virtual audio, OBS, Realtime voice, and emergency controls.
- Added [[live-presence]] as the source-of-truth runbook for Zoom, Skool, YouTube Live, TikTok Live, and similar local browser-presence work.
- Added focused tests in `test/live-presence.test.js`.

Remaining: desktop-agent device enumeration, dedicated Marcus browser-profile launcher, Zoom/Skool visible chat observers, OBS scene state, emergency mute indicators, and a second-machine/VM option for stronger echo isolation.

## 2026-08-15: Canonical Project Awareness And Memory Index

Status: implemented locally and covered by focused tests. Production deployment was authorized on 2026-08-15 and requires exact live verification after the GitHub push.

- Added durable `marcus-awareness.json` storage with stable registry binding, lifecycle history, atomic writes, backup recovery, and archived retrieval.
- Added root `marcus.txt` creation for desktop-created projects and trusted local refreshes.
- Added idempotent root-note append and awareness work history for terminal durable-operation outcomes.
- Added bounded project-note, README/package, and repository-manifest indexing with secret-path exclusions.
- Added awareness feed, search, detail, lifecycle, refresh, and context APIs.
- Scoped Visualizer conversation to stable awareness ids and replaced browser-local archive state with authenticated server lifecycle writes.
- Normalized `Done`, `Complete`, `Completed`, `Archive`, and `Archived` as historical for active evidence collection.
- Added focused tests for persistence, indexing, secret exclusion, lifecycle visibility, historical search, and Visualizer wiring.

Remaining: automatic append for meaningful work that never creates a durable operation, richer cross-vault link/tag retrieval, desktop-relay content indexing for trusted workspaces not mounted on the server, explicit correction/conflict records, and first-class Codex dispatch packets.

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

## Phase 1B: Context Memory And Obsidian Graph

Status: doctrine documented; implementation remains planned. See [[context-memory]].

Goal: Marcus should retrieve and maintain context across days, projects, people, clients, money obligations, decisions, relationships, and reusable systems rather than relying only on the active project or rolling chat. The Obsidian-compatible vault should contain concise linked notes with useful tags so Marcus can quickly answer questions such as what Mark worked on on a date, which client owes money, what was decided last time, and which prior project solved a similar problem.

Planned implementation:

- Automatic daily work-note creation from meaningful sessions, operations, and explicit note commands.
- Project, person/client, money, decision, and system notes with stable hyphen-case filenames, tags, and wiki links.
- Vault indexing by date, tag, entity, aliases, and links.
- Retrieval into Marcus chat, voice, active brief, and Codex handoffs when context is relevant.
- Secret filtering and source/provenance fields before note writes.
- Correction flow for outdated, wrong, sensitive, or low-value notes.

Acceptance:

- Ask by date and retrieve the correct daily work note.
- Ask about any project and retrieve related notes, operations, decisions, and client context.
- Ask who owes money and retrieve invoice/payment context without leaking credentials.
- Ask about a person/client and retrieve relationship history and open obligations.
- Confirm note writes are concise, tagged, wiki-linked, and free of secrets.

## Phase 1C: Visualizer Operational Awareness

Status: initial live implementation completed on 2026-08-15. `public/visualizer.html` consumes canonical awareness and persists lifecycle changes through authenticated APIs. See [[visualizer-operational-awareness]].

Goal: Marcus should maintain a compact desktop view of his own operational awareness: what he believes is active, what Codex is doing, what changed, what needs Mark, what is blocked or external, what has gone quiet, and what he recommends recovering next. Mark should steer this conversationally through project-scoped `Talk to MARCUS` interactions, not by administering tracker fields.

Implemented foundation:

- Durable awareness project store separate from the project registry.
- Incremental reconciliation across operations, evidence, project activity, registry, Codex jobs, and bounded local repository memory.
- Confidence, uncertainty, lifecycle, attention-state, archive, and recent-completion handling.
- Stable project-scoped MARCUS conversation context and server-authoritative archive/restore.
- Default dashboard suppression for archived, dormant, and older completed work without deleting searchable history.

Remaining expansion:

- Split and reconcile multiple initiatives that intentionally share one repository.
- Add explicit conflict correction and richer decay-recovery models.
- Extend automatic note writing beyond terminal operation outcomes into broader verified session summaries.
- Expand vault-wide semantic retrieval while preserving bounded reads, provenance, and secret exclusions.

Acceptance:

- The visualizer clearly distinguishes fixture data, live evidence, and inference.
- Quiet projects are evaluated with explanation and confidence rather than faded away.
- Every project row has a `Talk to MARCUS` path scoped to that project.
- Marcus can preserve intentional dormancy without deleting project context.
- Completion claims trigger verification against objective and evidence.
- Consequential actions remain approval-gated.

## Phase 1D: Evidence-Backed Operating State

Status: implemented as the current vertical slice. Project registry records now retain business area, current objective, definition of done, success evidence, objective cadence, durable memory categories, and archive history. Project activity snapshots now derive `operationalState`, `health`, `momentum`, `decay`, `lastMeaningfulMovementAt`, `lastVerifiedEvidenceAt`, and `nextExpectedEvent` from stored evidence plus durable operation state.

This slice intentionally does not claim the complete OS1 operating loop. It makes the early loop honest: Marcus can preserve canonical project truth, distinguish movement from raw activity, mark decay before a project disappears, and explain the evidence behind that assessment. Multi-objective/task graphs, autonomous agent lifecycle, full decision execution, offline mobile reconciliation, and global command search remain later phases.

Regression coverage:

- Registry persistence for objective, definition-of-done, durable memory, and archive history.
- Evidence-backed project health, momentum, decay stage, operational state, and next expected event.
- Existing ingestion deduplication, source provenance, Codex lifecycle reconstruction, desktop evidence, deployment mapping, archived-project exclusion, and activity tools.

## Phase 1E: Decision Preparation And Authority Records

Status: implemented for durable operation approvals. Approval requests now persist a Marcus decision package containing the decision statement, project/objective, reason, recommendation, supporting operation/step/policy evidence, alternatives, benefit/cost/risk, consequence of waiting, reversibility, rollback method, authority level, and available actions. Approvals with conditions and declines persist the outcome, Mark's reasoning, decision actor, timestamp, and conditions.

This is not yet the full decision system described in the product brief. It covers the existing operation approval path and preserves approval gates for high/critical actions. Discuss/defer workflows, multi-project decisions, automatic downstream notification routing, and non-operation decision queues remain later work.

Regression coverage:

- Pending approvals include an auditable decision package.
- Approve with conditions persists the conditional outcome and conditions.
- Decline persists Mark's reasoning and blocks the operation.

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

Current Codex support creates strong handoffs and launches both GitHub Actions and local desktop jobs. The local desktop path is enabled and verified in production.

Requirements:

- Generate prompt from execution brief.
- Start Codex when a direct adapter is available.
- Otherwise produce a clean external handoff.
- Store Codex job id, branch, artifacts, and status.
- Poll or reconcile status.

Status: handoff mode and direct launch are implemented. Production reports `direct_codex` through `desktop_codex_with_fallback`: exact attested Windows workspaces use the desktop, while repository-only work retains the configured HTTP/GitHub Actions fallback. Job `desktop_codex_094e6bbb13d8ec8ef2d98ae9` completed in the generated demo workspace, retained its original thread across a correction, rotated and reopened its kiosk capability for a later follow-up, and stored bounded events and file evidence.

Local desktop adapter environment:

- Hosted runtime default: enabled; `MARCUS_DESKTOP_CODEX_ENABLED=false` disables the desktop path
- `MARCUS_ALLOW_BROAD_WORKSPACE_ROOTS=true` on the desktop relay
- `MARCUS_ALLOWED_WORKSPACE_ROOTS` with explicit Windows roots
- `MARCUS_NEW_PROJECT_ROOT` for blank-project creation
- `MARCUS_CODEX_MONITOR_MODE=kiosk` or `app`

Status: implementation and production acceptance pass. The adapter queues each launch once, survives restart, validates the exact desktop agent and project registry binding, streams redacted Codex JSONL events, records final output and Git evidence, supports same-thread follow-up and cancellation, and serves a token-scoped real-time monitor. A routing adapter sends only exact attested local workspaces to desktop Codex and preserves the configured HTTP or GitHub Actions adapter for repository-only work. The scheduled task is running against the canonical Render host; production opened the exact Scoop Fairies workspace and completed the generated demo's implementation and correction jobs. Physical Android voice acceptance is independent and remains pending.

## Phase 3A: From-Scratch Project Operator

Status: implemented and accepted in production. Local workspace creation, attestation, VS Code launch, Codex implementation, same-thread correction, kiosk monitoring, GitHub publication, Cloudflare deployment, and independent checks pass.

`POST /api/marcus/project-bootstrap` and matching Marcus Live intent create a project registry record, reserve an exact pending Windows path, and create an eight-step durable operation covering local folder creation, local Codex implementation, GitHub repository creation, origin connection, publishing, Cloudflare deployment, and verification. Project switching is deterministic and opens the selected verified workspace. GitHub creation defaults to private and refuses to adopt or overwrite a pre-existing repository. External repository creation, publication, and Cloudflare deployment remain separate exact-target approvals.

Production operation `op_EejJJ-WR7eHJCw` created `C:\Users\markg\OneDrive\Documents\Marcus Projects\marcus-pc-bridge-demo`, initialized Git, and built the Worker through local Codex. Mark separately approved private repository creation, publication, and production deployment. GitHub read-back reports private repository `markgromer/marcus-pc-bridge-demo` at commit `2f5ea63018649caa0fccdb190684cefe3675f4a3`. The first deploy failed before Cloudflare ran because Windows rejected direct `npx.cmd` spawning; commit `b3b0aa9` fixed the launcher without rewriting the failed operation. Corrective operation `op_rh-nlu6uWEfZrA` deployed the exact registered workspace and completed after independent HTTP 200 evidence from `https://marcus-pc-bridge-demo.markgromer.workers.dev/` and `/health`.

## Phase 3B: Trusted PC Operator

Status: implemented, deployed, and live with an exact approved `C:\` full-PC policy on desktop agent `Marks_PC`. The scheduled relay loads a non-secret policy from `%APPDATA%/M.A.R.C.U.S/desktop-agent.json`, advertises its exact roots and capabilities, and implements inventory, filename search, directory listing, bounded non-secret text reads, installed-application discovery, and visible open/launch actions. Main chat and Marcus Live use synchronous typed tools, while `GET /api/marcus/pc/capabilities` and `POST /api/marcus/pc/actions` provide deterministic authenticated acceptance routes. The paired mobile `Verify` view can prepare an exact-agent critical drive-root operation through `POST /api/marcus/pc/access-request`; it cannot self-approve the grant. Cache `marcus-mobile-v22` also lists every redacted pending operation approval in risk order so a consequential action cannot hide behind one active-operation selection.

Safety is part of the capability contract. Secret-bearing files are refused. Files, pages, email, and tool results are untrusted and cannot authorize an action. Open/launch tools require Mark's direct current request. Arbitrary commands, deletion, downloads/installs, access changes, credentials, publishing, and external representation are not generic PC tools and retain exact approval paths.

Local unit coverage verifies root containment, prefix-escape rejection, bounded search, directory metadata, secret-file refusal, critical risk classification, no pre-approval queueing, exact-agent binding, persisted policy evidence, required read-back verification, preservation of PC evidence in the desktop action envelope, and the shell-free Windows Wrangler launcher. Server integration verifies the relay declaration and queue/result round trip. The complete suite passes `147/147`; syntax lint passes for 72 JavaScript files. Production reports the relay online with eight capabilities and active `C:\` full-PC scope. Inventory, search, directory listing, bounded non-secret read, and exact approved Cloudflare deployment acceptance pass through the hosted API.

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
- At that implementation snapshot, focused provider tests passed `7/7`, the complete suite passed `132/132`, and JavaScript syntax passed for 66 files.
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
- At that implementation snapshot, the complete local suite passed `98/98` with provider setup, redaction, authorization, no-send verification, approved-send/idempotency checks, deep GitHub audits, and mobile acceptance aggregation.

Production status: Quo and SMTP are both configured, fingerprint-bound, no-send verified, and accepted through exact approved drafts. `GET /api/marcus/acceptance` reports all four messaging gates true: both providers verified and both approved test sends backed by retained provider receipts. The earlier SMTP `550` remains historical failed-closed evidence and is not treated as delivery.

Production provider-onboarding acceptance on 2026-08-12:

- Render served service worker `marcus-mobile-v12` with the provider dialog at the canonical mobile URL.
- A fresh one-time pairing code authenticated a 390 x 844 Chromium session; the dialog opened with zero browser errors or warnings and no local/session storage entries.
- Unauthenticated and Live-session-only provider config requests returned 401; the paired durable-admin context succeeded.
- Config responses expose no API key or SMTP password. A dedicated Quo `os1` credential was later saved and verified without sending.
- Quo verification resolved the Operations line, phone-number id, and user id and retained `sent: false`. Resend SMTP authentication also passed with `sent: false`; neither provider verification path implicitly sends.

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

## Phase 8: External Presence

Status: architecture documented in [[external-presence]]. Existing email primitives support this phase, but the complete autonomous operating loop is not deployed.

Goal: Marcus should have a transparent assistant identity with a real email address, inbound mailbox checking, approval-gated replies, a social/community opportunity radar with copy/paste drafts for Mark, and meeting-note support.

Implemented foundation:

- SMTP outbound drafts and approved sends.
- IMAP mailbox fetch and inbox import.
- Email archive ingestion to Qdrant when configured.
- Exact-draft approval and provider receipt evidence.
- Durable admin requirements for provider configuration and send authority.

Planned implementation:

- Scheduled IMAP watcher.
- Reply-needed classifier for imported email.
- Automatic creation of unsent email reply drafts from inbox items.
- `social_opportunity` artifact type for Skool and future channels.
- Ranked interaction digest with copy/paste options and original-post ideas.
- Manual-post confirmation route before Marcus can claim a community/social comment was posted.
- Zoom transcript/recording ingestion route that produces notes, decisions, and follow-up drafts.
- Calendar connector or forwarded invite parsing for call preparation.

Blocked or explicitly gated:

- Skool or social scraping, auto-posting, auto-commenting, or direct-message automation.
- Undisclosed Marcus social/community profile.
- Zoom live attendance without visible identity and host/participant consent.
- Speaking or chatting in Zoom without Mark's direct request and exact approval.

Acceptance:

- A Marcus mailbox can be configured with IMAP and SMTP.
- IMAP sync imports a new message once and preserves thread identity.
- Marcus drafts a reply from an imported email without sending it.
- Mark approves the exact draft, sends it once, and retry returns the stored receipt.
- Skool/social monitoring produces an opportunity digest with copy/paste options only.
- Zoom transcript ingestion produces notes and follow-up drafts without live attendance claims.
- `GET /api/marcus/operator-health` and `GET /api/marcus/acceptance` distinguish implemented email capability from planned Skool/Zoom capability.

## Phase 7: Production Voice Interface

Decision: use OpenAI Realtime speech-to-speech over WebRTC as the primary mobile conversation layer. Keep ElevenLabs/browser speech as narration fallback and do not build a custom STT/TTS tuning stack.

Implemented slice:

- Server-minted short-lived Realtime client secrets.
- `gpt-realtime-2.1` and `cedar` defaults with environment overrides.
- Official `@openai/agents-realtime` browser session rather than a hand-written Realtime transport.
- Semantic VAD, barge-in, near-field noise reduction, and `gpt-live-transcribe` input transcription.
- Android PWA start/stop voice control.
- Background/phone-lock pause and foreground resume with a fresh ephemeral credential.
- Network and WebRTC disconnect recovery with bounded backoff.
- Scheduled renewal at 55 minutes before the Realtime session limit.
- A single `marcus_operator` bridge back to the durable Live chat and approval flow.
- Voice instructions that make Marcus speak directly as Marcus, allow ordinary conversation/advice without tool dispatch when no durable state is needed, use concise spoken answers, avoid unnecessary spoken machine identifiers, and allow natural variable tone with smart dry humor.
- A persistent active-work strip that follows the same durable operation across refresh and shows current step, step progress, verification counts, blockers, and approval/recovery state through the redacted operation-summary API.
- A server-side monitor that keeps execution-safe durable jobs moving when the phone is closed, without ticking approval-gated, blocked, paused, recovery-required, or terminal operations.
- Selective Realtime announcements for persisted completion, failure, cancellation, approval, blocker, and recovery transitions.
- Project-scoped conversation memory that resolves the current explicit repository before selecting retained requirements, with bounded per-project requirement storage and durable-operation recovery.
- Bounded context replies and direct project-switch regression coverage to prevent stale requirements from entering Codex prompts.
- Authenticated, business-scoped acceptance telemetry with strict field allowlisting and 1,000-event retention.
- Derived gates for speech recognition, assistant audio, interruption, operator completion, network/background recovery, and installed Android context.
- Unit and smoke coverage for session policy, auth, static assets, interruption state, network/background recovery, expired credentials, and stale-connection races.
- Unit coverage for monitor state allowlisting/serialization, summary redaction, tracker persistence/deduplication, terminal handoff, and voice announcements.

Physical acceptance exercises completed for this phase:

- Start and stop a voice session from the installed Android PWA.
- Hold a multi-turn project conversation without reselecting the project.
- Interrupt Marcus while it is speaking and continue naturally.
- Create a project audit/Codex operation by voice.
- Approve a waiting operation by voice and confirm the same durable operation advances.
- Confirm external communication and production mutations still pause for explicit approval.
- Verify recovery after phone lock, network interruption, and an expired Live token.
- Inspect the resulting acceptance session and confirm every physical-device gate is true without relying on transcript storage.

The production paired-admin `Verify` dashboard starts a fresh acceptance session, persists only its ID/start time/coarse platform-display context for up to two hours, shows each voice/provider gate, and enables physical confirmation only after all eight Android voice gates pass in installed standalone mode. Context matching prevents browser-tab evidence from being reused after a standalone launch. `GET /api/marcus/acceptance` combines that evidence with provider, approved-send, Codex, GitHub, Cloudflare, OpenAI, and desktop readiness. Service worker `marcus-mobile-v22` includes fingerprint-preserved provider settings, exact-draft retry after an approved delivery failure, the complete redacted operation-approval queue, and an explicit completed confirmation label. Physical-phone acceptance passes.

Status: the official SDK, recovery, acceptance telemetry, durable-work tracking, PWA install assets, exact-target operation approval, exact-draft messaging, and mobile deployment contract are deployed and accepted. Production cache `marcus-mobile-v22` and all required raster/maskable icons are live. Direct Live-token mutations return 401 and conversational Live-token approval requires durable reauthentication. Both provider credentials and approved sends pass. The accepted installed-Android session proves all eight lifecycle gates plus explicit physical confirmation; combined production acceptance passes 13/13.

Personality mode foundation: implemented locally. `marcus/voice/personality_modes.js` defines Operator, Dry, No-Bullshit, Meeting Shadow, Public Assistant, Demo, and Roast Light prompt fragments. `MARCUS_REALTIME_PERSONALITY_MODE` selects the default server-side Realtime mode, status/client-secret responses expose the normalized mode, Marcus Mobile has a local persisted Voice mode selector, `/obs-marcus.html` provides an OBS/demo sidecar, and the Realtime agent exposes `set_marcus_personality_mode` for spoken mode-switch commands. Active voice reconnects after a mode change so the new prompt takes effect. Remaining work: direct Zoom chat/transcript integration beyond pasted context or browser-supported capture, and production verification. Tests should continue proving that Demo/Roast wording cannot leak into Public Assistant and that no mode changes external communication, publishing, deployment, or approval authority.

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

Current requested from-scratch demo:

- Local project: `C:\Users\markg\OneDrive\Documents\Marcus Projects\marcus-pc-bridge-demo`
- Durable operation: `op_EejJJ-WR7eHJCw`
- Intended private GitHub repository: `https://github.com/markgromer/marcus-pc-bridge-demo`
- Intended Cloudflare Worker: configured by the project's `wrangler.jsonc`
- Current state: local implementation and all independent checks pass; GitHub creation, push, and Worker deployment await separate exact approvals

The following older demo remains a historical result-review and provider-mutation acceptance artifact; it does not satisfy the new from-scratch workflow by itself.

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
- Resend SMTP is configured at `smtp.resend.com` for `Marcus <marcus@gromore.media>`. The domain is verified, but the borrowed key rejected the approved production attempt with SMTP `550`; draft `V8uMUUZjiRz1` remains approved and retryable after a dedicated key is installed.
- The approved email-send gate later passed with provider receipt evidence. The remaining combined acceptance gate is physical installed-Android voice acceptance.

Production false-project correction on 2026-08-12:

- A conversational phrase containing `workflow/operation` had been treated as a raw GitHub repository and registered as project `operation`.
- The parser now requires strong repository syntax or context, and the project resolver excludes archived records.
- Archived records are also excluded from evidence collection, current focus, and bottleneck scoring while remaining historically queryable.
- Bug-created operation `op_MC80S_R81ha7ng` was cancelled before any provider launch. Registry record `registry_Vs8FTyGEW2MgmA` was archived, its active conversation binding and one project-memory entry were removed, and all 80 conversation messages plus Reggie memory were preserved.
- Production read-back showed only `op_wSMm8zWz7DGGiA` and `op_nA9c9c_bZYsMjg` still waiting for approval. The live acceptance report remained 10/12.

Production durable-recovery correction on 2026-08-12:

- Startup recovery had treated an already completed Codex job as active, reset its completed step to running, and added another `no_runnable_step` blocker on every Render restart.
- Recovery now reconciles a completed provider job only when its bound step is not complete. Stable blocked verification work is not rewritten, and blocker/event creation is idempotent.
- The correction passed the complete `132/132` suite, JavaScript syntax lint for 66 files, GitHub CI run `31622049442`, and a Render process replacement.
- Superseded Reggie operations `op_PNlbjXYY-r565w` and `op_BoKja2fbT0v6xQ` were cancelled after confirming both provider jobs were already complete. No GitHub branch or pull request was changed. Reggie PR #19 remains open for separate review; verified PR #16 remains the accepted implementation result.
- A fresh paired 390 x 844 production session then tracked the legitimate Cloudflare approval `op_nA9c9c_bZYsMjg`, showed 10/12 acceptance with messaging at 3/4, retained no browser token, and produced zero console warnings or errors.
