# Decision Log

## 2026-08-12: Codex Completion Claims Do Not Verify Codex Work

Context: The Reggie runner could complete, push a branch, and open a pull request, but the GitHub Actions adapter returned no artifacts and an empty diff. Marcus therefore had no independent target-repository evidence and could only wait for manually registered verification.

Decision: After runner completion, query GitHub directly for the target branch, pull request, head commit, changed files, bounded patches, check runs, and commit statuses. Bind that evidence to a separate AI review with a SHA-256 digest. Quarantine provider-supplied review artifacts, treat repository patches as untrusted model input, and permit the independent review to satisfy only `diff_review` when every acceptance criterion is visibly met. Keep all runtime, browser, deployment, merge, communication, and production gates independent.

Consequence: A successful runner no longer stands in for a successful implementation. Missing/truncated evidence, failed or pending target checks, malformed review output, incomplete criterion coverage, and digest mismatches fail closed. The target collector passed read-only against real demo PR #3 before deployment, and operation `op_NfHu37cdF1aSjQ` later passed the full production acceptance against PR #4.

Production correction: The first live review of demo PR #4 exposed a false-positive model response: it marked criteria met with blank evidence and claimed tests passed without target test evidence. The operation remained blocked on independent build/test/lint gates. Marcus now supplies a validated evidence catalog, requires every `met` criterion to reference one or more exact catalog IDs, rejects invented/blank references, and rejects execution claims that lack successful target checks or authenticated verification evidence.

## 2026-08-12: Mission Memory Is Durable Data, Not A Prompt Constant

Context: Marcus retained recent conversation and project requirements, but his overall mission, Mark's standing instructions, and cross-project preferences existed only in code, documentation, or the current conversation. That did not prove durable recall across chat eviction, process restart, business switching, or a later Codex handoff.

Decision: Add a business-scoped mission-memory store with typed records, provenance, priority, lifecycle status, atomic persistence, sibling backups, and corruption preservation. Seed the mission Mark already supplied. Accept new memory only through explicit operator commands or durable-admin routes, reject credential-like content, and retrieve relevant active records into both conversation prompts and project/Codex execution briefs.

Consequence: Marcus can explain what he durably remembers and carry those instructions into later work without relying on model inference or rolling history. Memory does not grant execution authority, does not bypass approval, and can be superseded or archived with an audit trail instead of silently disappearing.

## 2026-08-12: Codex Launch Requires Request-Ranked Repository Evidence

Context: The original project operator probed ten hard-coded filenames and collapsed each readable file to a 260-character single-line preview. It could answer immediately without seeing nested request-relevant source, repository topology, current commits, or open pull requests, and the real direct-Codex handoff truncated the brief at 12,000 characters.

Decision: Discover request-related repositories, index each recursive Git tree, rank safe text files against the complete request, read bounded redacted excerpts, and record coverage, failures, API calls, and elapsed time. Increase the retained architecture evidence to 30,000 characters and require Codex to reopen the relevant files, callers, dependents, and tests. Preserve named related repositories as explicit scope.

Consequence: Marcus can demonstrate what it inspected before asking for approval or starting Codex. Missing or truncated GitHub evidence is reported as partial instead of being hidden, obvious secret paths are never fetched, and a slow or broken repository is bounded by per-call deadlines and a small fallback probe.

## 2026-08-12: Physical Voice Acceptance Requires Derived Gates And Same-Session Confirmation

Context: Browser telemetry can prove protocol events and an Android standalone user agent can be emulated. Neither independently proves that Mark completed the workflow on his installed physical phone.

Decision: Add a fresh-session mobile `Verify` flow. Enable confirmation only after all eight voice gates pass in installed Android standalone context, accept only a boolean `physical_review_confirmed` event, and define `acceptedOnPhysicalDevice` as the conjunction of those facts. Combine voice, provider, approved-send, and operator gates in one authenticated acceptance report.

Consequence: Marcus has a concrete phone checklist and durable completion evidence without persisting transcript or note content. Synthetic browser runs remain useful but cannot close the physical-device gate.

## 2026-08-12: Provider Setup Lives Behind Durable Mobile Admin Auth

Context: Real text and email acceptance remained blocked because Quo and SMTP credentials were absent, and asking Mark to paste secrets into chat would expose them to conversation history.

Decision: Add an `Integrations` dialog to the paired mobile client and admin-only provider configuration/verification routes. Keep secrets write-only, return masked hints, reject Live-session-only access, verify Quo sender identity or SMTP transport without sending, retain only bounded timestamped evidence, and invalidate that evidence after a settings change.

Consequence: Mark can finish provider setup directly on Marcus Mobile without sharing credentials in chat. Sending remains a separate durable draft, explicit approval, and provider action; provider verification cannot send a message.

## 2026-08-12: Final Voice Transcripts Do Not Reopen Playback

Context: A one-shot production Reggie continuity conversation streamed assistant audio and emitted `response.output_audio.done`, then emitted its final output transcript. Treating both transcript deltas and the final transcript as playback-start signals put the UI back into `Speaking` after playback had ended.

Decision: Use output-audio transcript deltas to infer WebRTC playback start, use `response.output_audio.done` to end it, and treat the final transcript event as text-only. Reproduce that exact ordering in the browser client regression.

Consequence: Marcus returns to `Listening` after WebRTC playback instead of remaining stuck on `Speaking`, while assistant transcript callbacks and redacted transcript-length telemetry remain intact.

## 2026-08-12: Live Memory Is Project-Scoped Before Execution

Context: A deterministic production speech test correctly reached OpenAI Realtime and the Marcus operator, but the answer replayed stale requirements from another project because Live chat assembled recent history before resolving the explicitly named Reggie repository.

Decision: Store project metadata on both turns of every exchange, resolve an explicitly named project before assembling the operator request, select only matching user requirements, and generate a short requirement summary for context-only replies. Keep up to 12 deduplicated requirements for each of 40 projects. Preserve compatibility with older exchanges by pairing a user turn with its assistant turn's project metadata and by reconstructing missing requirements from matching durable operation requests. Exclude read-only and acceptance commands from requirement memory.

Consequence: A single spoken or typed command can switch projects and immediately request an audit or Codex handoff without contaminating the prompt with the formerly active project. Requirement recall no longer depends on the 80-turn rolling transcript. Regression coverage switches Reggie to Atlas and back, verifies zero work for an explicit read-only request, verifies a direct Reggie audit contains no Atlas requirement, erases chat and project memory, and then recovers Reggie's saved requirement from its durable operation without creating new work.

## 2026-08-12: Voice Acceptance Uses Redacted Durable Telemetry

Context: Production browser signaling tests and screenshots could show UI state, but they could not durably prove recognized speech, assistant audio, interruption, operator completion, or recovery on an installed Android PWA. Retaining transcripts or credentials for acceptance would create unnecessary privacy and security exposure.

Decision: Record only allowlisted voice lifecycle events and bounded metadata through an authenticated endpoint. Convert user and assistant transcripts to character counts, keep the client queue in memory, cap server retention at 1,000 events per business, and derive explicit acceptance gates. Treat Android standalone context as eligibility for physical review rather than proof of a physical device.

Consequence: One installed-phone run can produce durable, inspectable evidence without storing conversation content or credentials. Screenshots are supporting evidence instead of the acceptance record, while physical-device confirmation remains honest and separate.

## 2026-08-12: Project Conversation Does Not Imply Execution

Context: A production read-only Reggie continuity check mentioned a repository, auditing, and Codex while explicitly saying not to audit or start Codex. The broad keyword classifier resolved Reggie and inspected two repositories and six files before creating approval-gated operation `op_-3JoirxhsOnaLg`. No provider job started, and the operation was cancelled, but creating it violated the user's instruction.

Decision: Require a positive project-work action instead of treating `repo`, `site`, or `Codex` as execution intent. Remove explicitly negated audit clauses before durable-work classification. Preserve requirements across turns, but let the newest execution instruction supersede old "do not audit/start" controls without removing durable restrictions such as "do not deploy." Support audit/prompt preparation with `autoStart: false` when the current turn explicitly defers starting Codex.

Consequence: Marcus can discuss and retain project requirements without immediately auditing or launching work. Regression coverage proves a read-only Reggie turn creates zero operations, a later positive turn uses the retained API-token/slug requirements, and prepare-without-start creates no provider job. Production then repeated both stages: zero operations for the read-only turn, followed by a one-repository/three-file audit and 5,230-character prompt with zero provider jobs.

## 2026-08-12: Mobile Pairing Survives Render Process Replacement

Context: A production browser acceptance minted a valid pairing code while Render was replacing the application process. The code existed only in the old process's memory, so a pairing request routed to the replacement process returned `Invalid or expired pairing code`.

Decision: Persist only the HMAC hash and expiration in `data/mobile-pairing.json`. Serialize creation and consumption with an exclusive cross-process lock, delete the record after the first successful use, and cover mint -> process stop -> process start -> consume -> replay with an acceptance test.

Consequence: Deploys and process restarts no longer invalidate an otherwise valid mobile pairing challenge. Production accepted a pre-deploy code after process replacement and rejected replay on 2026-08-12. The six-digit code and durable admin credential are still not persisted together or returned to the browser.

## 2026-08-11: Marcus Voice Uses The Official Realtime SDK And Recoverable Sessions

Context: The first mobile voice client hand-built a raw WebRTC connection and treated a disconnect as a permanent stop. Phone lock, backgrounding, network changes, ephemeral-token expiry, and the Realtime session limit could leave voice unavailable or race a stale setup against a newer connection.

Decision: Use `@openai/agents-realtime` for the browser session, retain the single `marcus_operator` authority bridge, pause media while hidden, reconnect with bounded backoff and a new ephemeral credential, and renew at 55 minutes. Use semantic VAD with interruption enabled and live input transcription.

Consequence: Marcus keeps one durable server-side conversation while the disposable audio transport can recover. Local tests cover interruption, background/network recovery, expired credentials, and stale-connection races; deployed and hands-on Android acceptance remain separate gates.

## 2026-08-11: Mobile Conversation State Owns The Active Project

Context: Marcus answered each mobile message independently. A request could name `markgromer/Reggie`, then a follow-up such as "check the repo" would lose that context and fall back to the only previously registered demo project.

Decision: Persist a bounded Marcus Mobile transcript and active project on the server. Resolve explicit GitHub `owner/repository` references into the durable project registry and include recent user requirements when preparing the audit and Codex operation.

Consequence: Short follow-ups reuse the correct project and earlier requirements. Explicit repositories no longer depend on a manually pre-populated registry entry, while the durable operation still owns execution and verification.

## 2026-08-11: External Communication Uses A Provider-Receipt State Machine

Context: The external-action ledger supported drafts and approvals but could not execute a provider send. The previous health field also conflated inbound webhook authentication with outbound text capability.

Decision: Add a separate approved send action with `pending_approval -> approved -> sending -> sent/failed` states. Use SMTP for email and Quo for text, store provider receipts, and make successful replay idempotent. Report inbound webhook and outbound text capabilities separately.

Consequence: Marcus can send only after explicit approval and only claims `sent` after provider acceptance. Production remains blocked until real SMTP and Quo credentials pass live acceptance.

## 2026-08-11: Desktop Relay Credentials Stay Out Of Process Arguments

Context: The Windows startup task targeted a retired Render host, exposed the admin credential in its arguments, overlapped long polling cycles, and could exit on a transient process-spawn error.

Decision: Point the task at the canonical Render service, read the token from the protected Marcus application-data file, serialize polling, catch synchronous spawn failures, and configure task restart behavior.

Consequence: The desktop relay resumes at login without putting the token in the task or process command line and tolerates transient local command failures.

## 2026-08-12: Android Uses One-Time Pairing Instead Of A Copied Admin Secret

Context: The original mobile login required the same durable admin token used by the server operator, which was awkward on Android and caused stale-token unauthorized failures.

Decision: Add a single-use six-digit pairing code with a ten-minute lifetime, keyed server-side storage, failed-attempt throttling, and secure HttpOnly cookie issuance.

Consequence: Mark can pair the Android PWA without exposing or retaining the durable admin token. Existing cookie authentication and Live session tokens continue to protect API and Realtime requests.

## 2026-08-12: Production Uses Dedicated Cloudflare And Reggie Operator Credentials

Context: The local Wrangler OAuth token could manage developer resources but could not read DNS through Marcus's server API, and Render lacked GitHub, Cloudflare, and direct Codex variables.

Decision: Create the account-owned Cloudflare token `Marcus Production Operator` with Developer Services, DNS write, and zone read; store it only in Render. Configure production GitHub access and the Reggie GitHub Actions Codex adapter through Render secrets and source-controlled non-secret defaults.

Consequence: Production Marcus can read GitHub repositories, read Cloudflare zones and DNS, and start real Codex jobs. Billing, membership, token administration, merges, deployment, DNS mutation, and external communication remain outside automatic authority or behind approval.

## 2026-08-12: Direct Codex Is Accepted Only With Durable Verification

Context: A configured adapter is not proof that execution works. The initial acceptance dispatch failed in `actions/checkout@v5` because Node did not use the runner's system CA store.

Decision: Require a demo operation to survive dispatch failure, retry the same durable step, complete Reggie's `openai/codex-action`, open a review pull request, and pass independent build, test, syntax, artifact, and diff review evidence. Add `NODE_OPTIONS=--use-system-ca` to the Reggie runner and exclude the internal Codex report from generated pull requests.

Consequence: Operation `op_N_PUttVpm72mWw` completed with a real runner URL and PR rather than a handoff claim. Future runner jobs inherit the certificate and artifact fixes.

## 2026-08-12: Codex Result Review Separates Semantic Judgment From Durable Proof

Context: The first independent production reviewer described the demo change correctly but returned blank evidence for every acceptance criterion and claimed tests passed without target test-check evidence. Independent verification gates prevented operation completion. A stricter prompt then produced valid JSON but omitted the criterion array twice, correctly leaving the operation blocked and showing that free-form model compliance was not a reliable control boundary.

Decision: Use OpenAI strict structured output for semantic code-review coverage. Limit model evidence references to exact ids from a validated catalog. Prove Marcus-generated audit, handoff, and completion-control criteria deterministically from durable audit metadata, launched job identity, GitHub artifacts, and authenticated verification evidence. Exclude prior `diff_review` evidence to prevent circular proof. Keep build, test, lint, browser, deployment, merge, and communication verification independent from the AI review.

Consequence: Production operation `op_NfHu37cdF1aSjQ` completed only after PR #4's two patches, exact head and digest, five passing tests, Wrangler dry-run, syntax check, and open/unmerged plus live-404 evidence all agreed. Evidence refresh did not relaunch Codex, and the live Cloudflare Worker was not changed.

## 2026-08-12: OpenAI Realtime Is Marcus's Primary Voice Layer

Context: Marcus already has OpenAI credentials, a mobile PWA, durable project operations, and approval-aware tools. The previous voice path combined recorded audio, transcription, text chat, and separate speech synthesis, which could not provide fluid turn-taking without ongoing voice-pipeline tuning.

Decision: Use OpenAI Realtime speech-to-speech over WebRTC with `gpt-realtime-2.1` as the primary mobile voice layer. Expose only a `marcus_operator` function that routes substantive speech through Marcus's existing Live chat. Keep ElevenLabs/browser speech as fallback narration rather than a separate assistant brain. Do not add LiveKit unless direct WebRTC later fails a demonstrated transport requirement.

Consequence: Marcus gets native low-latency conversation and interruption while project memory, audits, Codex execution, credentials, approvals, and verification remain in the existing server. The standard OpenAI API key stays server-side; the mobile client receives only a short-lived Realtime client secret.

## 2026-08-12: Marcus Project Operator Is Durable-Operation First

Context: Marcus needs to turn project conversations into audited Codex execution instead of loose prompt drafting.

Decision: Route project/audit/Codex requests through `ProjectOperatorService`, resolve the project through the durable operations engine, gather context, compose a Codex prompt, and persist a durable operation.

Consequence: Marcus can track work, blockers, artifacts, and verification instead of treating a prompt as completed work.

## 2026-08-12: Direct Codex And Handoff Are Separate Modes

Context: The current environment does not expose a callable direct Codex session-launch service, but the operations engine already supports a direct adapter interface.

Decision: Keep default behavior in `codex_handoff` mode and add `HttpCodexAdapter` for direct mode when `MARCUS_CODEX_ADAPTER_URL` or `CODEX_ADAPTER_URL` is configured.

Consequence: Marcus can start direct Codex jobs when a real adapter endpoint exists, while operator health remains honest when it does not.

## 2026-08-12: External Communication Requires Explicit Approval

Context: Marcus should help draft texts and emails, but customer-facing communication is a high-trust external action.

Decision: Store email/text drafts as external actions with `pending_approval`, then allow explicit approve/reject transitions. Approval does not equal sent.

Consequence: Marcus can help prepare communication without silently sending or falsely reporting delivery.

## 2026-08-12: Marcus Docs Use An Obsidian-Compatible Vault

Context: Marcus needs durable documentation that future Codex sessions can maintain.

Decision: Treat `docs/marcus/` as the Marcus Obsidian-compatible vault and install a reusable local `obsidian-docs` skill under `~/.codex/skills`.

Consequence: Future documentation work has a standard page set, wiki-link style, and verification rules.

## 2026-08-12: Borrow Reggie's GitHub Actions Codex Runner Pattern

Context: Reggie already starts Codex work through a central GitHub Actions runner using `repository_dispatch` and `openai/codex-action@v1`.

Decision: Add `GitHubActionsCodexAdapter` and `.github/workflows/marcus-codex-runner.yml` so Marcus can use the same launch pattern when `MARCUS_CODEX_GITHUB_ACTIONS_ENABLED=true`. Default the runner repo to `markgromer/Reggie` so Marcus can use Reggie's existing runner secrets.

Consequence: Marcus no longer requires a custom HTTP Codex service to enter direct mode. The runner still depends on Reggie's GitHub/OpenAI workflow secrets remaining configured.

## 2026-08-12: Provider Verification Uses Canonical Sender Data Without Sending

Context: The live Quo phone-number response included both a display-formatted number and a canonical E.164 number. Marcus preferred the display value, then rejected it as invalid even though the dedicated `os1` credential and Operations line were valid.

Decision: Prefer Quo's canonical number, then the explicitly configured sender, and normalize display formatting only as a fallback. Model this response shape in the provider regression fixture. Keep provider verification separate from draft approval and send.

Consequence: Production Quo verification now resolves the Operations line and required ids with `sent: false`. The full suite passes `114/114`; no real message is sent until the exact pending draft is explicitly approved and sent.

## 2026-08-12: Reggie Acceptance Must Include Real Audit Time And Exact-Head Review

Context: Earlier mobile replies were immediate, lost the explicit Reggie context, and described work without inspecting repositories or waiting for Codex. That behavior did not satisfy Marcus's operator mission.

Decision: Exercise the exact `markgromer/Reggie` Sweep and Go request through production. Require repository indexing and file reads before prompt creation, a real Reggie GitHub Actions Codex run, authoritative PR evidence, independent criterion coverage, and isolated exact-head verification before completion.

Consequence: Operation `op_f6XKmXTWILGvpQ` audited 180 paths and 10 files, dispatched a real Codex job, opened PR #16, and completed only after GitHub and authenticated evidence passed. The PR remains open and unmerged, preserving separate merge and deployment authority.

## 2026-08-12: Durable Jobs Continue Independently Of The Mobile Conversation

Context: Marcus Mobile displayed only the first `/api/marcus/live/chat` reply. It discarded the returned operation id, did not show later Codex or verification state, and provider jobs advanced only when another request explicitly ticked the operation runner. This recreated the appearance of instant shallow answers even when durable work existed.

Decision: Add an allowlisted server operation monitor for execution-safe states and a read-only mobile tracker over the redacted operation-summary API. Persist only the active operation id and status signature on the phone. Emit one conversation update per material persisted transition, and speak only terminal, approval, blocker, or recovery transitions when Realtime voice is active.

Consequence: Codex polling and verification no longer depend on the phone remaining open. Marcus Mobile can show honest progress and verified completion without gaining prompt, artifact, provider, credential, or execution access. Approval and recovery boundaries remain fail-closed.
