# Execution Loop

Status: active

## Continuous Nervous-System Loop

The server now runs a bounded non-overlapping cycle:

`runtime and capability sensors -> normalized signals -> priority pathways -> attention classification -> existing operation/provider capability -> outcome signal -> durable journal -> next cycle`

The first integrated homeostatic pathway advances the existing durable-operation monitor. Operation state transitions return as `operation.status.changed` signals, while the established approval, recovery, and verification contracts remain authoritative. Runtime proprioception records that each business-scoped cycle occurred. The initial implementation is documented in [[nervous-system]].

## Visible Browser Loop

`Mark request or visualizer input -> authenticated browser route -> durable desktop action queue -> local desktop agent -> localhost-only MARCUS Chrome CDP -> compressed viewport relay -> /visualizer.html`

The browser status loop runs independently from the heavier desktop-awareness scan so click and navigation commands are claimed quickly. Browser commands now pass through the executable contracts in [[skill-system]]. The page frame, a successful click, or inserted text is observational evidence, not proof that the requested action completed. The selected skill must verify the intended surface and result. Provider receipts, page read-back, or the existing durable operation verification rules remain required before Marcus claims an external result.

Standalone Skool drafting follows:

`standalone-post intent -> skool.prepare-standalone-post -> community-root navigation -> feed-composer activation -> comment/thread rejection -> exact text insertion -> exact editor read-back -> pending publication approval`

Publishing follows:

`explicit authenticated approval -> exact stored draft -> composer re-preparation -> exact text verification -> approved submit control -> publication result -> durable published/failed state`

Before a project operation is created, Marcus selects a job-priming manifest and checks relevant mission-memory decisions for a locked conflict. A conflict stops operation creation until Mark identifies a permanent change or one-time exception. Completed operations that previously failed or entered recovery may bank the verified winning method.

Realtime voice receives a bounded continuity brief at session creation. Durable operator results pass through a speech-safe projection before the Realtime model sees them, retaining approval, blocker, and uncertainty signals while omitting machine identifiers. See [[operator-intelligence-layer]].

## Desired Flow

1. Mark talks to Marcus about a project.
2. Marcus resolves the project.
3. Marcus builds an execution brief.
4. Marcus audits the relevant system state.
5. Marcus writes a Codex prompt.
6. Marcus asks for approval when needed.
7. Marcus starts Codex or creates a handoff.
8. Marcus tracks the operation.
9. Marcus verifies the result.
10. Marcus reports what changed, what was verified, and what still needs a decision.

## Current Implemented Slice

The first working slice is:

`conversation -> active-project memory -> project resolution -> context audit -> Codex prompt -> durable operation -> direct Codex or external handoff`

This is implemented without relying on a model call for the core control flow. Explicit GitHub `owner/repository` references can be added to the project registry, and short mobile follow-ups reuse the active project plus recent requirements. Direct Codex launch remains adapter-dependent.

Durable mission memory follows:

`explicit remember/mission/preference instruction -> deterministic classification -> credential rejection -> business-scoped atomic store -> relevant retrieval -> normal conversation and Codex execution brief`

Marcus does not infer new standing instructions from ordinary conversation. Repeating the same explicit memory reconfirms the existing record instead of duplicating it. Administrative updates can supersede or archive a record; restart recovery uses the sibling backup and preserves corrupt input for inspection.

Conversation-only project context follows:

`project/repository mention -> resolve and retain project plus requirements -> wait for a positive audit/implementation request`

Mentioning `repo`, `site`, or `Codex` does not itself create work. Explicitly negated audit clauses are excluded from execution classification. A later positive instruction supersedes an older "do not audit/start" control while durable constraints such as "do not deploy" remain in the execution brief.

External communication now follows:

`conversation -> draft email/text -> pending approval -> approved/rejected -> provider send -> sent/failed evidence`

Approval does not mark a message sent. The separate send action claims an approved draft as `sending`, invokes SMTP or Quo, then stores `sent` only after provider acceptance. Repeating a successful send request returns the existing receipt instead of sending twice.

Inbound external presence follows the stricter pattern documented in [[external-presence]]:

`email/social/zoom source -> normalized evidence -> opportunity summary or draft -> exact approval when outbound provider action exists -> provider/manual send or post confirmation`

Email can use the existing IMAP sync and external-action draft/send path. Skool and future social/community channels use a watch-and-draft opportunity radar: Marcus surfaces where Mark can interact, explains why, and prepares copy/paste options while Mark remains the actor who posts. Zoom starts with transcript/recording ingestion after consent; live attendance requires a visible assistant identity and host/participant consent before implementation.

Provider onboarding is separate from external actions:

`paired durable admin -> save redacted server settings -> authenticate without sending -> retain bounded verification evidence`

A short-lived voice/Live token cannot configure or verify providers. Verification never creates, approves, or sends an external action.

GitHub and Cloudflare mutations follow:

`inspect provider -> resolve exact registered project -> create durable provider action -> freeze immutable target -> wait for action-specific approval -> re-read and refuse drift -> mutate once -> authoritative read-back -> provider evidence -> complete`

The model cannot manufacture request authority: provider preparation requires the authenticated original request and a high-confidence project binding. GitHub merges are pinned to the expected PR head SHA. DNS changes are pinned to the account, zone, record identity, and expected state. Worker deployment is pinned to the account, script, version, and current deployment id. An idempotent already-in-state result is recorded without a duplicate mutation. Unknown post-mutation state enters recovery and is never automatically retried.

Realtime voice follows:

`microphone -> OpenAI Realtime WebRTC -> marcus_operator -> /api/marcus/live/chat -> durable Marcus result -> spoken response`

The voice model handles conversational audio but cannot bypass the normal project operator, durable operation, or approval paths. A spoken follow-up such as "do it" is sent to the same pending-operation approval handler as typed chat.

Durable mobile follow-through now follows:

`chat or voice creates operation -> server monitor advances execution-safe states -> safe operation summary -> active-work strip -> one message per persisted material transition -> selective spoken terminal/approval/blocker announcement`

The phone does not tick operations or read full operation records. Closing or locking the phone does not stop the server monitor. Waiting approvals, blocked operations, recovery-required work, and terminal states are never advanced automatically by that monitor.

Startup recovery preserves the workflow's next real boundary. A completed Codex step does not trigger final-verification blocking while repository creation, publication, deployment, or another non-verification step remains unfinished. A pending exact approval restores `waiting_for_approval`; stale verification classification is resolved without executing the action.

Persistent PC scope changes follow:

`paired mobile Verify -> inspect live desktop manifest -> derive every fixed-drive root -> create critical durable operation -> typed strong confirmation -> queue exact-agent policy update -> persist local config -> same-agent read-back -> required verification -> completed`

Preparing this operation does not change local access. The desktop agent rejects an unbound policy action, and Marcus completes the operation only when runtime scope, persisted scope, exact roots, and credential-content blocking all match. Shell execution, installs, deletion, publishing, messages, and account changes remain outside this grant.

Voice transport recovery follows:

`background, network loss, connection loss, or 55-minute refresh -> close stale WebRTC session -> mint a new ephemeral credential -> reconnect -> resume listening`

The active project and recent requirements remain server-side during transport recovery; reconnecting does not create a second project conversation or grant new authority.

Project continuity follows:

1. Synchronize registry records into the business-scoped awareness store.
2. Discover exact recent Codex workspaces additively without approving their local paths.
3. Resolve active projects normally and explicitly named archived/completed projects historically.
4. Load indexed `marcus.txt`, matching project-note, README/package, and repository-manifest context when a stable awareness id is supplied.
5. Keep archived, dormant, completed, and active-attention lifecycle separate from whether project knowledge remains indexed.
6. Send execution requests through the existing project operator and durable operation engine; awareness never grants execution authority.

`explicit project in current turn -> resolve target -> merge durable project memory and matching operation evidence -> select only target-project requirements -> audit/plan/execute against that target`

The current explicit project wins over an older active project. Context-only requests return a bounded requirement summary; they do not echo unrelated conversation history or imply that an audit or Codex session ran. Requirement memory is bounded and survives rolling chat eviction. Matching durable operations provide a migration/recovery source when older conversations predate project memory.

Voice acceptance evidence follows:

`fresh installed-phone session -> allowlisted browser lifecycle events -> authenticated telemetry batch -> bounded business-scoped event file -> every derived gate passes -> Mark confirms on that phone -> physical acceptance evidence`

Transcripts, prompts, replies, credentials, IP addresses, and raw user agents are not part of this path. Android standalone context alone is not accepted as physical proof. Confirmation is enabled only after every voice gate passes and is stored as a boolean event without a note or conversation content.

Repository audit and Codex handoff follow:

`resolve project -> discover named/related GitHub repositories -> index recursive trees -> rank files against the complete request -> read redacted source/config/test evidence -> record coverage and failures -> compose 30,000-character execution brief -> create durable operation -> direct Codex or external handoff`

Codex treats the brief as preflight evidence and must reopen relevant files, callers, dependents, and tests. Related repositories remain explicit scope; the runner may not silently reduce a multi-repository request to the primary checkout.

Direct GitHub Actions result review follows:

`runner reports success -> query target repository through GitHub API -> resolve PR/branch/head SHA -> collect bounded redacted patches and target checks -> calculate evidence digest -> strict semantic review of implementation criteria + deterministic proof of Marcus control criteria -> retain independent build/test/browser/deployment gates`

Local visible Codex execution follows:

`phone request -> exact project resolution -> approved and attested Windows workspace -> durable Codex job -> desktop action claim -> visible Chrome kiosk monitor -> codex exec JSONL stream -> redacted job events -> changed-file and diff summary -> operation verification`

An independent review failure can call `POST /api/codex/jobs/:jobId/followup` with an authenticated, bounded correction brief. The desktop relay resumes the same Codex thread, retains prior job evidence, rotates the monitor capability, reopens the kiosk monitor on Mark's PC, and updates the same durable job instead of creating an unrelated session.

A command such as `switch to Scoop Fairies` updates the active conversation project and opens that project's verified workspace in VS Code. It does not reuse an unrelated active Codex project. A from-scratch request follows:

`extract safe project name -> reserve exact pending workspace -> create folder -> git init -> open VS Code -> local Codex build -> exact GitHub repository approval -> connect origin -> publish approval -> Cloudflare deployment approval -> artifact verification`

GitHub repository creation defaults to private unless Mark explicitly asks for public. Repository creation and Cloudflare publication are separate approval gates; completing the local build does not imply either one.

The runner's success proves only that the runner completed. Codex output and provider-supplied review claims remain untrusted. Marcus constructs a validated evidence catalog from authoritative changed-file patches, successful GitHub checks, authenticated verification, durable audit/handoff records, implementation artifacts, and PR state. A strict JSON schema forces the semantic reviewer to return one citation-bearing entry for every supplied implementation criterion. Marcus itself proves generated audit, handoff, and completion-control criteria from durable records; prior `diff_review` results are excluded so the review cannot prove itself. `diff_review` passes only when all changed files and patches are present, the digest matches the stored diff, every criterion is covered, confidence is at least 0.8, and no high/blocker finding, unsupported execution claim, or failed/pending target check exists. Otherwise the operation remains blocked for stronger evidence. Retrying verification invalidates the short evidence cache, re-queries GitHub, and re-runs review without launching Codex again, so settled evidence can advance without duplicate implementation.

## Execution Brief Contents

Every Codex-bound job should include:

- Project name and registry id.
- Business key.
- User's original request.
- Objective.
- Relevant project memory.
- Relevant durable mission, standing-instruction, and preference memory.
- Current repo and deployment metadata.
- Local workspace path, if trusted.
- Current architecture notes.
- Files or areas likely worth inspecting.
- Constraints.
- Approval boundaries.
- Acceptance criteria.
- Verification commands.
- Expected deliverables.

## Audit Before Prompting

Marcus should not send weak prompts to Codex.

Before launching Codex, Marcus should inspect:

- Project registry record.
- GitHub repository metadata.
- Relevant repo files when available.
- Cloudflare or Render deployment metadata.
- Existing durable operations for the same project.
- Project evidence.
- Recent inbox or client context.
- Desktop workspace context, if available.

The implemented GitHub audit records recursive tree statistics, head/recent commit state, open pull requests, request-ranked file excerpts, failed checks, API-call count, and elapsed time. Obvious secret paths are excluded before retrieval and retained text is redacted before it enters an operation.

## Completion Standard

Marcus should not treat a Codex handoff as completed work.

Completion requires evidence:

- Authoritative result/diff provenance, not only a Codex completion claim.
- Independent criterion-by-criterion diff review or explicit human review.
- Test, lint, build, or manual verification.
- Browser evidence when UI is involved.
- Explicit note of any skipped verification.
- Authoritative provider read-back for every GitHub or Cloudflare mutation.

Production Reggie acceptance on 2026-08-12 exercised this complete loop:

`explicit markgromer/Reggie request -> 180-path/10-file audit -> 18,731-character prompt -> Reggie GitHub Actions Codex runner -> PR #16 at exact head -> authoritative patches and passing GitHub check -> independent criterion review -> isolated npm run check and diff check -> authenticated evidence -> completed durable operation`

The provider run lasted almost three minutes before opening the pull request. Marcus then remained blocked until the independent and authenticated verification evidence passed. PR #16 remains open and unmerged, so completion of the implementation operation did not imply merge or deployment authority.

Production one-instruction demo acceptance exercised the revised conversation path:

`explicit install + start Codex request -> 2,493 ms six-file audit -> zero medium approvals -> one Reggie runner job -> PR #5 -> blocked on independent checks -> reviewer finds approval-boundary defect -> exact-head correction -> refreshed diff review -> 3/3 tests + Wrangler dry-run + syntax/diff checks -> completed operation`

Operation `op_-qcwlO85nndNkw` completed at PR head `4b49e5cd580b238402b07ff776cb82899206f34c`. PR #5 remains open and unmerged, `/operator-controls` remains HTTP 404 in production, and Cloudflare deployment `d8eb7206-6d65-434b-aaab-04cd51f62823` remains on the prior Worker version. Starting Codex did not imply merge or deployment authority.

Production Quo onboarding followed the separate provider path:

`paired admin -> save redacted os1 credential -> query Quo phone numbers -> normalize the canonical E.164 sender -> persist resolved sender ids -> mark provider verified with sent=false`

Production Quo acceptance action `rv1v4_RKB38v` passed the full approval and provider-receipt path on 2026-08-12. SMTP setup and no-send verification pass; the acceptance email send still requires explicit operator authority.

Production provider-operation preparation on 2026-08-12 exercised the new boundary without mutating customer state:

`live PR/Worker inspection -> exact Marcus Operator Demo registry binding -> GitHub merge operation op_wSMm8zWz7DGGiA -> Cloudflare deployment operation op_nA9c9c_bZYsMjg -> both waiting_for_approval -> authoritative re-inspection confirmed unchanged PR and deployment`

The complete local suite passed `128/128`; GitHub CI passed for implementation commit `0409400` and Cloudflare version-shape correction `17769b0`. Live execution of either prepared provider action remains an explicit operator decision.
