# Execution Loop

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

Conversation-only project context follows:

`project/repository mention -> resolve and retain project plus requirements -> wait for a positive audit/implementation request`

Mentioning `repo`, `site`, or `Codex` does not itself create work. Explicitly negated audit clauses are excluded from execution classification. A later positive instruction supersedes an older "do not audit/start" control while durable constraints such as "do not deploy" remain in the execution brief.

External communication now follows:

`conversation -> draft email/text -> pending approval -> approved/rejected -> provider send -> sent/failed evidence`

Approval does not mark a message sent. The separate send action claims an approved draft as `sending`, invokes SMTP or Quo, then stores `sent` only after provider acceptance. Repeating a successful send request returns the existing receipt instead of sending twice.

Provider onboarding is separate from external actions:

`paired durable admin -> save redacted server settings -> authenticate without sending -> retain bounded verification evidence`

A short-lived voice/Live token cannot configure or verify providers. Verification never creates, approves, or sends an external action.

Realtime voice follows:

`microphone -> OpenAI Realtime WebRTC -> marcus_operator -> /api/marcus/live/chat -> durable Marcus result -> spoken response`

The voice model handles conversational audio but cannot bypass the normal project operator, durable operation, or approval paths. A spoken follow-up such as "do it" is sent to the same pending-operation approval handler as typed chat.

Voice transport recovery follows:

`background, network loss, connection loss, or 55-minute refresh -> close stale WebRTC session -> mint a new ephemeral credential -> reconnect -> resume listening`

The active project and recent requirements remain server-side during transport recovery; reconnecting does not create a second project conversation or grant new authority.

Project continuity follows:

`explicit project in current turn -> resolve target -> merge durable project memory and matching operation evidence -> select only target-project requirements -> audit/plan/execute against that target`

The current explicit project wins over an older active project. Context-only requests return a bounded requirement summary; they do not echo unrelated conversation history or imply that an audit or Codex session ran. Requirement memory is bounded and survives rolling chat eviction. Matching durable operations provide a migration/recovery source when older conversations predate project memory.

Voice acceptance evidence follows:

`allowlisted browser lifecycle event -> authenticated telemetry batch -> bounded business-scoped event file -> derived acceptance gates`

Transcripts, prompts, replies, credentials, IP addresses, and raw user agents are not part of this path. The resulting gates show what the client observed, while installed-device physical acceptance remains an explicit operator check.

## Execution Brief Contents

Every Codex-bound job should include:

- Project name and registry id.
- Business key.
- User's original request.
- Objective.
- Relevant project memory.
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

## Completion Standard

Marcus should not treat a Codex handoff as completed work.

Completion requires evidence:

- Codex result or diff.
- Test, lint, build, or manual verification.
- Browser evidence when UI is involved.
- Explicit note of any skipped verification.
