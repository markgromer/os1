# Decision Log

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
