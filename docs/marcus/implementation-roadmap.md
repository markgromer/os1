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

Checks:

- Did the result address the original request?
- Were expected files changed?
- Were tests/build/lint run?
- Is browser verification needed?
- Is deployment or client communication still pending approval?

## Phase 5: External Communication

Marcus can draft text/email actions now. Approval remains mandatory. Provider-specific sending is a later explicit action that must record sent-message evidence before Marcus can say a message was sent.

Flows:

- Draft reply.
- Show recipient, channel, subject/body.
- Ask for approval.
- Send through configured provider as a separate approved provider action.
- Attach sent-message evidence to the project.

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
- Android PWA start/stop voice control.
- Native realtime turn-taking and interruption transport.
- A single `marcus_operator` bridge back to the durable Live chat and approval flow.
- Unit and smoke coverage for session policy, auth, and static assets.

Acceptance tests still required before this phase is complete:

- Start and stop a voice session from the installed Android PWA.
- Hold a multi-turn project conversation without reselecting the project.
- Interrupt Marcus while it is speaking and continue naturally.
- Create a project audit/Codex operation by voice.
- Approve a waiting operation by voice and confirm the same durable operation advances.
- Confirm external communication and production mutations still pause for explicit approval.
- Verify recovery after phone lock, network interruption, and an expired Live token.

Status: the integration is live at `https://task-tracker-5wsa.onrender.com/mobile.html`. A real installed-Android speech, barge-in, and recovery conversation is not yet verified.

Verified locally on 2026-08-12:

- The configured OpenAI account minted a short-lived `gpt-realtime-2.1` client secret.
- A Playwright mobile browser authenticated to Marcus, started the voice control, established the OpenAI WebRTC call with HTTP 201, and reached `Voice on` / `Listening` with no browser warnings or errors.
- This used a synthetic microphone track; it does not replace the installed-Android speech and interruption tests above.

Verified against production on 2026-08-12:

- The durable mobile admin credential authenticated on the canonical Render host.
- The PWA loaded under service worker cache `marcus-mobile-v4`.
- Production minted a short-lived `gpt-realtime-2.1` / `marin` client secret.
- A mobile Chromium session started voice, reached `Voice on` / `Listening`, and established the OpenAI WebRTC call with HTTP 201 and no browser warnings or errors.
- Production operator health reported `direct_codex`, GitHub ready, and Cloudflare ready.
- One-time six-digit mobile pairing is implemented so the Android device does not need the durable admin token.

## Demo Deployment

GitHub demo repo:

- `https://github.com/markgromer/marcus-operator-demo-worker`

Live Cloudflare Worker:

- `https://marcus-operator-demo-worker.markgromer.workers.dev`

The deployed Worker demonstrates the audit and handoff contract. Its `/codex/start` endpoint is intentionally simulated and should not be treated as proof that a real Codex implementation session exists. The real execution proof is the durable Marcus operation and Reggie runner evidence below.

Verified endpoints:

- `/health`
- `/demo`
- `/audit`
- `/codex/start`

Production Codex acceptance evidence:

- Durable operation: `op_N_PUttVpm72mWw`
- Resolved project: `Marcus Operator Demo`
- Audit scope: one repository and three important files
- Codex runner: `https://github.com/markgromer/Reggie/actions/runs/31566699387`
- Review pull request: `https://github.com/markgromer/marcus-operator-demo-worker/pull/3`
- Result: operation completed with build, test, syntax/lint substitute, artifact, and diff-review evidence passed
- Verified branch: five tests passed and `wrangler deploy --dry-run` passed
- The pull request remains unmerged and undeployed, preserving the review and production-approval boundary.
