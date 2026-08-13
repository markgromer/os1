# Access Model

## Desired Access

Marcus should have full operational read access to:

- GitHub repositories, branches, pull requests, workflow status, commits, and key repo files.
- Cloudflare zones, DNS records, Pages deployments, Workers deployments, and account-level project metadata.
- Render services and deploy history when relevant.
- Local trusted workspaces through the desktop agent.
- Project memory, tasks, inbox items, notes, and communication history.

Marcus may prepare but should require approval for:

- Sending texts.
- Sending emails.
- Posting to Slack.
- Publishing, deploying, or changing DNS.
- Merging pull requests.
- Pushing commits to protected or production branches.
- Billing or invoice actions.
- Any customer-facing communication.

## Mark's Full-PC Authorization

Status: implemented locally; production relay restart and physical acceptance are pending.

Mark explicitly authorizes Marcus to use his PC and the files available to his Windows account for project discovery, context gathering, local application work, and launching visible Codex jobs. The desktop relay advertises that authorization explicitly; the server does not infer it from a broad filesystem path.

The grant is configured with:

- `MARCUS_ALLOW_BROAD_WORKSPACE_ROOTS=true`
- `MARCUS_ALLOWED_WORKSPACE_ROOTS` for the exact Windows roots Marcus may inspect
- `MARCUS_NEW_PROJECT_ROOT` for from-scratch project creation
- `MARCUS_CODEX_MONITOR_MODE=kiosk` for visible local execution

Broad discovery does not make every command or external mutation implicit. Local Codex is still launched in `workspace-write` mode against one exact project path. The desktop action allowlist, project binding, durable operation state, and provider approval policy remain authoritative. Deleting data, exposing or changing credentials, sending messages, publishing Git changes, creating external repositories, deploying Cloudflare resources, changing DNS, and other consequential external actions require an exact reviewed action or a separately recorded standing policy.

## Current Code Support

The server already has environment-driven provider config for:

- `GITHUB_TOKEN`
- `GITHUB_OWNER`
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_DEFAULT_ZONE_ID`
- `RENDER_API_KEY`
- `OPENAI_API_KEY`
- `MARCUS_REALTIME_MODEL` and `MARCUS_REALTIME_VOICE` as non-secret voice configuration overrides
- Email IMAP/SMTP settings
- Slack credentials
- Twilio/Quo webhook verification
- Quo outbound API settings: `QUO_API_KEY`, `QUO_DEFAULT_PHONE_NUMBER_ID`, `QUO_FROM_NUMBER`, and `QUO_USER_ID`

Local/saved settings are also supported for operator provider access:

- `githubToken`
- `githubOwner`
- `cloudflareApiToken`
- `cloudflareAccountId`
- `cloudflareDefaultZoneId`
- `renderApiKey`
- `quoApiKey`, `quoDefaultPhoneNumberId`, `quoFromNumber`, and `quoUserId`
- `smtpHost`, `smtpPort`, `smtpSecure`, `smtpUsername`, `smtpPassword`, and `smtpFromAddress`

Environment variables win when both env and saved settings are present. `/api/settings` reports configured/source/hint fields but redacts the actual saved secrets.

The backend exposes structured GitHub, Cloudflare, and Render status/list/read endpoints. The conversational project operator uses GitHub evidence automatically during preflight audits. Cloudflare and Render remain part of the project-evidence layer rather than the same recursive repository transaction.

Authenticated provider inspection routes include:

- `GET /api/integrations/github/pull-request` for the current PR head, merge state, and checks.
- `GET /api/integrations/cloudflare/workers`.
- `GET /api/integrations/cloudflare/worker-versions`.
- `GET /api/integrations/cloudflare/worker-deployments`.

`POST /api/operations/provider-action` prepares a durable, approval-gated provider operation. It cannot approve or execute the action. The implemented mutation allowlist is:

- GitHub: `merge_pull_request`.
- Cloudflare: `upsert_dns_record`, `delete_dns_record`, and `deploy_worker_version`.

Every mutation is bound to one high-confidence project-registry record and an immutable execution target. GitHub merges require the exact registered repository, pull request, and expected 40-character head SHA. Cloudflare actions require the exact registered account plus zone/DNS record or Worker/version/current-deployment target. The runner re-reads provider state after approval, refuses drift, performs at most one mutation, and requires authoritative read-back before completion. DNS deletion also requires strong confirmation. An accepted mutation with unavailable read-back enters `recovery_required`; it is not blindly retried or reported as failed.

An authenticated implementation request authorizes Codex to commit and push only to the operation's suggested nonproduction work branch and create or update that branch's review pull request. It does not authorize a push to the default, protected, or production branch, merge, deployment, DNS change, credential change, or external communication. `marcus/providers/codex_provider.js` writes this boundary into every Codex handoff.

Capability truth source:

- `GET /api/marcus/operator-health`

This endpoint should be used by the UI and by Marcus responses before claiming what it can actually do. It distinguishes server-side provider credentials from user-machine tooling such as `gh` and `wrangler`.

Production status verified on 2026-08-12:

- GitHub server access can enumerate repositories and read repository files.
- GitHub Actions dispatch can start the Reggie `openai/codex-action` runner and reconcile its result.
- Completed GitHub Actions jobs are independently resolved to target-repository PR, commit, diff, and check evidence before result review.
- GitHub PR inspection resolved demo PR #4 as open, clean, checks-settled, and fixed at head `4ee4135eb98be5bc57385be0ff128ee78fa42729`.
- Cloudflare server access can enumerate all 29 zones and read DNS records for the configured default zone.
- Cloudflare Worker inspection resolved `marcus-operator-demo-worker`, two versions, and deployment `d8eb7206-6d65-434b-aaab-04cd51f62823` with version `a51aa87d-a3e8-4dc3-ab81-2b9577a5a17c` at 100 percent.
- The Cloudflare credential is the dedicated account token `Marcus Production Operator`, not the local Wrangler OAuth token.
- Operator health reports the approved GitHub and Cloudflare mutation paths available. Production preparation operations `op_wSMm8zWz7DGGiA` and `op_nA9c9c_bZYsMjg` stopped at `waiting_for_approval`; neither the PR nor Worker deployment changed.
- Production conversation operation `op_-qcwlO85nndNkw` used the nonproduction Codex authority: it audited six files, started one real runner without a redundant medium approval, opened PR #5, and completed after exact-head independent verification. PR #5 remains open and unmerged; the Worker deployment remained unchanged.
- Render admin authentication and the canonical `BASE_URL` point to `https://task-tracker-5wsa.onrender.com`.
- `RENDER_API_KEY` remains unconfigured; Render management currently uses authenticated operator tooling rather than Marcus server API access.

## Realtime Voice Access

`POST /api/marcus/realtime/client-secret` is authenticated with the existing Marcus admin or Live session token. It uses the standard OpenAI API key only on the server and returns a short-lived client secret to the browser for WebRTC setup.

The Realtime session exposes one function, `marcus_operator`. That function calls Marcus's authenticated Live chat route; it does not expose GitHub, Cloudflare, Codex, email, text, deployment, or DNS credentials to the browser or voice model. Existing approval rules remain authoritative.

Every initial connection, reconnect, foreground resume, and scheduled session refresh requests a new ephemeral credential. The browser does not persist the credential, and stale connection attempts are prevented from replacing a newer session.

## Mission Memory Access

`GET/POST/PATCH /api/marcus/memory` requires the durable admin token or pairing cookie. A short-lived Live token receives 401 on those administrative routes. A Live or Realtime conversation may invoke an explicit `remember`, mission, preference, or `from now on` command through the existing authenticated operator bridge; the server records Mark as the source without granting the voice model direct memory API authority.

Memory input is bounded and redacted. Content that matches credential, token, password, private-key, or API-key assignment patterns is rejected rather than stored in redacted but misleading form. Memory is isolated by business key and never broadens project, provider, deployment, communication, or approval authority.

Live-session tokens are process-bound. If a Render restart invalidates one while the installed mobile client still has its valid HttpOnly pairing cookie, authentication falls back to that cookie in the same request and the client obtains a fresh Live token on reconnect. An invalid stale bearer header does not mask a valid pairing cookie.

Voice acceptance telemetry uses the same admin-cookie or short-lived Live-session authentication. `POST /api/marcus/realtime/telemetry` drops every field outside its strict allowlist before persistence. User and assistant transcripts are represented only by bounded character counts. The server stores no request text, reply text, credential, IP address, or raw user agent, and retains at most 1,000 events per business. `GET /api/marcus/realtime/acceptance` exposes only derived gates and coarse client context. `GET /api/marcus/acceptance` combines those gates with non-secret provider and operator readiness; it returns message action IDs/timestamps but no message bodies or credentials.

An Android/standalone context is not treated as proof of physical possession. It makes a telemetry session eligible for physical review. The final `physical_review_confirmed` event is accepted only as a boolean and counts only when all derived voice gates already pass in installed Android standalone context; Mark's installed-device run remains a separate acceptance requirement in [[implementation-roadmap]].

## Mobile Pairing

An authenticated operator can request one active six-digit code from `POST /api/auth/pairing-code`. The code expires after ten minutes and is stored only as an HMAC hash in `data/mobile-pairing.json`. An exclusive file lock makes replacement and consumption single-use across Node process replacement. Failed attempts are limited per client. Successful pairing sets the secure HttpOnly admin cookie; the durable admin token is not returned to or stored by the phone.

Local operator helper: `scripts/create-marcus-mobile-pairing-code.ps1`.

Production process-replacement acceptance passed on 2026-08-12: a code minted before deployment `858a0ba` was accepted by the replacement process, the issued cookie authenticated successfully, and replay from a fresh session returned 401.

External communication draft source:

- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/send`
- `POST /api/marcus/external-actions/:id/reject`

Marcus can create email and text drafts with recipients, subject/body, project context, and the reason approval is needed. Approval changes the draft status to `approved`. A separate send call uses SMTP for email or Quo for text, records provider evidence, and changes the status to `sent`. Provider credentials remain server-side and are never exposed to the mobile browser or Realtime model.

All five external-action routes require the durable admin token or paired HttpOnly cookie. A Live token alone cannot list full drafts, create one directly, approve, send, or reject. `/api/marcus/live/chat` may still understand an approval phrase, but it executes the approval only when the same request also carries durable admin authentication. This lets the paired installed app and voice bridge authorize work while a copied ephemeral token cannot.

The paired mobile client can configure these providers through `GET/PUT /api/marcus/providers/config` and verify them through `POST /api/marcus/providers/verify`. These routes require the durable admin token or pairing cookie; a Live-session token is deliberately insufficient. Read responses contain only non-secret fields, masked hints, and bounded verification evidence. Blank secret inputs preserve an existing secret. A provider setting change clears that provider's previous verification. A non-reversible fingerprint also binds verification to the exact effective credentials, host, account, and sender, preventing stale verified status after an environment or legacy-settings change. Verification performs a Quo sender lookup or SMTP authentication handshake and never creates, approves, or sends an external action.

Production provider status on 2026-08-12: outbound code, admin-only mobile configuration, secret redaction, provider authentication, and mock-provider send acceptance pass locally. Service worker `marcus-mobile-v19`, one-time pairing, provider configuration, exact-target operation approval, and exact-draft message review pass on Render. The redacted operation summary can advertise one pending operation approval but cannot execute it. Full message drafts and all direct external-action mutations reject a Live token, and Live-token-only conversational approval returns `reauthenticationRequired` without changing the draft. Quo has provider-accepted `sent` evidence in action `rv1v4_RKB38v`. Resend SMTP authenticated for `Marcus <marcus@gromore.media>`, but the first approved production attempt for draft `V8uMUUZjiRz1` was rejected with SMTP `550` because the borrowed API key was not authorized for that sender domain. No email was sent. The approved draft remains retryable only after a dedicated Resend key scoped to `gromore.media` is saved and verified. Marcus must not claim a real email was sent until provider acceptance and receipt evidence exist.

## Desktop Relay Credential

The Windows desktop relay reads the durable admin credential from `%APPDATA%/M.A.R.C.U.S/mobile-live-admin-token.txt`. The scheduled task contains the server URL and agent path only. This avoids placing the credential in Task Scheduler arguments or process command lines.

## Security Posture

Full access should mean Marcus can inspect and prepare confidently. It should not mean every action is autonomous.

Implemented policy:

- Read actions: allowed when authenticated.
- Local workspace actions: allowed only for approved, attested workspace mappings.
- Code edits through Codex: allowed after Marcus creates a durable operation.
- External communication: explicit approval required.
- GitHub, DNS, and Worker mutation: explicit action-specific approval required; critical deletion also requires strong confirmation.
- Broad PC access: accepted only when the desktop relay sends an explicit authorization declaration; exact discovered workspaces are challenged and attested before use.
- New local projects: an exact folder is created below `MARCUS_NEW_PROJECT_ROOT`, initialized as Git, opened in VS Code, and then used by the local Codex bridge.
- Local Codex: the prompt is sent to the desktop relay, which runs `codex exec --json --sandbox workspace-write` in the bound workspace and streams bounded, redacted events to a token-scoped monitor.
