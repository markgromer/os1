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

Environment variables win when both env and saved settings are present. `/api/settings` reports configured/source/hint fields but redacts the actual saved secrets.

The backend already exposes GitHub, Cloudflare, and Render status/list/read endpoints. The next step is to make the conversational operator use those capabilities automatically during project execution planning.

Capability truth source:

- `GET /api/marcus/operator-health`

This endpoint should be used by the UI and by Marcus responses before claiming what it can actually do. It distinguishes server-side provider credentials from user-machine tooling such as `gh` and `wrangler`.

Production status verified on 2026-08-12:

- GitHub server access can enumerate repositories and read repository files.
- GitHub Actions dispatch can start the Reggie `openai/codex-action` runner and reconcile its result.
- Cloudflare server access can enumerate all 29 zones and read DNS records for the configured default zone.
- The Cloudflare credential is the dedicated account token `Marcus Production Operator`, not the local Wrangler OAuth token.
- Render admin authentication and the canonical `BASE_URL` point to `https://task-tracker-5wsa.onrender.com`.
- `RENDER_API_KEY` remains unconfigured; Render management currently uses authenticated operator tooling rather than Marcus server API access.

## Realtime Voice Access

`POST /api/marcus/realtime/client-secret` is authenticated with the existing Marcus admin or Live session token. It uses the standard OpenAI API key only on the server and returns a short-lived client secret to the browser for WebRTC setup.

The Realtime session exposes one function, `marcus_operator`. That function calls Marcus's authenticated Live chat route; it does not expose GitHub, Cloudflare, Codex, email, text, deployment, or DNS credentials to the browser or voice model. Existing approval rules remain authoritative.

Every initial connection, reconnect, foreground resume, and scheduled session refresh requests a new ephemeral credential. The browser does not persist the credential, and stale connection attempts are prevented from replacing a newer session.

## Mobile Pairing

An authenticated operator can request one active six-digit code from `POST /api/auth/pairing-code`. The code expires after ten minutes and is stored only as an HMAC hash in `data/mobile-pairing.json`. An exclusive file lock makes replacement and consumption single-use across Node process replacement. Failed attempts are limited per client. Successful pairing sets the secure HttpOnly admin cookie; the durable admin token is not returned to or stored by the phone.

Local operator helper: `scripts/create-marcus-mobile-pairing-code.ps1`.

External communication draft source:

- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/send`
- `POST /api/marcus/external-actions/:id/reject`

Marcus can create email and text drafts with recipients, subject/body, project context, and the reason approval is needed. Approval changes the draft status to `approved`. A separate send call uses SMTP for email or Quo for text, records provider evidence, and changes the status to `sent`. Provider credentials remain server-side and are never exposed to the mobile browser or Realtime model.

Production provider status on 2026-08-11: the outbound code and mock-provider acceptance tests pass, but real SMTP and Quo outbound credentials are not configured. Marcus must report that condition and must not claim a real message was sent.

## Desktop Relay Credential

The Windows desktop relay reads the durable admin credential from `%APPDATA%/M.A.R.C.U.S/mobile-live-admin-token.txt`. The scheduled task contains the server URL and agent path only. This avoids placing the credential in Task Scheduler arguments or process command lines.

## Security Posture

Full access should mean Marcus can inspect and prepare confidently. It should not mean every action is autonomous.

Recommended policy:

- Read actions: allowed when authenticated.
- Local workspace actions: allowed only for approved, attested workspace mappings.
- Code edits through Codex: allowed after Marcus creates a durable operation.
- External communication: explicit approval required.
- Production mutation: explicit strong approval required.
