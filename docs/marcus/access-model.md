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
- Email IMAP/SMTP settings
- Slack credentials
- Twilio/Quo webhook verification

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

External communication draft source:

- `GET /api/marcus/external-actions`
- `POST /api/marcus/external-actions/draft`
- `POST /api/marcus/external-actions/:id/approve`
- `POST /api/marcus/external-actions/:id/reject`

Marcus can create email and text drafts with recipients, subject/body, project context, and the reason approval is needed. Approval changes the draft status to `approved`; sending through an email or text provider remains a separate explicit provider action so there is no ambiguity between "approved to send" and "sent."

## Security Posture

Full access should mean Marcus can inspect and prepare confidently. It should not mean every action is autonomous.

Recommended policy:

- Read actions: allowed when authenticated.
- Local workspace actions: allowed only for approved, attested workspace mappings.
- Code edits through Codex: allowed after Marcus creates a durable operation.
- External communication: explicit approval required.
- Production mutation: explicit strong approval required.
