# M.A.R.C.U.S. — Modular Autonomous Routing & Coordination Utility System

M.A.R.C.U.S. = **Modular Autonomous Routing & Coordination Utility System**.

This is a tiny “project command center” that runs locally and stores everything in a OneDrive-sync-friendly JSON file.

- **Works locally** on your computer
- **Syncs via OneDrive** because the data is stored in `data/tasks.json` inside this folder
- **Projects-first views**: Today / This Week / Long Term / All (sorted by due date)
- **Per-project workspace**: working notes scratchpad, call notes/summaries, comms drafting, and an AI assistant chat (optional)

## Run it

1. Install dependencies:

   ```bash
   npm install
   ```

2. Start the app:

   ```powershell
   $env:MARCUS_ALLOW_UNAUTHENTICATED_LOCAL = "true"
   $env:MARCUS_HOST = "127.0.0.1"
   npm start
   ```

   Unauthenticated mode is intentionally limited to an explicit loopback-only development setting. Alternatively, set `ADMIN_TOKEN` locally.

3. Open: `http://localhost:3030`

Tip: during active tinkering, use:

```bash
npm run dev
```

## Deploy on Render (recommended for cloud run)

This repo now includes a Render blueprint file: `render.yaml`.

### Quick setup

1. Push this repo to GitHub.
2. In Render: **New +** → **Blueprint** → select this repo.
3. Confirm the service + disk creation.
4. Set required secrets in Render env vars.

### Required Render env vars

- `BASE_URL` = your Render public URL (example `https://task-tracker.onrender.com`)
- `ADMIN_TOKEN` = long random token for API/UI protection
- `SLACK_SIGNING_SECRET` = from Slack app
- `FIREFLIES_SECRET` = shared secret used by Fireflies webhook header `x-fireflies-secret`

### Optional but recommended env vars

- `SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET` (for Slack OAuth install flow)
- `SLACK_BOT_TOKEN` (if not using OAuth install in-app)
- `TWILIO_AUTH_TOKEN` (if using Quo/Twilio signature verification)
- `QUO_WEBHOOK_TOKEN` (non-Twilio shared-token webhook verification)
- `IMAP_HOST`, `IMAP_PORT`, `IMAP_SECURE`, `IMAP_USERNAME`, `IMAP_PASSWORD`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_SECURE`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM_ADDRESS`
- `OPENAI_API_KEY`, `OPENAI_MODEL`
- `ELEVENLABS_API_KEY` = ElevenLabs API key for Marcus voice output
- `ELEVENLABS_VOICE_ID` = ElevenLabs voice ID to use for Marcus
- `ELEVENLABS_MODEL_ID` = optional, defaults to `eleven_flash_v2_5`
- `ELEVENLABS_OUTPUT_FORMAT` = optional, defaults to `mp3_44100_128`
- `QDRANT_URL`, `QDRANT_COLLECTION`, `QDRANT_API_KEY` (Marcus knowledge base)
- `QDRANT_EMBEDDING_MODEL` (optional, default `text-embedding-3-small`)
- `QDRANT_VECTOR_SIZE` (optional, default matches embedding model)
- `QDRANT_TOP_K` (optional, default `6`)

### Persistence on Render

The blueprint mounts a persistent disk at `/var/data/task-tracker` and stores:

- tasks data: `/var/data/task-tracker/data`
- settings: `/var/data/task-tracker/settings`
- backups: `/var/data/task-tracker/backups`

### Webhook URLs on Render

- Slack events: `https://<your-render-url>/api/integrations/slack/events`
- Fireflies ingest: `https://<your-render-url>/api/integrations/fireflies/ingest`
- Quo/Twilio SMS: `https://<your-render-url>/api/integrations/quo/sms`
- Quo/Twilio calls: `https://<your-render-url>/api/integrations/quo/calls`

## Qdrant knowledge base

Marcus can use Qdrant as a retrieval-backed knowledge base. The backend supports both env-var configuration and saved settings, but env vars are the cleanest approach on Render.

Recommended Render env vars:

- `QDRANT_URL` = your cluster URL, for example `https://xxxxx.us-east.aws.cloud.qdrant.io`
- `QDRANT_COLLECTION` = collection name, for example `marcus-knowledge`
- `QDRANT_API_KEY` = API key if your cluster requires auth
- `OPENAI_API_KEY` = used to generate embeddings for upsert/search

Optional env vars:

- `QDRANT_EMBEDDING_MODEL` = embedding model for document/query vectors
- `QDRANT_VECTOR_SIZE` = vector width if you want to override the default
- `QDRANT_DISTANCE` = `Cosine`, `Dot`, `Euclid`, or `Manhattan`
- `QDRANT_TOP_K` = default retrieval count for Marcus chat context

Backend endpoints:

- `GET /api/integrations/qdrant/status`
- `POST /api/integrations/qdrant/test`
- `POST /api/integrations/qdrant/ensure-collection`
- `POST /api/integrations/qdrant/upsert`
- `POST /api/integrations/qdrant/search`

Example upsert payload:

```json
{
   "documents": [
      {
         "title": "Scoop Doggy Logs pricing",
         "text": "Weekly residential yard service starts at $19 per visit.",
         "source": "pricing-sheet",
         "tags": ["pricing", "sales"],
         "metadata": {
            "owner": "Mark"
         }
      }
   ]
}
```

Example search payload:

```json
{
   "query": "What is the starting weekly residential price?",
   "limit": 5
}
```

When Qdrant is configured and enabled, Marcus chat will automatically pull a small set of knowledge-base hits into its context for the active business.

## Email integration (IMAP / SMTP)

Marcus now supports IMAP inbox sync, SMTP outbound mail, and archive ingestion into Qdrant.

Recommended env vars:

- `IMAP_HOST`, `IMAP_PORT`, `IMAP_SECURE`, `IMAP_USERNAME`, `IMAP_PASSWORD`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_SECURE`, `SMTP_USERNAME`, `SMTP_PASSWORD`
- `SMTP_FROM_ADDRESS` (optional sender identity for SMTP)

You can also save these in Settings instead of env vars.

Backend endpoints:

- `GET /api/integrations/email/status`
- `POST /api/integrations/email/test`
- `POST /api/integrations/email/send`
- `POST /api/integrations/email/sync`
- `POST /api/integrations/email/archive-to-qdrant`

Typical flows:

- Use `POST /api/integrations/email/test` to verify IMAP and SMTP connectivity.
- Use `POST /api/integrations/email/sync` to pull recent messages from configured IMAP folders into Inbox as `source: email` items.
- Use `POST /api/integrations/email/archive-to-qdrant` to ingest archived mail into Qdrant for Marcus retrieval.

Example sync payload:

```json
{
   "limitPerFolder": 25,
   "sinceDays": 30,
   "unseenOnly": false
}
```

Example archive-to-Qdrant payloads:

```json
{
   "source": "imap",
   "limitPerFolder": 50,
   "sinceDays": 3650
}
```

```json
{
   "source": "local"
}
```

The `imap` mode pulls from configured archive folders such as `Archive` or `All Mail`. The `local` mode pushes already-archived Inbox email items into Qdrant instead.

## MCP on Render (Option 1: stdio servers in same container)

This app supports MCP (Model Context Protocol) servers over **stdio**. On Render, this means M.A.R.C.U.S. spawns MCP servers as child processes inside the same container.

### Why this is the easiest option

- No extra services to deploy
- No extra networking/auth between services
- Tools can be called as needed (best-effort, on-demand)

### Multi-server setup (recommended)

In **Settings → Advanced**, add an `mcpServers` array. Each server must have a unique `name`.

Example:

```json
{
   "mcpServers": [
      {
         "name": "crm",
         "enabled": true,
         "command": "node",
         "args": "crm-mcp-server.js",
         "cwd": "/opt/render/project/src"
      },
      {
         "name": "slack",
         "enabled": true,
         "command": "node",
         "args": "slack-mcp-server.js",
         "cwd": "/opt/render/project/src"
      }
   ]
}
```

Tool names are **namespaced** as:

- `crm.<toolName>`
- `slack.<toolName>`

Example: call `crm.search_leads` or `slack.post_message` depending on what your MCP server exposes.

### Notes / best practices

- Prefer putting secrets in Render **env vars** (not in `args`), so they are not stored/shown in Settings.
- Ensure the MCP server code is available in the container (in this repo, or installed as a dependency) so the `command`/`args` works at runtime.

### Fireflies payload notes

- `summary` is required.
- `projectId`/`projectName` are optional.
- Incoming Fireflies summaries now always create an Inbox item, and will auto-link if project context matches.

## Run it from VS Code (one click)

This repo includes VS Code tasks so you can run everything without remembering commands.

1. In VS Code: **Terminal → Run Task…**
2. Pick one of:
   - **M.A.R.C.U.S.: Start server (free port 3030)** (best default)
   - **M.A.R.C.U.S.: Start server**
   - **M.A.R.C.U.S.: Open in browser**
   - **M.A.R.C.U.S.: Stop server (kill node)**

If you want a keyboard shortcut, you can bind it to the task label in VS Code.

## How syncing works (VA access)

Because this folder is in OneDrive, `data/tasks.json` will sync.

- If you and your VA edit at the **same time**, the app will show a “Revision mismatch” message for one of you.
- Fix is simple: click **Reload**, re-apply the change.

Practical workflow:
- You maintain project due dates and keep the **Today / This Week** lists tight.
- VA can open a project and paste in **Fireflies/Zoom summaries**, add call notes, and draft comms.

## What goes where

- **Working Notes (scratchpad)**: living, messy notes while you work (autosaves)
- **Call Notes / Summaries**: one entry per call/summary (paste Fireflies/Zoom recaps here)
- **Comms to Account Manager**: draft a message you can copy or email
- **AI Assistant**: ask for next actions, delegation lists, or an AM/client update using the project context

## Project links (VS Code + Airtable)

Each project has two optional link fields in the **Project** panel:

- **VS Code folder (optional)**: paste the local folder path (example: `C:\Users\markg\OneDrive\Documents\Client Project`).
   - This enables a **VS Code** button in the project list and an **Open in VS Code** button in the workspace.
   - When the app is running locally on Windows, the server launches VS Code directly with `code <folder>`.
   - When the app is hosted, the server queues an `open-vscode` desktop action for the local desktop agent.
- **Airtable link (optional)**: paste any Airtable URL (base/table/view/record).
   - This enables an **Airtable** button in the project list and an **Open Airtable** button in the workspace.

### Desktop agent local actions

Run the desktop agent on the machine that should open projects:

```powershell
$env:MARCUS_DESKTOP_AGENT_ID = "mark-primary-desktop"
$env:MARCUS_ALLOWED_WORKSPACE_ROOTS = "C:\Users\markg\OneDrive\Documents\Projects;D:\TrustedWork"
node desktop-agent.cjs https://your-app.onrender.com yourAdminToken
```

`MARCUS_ALLOWED_WORKSPACE_ROOTS` must contain specific project-parent folders, not a drive root, home directory, Documents directory, or OneDrive Documents root. Durable operations require both authenticated operator approval and real-path validation. A same-machine server validates the registered path directly. A hosted server creates a pending `validate-workspace` challenge for the bound `MARCUS_DESKTOP_AGENT_ID`; the registry remains pending until the matching agent attests the business, project, registered path, canonical path, and challenge ID. Legacy workspace paths are never approved merely because they contain an agent ID. The agent resolves real paths before every action and rejects traversal, symlink/junction escapes, unregistered operations, and paths outside the allowed roots.

Supported local desktop actions:

- `open-vscode`: open a saved project workspace in VS Code.
- `prepare-publish`: inspect the local git repo before publishing, including branch, origin, changed files, recent commits, and available npm scripts.
- `run-project-script`: run a named npm script from `package.json`, such as `build`, `test`, or `lint`.
- `clone-github-project`: clone a GitHub repo into a local projects folder and optionally open it in VS Code.
- `publish-project-changes`: after explicit approval, optionally run npm scripts, commit local changes, and push the current branch.

Example Marcus requests:

```text
Open the Acme website project in VS Code.
Prepare Acme website for publish.
Run build for the Acme website.
Clone https://github.com/example/acme-site and open it locally.
Publish the approved Acme changes with commit message "Update homepage revision".
Run build before publishing the Acme changes.
```

Marcus should still ask before high-impact actions like deploys, merges, production publishes, billing changes, deletes, or client sends.
Commit, push, deploy, publish, and merge authorization is action-specific. A local commit approval cannot authorize push or deploy, and a negation such as “do not push” overrides generic language such as “go ahead” or “do it.” `publish-project-changes` queues only the exact commit/push actions authorized by the authenticated message.

## Marcus Live

Marcus Live is the live operations cockpit for:

- system performance from the desktop agent
- one-click performance profiles: optimize, performance, balanced, and power saver
- current focus projects based on desktop activity, recent work, urgent tasks, and pending communications
- pending communication pills linked to accounts/projects when possible
- stale active website projects that are likely done and should stop polluting current context

Marcus chat also uses the same freshness logic so old website work is excluded from current project context unless it has recent activity, a pending communication, an urgent task, or is the active desktop workspace.

## Durable Operations Engine

M.A.R.C.U.S. includes a restart-safe operations layer for multi-step project outcomes. It is additive to the existing project/task stores and does not replace the dashboard, task APIs, desktop agent, or integrations.

The engine is split into reusable modules under `marcus/`:

- `operations/`: validated operation/step types, atomic persistence, lifecycle service, deterministic runner, recovery, verification, and Marcus tools
- `projects/`: universal project registry plus deterministic resolver
- `approvals/`: runtime risk classification and approval records
- `providers/`: Codex, desktop, and GitHub-read provider boundaries
- `api/operations_routes.js`: authenticated operation and registry routes

### Data files and migration

Durable state is isolated by business:

```text
data/businesses/<businessKey>/operations.json
data/businesses/<businessKey>/project-registry.json
data/desktop-actions.json
```

Writes are serialized and atomic, the previous valid file is retained as `.bak`, and corrupted primary files are preserved rather than silently overwritten. The global desktop-action file contains only bounded dispatch envelopes, while every operation binding remains business-scoped and is revalidated when a result returns. At startup, interrupted work is reconciled from durable provider state; completion is never assumed. Paused operations remain paused across restart.

The project registry synchronizes additively from existing project fields such as `repoUrl`, `workspacePath`, owner, Airtable/docs links, and known deployment fields. Existing registry values win; synchronization only creates missing records or fills blank fields.

### Lifecycle and safety

Operation statuses are `draft`, `planned`, `waiting_for_approval`, `queued`, `running`, `awaiting_provider`, `paused`, `blocked`, `recovery_required`, `verifying`, `completed`, `failed`, and `cancelled`. Executable step types currently include `internal`, `desktop`, `github_read`, `codex`, `verification`, and `approval`.

`completed`, `failed`, and `cancelled` are monotonic terminal states. Every provider result is checked against the current operation status, active step, attempt number, idempotency key, and provider action before it can change lifecycle state. Late results are retained as bounded, redacted audit evidence; they cannot resume subsequent steps. A late Codex job ID after cancellation triggers a best-effort provider cancellation while the local operation remains cancelled.

Runtime policy classifies every action independently of model-generated `riskLevel` or `approvalRequired` values:

- low: read/inspect/plan/build/test/lint/typecheck and internal evidence work
- medium: work-branch modifications, branch/commit/draft-PR metadata, and preview actions; allowed only by an explicit request or configured autonomy
- high: push, normal PR, merge, production deploy, environment/DNS changes, client sends, migrations, automation, and permissions; always explicitly approved
- critical: destructive production data/infrastructure, billing/legal/account, and outage-risk credential actions; explicit approval plus strong confirmation

Only allowlisted internal and desktop actions can execute. Every queued asynchronous desktop operation has a durable correlation containing the operation, step, action, business, project, agent, idempotency key, and attempt. Desktop dispatch uses a persisted lease instead of destructive polling: an action remains durable until its matching result is accepted, and an unacknowledged lease can be redelivered after a restart. Results must match every binding and are idempotent across restart. Project verification uses registered package-script identities (`build`, `test`, `lint`, and `typecheck`) through the existing desktop agent; arbitrary shell commands and request-supplied filesystem paths are not accepted by the runner.

For chat-created operations, the authenticated user message is the only request used to derive authorization provenance. Model-supplied request text, risk, approval, metadata, and authorization fields are untrusted. Changing the bound project revokes existing provenance instead of expanding it.

### Codex integration

The provider interface supports `startJob`, `getJobStatus`, `sendFollowup`, `getArtifacts`, `getDiff`, `cancelJob`, and optional `pauseJob`/`resumeJob`. Pausing never converts an existing job into a new launch attempt: unsupported providers may continue externally, and explicit resume polls the same durable job. This deployment has no supported direct Codex launch API, so it deliberately uses `external_handoff` mode:

1. M.A.R.C.U.S. resolves the project and persists the operation.
2. The runner generates and stores a complete Codex handoff artifact.
3. The Codex step becomes honestly blocked/waiting; it is not reported as running.
4. Mark can register a real Codex run ID, branch, commit, diff, artifacts, or completion result.
5. The same operation resumes into verification.
6. Required checks must pass or have a recorded approved waiver before completion.

Codex output is implementation evidence, not verification. Any Codex-supplied `verificationResults` are quarantined as untrusted evidence. Automated checks run independently; authenticated manual evidence uses a separate route, requires a meaningful note or artifact, and records supplier/time provenance. Waivers remain explicit approval records.

The Operations UI provides the list/detail timeline, approvals, blockers, artifacts, verification evidence, lifecycle controls, handoff copy, and external Codex registration. Marcus Live only adds a compact operations section for running, blocked, approval-gated, failed-verification, and recently completed operations.

### Operation APIs

All routes use the existing API authentication and active-business context. Looking up an ID only searches the active business store.

```text
GET    /api/operations
GET    /api/operations/readiness
POST   /api/operations
GET    /api/operations/:id
PATCH  /api/operations/:id
POST   /api/operations/:id/plan
POST   /api/operations/:id/start
POST   /api/operations/:id/pause
POST   /api/operations/:id/resume
POST   /api/operations/:id/cancel
POST   /api/operations/:id/retry
POST   /api/operations/:id/tick
POST   /api/operations/:id/approvals/:approvalId/approve
POST   /api/operations/:id/approvals/:approvalId/reject
POST   /api/operations/:id/external-job
POST   /api/operations/:id/manual-verification-evidence
POST   /api/operations/:id/verification/:verificationId/waive

GET    /api/project-registry
POST   /api/project-registry
PATCH  /api/project-registry/:id
POST   /api/project-registry/:id/approve-workspace
POST   /api/project-registry/resolve
```

Marcus chat exposes matching operation tools. Strict code/project ownership requests such as “Own the WARREN mobile problem and get Codex working on it” also enter the durable path deterministically, so this behavior does not depend on the model choosing a tool or an AI key being configured.

### Validation

```bash
npm test
npm run lint
```

The test suite uses temporary data directories and covers normalization, terminal-state races, delayed launch/poll/artifact cancellation, pause/restart/resume without relaunch, business isolation, resolver scoring, authenticated authorization provenance, action-scoped publish approval, durable desktop dispatch leases and recovery, general desktop reconciliation, workspace approval challenges, independent verification, retry limits, external Codex handoffs, route authentication, and isolated startup.

Dependency audit note: Nodemailer is upgraded to the fixed 9.x line. The remaining audit findings are moderate transitive `uuid`/Google API findings; npm currently requires a major `googleapis` upgrade to clear them, so that upgrade remains a separately testable compatibility change.

## AI help (optional)

The per-project **AI Assistant** works in two modes:

- **No API key set**: uses a simple local fallback based on your notes + existing tasks.
- **With API key**: calls the OpenAI API for smarter next actions.

### Option A (easy): in-app Settings

Open the app and use the **Settings** card to paste your OpenAI API key and (optionally) a model.

- The key is saved to a local file under Windows AppData (not inside this OneDrive folder).
- Leave the key blank and save to disable AI.

### Option B (advanced): environment variables

Environment variables still work and override the saved Settings values.

To enable real AI (PowerShell):

```powershell
$env:OPENAI_API_KEY = "YOUR_KEY_HERE"
# optional:
$env:OPENAI_MODEL = "gpt-4.1-mini"
npm start

Note: `$env:...` sets it for the current terminal session. For a permanent user-level env var, set it in Windows "Environment Variables" and then restart your terminal.
```

## Deploy on a public subdomain (SiteGround)

This app was originally built for **local use**. If you deploy it to the public internet, you should enable server-side auth.

### 1) Create a subdomain + HTTPS

- Create a subdomain like `https://ops.yourdomain.com` in SiteGround.
- Ensure SiteGround issues an SSL cert for the subdomain (HTTPS is required for Slack OAuth and strongly recommended for webhooks).

### 2) Deploy the Node app on SiteGround

SiteGround typically supports Node apps via **Site Tools → Devs → Node.js** (exact UI may vary by plan).

Minimum runtime requirements:
- Node.js **>= 18** (this server uses `fetch()`)

Set these environment variables in your SiteGround Node app:

- `BASE_URL` = `https://ops.yourdomain.com`
- `PORT` = whatever SiteGround assigns (often provided automatically)

Required for every hosted or production runtime:

- `ADMIN_TOKEN` = a long random string
   - The server refuses hosted/production startup without it.
   - All `/api/*` routes require the token except explicitly verified inbound webhooks, OAuth callbacks, health, and auth bootstrap routes.
   - The browser UI will prompt once and remember it in an HttpOnly cookie.

Data paths (optional):
- `TASK_TRACKER_DATA_DIR` = absolute path where `tasks.json` should live
- `TASK_TRACKER_SETTINGS_DIR` = absolute path where `settings.json` (secrets) should live

### 3) Slack “I want it all” setup

In your Slack App config:

**OAuth & Permissions**
- Add Redirect URL:
   - `https://ops.yourdomain.com/api/integrations/slack/oauth/callback`
- Add the same scopes as the server requests (broad message + lookup scopes):
   - `users:read`
   - `channels:read`, `groups:read`, `im:read`, `mpim:read`
   - `channels:history`, `groups:history`, `im:history`, `mpim:history`

**Event Subscriptions**
- Enable Events
- Request URL:
   - `https://ops.yourdomain.com/api/integrations/slack/events`
- Subscribe to bot events (typical):
   - `message.channels`, `message.groups`, `message.im`, `message.mpim`

**Basic Information**
- Copy the **Signing Secret** (still required even with OAuth)

In the app Settings UI (or via env vars):
- Paste `Slack Client ID`, `Slack Client Secret`, and `Slack Signing Secret`
- Click **Connect** to install the app and store the bot token

### 4) Quo/Twilio webhooks setup

Set your webhook URLs to:

- SMS:
   - `https://ops.yourdomain.com/api/integrations/quo/sms`
- Missed calls:
   - `https://ops.yourdomain.com/api/integrations/quo/calls`

Make sure:
- `BASE_URL` matches the public URL exactly (scheme + host)
- Your Quo/Twilio **Auth Token** is saved in Settings (used to verify `X-Twilio-Signature`)

## Suggested conventions (so it stays clean)

- **Project status**:
  - `Active`: you’re actively moving it
  - `On Hold`: blocked / parked
  - `Done`: completed
- **Type**: Build / Rebuild / Revision / Workflow / Cleanup / Other

## Notes

- Data lives in `data/tasks.json`.
- The server still includes legacy `/api/tasks` endpoints from the original task-centric version, but the current UI is projects-first.

## Optional upgrade path: Airtable “Command Center” (recommended long-term)

Since you already use Airtable + Slack with your VA, you can use this as your long-term home and keep this app as a lightweight backup.

### Airtable base structure

**Table: Projects**
- `Name` (single line): Agency, GHL SaaS, Pet Waste, Skool, Other

**Table: Tasks**
- `Task` (primary)
- `Project` (link to Projects)
- `Priority` (single select: P1, P2, P3)
- `Status` (single select: Next, Doing, Waiting, Done)
- `Owner` (collaborator)
- `Due` (date)
- `Created` (created time)
- `Updated` (last modified time)

### Airtable views you want
- **Today**: Status != Done AND (Due is today OR Due is before today OR Status = Doing)
- **This Week**: Status != Done AND Due is within next 7 days
- **Weekly Review**: Status != Done AND (Due is empty OR Status = Waiting OR Due is before today)
- **By Project**: grouped by Project

### Slack automations (simple + effective)
- Daily 8am message to Slack: list of Today tasks grouped by Owner
- Immediate alert when a P1 task is created with Due = today

If you want, tell me:
- Your Slack workspace/channel name(s)
- Whether tasks should default to you or your VA

…and I’ll write the exact automation spec (including what the Slack message should look like).
