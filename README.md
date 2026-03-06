# Task Tracker (OneDrive-synced)

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

   ```bash
   npm start
   ```

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
- `OPENAI_API_KEY`, `OPENAI_MODEL`

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

## MCP on Render (Option 1: stdio servers in same container)

This app supports MCP (Model Context Protocol) servers over **stdio**. On Render, this means OS.1 spawns MCP servers as child processes inside the same container.

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
   - **Task Tracker: Start server (free port 3030)** (best default)
   - **Task Tracker: Start server**
   - **Task Tracker: Open in browser**
   - **Task Tracker: Stop server (kill node)**

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
   - Your browser may prompt you to allow opening the external `vscode://` link.
- **Airtable link (optional)**: paste any Airtable URL (base/table/view/record).
   - This enables an **Airtable** button in the project list and an **Open Airtable** button in the workspace.

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

Optional but recommended:

- `ADMIN_TOKEN` = a long random string
   - When set, all `/api/*` routes require the token **except** inbound webhooks and OAuth callbacks.
   - The browser UI will prompt once and remember it in `localStorage`.

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
