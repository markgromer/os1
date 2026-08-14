# External Presence

Status: architecture and rollout checklist. Email send, SMTP provider verification, IMAP fetch, inbox import, and approved outbound drafts already exist in `server.js`. Social/community monitoring is planned as an opportunity radar with copy/paste drafts for Mark, not autonomous posting. Live Zoom attendance is not implemented and is intentionally constrained until consent rules are satisfied.

## Purpose

Marcus should have an externally reachable identity that can read inbound communication, prepare useful responses, join Mark's operating rhythm, surface good places for Mark to interact, and support calls without pretending to be a human or bypassing approvals.

The working model is:

`incoming source -> normalized evidence -> Marcus summary/draft -> Mark approval -> provider-safe outbound action -> receipt/audit trail`

## Identity

Recommended address:

- `Marcus <marcus@gromore.media>` for Mark-owned work.
- Alternate per-business aliases only when the underlying mailbox and signature remain transparent.

Every external profile should disclose the role:

- Display name: `Marcus - Mark's AI Assistant`.
- Email signature: `Marcus is Mark Gromer's AI assistant. Mark reviews and approves outbound decisions and external messages.`
- Community bio: `AI assistant for Mark Gromer. Posts and comments are reviewed by Mark unless explicitly stated otherwise.`

Marcus should not use a fake human identity, shared personal account, or hidden automation account.

## Email Capability

Implemented primitives:

- `getEmailConfig()` reads IMAP/SMTP settings from environment or saved settings.
- `GET /api/integrations/email/status` reports redacted IMAP/SMTP status.
- `POST /api/integrations/email/test` performs bounded connectivity probes.
- `POST /api/integrations/email/sync` reads IMAP folders and imports messages into the inbox integration store.
- `POST /api/integrations/email/archive-to-qdrant` can index email knowledge when Qdrant is configured.
- `POST /api/marcus/external-actions/draft` creates unsent email/text drafts.
- `POST /api/marcus/external-actions/:id/approve` records Mark's explicit approval.
- `POST /api/marcus/external-actions/:id/send` sends only approved drafts and records provider evidence.

Ready-state requirement:

- IMAP and SMTP configured for the same Marcus mailbox.
- SMTP provider verification passes.
- IMAP sync against `INBOX` returns messages.
- At least one inbound email becomes a local inbox item without marking it read or sending a reply.
- A reply draft is created from that item and remains `pending_approval`.
- The exact approved draft is sent once and records provider receipt.

Operational loop:

1. IMAP sync fetches recent unread or recent inbox mail.
2. Marcus imports each unique message by `externalId` and `threadKey`.
3. Marcus classifies the message as ignore, summarize, task, reply-needed, or escalation.
4. Reply-needed messages create an external email draft with recipient, subject, body, source message id, and reason.
5. Mark approves or rejects the exact draft.
6. Only an approved draft can be sent.
7. Sent state requires SMTP acceptance evidence for that draft.

Planned enhancement:

- Add a scheduled inbound-mail watcher that calls the existing sync path on a conservative interval and creates approval-gated reply drafts automatically.
- Add conversation-thread context so replies include `In-Reply-To` and `References` headers where available.
- Add UI grouping for inbox-derived drafts in the `Verify` dashboard.

## Social Opportunity Radar

Status: planned. This covers Skool first, then any future connected social or community channel that can be monitored through a permitted source.

Boundary:

- Marcus may read community/social content that Mark manually supplies, exports, forwards by email, screenshots, notifications, RSS, official APIs, or another permitted connector.
- Marcus may watch for high-signal interaction opportunities: unanswered questions, buying-intent discussions, confusion Marcus can clarify, relationship-building openings, testimonials, objection patterns, repeated pain points, and good original-post ideas.
- Marcus may summarize why an opportunity matters and prepare multiple copy/paste options.
- Marcus must not scrape a platform, auto-post, auto-comment, auto-message members, farm engagement, or operate an undisclosed automation account.
- If Marcus ever has a platform profile, it should clearly identify Marcus as Mark's AI assistant.

Safe loop:

1. Mark connects or supplies a permitted source such as Skool notifications, copied posts, screenshots, exports, or an official platform connector.
2. Marcus ranks recent items by relevance, urgency, relationship value, sales value, and fit with Mark's voice.
3. Marcus sends Mark a compact opportunity brief.
4. Each opportunity includes context, recommended angle, and two or three copy/paste options.
5. Mark chooses whether to post manually.
6. Marcus records the final posted text only when Mark confirms it or a permitted read-back source proves it.

Opportunity brief format:

- Source: platform/community, post title or author, and stable link/id when available.
- Why it matters: one or two lines.
- Recommended move: comment, DM Mark should send manually, original post idea, follow-up question, or no action.
- Copy/paste options: short, medium, and direct versions when useful.
- Risk note: promotional, sensitive, heated, legal/medical/financial, or low-confidence context.

Skool-specific readiness gate:

- Monitoring source is permitted: notification emails, manual copy/paste, screenshots, export, official API, or written community/platform permission.
- Marcus does not auto-post, auto-comment, or auto-message.
- Mark remains the actor who posts in Skool.
- Audit trail links each recommendation to source context and any Mark-confirmed final post.

Until those gates exist, Skool remains `watch_and_draft_only`.

## Zoom Capability

Status: transcript/recording ingestion is the first safe implementation path. Autonomous live attendance is a later phase that requires meeting consent and clear participant disclosure.

Safe loop:

1. Marcus is invited to the calendar event by email.
2. The meeting host records or provides transcript access with participant consent.
3. Mark uploads, forwards, or connects the recording/transcript to Marcus.
4. Marcus produces notes, decisions, unanswered questions, and follow-up drafts.
5. Follow-up emails/texts/community comments stay approval-gated.

Live-response loop:

`Mark asks Marcus during call -> local Marcus voice/chat session -> Marcus answers Mark -> Mark chooses whether to say it to the room`

This avoids hidden live meeting automation while still letting Marcus sit beside Mark as a working assistant.

Future live-attendance gate:

- The meeting host approves Marcus as an assistant participant.
- Participants can see the assistant identity in the participant list.
- Recording/transcription consent is honored.
- Marcus records notes but does not speak or chat unless Mark directly asks and approves the exact response mode.

## Approval Matrix

| Capability | Default authority | Required approval |
| --- | --- | --- |
| Read Marcus mailbox by IMAP | Allowed when configured | Provider setup by durable admin |
| Import inbound email to Marcus inbox | Allowed when configured | None beyond provider setup |
| Draft email reply | Allowed | None; draft remains unsent |
| Send email reply | Blocked | Exact draft approval plus send call |
| Watch permitted social/community sources | Allowed when configured | Source must be permitted by platform/community rules |
| Scrape Skool or another platform | Blocked | Do not implement without explicit permission |
| Recommend interaction opportunities | Allowed | None; recommendation remains internal |
| Draft copy/paste comment/post options | Allowed | None; draft remains unposted |
| Post/comment/message as Marcus or Mark | Blocked | Manual Mark posting or written compliant integration approval |
| Ingest Zoom transcript/recording | Allowed when Mark has rights to provide it | Meeting consent and source access |
| Join Zoom live as visible assistant | Blocked by default | Host/participant consent and transparent identity |
| Speak/chat in Zoom | Blocked by default | Mark's direct request plus exact response approval |

## Audit Trail

Each external-presence action should retain:

- Source channel.
- Source object id or stable local id.
- Source timestamp.
- Summary generated by Marcus.
- Draft body, if any.
- Approval decision, approver, and timestamp.
- Provider receipt or manual-post confirmation.
- Any skipped verification or unresolved consent/platform issue.

Secrets are never stored in the audit trail. Email bodies should be bounded and redacted before entering long-term knowledge storage.

## Implementation Checklist

Email:

- Configure IMAP and SMTP for the Marcus mailbox.
- Verify provider settings through existing admin-only routes.
- Run one IMAP sync and confirm imported inbox item deduplication.
- Create a reply draft from an imported message.
- Approve and send one test reply.
- Confirm exact draft id, SMTP receipt, and no duplicate send on retry.

Social/community:

- Start with Skool notification emails, manual copies, screenshots, or exports.
- Build a ranked interaction-opportunity digest.
- Include two or three copy/paste options per recommended interaction.
- Include original-post ideas when Marcus sees repeated pain points or questions.
- Keep initial workflow watch-and-draft-only.
- Store recommendations as internal opportunity artifacts, not provider sends.
- Add manual-post confirmation before claiming anything was posted.

Zoom:

- Use calendar invitation to route meeting context to Marcus's mailbox.
- Ingest transcript/recording after the call.
- Generate note, decision, and follow-up artifacts.
- Add live assistant attendance only after consent and identity requirements are satisfied.

## Completeness Audit

Ready now:

- Marcus has approval-gated outbound email capability through SMTP.
- Marcus has IMAP read/import primitives.
- Marcus has durable approval and receipt semantics for external communication.
- Marcus Mobile has exact-draft review patterns that can extend to inbox replies.

Not ready yet:

- No scheduled inbound mailbox watcher is documented as deployed.
- No Skool official integration is configured or verified.
- No social/community opportunity watcher is deployed.
- No Skool or social posting should be automated under current policy.
- No Zoom bot/live meeting participant is configured or consent-audited.
- No calendar connector is configured for automatic meeting prep in this repo.

Next build slice:

1. Add a scheduled email sync worker around `fetchImapMessages`.
2. Promote reply-needed inbound email into `external-actions` drafts.
3. Add a `social_opportunity` artifact type for Skool and future channels.
4. Add copy/paste option generation for comments, follow-up questions, and original post ideas.
5. Add transcript ingestion for Zoom notes before attempting live attendance.

Related: [[access-model]], [[execution-loop]], [[current-system-map]], [[implementation-roadmap]], [[context-memory]], [[completion-audit]].
