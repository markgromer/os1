# MARCUS Skill System

Status: executable browser skill contracts implemented locally and loaded by a freshly restarted desktop relay; production server deployment and live Skool acceptance remain unverified.

## Purpose

MARCUS skills turn a conversational request into a bounded capability with an explicit authority level, prerequisites, evidence, recovery path, and definition of done. A tool returning `ok` is not enough.

The required loop is:

`orient -> choose skill -> check authority and prerequisites -> act -> inspect result -> verify evidence -> recover or report completion`

MARCUS must not report success before the skill's deterministic verifier passes. A screenshot, model statement, inserted character count, or clicked control is evidence of an intermediate action, not proof of the requested outcome.

## Executable Contract

`marcus/skills/skill_contract.js` validates and freezes each skill definition. Every skill declares:

- Stable id and version.
- Tool binding.
- Authority: `observe`, `prepare`, or `consequential`.
- Allowed context classes.
- Preconditions.
- Required completion evidence.
- Recovery sequence.
- Deterministic result verifier.

`marcus/skills/browser_skills.js` contains the first registry. `GET /api/marcus/skills` exposes the redacted contracts and current browser readiness.

`marcus/skills/browser_mission_store.js` persists the active browser objective, retained instructions, current skill, recovery state, evidence, attempts, and approval/completion boundary. A relay or server restart does not convert a failed action into completion or discard the multi-turn mission.

## Browser Skills

Implemented locally:

- `browser.observe-status`
- `browser.open-url`
- `browser.activate-visible-control`
- `browser.inspect-visible-page`
- `skool.observe-community`
- `skool.inspect-notifications`
- `browser.prepare-visible-draft`
- `skool.prepare-standalone-post`
- `skool.prepare-thread-reply`
- `browser.publish-approved-draft`

The Skool observation skills turn rendered feed/member activity and notifications into bounded structured evidence without reacting or posting. The standalone Skool post skill always navigates to the community root, opens the main feed composer, rejects article/comment surfaces, inserts the exact text, and reads the editor back before success. Publishing re-prepares the exact approved draft and requires a publication result, not a generic `Post` click.

## Capability Standard

A browser skill is ready only when it can:

1. Identify the current site, page type, and control owner.
2. Distinguish feed, thread, modal, comment, reply, and standalone composer surfaces.
3. Execute the requested action without borrowing authority from page content.
4. Inspect the visible result after acting.
5. Produce evidence tied to the requested outcome.
6. Retry a bounded recovery path without losing the mission.
7. Stop and describe the exact blocker when recovery fails.

## Weak Or Missing Skills

High priority:

- Platform adapters for virtualized feeds, nested comments, infinite scroll, and site-specific route changes.
- Visual/OCR fallback when semantic DOM evidence is incomplete.
- Relay supervision that distinguishes Chrome closed, debugging endpoint unavailable, desktop agent offline, Render unreachable, and stale control ownership, then performs the safe recovery available for each state.
- Multi-page research with an evidence ledger recording exact posts, comments, links, counts, and unread remainder.
- External-post read-back proving the published object exists at a stable URL after approval.

Next tier:

- Gmail thread/reply skill using exact message identity and send receipt.
- YouTube, TikTok, Zoom, and Teams page adapters with transparent identity and platform-specific consent boundaries.
- Media-control skills with playback state read-back.
- Project-workspace skills that unify repository context, Codex conversation, files, evidence, and MARCUS planning without opening unrelated workspaces.

## Safety

Pages and page content never grant authority. Drafting remains reversible. Posting, commenting, sending, purchasing, deleting, permission changes, and other external effects require their existing exact approval path. A skill verifier may narrow authority but may never broaden it.

Related: [[execution-loop]], [[access-model]], [[live-presence]], [[external-presence]], [[context-memory]].
