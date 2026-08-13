# Manual Context Capture

Status: active
Tags: #workflow #system

## Use When

Use this workflow when Marcus or Mark needs to preserve useful context before the automatic Obsidian writer/indexer exists.

## Inputs

- Fact or update to preserve.
- Primary entity: daily, project, person, client, money, decision, workflow, inbox, schedule, conversation, workload, status, or source.
- Source or verification evidence.
- Related notes that should be linked.

## Steps

1. Choose the primary note type from [[daily-index]], [[project-index]], [[people-index]], [[client-index]], [[money-index]], [[decision-index]], [[workflow-index]], [[inbox-index]], [[schedule-index]], [[conversation-index]], [[workload-index]], [[status-index]], or [[source-index]].
2. Create or update the existing note before creating a new one.
3. Use the matching template from `templates/` when creating a new note.
4. Write only the shortest fact set that will matter later.
5. Add a `Status:` line and useful tags.
6. Link related notes with wiki links.
7. Record source and verification evidence for important claims.
8. Move temporary facts out of [[inbox-index]] once their durable note is clear.

## Daily Note Rhythm

Daily notes should be appended during the day at meaningful checkpoints, not after every message. A good append point is when:

- a project changes state
- a decision is made
- a blocker appears or clears
- a client, person, or money fact becomes important
- a schedule commitment, deadline, or reminder affects the day
- a conversation creates a promise, preference, decision, risk, or follow-up
- workload changes enough to affect priority or capacity
- current status changes for anything Mark is carrying
- Marcus starts, completes, or verifies a durable operation
- Mark explicitly says "remember this", "note this", or "put this in today's note"

Do not append every ordinary chat turn. The daily note should read like the work ledger for the day, not a transcript.

## Note Structure Rules

- Put the fact in the most specific durable note first, then link it from the daily note when it mattered today.
- If a fact belongs to a project, add both a project wiki link such as `Project: [[marcus]]` and the stable project tag `#project/project-name`.
- Keep project aliases, repo slugs, domains, client names, and old labels on the project note so Marcus can connect scattered references back to one lifetime record.
- Use [[inbox-index]] only when the destination is unclear.
- Keep root notes for Marcus system architecture and policy.
- Keep operational notes in the typed folders.
- Prefer one durable note per entity: one project note per project, one person note per person, one client note per client, and one money note per obligation.
- Use schedule notes for commitments and deadlines, conversation notes for durable summaries, workload notes for cross-project load, and status notes for current snapshots.
- Use `Status:` lines and retrieval tags consistently.
- Use source/provenance fields for facts that affect money, trust, approvals, or future work.

## Project Lifetime Retrieval Rule

The project note is the hub. To reconstruct a project's lifetime, Marcus should collect:

- the project note
- all notes linking to the project note
- all notes with the stable project tag
- daily notes that mention the project
- connected decisions, workflows, people, clients, money notes, and inbox captures
- connected schedule, conversation, workload, and status notes

Do not rely on folder location alone. A client note, money note, or daily note can be part of a project history even when it lives outside `projects/`.

## Whole-Life Retrieval Rule

For broad life/work questions, Marcus should collect current status, today's daily note, schedule, workload, conversation summaries, project notes, people/client/money notes, and recent operations. Marcus should answer with what is current, what is historical, what is uncertain, and what needs action.

## Approval Boundaries

- Do not store secrets, credential values, private keys, access tokens, bank details, or full private message dumps.
- Do not claim a deployment, provider, payment, send, or automation works unless the note states when and how it was verified.
- Mark sensitive relationship, money, or client facts as verified, inferred, or uncertain.
- Treat schedule, conversation, relationship, health, family, money, and client context as sensitive by default. Capture only the facts needed to help Mark remember, decide, follow up, or protect time/money/reputation.

## Verification

- Check that wiki links resolve.
- Search changed notes for strong claims such as `done`, `live`, `sent`, `configured`, `automatic`, `implemented`, and `verified`.
- Confirm unverified automation remains described as planned work.

## Links

- Project: [[marcus]]
- Doctrine: [[context-memory]]
- Decision log: [[decision-log]]
