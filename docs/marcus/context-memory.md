# Context Memory

Status: doctrine and manual note structure. Marcus currently has durable mission memory, project-scoped conversation requirements, active brief intelligence, relationship signals, and this Obsidian-compatible vault. The vault now has manual indexes and templates for daily, project, person, client, money, decision, workflow, inbox, schedule, conversation, workload, status, and source notes. A full automatic Obsidian write/index loop remains planned work unless a note is manually maintained here.

## Objective

Marcus should live in a world of context, not isolated chat instances. Mark should be able to ask what happened on a given day, what any project needs, who owes money, what his schedule looks like, what was discussed with someone, what is on his plate, what is stale, what a client tends to care about, what was decided last time, or how two projects relate, and Marcus should retrieve the relevant context quickly instead of making Mark reconstruct it.

## Voice And Personality Context

Marcus should sound like Marcus across text and voice: concise, sharp, naturally human, lightly sarcastic when appropriate, and protective of Mark's time and resources. Tone may shift with the situation: serious for risk, dry when the point is obvious, warm when the moment is personal, blunt when waste is showing up, and pleased when something actually lands. Humor must never obscure uncertainty, approval boundaries, money risk, or bad news.

## Context Surfaces

Durable context should be organized into linked Markdown notes where possible:

- Daily notes: [[daily-index]] tracks what Mark worked on, important outcomes, unresolved threads, follow-ups, and decisions.
- Project notes: [[project-index]] tracks objective, current state, latest meaningful update, repo/site/provider links, blockers, decisions, requirements, and next action.
- People notes: [[people-index]] tracks relationship context, preferences, trust signals, communication style, obligations, and unresolved promises.
- Client notes: [[client-index]] tracks organizations, buyers, vendors, recurring expectations, projects, and open obligations.
- Money notes: [[money-index]] tracks invoices, payment status, who owes what, risk, due dates, and collection context.
- Decision notes: [[decision-index]] tracks standalone operational decisions; [[decision-log]] remains the Marcus system decision log.
- Workflow notes: [[workflow-index]] tracks reusable workflows, prompts, infrastructure patterns, tools, and lessons.
- Inbox notes: [[inbox-index]] temporarily holds unsorted facts until they are moved to the correct durable note.
- Schedule notes: [[schedule-index]] tracks appointments, deadlines, reminders, preparation windows, and timing constraints.
- Conversation notes: [[conversation-index]] tracks durable facts from calls, texts, emails, meetings, chats, and Marcus conversations without storing transcript dumps.
- Workload notes: [[workload-index]] tracks cross-project load, waiting states, blockers, priority, and capacity risk.
- Status notes: [[status-index]] tracks current snapshots, including active, waiting, blocked, stale, and next-action states.
- Source notes: [[source-index]] documents where Marcus can learn context from and the capture/approval boundaries for each source.

## Tagging

Use tags for retrieval and filtering, not decoration. Preferred tags include:

`#daily`, `#project`, `#client`, `#person`, `#relationship`, `#money`, `#invoice`, `#decision`, `#blocker`, `#follow-up`, `#workflow`, `#system`, `#preference`, `#risk`, `#inbox`, `#schedule`, `#conversation`, `#workload`, `#status`, `#source`, `#current`, `#waiting`, `#stale`

Use wiki links for local relationships, such as `[[current-system-map]]`, `[[execution-loop]]`, `[[voice-interface]]`, and project or person notes when they exist.

For project lifetime retrieval, every note that materially belongs to a project should include both:

- a wiki link to the project note, such as `Project: [[marcus]]`
- a stable project tag, such as `#project/marcus`

The project note should list aliases and external references so Marcus can connect related names, repo slugs, domains, client language, and older labels back to the same lifetime project record.

## Retrieval Standard

Before answering questions about work history, obligations, relationships, money, schedule, conversations, prior decisions, similar projects, current workload, or what Mark has been focused on, Marcus should search beyond the active project. Current active context is only one signal. Useful answers should combine active brief, durable mission memory, project requirements, client records, operations, inbox/client communication, desktop context, schedule/conversation/workload/status notes, and Obsidian notes when available.

## Storage Standard

Store the shortest note that preserves operational truth. Do not dump transcripts. Capture the facts that compound:

- what changed
- why it matters
- who is involved
- what is owed
- what decision was made
- what assumption is uncertain
- what should happen next
- which project, person, client, or system it links to
- when something is due or scheduled
- what was promised and by whom
- what is active, waiting, blocked, stale, or risky

Separate verified facts from inference. When new information conflicts with old context, prefer the latest explicit correction and mark the old context as outdated rather than silently deleting history.

## Note Placement

Use the folder that matches the primary entity:

- `daily/yyyy-mm-dd.md` for daily work summaries.
- `projects/project-name.md` for project state.
- `people/person-name.md` for individual relationship context.
- `clients/client-name.md` for organization or account context.
- `money/money-item.md` for invoices, payments, and obligations.
- `decisions/decision-title.md` for standalone operational decisions.
- `workflows/workflow-name.md` for reusable operating procedures.
- `inbox/topic-or-date.md` for temporary unsorted captures.
- `schedule/schedule-item.md` for appointments, deadlines, reminders, and preparation windows.
- `conversations/conversation-topic.md` for durable conversation summaries.
- `workload/workload-scope.md` for cross-project load and priority snapshots.
- `status/status-scope.md` for current-state snapshots.
- `sources/source-name.md` for source and connector rules.

Use lowercase hyphen-case filenames. Link across notes with wiki links. Start new manual notes from the matching template in `templates/`.

## Project Lifetime Retrieval

To answer "show me everything we have done on this project", Marcus should gather:

- the project note in `projects/`
- all notes linking to that project note
- all notes carrying the stable project tag
- daily notes that mention or link to the project
- decisions, workflows, money notes, client notes, people notes, inbox captures, and system docs connected by wiki links
- known repo, domain, provider, client, and alias references listed in the project note

The project note is the hub. Daily notes are the timeline. Other typed notes are the durable context around people, clients, money, decisions, and workflows.

## Whole-Life Operating Retrieval

To answer "what is going on", "what am I working on", "what am I behind on", "what did I talk to them about", or "what needs my attention", Marcus should gather:

- [[current-status]]
- today's daily note
- relevant project notes and project tags
- schedule notes for upcoming and recent commitments
- conversation notes for recent or related discussions
- workload notes for cross-project pressure and priority
- person, client, money, decision, workflow, and inbox notes
- recent operations, PRs, local workspace evidence, and source notes

Marcus should separate current state from historical evidence. Old notes are useful history, not proof that a status is still current.

## Open Implementation Work

- Add an automatic Obsidian writer for daily/project/person/client/money/decision/workflow/schedule/conversation/workload/status notes using the existing templates.
- Add a retrieval index that can search this vault by folders, tags, links, aliases, dates, entity names, source systems, freshness, and status.
- Connect money and invoice context from Airtable/client records into dedicated notes without exposing credentials.
- Connect schedule/calendar, approved communication summaries, workload/task records, local workspace activity, GitHub, and operations into the context graph.
- Add voice commands for "remember this", "note this under X", "what did we work on yesterday", "what is on my plate", "what did I talk to them about", "what is waiting", "what matters today", and "who owes me money" that route to the correct durable store.
- Add tests proving note creation avoids secrets, uses stable filenames, and preserves wiki links.
