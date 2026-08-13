# Source Map

Status: draft
Tags: #source #system #project/marcus

Projects:
- [[marcus]]

## Purpose

This note defines the context sources Marcus should eventually use to understand Mark's schedule, conversations, workload, project state, current status, obligations, people, clients, money, and decisions.

## Source Classes

- Marcus conversations: explicit instructions, decisions, requests, approvals, corrections, and "remember this" commands.
- Obsidian notes: durable human-readable context graph.
- GitHub: repositories, branches, pull requests, commits, reviews, checks, and Codex/agent PRs.
- Local workspace and desktop context: active projects, files, windows, local Codex jobs, and non-secret text evidence.
- Calendar: appointments, deadlines, availability, reminders, and preparation windows.
- Email/text/chat: relationship context, client commitments, promises, inbound requests, and external communication state.
- Task/client systems: projects, tasks, client records, statuses, inbox items, and obligations.
- Money systems: invoices, payment promises, due dates, collection context, and financial risk.

## Capture Rules

- Store summaries and facts, not raw streams.
- Capture source, timestamp, confidence, and linked entity.
- Prefer explicit user correction over older context.
- Mark stale facts as outdated instead of silently deleting history.
- Never store credentials or secret values.
- Treat private conversation and money context as sensitive by default.

## Retrieval Rules

For broad questions like "what is going on", "what am I behind on", "what did I talk to them about", or "what matters today", Marcus should combine:

- [[current-status]]
- today's daily note
- relevant project notes
- schedule notes
- conversation notes
- workload notes
- client/person/money notes
- recent operations and PR evidence

## Open Work

- Implement connectors and authorization boundaries for each source.
- Add automated note-writing tests that prove sensitive data is filtered.
- Add freshness metadata so Marcus can distinguish current state from old history.
