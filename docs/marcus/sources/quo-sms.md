# Quo SMS

Status: planned ingestion policy
Tags: #source #conversation #inbox

## Source

Quo SMS is the text-message provider for Marcus outbound text sends and inbound SMS webhook capture.

## Current Runtime

- Outbound text drafts use the approval-gated external-action path.
- A text is sent only after Mark approves the exact draft and a separate send call receives provider acceptance.
- Inbound SMS webhook handling exists at `POST /api/integrations/quo/sms`.
- Inbound SMS can be routed into inbox items with sender, recipient, thread key, business routing, and optional project match.
- Low-signal acknowledgements can be filtered before inbox capture.

## Storage Rule

Do not turn SMS threads into raw transcript dumps. Store the shortest useful operational facts:

- who was involved
- what project, client, person, money item, or schedule item it affects
- promise, decision, blocker, question, preference, risk, or follow-up
- source timestamp and thread identifier
- confidence and whether the project/person match was exact, mapped, or inferred

## Required Future Behavior

Automatic Quo SMS context should promote useful inbound messages into:

- inbox items when the destination is unclear
- project communications when the project match is exact
- conversation notes for durable summaries
- daily notes when the message materially changes the day
- project/person/client/money/schedule notes when the fact belongs there
- project evidence only when the message is relevant to project state or verification

## Retrieval Rule

When Mark asks what was discussed, what is waiting, who promised what, or what a client needs, Marcus should search Quo-derived conversation summaries alongside [[conversation-index]], [[current-status]], project notes, inbox items, project communications, tasks, and durable operations.

## Approval Boundary

Inbound reading and summarization do not authorize replies. Any outgoing text remains:

`summary/context -> draft -> exact approval -> provider send -> provider receipt`

## Related

- [[source-map]]
- [[conversation-index]]
- [[context-memory]]
- [[external-presence]]
- [[access-model]]
