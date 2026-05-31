# MARCUS Operational Intelligence

This layer turns existing project, task, inbox, business, and desktop context into a structured ActiveBrief. Doctrine guides behavior, but intelligence state drives the UI.

## Domain model

`domain.js` normalizes data into JSON-compatible operational objects:

- `Project`, `Client`, `Business`, `Conversation`, `Task`, `Blocker`, `Risk`, `Opportunity`, `Decision`, `ActionDraft`, `SystemSignal`, and `ActiveBrief`
- Shared fields include identity, business/project/client links, status, owner, priority, urgency, confidence, timestamps, next action, autonomy/approval flags, and evidence/source refs.

No database is introduced. The normalizer reads the existing per-business task stores and keeps all objects lightweight.

## Attention scoring

`attention_scoring.js` scores normalized signals using MARCUS Attention Radar behavior:

- urgency and priority
- deadline proximity
- client relationship and financial risk
- blocked work, stale work, delayed replies, and revision loops
- active vs historical project status
- whether Mark is required
- whether work is delegated or autonomously preparable
- confidence and noise suppression

Signals are bucketed into `interrupt_now`, `today`, `soon`, `waiting`, `delegated`, `monitor`, or `archive/noise`.

## ActiveBrief generation

`active_brief.js` creates the server-side ActiveBrief:

- current focus
- top priorities
- urgent interrupts
- waiting-on-Mark, client, and team queues
- stalled projects
- risks and opportunities
- prepared action drafts
- suggested delegations
- suppressed low-signal count
- confidence and narrative summary

The route keeps compatibility fields (`projects`, `conversations`, `tasks`, `messageDrafts`, `stats`) so older frontend code still works while the HUD can render richer structured sections.

## Project activity handling

Projects are classified as:

- `active`
- `warming`
- `waiting`
- `parked`
- `historical`
- `archived`

The heuristic favors recent activity, open tasks, deadlines, unread/client communication, explicit pins, and desktop focus. Old projects without live pressure are suppressed instead of competing with current work.

## Future inputs

Future integrations should feed normalized signals here instead of directly driving the UI:

- email and SMS thread state
- Slack mentions
- meeting transcripts
- invoices and billing events
- deployment/build health
- website monitoring
- calendar commitments
- desktop/file activity

The next useful step is to attach stronger project/client IDs to conversations so MARCUS can reason across channels with less ambiguity.
