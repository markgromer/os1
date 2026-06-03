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
- explicit importance
- money, relationship, and risk impact
- deadline proximity
- client relationship and financial risk
- blocked work, stale work, delayed replies, and revision loops
- active vs historical project status
- whether Mark is required
- whether work is delegated or autonomously preparable
- signal expiry
- confidence and noise suppression

Signals are bucketed into `interrupt_now`, `today`, `soon`, `waiting`, `delegated`, `monitor`, or `archive/noise`.

Normalized signals now carry the first-class signal contract expected by the MARCUS command layer: `id`, `title`, `summary`, `source`, `relatedEntities`, `urgency`, `importance`, `confidence`, `createdAt`, `updatedAt`, `expiresAt`, `recommendedAction`, impact fields, evidence/source refs, and dismiss/snooze/convert controls through the operational overlay.

The ActiveBrief compresses semantically duplicated scored signals before building attention queues. Cross-type rows that represent the same visible work item, such as a task and system/website signal with the same business and title, collapse into one higher-confidence attention item with `duplicateCount` and `duplicateIds` preserved for explanation. This keeps "what matters" from repeating the same concern under different database categories.

Signal generation now includes operational system context beyond projects/tasks/messages: website, domain, deployment, automation, integration, billing, invoice, and payment language in tasks, messages, and client records is normalized into `Website`, `Tool`, `Payment`, or `SystemSignal` objects. Settings-level health also emits signals for AI routing, Google credentials, and pending automation approvals so Systems and Signals can surface disconnected tools or approval queues.

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
- session briefing
- communication intelligence: waiting on Mark, waiting on others, draftable replies, follow-ups due, unusual silence, and possible missed opportunities
- decision queue: ranked approval points, blockers, confirmations, and other items where Mark must choose
- ignore queue: low-signal, dormant, historical, or below-threshold items that can safely wait
- system health
- memory pulse
- world model summary and relationships
- action queue lifecycle scaffolding
- suppressed low-signal count
- confidence and narrative summary

The route keeps compatibility fields (`projects`, `conversations`, `tasks`, `messageDrafts`, `stats`) so older frontend code still works while the HUD can render richer structured sections.

The world model now includes operational entities beyond legacy records: `signals`, `decisions`, `actions`, `systems`, and `memory` are searchable alongside people, clients, businesses, projects, tasks, and messages. Relationships connect signals to their related entities, decisions to source signals/actions, actions to prepared source signals, and memory records to the entities they describe.

The frontend now layers local operator controls over the brief:

- signal controls: dismiss, snooze, and convert-to-action
- memory controls: mark important, pin, archive, mark outdated, and forget
- work-state controls: keep active, known history, and complete/archive
- focus controls: pin or clear the current focus lane without mutating project history
- proactive modes: quiet, normal, aggressive, focus, and away
- ambient presence state based on attention items, approval drafts, and system warnings

The MARCUS navigation now uses operational IA rather than database-table IA:

- Command, Now, Focus, and Signals are attention surfaces.
- People is a world-model relationship view with legacy clients still reachable.
- Work separates active work from known history with legacy projects still reachable.
- Actions exposes suggested, draft, approved, completed, and dismissed action lifecycle lanes without external execution.
- Systems and Memory expose operations and knowledge transparency.
- Control centralizes proactive mode, saved signal controls, memory rules, and persisted lifecycle overlays while preserving legacy settings access.

Command is now the default home entry. Business switching, auth return, project cleanup/move flows, popout sync defaults, and first-paint shell title route back to Command or Work instead of the legacy dashboard; the old dashboard remains available only through explicit legacy navigation or `?view=dashboard`.

The floating command drawer is ambient by default instead of open on first paint. The Command Surface remains the primary experience, while the global Command presence button, proactive nudges, item inspection, and command shortcuts intentionally open the drawer when a conversational transcript is useful.

The global MARCUS presence now exposes explicit operational states instead of a generic chat label. Runtime thinking/responding states take priority, followed by muted, system warning, approval, signal, focus, quiet/away, and idle command states. The header label, aria label, badge tone, orb class, and hover peek all derive from this state so the presence can represent listening, thinking, meaningful signal, approval needed, running action, system warning, quiet mode, focus mode, mute, and minimize without dominating the UI.

The keyboard command palette is aligned to this IA. It prioritizes Command/Now/Focus/Signals/People/Work/Actions/Systems/Memory/Control, operational commands like "what matters", "what needs a decision", "what can wait", "brief me", proactive modes, entity search, and live attention/decision/action inspection entries. Legacy dashboard/inbox/project actions remain available but no longer define the palette language.

The primary rail now stays focused on the operating-intelligence IA. Legacy inbox capture and Slack assignment remain reachable through command/palette or legacy views, but they are not first-class rail or header affordances on Command, Now, Focus, Signals, People, Work, Actions, Systems, Memory, or Control.

Operator controls are persisted in `data/marcus-operational-controls.json` through `/api/marcus/operational-controls/*`. The source stores remain unchanged; controls are a server-side overlay on top of generated signals, memory, and actions so future execution can plug in without mutating project history prematurely.

`/api/marcus/active-brief` applies this overlay before returning data. Dismissed, snoozed, and converted signals are removed from active attention arrays; memory records can be marked important, pinned, archived, marked outdated, or forgotten; project activity can be kept active, moved to known history, or marked complete/archive; focus lanes can be pinned or cleared; action lifecycle controls are merged into `actionQueue`; and the response includes `controlledAttention`, `activeActionQueue`, `projectControlPolicy`, `focusPolicy`, and `attentionPolicy` with counts for mode/control suppression.

Session check-ins are persisted in `data/marcus-session-state.json`. `/api/marcus/active-brief` includes `sessionContext` and enriches `sessionBriefing.changedSinceLastTime` with items changed since the last check-in. The base ActiveBrief no longer invents changed-since-last-time rows from top priorities; if no baseline exists, the changed list stays empty until the session-state overlay can prove a real delta. `POST /api/marcus/session/check-in` records the current brief as the new baseline without mutating projects, messages, tasks, memory, or signals. Can-ignore briefing rows remain structured objects with source, confidence, reason, and recommended action.

`/api/marcus/command` provides deterministic operational responses backed by the controlled ActiveBrief before falling back to generic AI chat. It currently handles common command-surface intents such as what matters, what Mark is forgetting, waiting-on-Mark, stale work, confidence/ranking explanation, system health, memory review, briefing, and action queue review. Responses include `cards`, `suggestedActions`, and `evidence` so the command drawer can render short readouts with why/source pills and action buttons instead of behaving like a plain chatbot transcript.

The command endpoint also covers operational comparison and source inspection language: blocked projects/work, stale clients/silent relationships, compare X and Y, show sources/evidence, and "what changed with X" are routed into deterministic card-backed responses rather than long generic chat. These responses reuse world-model inspection, session changed-since-check-in rows, source counts, and existing card controls.

Brief-specified command examples are handled as operational intents. "Draft the reply but don't send" creates a draft-reply action instead of opening communication inspection; "remind me..." and "create a follow-up" create clean approval-gated action titles; and "what did I say about X last time" routes through entity context so memory, related signals, and changed-since-check-in context can answer when available.

The Command Surface now includes its own persistent command bar with fill chips for changed-since-check-in, entity context, follow-up creation, memory correction, and draft-only replies. It submits through the same deterministic command endpoint, so the first screen can ask, inspect, decide, draft, update memory, and create action intent without making the floating drawer the primary product.

The Now view now renders a dedicated session briefing strip above the general operating layout. It summarizes changed-since-check-in, what needs Mark, what is waiting elsewhere, top prepared actions, systems, opportunities, active attention, and can-ignore items, with check-in and brief-me controls.

The Command and Now views now render a top-level communication intelligence strip. It treats communication as obligation tracking rather than an inbox by separating waiting-on-Mark, waiting-elsewhere, draftable replies, follow-ups due, unusual silence, and missed opportunities with direct command shortcuts for inspection, reply drafting, and follow-up creation.

The People view now renders relationship intelligence rather than a contact directory. It summarizes people/entities, relationship graph edges, waiting-on-Mark, waiting-elsewhere, follow-ups, unusual silence, missed opportunities, and relationship-specific signals; individual entity cards show related relationship, signal, and action counts with direct context inspection.

The Focus view now renders a dedicated focus lane strip. It shows the pinned or inferred focus, active lanes, blockers, focus-specific next actions, and what to ignore while focused, with controls for Focus mode, "what next?", ignore inspection, and clearing a pinned lane.

The Signals view now renders a dedicated signal stream strip. It summarizes urgency buckets, source mix, confidence, high-impact items, risks, opportunities, waiting-on-Mark signals, and operator controls so Signals reads as the intake and ranking system rather than another copy of the generic attention queue.

The Memory view now renders a dedicated memory transparency strip. It shows durable records, source mix, average confidence, new facts, stale or uncertain assumptions, conflict counts, related entities, and important/pin/archive/outdated/forget/correct controls so memory can be inspected and corrected before stale context pollutes attention.

The Systems view now renders a dedicated systems health strip. It separates operational infrastructure from project work by showing system status counts, source mix, credential/setup warnings, recommended draft actions, inspectable system records, and command shortcuts for health, credentials, and can-wait decisions.

The ActiveBrief includes `decisionQueue`, which turns waiting-on-Mark signals, blockers, approval-gated drafts, and explicit decision language into ranked decision prompts. The command surface can answer "What needs a decision?" directly and the home view shows decisions as their own operational lane instead of burying them in generic attention.

Decision cards now carry source signal/action IDs through to the UI. Decisions created from approval-gated actions can be approved or dismissed directly; decisions created from signals can be snoozed, dismissed, or converted to an action draft from the same card.

The ActiveBrief also includes `ignoreQueue`, which makes "what can wait?" explicit. It is derived from suppressed low-signal records, historical/dormant work, and below-threshold items, with a reason and recommended action so MARCUS can explain what it is intentionally not putting in front of Mark.

Memory correction language such as "that is wrong", "forget this", "archive this memory", "pin this", "this is important", and "this is outdated" routes to a deterministic memory-correction response. MARCUS presents candidate memory records and lets the operator apply important/pin/archive/outdated/forget controls from the command response cards; ambiguous correction commands do not mutate source records directly.

Work-state correction language such as "keep this active", "reactivate this", "project is done", and "archive this" routes to a deterministic work-state response. MARCUS presents candidate work items and applies project controls as an overlay so old work can stop competing for attention without deleting historical context.

Entity context is exposed through `/api/marcus/entities/search` and `/api/marcus/entities/inspect`. These endpoints use the ActiveBrief world model to find people, clients, projects, tasks, messages, businesses, signals, decisions, actions, systems, memory, and their relationships, enabling commands like "show me everything related to Jeremy" without treating the app as a generic project list. Inspect responses return typed related groups for signals, decisions, actions, systems, and memory so command answers can show the actual operational context around an entity.

Item-level inspection is exposed through `/api/marcus/items/inspect`. Attention cards use this for "Why" inspection, returning the item summary, ranking reason, source, confidence, recommended action, related context, evidence, and available controls without routing through generic chat.

Action lifecycle transitions are persisted through `/api/marcus/actions/:id/transition`. Supported states are `suggested_action`, `draft_action`, `approved_action`, `completed_action`, and `dismissed_action`. Approval records intent only; approved actions carry `executionStatus: approved_pending_execution` and `executionDeferred: true` so future execution tooling can pick them up without implying anything already ran.

The Actions view renders those lifecycle states as separate lanes. It can approve, complete, or dismiss a tracked action, inspect source signals, and create follow-up or draft-reply intents through the command layer while keeping execution explicitly out of scope for this redesign phase.

Manual action creation is also supported through command language such as "create a follow-up", "remind me", "turn this into an action", and "draft a reply". These commands create `action:manual:*` entries in the same operational-controls action overlay and surface them through `activeActionQueue`; they remain approval-gated and do not execute external side effects.

Project creation language such as "turn this into a project" and "create a project for ..." creates a `create_project_draft` action in the same queue. It does not mutate project stores during this redesign phase; it records the intended project draft so future approved execution can create the real project cleanly.

Proactive mode can be changed from the command surface with explicit mode language such as "go quiet", "set focus mode", "switch to aggressive mode", "normal mode", and "away mode". This writes the same `proactiveMode` overlay used by the Control view and returns the resulting `attentionPolicy` in the command response.

Current focus can be pinned from command language such as "set current focus to PoopSites redesign" or cleared with "clear current focus". Pinned focus writes to the `focus` control overlay, overrides inferred focus in the ActiveBrief, and affects Focus-mode attention filtering.

## Project activity handling

Projects are classified as:

- `active`
- `warming`
- `waiting`
- `parked`
- `historical`
- `archived`

The heuristic favors recent activity, open tasks, deadlines, unread/client communication, explicit pins, and desktop focus. By default, projects without meaningful activity in the last 14 days are suppressed from active attention unless pinned or otherwise made current.

Project activity also exposes the richer `projectState` vocabulary from the redesign brief: `idea`, `active`, `waiting_on_mark`, `waiting_on_client`, `waiting_on_team`, `blocked`, `review`, `launched`, `complete`, `dormant`, and `archived`. `activityStatus` remains as a compatibility bucket for existing filtering, while `projectState` is used by the Work view and command cards to explain the actual operating state.

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
