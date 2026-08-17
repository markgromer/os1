# Operator Intelligence Layer

Status: implemented locally; production deployment not verified

## Purpose

This layer makes Marcus sound and operate like a continuous teammate without giving the Realtime voice session independent authority.

## Capabilities

### Human spoken references

`marcus/voice/spoken_reference.js` projects durable operator results into a speech-safe object. Raw operation IDs, record IDs, hashes, URLs, and pull-request numbers remain in durable state and telemetry but do not enter the tool result seen by the voice model. Marcus instead uses project, artifact, purpose, and recency labels.

### Relationship continuity

`marcus/voice/continuity_brief.js` creates a bounded brief from active mission memory, the active project, and recent Live exchanges. Public Assistant and Meeting Shadow modes exclude relationship memories and private recent exchanges. The brief enters both the server-created Realtime session and the browser SDK agent instructions.

### Job priming

`marcus/jobs/job_priming.js` routes recurring requests to a narrow manifest of context to load and unrelated context to exclude. The selected manifest is prepended to the project execution brief and recorded in durable operation metadata.

### Semantic interruption checks

`marcus/voice/conversation_alignment.js` compares an interrupted request, the redirected request, and the answer. It fails when the answer follows stale subject matter more strongly than the new turn.

### Winning-method memory

`marcus/memory/winning_method_store.js` records a compact dead-end, successful method, verification summary, and last-verified time only when an operation with failure or recovery history later completes. Ordinary clean successes do not accrete into this store.

### Locked decisions

`marcus/memory/locked_decisions.js` detects change intent that overlaps an active locked decision. Project operation preparation returns `locked_decision_conflict` before creating an operation and asks whether Mark intends a permanent change or one-time exception.

## Demo

Run `npm.cmd run demo:marcus`. The demo covers all six capabilities with visible pass/fail evidence.

## Related Notes

- [[voice-interface]]
- [[context-memory]]
- [[execution-loop]]
- [[current-system-map]]
- [[decision-log]]
