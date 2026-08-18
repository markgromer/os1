# MARCUS Nervous System

Status: connective tissue, continuous loop, durable attention, outcome feedback, health telemetry, and initial cross-system receptors implemented locally

MARCUS is organized as a nervous system rather than a single brain. Specialized capabilities remain responsible for their own work. The nervous system carries observations between them, selects bounded responses, records outcomes, and raises consequential or uncertain conditions into attention.

## Operating Cycle

`sense -> normalize signal -> route pathways -> reflex or attention -> act through existing capability -> observe outcome -> retain learning -> repeat`

The implementation lives in `marcus/nervous_system/`:

- `signal.js` defines the shared signal envelope, provenance, confidence, severity, evidence, and trace identity.
- `signal_bus.js` routes exact or namespace-matched signals through priority-ordered pathways. One failed pathway does not stop later pathways.
- `signal_journal.js` maintains a bounded business-scoped `marcus-signal-journal.jsonl` under the existing data root.
- `operating_loop.js` runs non-overlapping sensory and homeostatic passes on a configurable interval.
- `attention_store.js` owns deduplicated open, acknowledged, deferred, resolved, and superseded attention with Mark/MARCUS/shared/external ownership.
- `outcome_ledger.js` stores pathway responses, authoritative evidence, corrections, and reusable-method flags.
- `reflex_engine.js` runs named deterministic reflexes, records their outcomes, and converts failed reflexes into attention instead of stopping the arc.

`server.js` starts the loop with runtime proprioception, durable-operation reconciliation, and deferred-attention reconciliation. Operation, evidence, project-decay, awareness, mission-memory, and browser-mission changes return to the bus as signals. Warning, critical, and low-confidence signals become deduplicated attention items. Existing operation authority, verification, recovery, and approval behavior remains authoritative.

Operational visibility is available at `/marcus-nervous-system.html`. `GET /api/marcus/nervous-system/status` returns loop health, recent signals, open attention, and recent outcomes. Authenticated routes also support attention transitions, outcome corrections, and an explicit cycle trigger.

## Compatibility Rule

The nervous system coordinates capabilities; it does not absorb their implementations. Providers, skills, operations, awareness, memory, evidence, and interfaces retain their existing contracts. Migration proceeds by emitting signals beside proven behavior, then moving only deterministic and reversible coordination into named pathways.

## Safety Boundaries

- Signals are observations, not authority to act.
- A pathway cannot bypass an operation approval or provider policy.
- Provider uncertainty continues to enter recovery rather than automatic retry.
- Model deliberation is reserved for novelty, conflict, low confidence, failed reflexes, and meaningful consequences.
- The loop skips overlapping cycles instead of accumulating work.

## Further Expansion

1. Add provider-specific sensors where direct provider webhooks are available.
2. Promote a corrected outcome into a new reflex only through explicit review; `reusable` is evidence, not automatic code generation.
3. Tune cadence and attention thresholds from production signal volume.
4. Fold the standalone nervous-system console into the main Visualizer after production behavior is verified.

Related: [[execution-loop]], [[current-system-map]], [[access-model]], [[implementation-roadmap]], [[skill-system]].
