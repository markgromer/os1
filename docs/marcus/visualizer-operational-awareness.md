# Visualizer Operational Awareness

Status: prototype direction locked, runtime awareness layer not yet implemented.

## Product Truth

The MARCUS Visualizer is Mark's persistent desktop window into what Marcus understands, monitors, questions, coordinates, and worries may be forgotten. It is not a project tracker that Mark maintains.

The governing model is:

`Marcus operational awareness -> visualized for Mark -> conversationally steerable by Mark`

Mark should not add projects, assign status, connect repositories, calculate decay, move cards, or keep the view accurate. Marcus owns discovery, reconciliation, lifecycle conclusions, uncertainty labels, recommendations, and Codex coordination.

## Phase 1 Assessment

Current useful pieces:

- `public/visualizer.html` is a static public demo served through Express static hosting. It has no live data connection and must be labeled as demo data.
- `marcus/evidence/evidence_types.js` defines normalized project evidence with source, type, timestamp, project registry id, confidence, provenance, repository, branch, commit, PR, deployment, workspace, Codex job, and operation references.
- `marcus/evidence/project_evidence_service.js` can collect trusted evidence from operations, Codex lifecycle callbacks, GitHub, desktop context/actions, browser verification, Cloudflare/Render-style deployment signals, Airtable-derived legacy state, and authenticated manual ingestion.
- `marcus/evidence/activity_engine.js` already computes project activity snapshots, current focus, stale projects, bottlenecks, confidence, weighted signals, risks, missing expected signals, and suggested actions.
- `marcus/projects/project_registry.js` stores durable registered projects with canonical name, aliases, repo, local workspace, deployments, documentation, commands, permissions, and metadata.
- `marcus/projects/project_resolver.js` resolves requests against registry records using names, aliases, repo names, domains, current project context, desktop context, recent Codex workspaces, notes, and recent operations.
- `marcus/operations` provides durable operation state, approvals, blockers, verification, recovery, provider actions, and concise summaries through `GET /api/operations/summary`.
- `marcus/providers/desktop_codex_adapter.js` stores local Codex job status, bounded events, final output, changed files, diff summary, follow-up, cancellation, and public monitor data.
- `marcus/operators/project_operator_service.js` can assemble audited project context and scoped Codex prompts from registry, legacy store, GitHub, desktop, mission memory, and recent conversation context.
- `public/mobile.html` and `client/mobile-operation-tracker.js` already show the pattern for read-only operation polling plus separate exact approval flows.

Missing capabilities:

- No canonical "Marcus awareness project" model exists above the registry. Today a registry record is often treated as the project boundary.
- No reconciliation service merges duplicate or overlapping signals into one initiative when a repository contains multiple initiatives or one initiative spans multiple systems.
- No durable per-project interpretation record stores "what I believe", "what changed", "what I am waiting on", explicit Mark corrections, conflicting evidence, uncertainty, or decay rationale.
- No project-scoped visualizer conversation endpoint exists. Existing Live chat can carry project context, but the UI does not yet send a scoped message with an awareness project id and classification intent.
- No Codex dispatch packet object exists as a first-class persisted artifact. The project operator builds prompts, but the visualizer needs a visible prepare -> dispatch -> active -> completed -> verified lifecycle.
- The current visualizer has only fixture state. It must not imply live Marcus awareness until adapters are connected incrementally.

Data-source map:

- Existing operations: `GET /api/operations/summary`, full operations via `GET /api/operations`, approvals through operation approval routes.
- Existing project registry: `GET /api/project-registry` and resolver route `POST /api/project-registry/resolve`.
- Existing evidence: `GET /api/project-evidence`, `GET /api/project-activity`, `GET /api/project-activity/stale`, and `GET /api/project-activity/bottlenecks`.
- Existing Codex jobs: `GET /api/codex/jobs` for desktop Codex jobs and `GET /api/codex-monitor/jobs/:jobId` for token-scoped monitor data.
- Existing conversation/operator route: `POST /api/marcus/live/chat`.
- Existing health/capability route: `GET /api/marcus/operator-health`.

Risks and uncertainties:

- Repository identity is not sufficient project identity. The visualizer must not collapse all activity in `markgromer/os1` or `markgromer/Reggie` into one project.
- Current decay logic is useful but activity-oriented; attention recovery needs stronger semantics around stated objectives, explicit pauses, pending approvals, open PRs, external dependencies, and completion evidence.
- Manual evidence exists but should not become a backdoor for Mark-administered tracker fields.
- Operation summaries intentionally redact detail. The visualizer may need a safe expanded view without exposing prompts, patches, credentials, or raw command noise by default.
- The first live connection should remain visibly partial until every conclusion shown can cite evidence or be labeled inference.

## Phase 2 Architecture

### Canonical Awareness Project

Create a durable `MarcusAwarenessProject` layer separate from the registry:

- `id`: stable awareness id.
- `canonicalIdentity`: normalized title plus durable aliases and source fingerprints.
- `displayName`: human-readable name.
- `objectiveBelief`: Marcus's current interpretation of what Mark is trying to accomplish.
- `systems`: repositories, branches, workspaces, deployments, docs, operations, Codex jobs, and external services involved.
- `state`: one of `moving`, `monitoring`, `waiting_on_codex`, `waiting_on_mark`, `waiting_external`, `blocked`, `quiet`, `cooling`, `losing_thread`, `intentionally_dormant`, `completed`, `archived`.
- `confidence`: numeric score plus `high`, `medium`, or `low`.
- `stateBasis`: `observed`, `inferred`, `explicit_mark_instruction`, or mixed.
- `lastMeaningfulActivityAt` and `lastObservedActivityAt`.
- `latestMeaningfulChange`.
- `currentActivity`.
- `blockerOrDependency`.
- `likelyNextStep`.
- `neededNext`: `marcus`, `mark`, `codex`, `external`, or `none`.
- `evidenceRefs`, `conflictingEvidenceRefs`, and `uncertaintyNotes`.
- `decayState`, `decayReason`, and `recommendedRecoveryOutcome`.
- `conversationDecisions` and explicit corrections from Mark.
- `archivalPolicy`: active, intentionally dormant, completed, or archived without loss of context.

### Evidence Model

Reuse normalized evidence records and add an awareness association wrapper:

- `evidenceId`
- `source`
- `observedAt`
- `eventAt`
- `rawReference`
- `normalizedEvent`
- `associationCandidates`
- `selectedAwarenessProjectId`
- `associationConfidence`
- `claimType`: `observed_fact`, `inference`, `mark_statement`, `system_interpretation`
- `supports`, `contradicts`, or `updates` relationship to current project belief.

### Reconciliation Rules

1. Strong identity matches: explicit registry id, operation projectRegistryId, desktop workspace proof, exact GitHub repo plus operation branch, or known Codex job binding.
2. Medium identity matches: alias/domain/name match with supporting evidence from recent conversation, desktop workspace, branch name, PR title, or docs.
3. Split signals when one repository contains distinct objectives, branches, PR titles, or conversation-scoped operations.
4. Merge signals when different systems cite the same objective, same operation, same Codex job, same approved workspace, or explicit Mark correction.
5. Retain conflicts instead of overwriting. A Mark correction can supersede Marcus interpretation but does not delete contradictory evidence.

### Confidence Model

Confidence should increase with independent trusted sources, recent observed evidence, exact operation/Codex bindings, explicit Mark confirmations, and successful verification. It should decrease with stale evidence, single-source inference, conflicting records, ambiguous repo association, missing expected signals, and unverified completion claims.

UI rule: inferred conclusions must say "I think", "I may", or show low/medium confidence. Verified evidence can use firmer language.

### Decay Model

Decay is attention recovery, not visual fading.

Marcus should evaluate:

- unfinished Codex work;
- previously stated next step;
- open branch or PR;
- waiting approval;
- external dependency;
- explicit intentional pause;
- completed objective with verification;
- possible work outside GitHub;
- project importance;
- whether evidence is sufficient.

The recovery section is titled `I MAY BE LOSING THE THREAD` and should recommend exactly one of:

- Recover it now.
- Ask Mark for clarification.
- Continue monitoring.
- Treat it as intentionally dormant.
- Verify completion.
- Archive it while preserving context.

### Project-Scoped Conversation

Each project row exposes `Talk to MARCUS`. This opens a lightweight conversation scoped to the awareness project, not an edit form.

Incoming messages are classified as:

- question;
- new context or correction;
- recommendation from Mark;
- request to change Marcus attention state;
- request for Marcus to coordinate Codex work;
- consequential action requiring approval.

Marcus then updates his own understanding. "Remove this for now" becomes intentional dormancy, not deletion. "This is done" triggers completion verification, not blind completion. Corrections become explicit decisions while conflicts are preserved.

### Codex Dispatch Model

A project-scoped Codex work packet should include:

- awareness project id and registry binding if known;
- repository, branch, workspace, and deployment context;
- current objective;
- relevant decisions and Mark corrections;
- supporting and conflicting evidence;
- constraints and approval boundaries;
- last completed work;
- unfinished next step;
- acceptance criteria;
- required verification;
- required approvals.

The visible lifecycle is:

`Marcus recommends work -> Mark asks or approves -> Marcus prepares Codex packet -> Codex active -> work returned -> verification pending -> verified and incorporated`

Consequential actions remain gated by the existing approval model.

### Service Boundaries

Planned service modules:

- `marcus/awareness/awareness_store.js`: durable awareness projects, revisions, decisions, corrections, and archived context.
- `marcus/awareness/evidence_reconciler.js`: maps normalized evidence to awareness projects.
- `marcus/awareness/confidence.js`: confidence scoring and basis labels.
- `marcus/awareness/decay.js`: attention recovery classification and recommendations.
- `marcus/awareness/conversation.js`: project-scoped message classification and understanding updates.
- `marcus/awareness/codex_packet.js`: scoped Codex dispatch packet assembly.
- `marcus/api/awareness_routes.js`: read-only visualizer feed first, conversation and dispatch later.

Initial API shape:

- `GET /api/marcus/awareness`: compact projects, right-rail sections, system state, and latest events.
- `GET /api/marcus/awareness/projects/:id`: expanded project detail and evidence refs.
- `POST /api/marcus/awareness/projects/:id/chat`: scoped conversation with Marcus.
- `POST /api/marcus/awareness/projects/:id/codex-packet`: prepare a Codex packet without dispatching consequential work.

### Persistence Requirements

Store awareness data under the existing business data directory, separate from operations:

- `data/businesses/<business>/marcus-awareness.json`
- atomic writes and backup/recovery matching existing store patterns;
- revision numbers for conflict-safe updates;
- redacted evidence references, not raw secret-bearing content;
- archived and intentionally dormant records retained for recovery.

## Prototype Requirements

The fixture visualizer must:

- label itself as demonstration data;
- speak from Marcus's perspective;
- show `WHAT I'M TRACKING`, `I NEED MARK`, `I MAY BE LOSING THE THREAD`, `MY RECOMMENDATION`, and `WHAT I'VE SEEN CHANGING`;
- include `Talk to MARCUS` buttons instead of edit controls;
- show evidence-backed versus inferred conclusions;
- exercise active Codex work, required approval, conflicting evidence, ambiguous decay, intentional dormancy, and verified completion.

## Verification Plan

Future tests should cover duplicate project reconciliation, incorrect repository association, conflicting evidence, explicit Mark correction, intentional pause versus accidental decay, completion verification, approval requirements, project-scoped Codex dispatch, and recovery of archived or dormant context.

UI checks should cover desktop compact density, keyboard access, readable type, reduced motion, responsive behavior, visible fixture/live state, source-evidence inspection, and failure states.
