# Decision Log

Status: active

## 2026-08-17: Private Voice Does Not Narrate Its Safety Net

Decision: Operator mode calls required tools silently and speaks after evidence exists. It does not pre-announce verification, safeguards, anti-duplication checks, or a later update. Private voice defaults to candid opinions, quick wit, and earned jabs; public modes retain their audience restrictions.

Consequence: Execution discipline remains enforced in code and tools without becoming the conversational personality. Training-wheel language is covered by a deterministic voice-style evaluation.

## 2026-08-17: Current Visible Pages Do Not Require Mark To Repeat The URL

Context: Mark asked MARCUS to look through the ScoopOS community while the live visualizer visibly showed that page. MARCUS incorrectly said he could not browse without an exact URL because his tools exposed only one viewport of status plus explicit navigation.

Decision: Add `marcus_browser_read`, a direct-request tool that scans up to 12 rendered viewports on the current approved page, deduplicates bounded text, excludes hidden/form/editable content, and restores the original scroll position. Explicitly instruct MARCUS to call status/read before claiming he cannot inspect the page already open; exact URLs remain necessary only for opening a different page.

Consequence: MARCUS can review the ScoopOS feed already in his window and answer content-strategy questions from real visible posts. The local acceptance read eight viewports and 17,412 characters. Page content remains untrusted context and does not grant action authority. Related: [[live-presence]], [[access-model]], [[current-system-map]].

## 2026-08-17: Route Only MARCUS Speech Into Zoom's Virtual Microphone

Context: MARCUS and Zoom run in the same dedicated Chrome profile. Sending all Chrome output to a virtual cable would feed incoming Zoom voices back into the meeting and create echo.

Decision: Give the Call Console a dedicated Realtime audio element and select its output with `setSinkId()`. Keep Zoom output on Mark's headphones, capture the Zoom tab with shared audio for Realtime input, route only the MARCUS audio element to Pack45's `Speakers (VB-Audio Virtual Cable)` playback endpoint (or older `CABLE Input` labels), and configure Zoom's MARCUS microphone as `CABLE Output`. Serve the sidecar at `/call-marcus.html`; keep `/obs-marcus.html` for compatibility and reserve OBS Studio for streaming/scene work.

Consequence: MARCUS can join the same Zoom meeting as a normal browser participant without the OBS desktop application or Zoom APIs, while maintaining a separable echo-resistant audio path. VB-CABLE still requires one administrator installation, reboot, and physical call acceptance. Related: [[live-presence]], [[current-system-map]], [[voice-interface]].

## 2026-08-17: Meeting Notes Persist Summaries, Not Transcript Dumps

Context: MARCUS could hear Realtime audio and read bounded live-page context, but stopping the sidecar discarded the recognized conversation. Mark requires every call to contribute useful notes to the Obsidian brain.

Decision: Keep at most 20,000 characters of recognized participant/MARCUS speech in OBS page memory. Every five minutes and on stop, send that bounded window to the existing transcript analyzer. Queue only the redacted derived summary, decisions, commitments, follow-ups, source, timing, and confidence to a dedicated desktop action. Confine atomic note writes to `docs/marcus/conversations/` and overwrite the same session note as checkpoints improve it.

Consequence: Live sessions create concise durable Obsidian conversation notes even when they are not tied to a project, while raw transcript text stays out of the vault and durable desktop queue. Important commitments remain reviewable AI-derived facts; entity linking and the first physical call remain acceptance work. Related: [[live-presence]], [[conversation-index]], [[context-memory]].

## 2026-08-17: Relay Bounded Visible Context, Not Browser Secrets

Context: Browser viewport control let Mark watch MARCUS navigate, but MARCUS Realtime could not automatically read live Zoom/Skool page text. The original `Open` command also navigated the active tab and displaced Gmail.

Decision: Make `Open` create a new Chrome tab. On an explicit Gmail/live-platform allowlist, extract only rendered text nodes currently inside the viewport, capped at 6,000 characters. Exclude form controls, editable regions, hidden elements, and off-screen text. Clear context during password focus and retain it only in the short-lived authenticated relay cache. Automatically send changed context to active Realtime sessions only for live platforms, never Gmail.

Consequence: MARCUS can retain concurrent Gmail, Zoom, and Skool tabs; inspect visible Gmail when explicitly requested; and read changing visible live-page context while listening through the existing OBS/Realtime audio path. Cookies, passwords, browser storage, form values, raw HTML, and unrestricted DOM remain outside the relay. Embedded/virtualized platform chat may still need dedicated adapters, and live-call consent plus physical audio acceptance remain required. Related: [[live-presence]], [[current-system-map]], [[access-model]].

## 2026-08-17: Use the Existing PC Bridge for Visible MARCUS Chrome

Context: Marcus needs to operate a normal, visible Chrome profile on Mark's PC while Mark watches and can take control from the hosted visualizer.

Decision: Extend `desktop-agent.cjs` with a localhost-only Chrome DevTools Protocol bridge in `desktop-marcus-browser.cjs`. Use the isolated MARCUS profile, stream compressed page viewports to the authenticated server, queue a narrow command set through the durable desktop action queue, and maintain one explicit Mark/MARCUS control owner. Suppress password-field frames and remote password typing. Use port `9333` because `9229` is already occupied by a Cloudflare Worker debugger on this PC.

Consequence: The hosted visualizer can display and steer the real MARCUS Chrome session without receiving cookies or profile storage. This is the browser foundation for Gmail, Skool, Zoom, YouTube, and TikTok, but it does not by itself complete audio routing, consent, visible chat observation, or live-call acceptance. Related: [[live-presence]], [[access-model]], [[execution-loop]], [[current-system-map]].

## 2026-08-17: Separate Machine Evidence From Spoken References

Decision: Durable results retain exact identifiers, while Realtime voice receives a speech-safe human projection. Relationship continuity is bounded and filtered from public modes. Recurring work loads request-specific priming manifests. Deliberate decisions may be marked locked and require an explicit permanent-change or one-time-exception choice before conflicting operations are created.

Consequence: Marcus can speak naturally without weakening evidence, authority, privacy, or approval boundaries. See [[operator-intelligence-layer]].

## 2026-08-15: Snark Is A Mode Permission, Not The Default Voice

Context: Mark wants Marcus to eventually join or support Zoom/OBS sessions, including playful demos where Marcus can be more fun, edgy, and snarky. The existing Realtime voice personality already permits dry humor, but the same prompt currently applies across private and potentially external contexts.

Decision: Define explicit communication modes in [[personality-modes]]. Keep Operator as the default private work mode. Treat Meeting Shadow and Public Assistant as the serious-call modes. Treat Demo and Roast Light as opt-in playful contexts for show-and-tell or friendly/internal sessions, not client-safe defaults. Snark may target broken workflows, vague strategy, meeting theater, tool chaos, and Mark-approved experiments, but not guests, clients, employees, private personal details, or protected/personal characteristics.

Consequence: Marcus can become more entertaining in controlled demos without training the whole system to be casually risky everywhere. The runtime now has shared prompt fragments, an environment-selected default mode, per-session client mode selection, a Marcus Mobile selector, an OBS/demo sidecar, and a spoken-command mode tool. Future work should add direct Zoom chat/transcript integration beyond pasted context or browser-supported capture and continue testing that Demo/Roast language stays out of Public Assistant while preserving all approval boundaries.

## 2026-08-15: Archive Removes Attention, Not Knowledge

Context: Mark needs a compact Visualizer that excludes inactive work while Marcus retains immediate access to every known project, including archived history.

Decision: Store canonical project lifecycle and indexed-memory metadata in a separate business-scoped awareness file. Treat archived, dormant, completed, and active attention as lifecycle concerns independent from memory retention. Replace browser-local Visualizer dismissal with authenticated server lifecycle updates. Keep historical records searchable and allow an explicitly named archived/completed project to resolve without reactivating it.

Consequence: The default dashboard can stay focused while `marcus.txt`, project notes, repository manifests, registry identity, operations, and evidence remain recoverable. Awareness and indexing do not add provider or filesystem authority; normal project trust, durable operations, approvals, and verification still apply.

## 2026-08-13: Marcus Voice Must Not Sound Like Generic ChatGPT

Context: Mark tested the Android Realtime voice using `gpt-realtime-2.1` and `cedar` and found the conversation style unacceptable: too much like a ChatGPT shell, too many polite assistant tails, too much recap of Mark's own request, and too much conversational padding. The problem is not only the acoustic voice; it is the spoken interaction contract.

Decision: Add explicit Realtime voice instructions that ban generic assistant filler, customer-support closers, "let me know" tails, recap-before-answer behavior, unnecessary next-step menus, and motivational padding. Marcus should answer the last thing Mark said, stop when the useful answer is complete, and acknowledge voice/style frustration briefly rather than explaining at length. Lower the Realtime session `max_output_tokens` from 1200 to 480 to reduce bloat pressure.

Consequence: Marcus's voice contract is now stricter and more opinionated. This does not prove the selected OpenAI voice/model is the final product fit; it creates a better baseline for testing whether prompt/style control is enough before moving primary voice identity to ElevenLabs or another stack.

## 2026-08-13: Accept The Original Trusted-Operator Goal

Context: The goal required durable memory, deep project audits, direct Codex work, GitHub and Cloudflare capability, approved messaging, Obsidian documentation, secure verification, Android voice, full-PC project use, durable hosting, and a from-scratch repository deployed to a live Worker. Earlier acceptance was incomplete because physical Android lifecycle gates and the new demo publication were still pending.

Decision: Accept the original goal after the combined production report reached 13/13, the installed Android session passed all eight voice gates plus explicit phone confirmation, the private demo repository and exact commit were read back from GitHub, the approved Wrangler deployment completed, and independent root and health requests returned HTTP 200. Preserve the original failed deployment operation as immutable evidence and use the completed corrective operation as the success record.

Consequence: The durable operator foundation is accepted. Automatic whole-context Obsidian writing/indexing, more connectors, and future voice refinements remain explicit product enhancements in [[context-memory]] and [[implementation-roadmap]], not hidden blockers or inflated claims about current behavior.

## 2026-08-13: Marcus Memory Covers The Whole Operating Context

Context: Mark clarified that Obsidian notes must cover everything Marcus and Mark work on, not just pull requests, Codex runs, or project history. Marcus should eventually know Mark's schedule, conversations, workload, current status, obligations, project state, people, clients, money, and decisions.

Decision: Expand the vault model with [[schedule-index]], [[conversation-index]], [[workload-index]], [[status-index]], and [[source-index]]. Keep notes concise and source-grounded. Store conversation summaries, not transcript dumps. Treat schedule, relationship, client, money, health, family, and private communication context as sensitive by default. Use [[current-status]] for current rollups and [[source-map]] for source/connector boundaries.

Consequence: Marcus now has a documented target memory model for whole-life operating context. Automatic ingestion, calendar/communication connectors, workload rollups, and current-status refresh remain planned work in [[context-memory]] until implemented and verified.

## 2026-08-13: Obsidian Context Uses Typed Note Folders And Templates

Context: Mark wants to set up the Obsidian notes the best way possible before implementing automatic memory writes. The vault already had Marcus system notes, but it did not yet have durable operational locations for daily work, projects, people, clients, money, standalone decisions, workflows, or unsorted inbox captures.

Decision: Keep the root `docs/marcus/` notes as the Marcus system layer. Add typed operational indexes for [[daily-index]], [[project-index]], [[people-index]], [[client-index]], [[money-index]], [[decision-index]], [[workflow-index]], and [[inbox-index]], plus matching templates for manual notes. Use lowercase hyphen-case filenames, wiki links, status lines, tags, source/provenance fields, and concise fact capture instead of transcript dumps.

Consequence: Manual notes now have stable places to live, and the future automatic writer/indexer has an explicit target structure. This is a documentation and organization change only; automatic note creation and retrieval remain planned work in [[context-memory]].

## 2026-08-13: Marcus Is A Context Partner, Not An Instance Chatbot

Context: Mark wants Marcus to feel like Marcus in voice and text: concise, naturally variable in tone, smart with dry humor, protective of time and resources, and aware across days, projects, people, clients, money, decisions, and relationships. The existing system has mission memory, active brief intelligence, client/project stores, operation history, desktop context, and an Obsidian-compatible documentation vault, but it does not yet automatically maintain a complete Obsidian context graph.

Decision: Update runtime personality and knowledge doctrine so Marcus speaks with natural tone, can use light sarcasm and dry humor when appropriate, protects Mark's time/money/reputation, and retrieves context beyond the active project. Add [[context-memory]] as the target architecture for daily, project, person/client, relationship, money, decision, and system notes with tags and wiki links. Keep the automatic Obsidian writer/indexer as planned work until implemented and verified.

Consequence: Marcus's voice and text prompts now have clearer style and memory expectations, while the docs honestly distinguish existing durable memory from the planned Obsidian graph. Future work has an explicit acceptance standard for date/project/person/money context retrieval.

## 2026-08-13: Persistent Drive Access Is A Critical Durable Operation

Context: Mark wants Marcus to use the whole PC, but the prior safe installation covered only Documents, OneDrive Documents, and Downloads. A command-line `C:\` grant was rejected because a broad statement was not an exact informed authorization for persistent drive scope and hosted metadata/content relay.

Decision: Put drive-scope preparation in the paired mobile `Verify` view. Derive every fixed-drive root from the exact online relay, create one critical durable operation, require typed strong confirmation, and bind both policy mutation and verification to the same desktop-agent id and idempotent operation attempt. The relay persists only its non-secret policy, rejects unbound access actions, and reports runtime plus persisted read-back. Keep credential content, arbitrary shell execution, installs, deletion, publishing, messages, and account changes outside this grant.

Consequence: Marcus can ask for the exact authority it needs without treating conversation as a hidden permanent setting. Local acceptance proves no queueing before approval and completion only after matching policy evidence; actual drive-root scope remains inactive until Mark approves it in the paired app.

## 2026-08-12: Full PC Use Is A Capability Grant, Not Arbitrary Remote Shell

Context: Mark explicitly wants Marcus to use his PC and everything on it. The existing desktop bridge could operate registered projects and visible Codex jobs, but it could not truthfully search the wider PC, inspect ordinary files, or launch installed applications. Treating that request as an unrestricted persistent shell would also expose credentials, destructive commands, and third-party prompt injection to the same conversational surface.

Decision: Add a typed PC-operator layer with a live root/capability manifest, bounded filename search, directory and application inventory, non-secret text reads, and direct-request-only open/launch actions. Persist non-secret relay policy outside Task Scheduler arguments. Refuse credential-bearing files and keep shell execution, deletion, installs, access/credential changes, publishing, external communication, and irreversible actions behind exact approval paths. Do not enable whole-drive `C:\` persistence until Mark approves that exact scope and understands that selected metadata/content transits the hosted Render service.

Consequence: Marcus now has real general-PC assistance over Documents, OneDrive Documents, and Downloads instead of merely claiming it. The live server reports eight capabilities. Whole-drive access remains a visible unresolved authorization decision rather than a hidden configuration change.

## 2026-08-12: Post-Codex Approval Work Precedes Final Verification

Context: the from-scratch demo completed local Codex and then correctly stopped for private GitHub repository approval. A Render restart saw that final verification was not yet passed, ignored the unfinished repository/push/deployment steps, and mislabeled the whole operation `blocked` with a verification-required blocker even though its current step remained `waiting_for_approval`.

Decision: startup recovery treats unfinished non-verification steps as authoritative workflow state. A pending exact approval keeps the operation `waiting_for_approval`; final verification classification applies only after post-implementation work is no longer pending. Recovery repairs an existing stale verification blocker and status without approving or executing the action.

Consequence: the phone and durable record agree about what Mark is being asked to authorize, and a restart cannot turn a normal approval pause into an unrelated verification failure. Focused recovery tests cover both an already-correct approval wait and repair of the stale production shape.

## 2026-08-12: Full PC Access Is Explicit, Project-Scoped, And Visible

Context: Mark wants Marcus to use everything available on his PC, switch projects reliably, create applications from an empty folder, and make local Codex work visible in real time. The former relay exposed only a narrow action list and could confuse the active Codex workspace with the project named in conversation.

Decision: Let the desktop relay explicitly declare broad-root authorization and a dedicated new-project root. Attest each exact workspace before execution, launch Codex locally with `workspace-write` scope, stream bounded events to a per-job capability monitor, and open that monitor in Chrome kiosk mode. Add deterministic project switching and a durable blank-project workflow. Keep destructive filesystem actions, credential changes/disclosure, messages, repository creation/publication, deployments, DNS, and production mutations behind typed exact-action approvals.

Consequence: Marcus can inspect Mark's authorized Windows files and perform ordinary project work without repeated folder approvals, while consequential actions remain reviewable and recoverable. Hosted Marcus enables the desktop route by default but uses it only for an exact attested local workspace; remote-only work retains its configured Codex fallback. Production phone-to-PC acceptance now proves exact project switching, new-folder creation, local Codex, same-thread correction, and kiosk monitoring. GitHub creation, push, and Cloudflare deployment remain separate exact approvals.

## 2026-08-12: Voice Acceptance Survives Reload But Cannot Cross Install Contexts

Context: The acceptance session existed only in JavaScript memory, so an Android process replacement or reload could split one physical test across session IDs. The verification dialog also told Mark to start voice while its own modal blocked the main voice control, and the manifest supplied only one SVG icon without explicit Chromium raster install sizes.

Decision: Persist only the random acceptance ID, start time, and coarse platform/display context for two hours. Reuse it only in the same context and discard it when moving between browser and standalone display modes or between platforms. Add voice and install controls inside the dialog, stable manifest app identity, and explicit 192x192/512x512 `any` and `maskable` PNG icons. Keep telemetry events, transcripts, requests, replies, and credentials out of persisted browser storage.

Consequence: One installed-phone acceptance run can survive ordinary Android reload/process replacement without letting browser-tab evidence satisfy the installed-app gate. Service worker v15, local 390x844 Playwright checks, the 128-test suite, and production asset inspection verify the implementation; actual microphone, barge-in, network, lock/background, and physical confirmation evidence still requires Mark's installed phone.

## 2026-08-12: Explicit Implementation Language Starts Codex Without A Redundant Approval

Context: Marcus treated direct requests such as install the new system, replace the legacy system, get Codex fixing it, and get it going in Codex as an untrusted medium-risk step. The mobile conversation therefore stopped for a vague approval after Mark had already issued the implementation instruction, reproducing the shallow approval loop shown in the original screenshots.

Decision: Derive Codex implementation authority only from the authenticated original request, recognize an explicit bounded set of implementation verbs and Codex execution phrases, and retain negation/read-only checks. Let that authority satisfy the medium-risk Codex step. Keep merge, deployment, DNS, publish, external communication, and all high/critical actions behind their separate runtime approvals.

Consequence: Marcus can audit and start the requested Codex work from one clear instruction, while consequential follow-on actions still stop at their exact approval boundary. Immediate replies remain concise but include the measured audit summary. The generated handoff authorizes only the nonproduction operation branch and its review PR. Regression coverage includes install/replace, get Codex fixing, get it going in Codex, read-only continuity, the genuine unauthorised audit/plan approval path, and the branch boundary. Production operation `op_-qcwlO85nndNkw` passed this flow through open/unmerged PR #5 without changing the Worker.

## 2026-08-12: Provider Credentials Are Broad, Mutation Actions Are Narrow

Context: Mark wants Marcus to operate GitHub and Cloudflare, but exposing arbitrary provider APIs to a conversation model would let a mistaken target or prompt become an account-wide mutation. Read-only credentials and Codex-created pull requests were not enough to merge reviewed work, manage project DNS, or promote a prepared Worker version.

Decision: Keep account credentials server-side and expose only typed durable actions: exact-head GitHub PR merge, project-bound DNS upsert/delete, and exact-version Worker deployment. Bind each action to a high-confidence project-registry record and immutable target, require runtime approval, re-read provider state to reject drift, perform at most one mutation, and require authoritative read-back. Treat uncertain post-write state as recovery-required rather than failure or an automatic retry.

Consequence: Marcus can prepare useful GitHub and Cloudflare work from conversation while the model never receives generic account authority. The production paths are deployed and two exact demo actions are waiting for approval; no live merge or Worker deployment occurred during preparation.

## 2026-08-12: Codex Completion Claims Do Not Verify Codex Work

Context: The Reggie runner could complete, push a branch, and open a pull request, but the GitHub Actions adapter returned no artifacts and an empty diff. Marcus therefore had no independent target-repository evidence and could only wait for manually registered verification.

Decision: After runner completion, query GitHub directly for the target branch, pull request, head commit, changed files, bounded patches, check runs, and commit statuses. Bind that evidence to a separate AI review with a SHA-256 digest. Quarantine provider-supplied review artifacts, treat repository patches as untrusted model input, and permit the independent review to satisfy only `diff_review` when every acceptance criterion is visibly met. Keep all runtime, browser, deployment, merge, communication, and production gates independent.

Consequence: A successful runner no longer stands in for a successful implementation. Missing/truncated evidence, failed or pending target checks, malformed review output, incomplete criterion coverage, and digest mismatches fail closed. The target collector passed read-only against real demo PR #3 before deployment, and operation `op_NfHu37cdF1aSjQ` later passed the full production acceptance against PR #4.

Production correction: The first live review of demo PR #4 exposed a false-positive model response: it marked criteria met with blank evidence and claimed tests passed without target test evidence. The operation remained blocked on independent build/test/lint gates. Marcus now supplies a validated evidence catalog, requires every `met` criterion to reference one or more exact catalog IDs, rejects invented/blank references, and rejects execution claims that lack successful target checks or authenticated verification evidence.

## 2026-08-12: Mission Memory Is Durable Data, Not A Prompt Constant

Context: Marcus retained recent conversation and project requirements, but his overall mission, Mark's standing instructions, and cross-project preferences existed only in code, documentation, or the current conversation. That did not prove durable recall across chat eviction, process restart, business switching, or a later Codex handoff.

Decision: Add a business-scoped mission-memory store with typed records, provenance, priority, lifecycle status, atomic persistence, sibling backups, and corruption preservation. Seed the mission Mark already supplied. Accept new memory only through explicit operator commands or durable-admin routes, reject credential-like content, and retrieve relevant active records into both conversation prompts and project/Codex execution briefs.

Consequence: Marcus can explain what he durably remembers and carry those instructions into later work without relying on model inference or rolling history. Memory does not grant execution authority, does not bypass approval, and can be superseded or archived with an audit trail instead of silently disappearing.

## 2026-08-12: Codex Launch Requires Request-Ranked Repository Evidence

Context: The original project operator probed ten hard-coded filenames and collapsed each readable file to a 260-character single-line preview. It could answer immediately without seeing nested request-relevant source, repository topology, current commits, or open pull requests, and the real direct-Codex handoff truncated the brief at 12,000 characters.

Decision: Discover request-related repositories, index each recursive Git tree, rank safe text files against the complete request, read bounded redacted excerpts, and record coverage, failures, API calls, and elapsed time. Increase the retained architecture evidence to 30,000 characters and require Codex to reopen the relevant files, callers, dependents, and tests. Preserve named related repositories as explicit scope.

Consequence: Marcus can demonstrate what it inspected before asking for approval or starting Codex. Missing or truncated GitHub evidence is reported as partial instead of being hidden, obvious secret paths are never fetched, and a slow or broken repository is bounded by per-call deadlines and a small fallback probe.

## 2026-08-12: Physical Voice Acceptance Requires Derived Gates And Same-Session Confirmation

Context: Browser telemetry can prove protocol events and an Android standalone user agent can be emulated. Neither independently proves that Mark completed the workflow on his installed physical phone.

Decision: Add a fresh-session mobile `Verify` flow. Enable confirmation only after all eight voice gates pass in installed Android standalone context, accept only a boolean `physical_review_confirmed` event, and define `acceptedOnPhysicalDevice` as the conjunction of those facts. Combine voice, provider, approved-send, and operator gates in one authenticated acceptance report.

Consequence: Marcus has a concrete phone checklist and durable completion evidence without persisting transcript or note content. Synthetic browser runs remain useful but cannot close the physical-device gate.

## 2026-08-12: Provider Setup Lives Behind Durable Mobile Admin Auth

Context: Real text and email acceptance remained blocked because Quo and SMTP credentials were absent, and asking Mark to paste secrets into chat would expose them to conversation history.

Decision: Add an `Integrations` dialog to the paired mobile client and admin-only provider configuration/verification routes. Keep secrets write-only, return masked hints, reject Live-session-only access, verify Quo sender identity or SMTP transport without sending, retain only bounded timestamped evidence, and invalidate that evidence after a settings change.

Consequence: Mark can finish provider setup directly on Marcus Mobile without sharing credentials in chat. Sending remains a separate durable draft, explicit approval, and provider action; provider verification cannot send a message.

## 2026-08-12: Final Voice Transcripts Do Not Reopen Playback

Context: A one-shot production Reggie continuity conversation streamed assistant audio and emitted `response.output_audio.done`, then emitted its final output transcript. Treating both transcript deltas and the final transcript as playback-start signals put the UI back into `Speaking` after playback had ended.

Decision: Use output-audio transcript deltas to infer WebRTC playback start, use `response.output_audio.done` to end it, and treat the final transcript event as text-only. Reproduce that exact ordering in the browser client regression.

Consequence: Marcus returns to `Listening` after WebRTC playback instead of remaining stuck on `Speaking`, while assistant transcript callbacks and redacted transcript-length telemetry remain intact.

## 2026-08-12: Live Memory Is Project-Scoped Before Execution

Context: A deterministic production speech test correctly reached OpenAI Realtime and the Marcus operator, but the answer replayed stale requirements from another project because Live chat assembled recent history before resolving the explicitly named Reggie repository.

Decision: Store project metadata on both turns of every exchange, resolve an explicitly named project before assembling the operator request, select only matching user requirements, and generate a short requirement summary for context-only replies. Keep up to 12 deduplicated requirements for each of 40 projects. Preserve compatibility with older exchanges by pairing a user turn with its assistant turn's project metadata and by reconstructing missing requirements from matching durable operation requests. Exclude read-only and acceptance commands from requirement memory.

Consequence: A single spoken or typed command can switch projects and immediately request an audit or Codex handoff without contaminating the prompt with the formerly active project. Requirement recall no longer depends on the 80-turn rolling transcript. Regression coverage switches Reggie to Atlas and back, verifies zero work for an explicit read-only request, verifies a direct Reggie audit contains no Atlas requirement, erases chat and project memory, and then recovers Reggie's saved requirement from its durable operation without creating new work.

## 2026-08-12: Voice Acceptance Uses Redacted Durable Telemetry

Context: Production browser signaling tests and screenshots could show UI state, but they could not durably prove recognized speech, assistant audio, interruption, operator completion, or recovery on an installed Android PWA. Retaining transcripts or credentials for acceptance would create unnecessary privacy and security exposure.

Decision: Record only allowlisted voice lifecycle events and bounded metadata through an authenticated endpoint. Convert user and assistant transcripts to character counts, keep the client queue in memory, cap server retention at 1,000 events per business, and derive explicit acceptance gates. Treat Android standalone context as eligibility for physical review rather than proof of a physical device.

Consequence: One installed-phone run can produce durable, inspectable evidence without storing conversation content or credentials. Screenshots are supporting evidence instead of the acceptance record, while physical-device confirmation remains honest and separate.

## 2026-08-12: Project Conversation Does Not Imply Execution

Context: A production read-only Reggie continuity check mentioned a repository, auditing, and Codex while explicitly saying not to audit or start Codex. The broad keyword classifier resolved Reggie and inspected two repositories and six files before creating approval-gated operation `op_-3JoirxhsOnaLg`. No provider job started, and the operation was cancelled, but creating it violated the user's instruction.

Decision: Require a positive project-work action instead of treating `repo`, `site`, or `Codex` as execution intent. Remove explicitly negated audit clauses before durable-work classification. Preserve requirements across turns, but let the newest execution instruction supersede old "do not audit/start" controls without removing durable restrictions such as "do not deploy." Support audit/prompt preparation with `autoStart: false` when the current turn explicitly defers starting Codex.

Consequence: Marcus can discuss and retain project requirements without immediately auditing or launching work. Regression coverage proves a read-only Reggie turn creates zero operations, a later positive turn uses the retained API-token/slug requirements, and prepare-without-start creates no provider job. Production then repeated both stages: zero operations for the read-only turn, followed by a one-repository/three-file audit and 5,230-character prompt with zero provider jobs.

## 2026-08-12: Mobile Pairing Survives Render Process Replacement

Context: A production browser acceptance minted a valid pairing code while Render was replacing the application process. The code existed only in the old process's memory, so a pairing request routed to the replacement process returned `Invalid or expired pairing code`.

Decision: Persist only the HMAC hash and expiration in `data/mobile-pairing.json`. Serialize creation and consumption with an exclusive cross-process lock, delete the record after the first successful use, and cover mint -> process stop -> process start -> consume -> replay with an acceptance test.

Consequence: Deploys and process restarts no longer invalidate an otherwise valid mobile pairing challenge. Production accepted a pre-deploy code after process replacement and rejected replay on 2026-08-12. The six-digit code and durable admin credential are still not persisted together or returned to the browser.

## 2026-08-11: Marcus Voice Uses The Official Realtime SDK And Recoverable Sessions

Context: The first mobile voice client hand-built a raw WebRTC connection and treated a disconnect as a permanent stop. Phone lock, backgrounding, network changes, ephemeral-token expiry, and the Realtime session limit could leave voice unavailable or race a stale setup against a newer connection.

Decision: Use `@openai/agents-realtime` for the browser session, retain the single `marcus_operator` authority bridge, pause media while hidden, reconnect with bounded backoff and a new ephemeral credential, and renew at 55 minutes. Use semantic VAD with interruption enabled and live input transcription.

Consequence: Marcus keeps one durable server-side conversation while the disposable audio transport can recover. Local tests cover interruption, background/network recovery, expired credentials, and stale-connection races; deployed and hands-on Android acceptance remain separate gates.

## 2026-08-11: Mobile Conversation State Owns The Active Project

Context: Marcus answered each mobile message independently. A request could name `markgromer/Reggie`, then a follow-up such as "check the repo" would lose that context and fall back to the only previously registered demo project.

Decision: Persist a bounded Marcus Mobile transcript and active project on the server. Resolve explicit GitHub `owner/repository` references into the durable project registry and include recent user requirements when preparing the audit and Codex operation.

Consequence: Short follow-ups reuse the correct project and earlier requirements. Explicit repositories no longer depend on a manually pre-populated registry entry, while the durable operation still owns execution and verification.

## 2026-08-11: External Communication Uses A Provider-Receipt State Machine

Context: The external-action ledger supported drafts and approvals but could not execute a provider send. The previous health field also conflated inbound webhook authentication with outbound text capability.

Decision: Add a separate approved send action with `pending_approval -> approved -> sending -> sent/failed` states. Use SMTP for email and Quo for text, store provider receipts, and make successful replay idempotent. Report inbound webhook and outbound text capabilities separately.

Consequence: Marcus can send only after explicit approval and only claims `sent` after provider acceptance. Production remains blocked until real SMTP and Quo credentials pass live acceptance.

## 2026-08-11: Desktop Relay Credentials Stay Out Of Process Arguments

Context: The Windows startup task targeted a retired Render host, exposed the admin credential in its arguments, overlapped long polling cycles, and could exit on a transient process-spawn error.

Decision: Point the task at the canonical Render service, read the token from the protected Marcus application-data file, serialize polling, catch synchronous spawn failures, and configure task restart behavior.

Consequence: The desktop relay resumes at login without putting the token in the task or process command line and tolerates transient local command failures.

## 2026-08-12: Android Uses One-Time Pairing Instead Of A Copied Admin Secret

Context: The original mobile login required the same durable admin token used by the server operator, which was awkward on Android and caused stale-token unauthorized failures.

Decision: Add a single-use six-digit pairing code with a ten-minute lifetime, keyed server-side storage, failed-attempt throttling, and secure HttpOnly cookie issuance.

Consequence: Mark can pair the Android PWA without exposing or retaining the durable admin token. Existing cookie authentication and Live session tokens continue to protect API and Realtime requests.

## 2026-08-12: Production Uses Dedicated Cloudflare And Reggie Operator Credentials

Context: The local Wrangler OAuth token could manage developer resources but could not read DNS through Marcus's server API, and Render lacked GitHub, Cloudflare, and direct Codex variables.

Decision: Create the account-owned Cloudflare token `Marcus Production Operator` with Developer Services, DNS write, and zone read; store it only in Render. Configure production GitHub access and the Reggie GitHub Actions Codex adapter through Render secrets and source-controlled non-secret defaults.

Consequence: Production Marcus can read GitHub repositories, read Cloudflare zones and DNS, and start real Codex jobs. Billing, membership, token administration, merges, deployment, DNS mutation, and external communication remain outside automatic authority or behind approval.

## 2026-08-12: Direct Codex Is Accepted Only With Durable Verification

Context: A configured adapter is not proof that execution works. The initial acceptance dispatch failed in `actions/checkout@v5` because Node did not use the runner's system CA store.

Decision: Require a demo operation to survive dispatch failure, retry the same durable step, complete Reggie's `openai/codex-action`, open a review pull request, and pass independent build, test, syntax, artifact, and diff review evidence. Add `NODE_OPTIONS=--use-system-ca` to the Reggie runner and exclude the internal Codex report from generated pull requests.

Consequence: Operation `op_N_PUttVpm72mWw` completed with a real runner URL and PR rather than a handoff claim. Future runner jobs inherit the certificate and artifact fixes.

## 2026-08-12: Codex Result Review Separates Semantic Judgment From Durable Proof

Context: The first independent production reviewer described the demo change correctly but returned blank evidence for every acceptance criterion and claimed tests passed without target test-check evidence. Independent verification gates prevented operation completion. A stricter prompt then produced valid JSON but omitted the criterion array twice, correctly leaving the operation blocked and showing that free-form model compliance was not a reliable control boundary.

Decision: Use OpenAI strict structured output for semantic code-review coverage. Limit model evidence references to exact ids from a validated catalog. Prove Marcus-generated audit, handoff, and completion-control criteria deterministically from durable audit metadata, launched job identity, GitHub artifacts, and authenticated verification evidence. Exclude prior `diff_review` evidence to prevent circular proof. Keep build, test, lint, browser, deployment, merge, and communication verification independent from the AI review.

Consequence: Production operation `op_NfHu37cdF1aSjQ` completed only after PR #4's two patches, exact head and digest, five passing tests, Wrangler dry-run, syntax check, and open/unmerged plus live-404 evidence all agreed. Evidence refresh did not relaunch Codex, and the live Cloudflare Worker was not changed.

## 2026-08-12: OpenAI Realtime Is Marcus's Primary Voice Layer

Context: Marcus already has OpenAI credentials, a mobile PWA, durable project operations, and approval-aware tools. The previous voice path combined recorded audio, transcription, text chat, and separate speech synthesis, which could not provide fluid turn-taking without ongoing voice-pipeline tuning.

Decision: Use OpenAI Realtime speech-to-speech over WebRTC with `gpt-realtime-2.1` as the primary mobile voice layer. Expose only a `marcus_operator` function that routes substantive speech through Marcus's existing Live chat. Keep ElevenLabs/browser speech as fallback narration rather than a separate assistant brain. Do not add LiveKit unless direct WebRTC later fails a demonstrated transport requirement.

Consequence: Marcus gets native low-latency conversation and interruption while project memory, audits, Codex execution, credentials, approvals, and verification remain in the existing server. The standard OpenAI API key stays server-side; the mobile client receives only a short-lived Realtime client secret.

## 2026-08-12: Marcus Project Operator Is Durable-Operation First

Context: Marcus needs to turn project conversations into audited Codex execution instead of loose prompt drafting.

Decision: Route project/audit/Codex requests through `ProjectOperatorService`, resolve the project through the durable operations engine, gather context, compose a Codex prompt, and persist a durable operation.

Consequence: Marcus can track work, blockers, artifacts, and verification instead of treating a prompt as completed work.

## 2026-08-12: Direct Codex And Handoff Are Separate Modes

Context: The current environment does not expose a callable direct Codex session-launch service, but the operations engine already supports a direct adapter interface.

Decision: Keep default behavior in `codex_handoff` mode and add `HttpCodexAdapter` for direct mode when `MARCUS_CODEX_ADAPTER_URL` or `CODEX_ADAPTER_URL` is configured.

Consequence: Marcus can start direct Codex jobs when a real adapter endpoint exists, while operator health remains honest when it does not.

## 2026-08-12: External Communication Requires Explicit Approval

Context: Marcus should help draft texts and emails, but customer-facing communication is a high-trust external action.

Decision: Store email/text drafts as external actions with `pending_approval`, then allow explicit approve/reject transitions. Approval does not equal sent.

Consequence: Marcus can help prepare communication without silently sending or falsely reporting delivery.

## 2026-08-12: Marcus Docs Use An Obsidian-Compatible Vault

Context: Marcus needs durable documentation that future Codex sessions can maintain.

Decision: Treat `docs/marcus/` as the Marcus Obsidian-compatible vault and install a reusable local `obsidian-docs` skill under `~/.codex/skills`.

Consequence: Future documentation work has a standard page set, wiki-link style, and verification rules.

## 2026-08-12: Borrow Reggie's GitHub Actions Codex Runner Pattern

Context: Reggie already starts Codex work through a central GitHub Actions runner using `repository_dispatch` and `openai/codex-action@v1`.

Decision: Add `GitHubActionsCodexAdapter` and `.github/workflows/marcus-codex-runner.yml` so Marcus can use the same launch pattern when `MARCUS_CODEX_GITHUB_ACTIONS_ENABLED=true`. Default the runner repo to `markgromer/Reggie` so Marcus can use Reggie's existing runner secrets.

Consequence: Marcus no longer requires a custom HTTP Codex service to enter direct mode. The runner still depends on Reggie's GitHub/OpenAI workflow secrets remaining configured.

## 2026-08-12: Provider Verification Uses Canonical Sender Data Without Sending

Context: The live Quo phone-number response included both a display-formatted number and a canonical E.164 number. Marcus preferred the display value, then rejected it as invalid even though the dedicated `os1` credential and Operations line were valid.

Decision: Prefer Quo's canonical number, then the explicitly configured sender, and normalize display formatting only as a fallback. Model this response shape in the provider regression fixture. Keep provider verification separate from draft approval and send.

Consequence: Production Quo verification resolves the Operations line and required ids without sending. A later explicit approval advanced acceptance action `rv1v4_RKB38v` through provider-accepted `sent` evidence; verification alone still cannot send a message.

## 2026-08-12: Reggie Acceptance Must Include Real Audit Time And Exact-Head Review

Context: Earlier mobile replies were immediate, lost the explicit Reggie context, and described work without inspecting repositories or waiting for Codex. That behavior did not satisfy Marcus's operator mission.

Decision: Exercise the exact `markgromer/Reggie` Sweep and Go request through production. Require repository indexing and file reads before prompt creation, a real Reggie GitHub Actions Codex run, authoritative PR evidence, independent criterion coverage, and isolated exact-head verification before completion.

Consequence: Operation `op_f6XKmXTWILGvpQ` audited 180 paths and 10 files, dispatched a real Codex job, opened PR #16, and completed only after GitHub and authenticated evidence passed. The PR remains open and unmerged, preserving separate merge and deployment authority.

## 2026-08-12: Durable Jobs Continue Independently Of The Mobile Conversation

Context: Marcus Mobile displayed only the first `/api/marcus/live/chat` reply. It discarded the returned operation id, did not show later Codex or verification state, and provider jobs advanced only when another request explicitly ticked the operation runner. This recreated the appearance of instant shallow answers even when durable work existed.

Decision: Add an allowlisted server operation monitor for execution-safe states and a read-only mobile tracker over the redacted operation-summary API. Persist only the active operation id and status signature on the phone. Emit one conversation update per material persisted transition, and speak only terminal, approval, blocker, or recovery transitions when Realtime voice is active.

Consequence: Codex polling and verification no longer depend on the phone remaining open. Marcus Mobile can show honest progress and verified completion without gaining prompt, artifact, provider, credential, or execution access. Approval and recovery boundaries remain fail-closed.

## 2026-08-12: Repository Discovery Requires Strong Explicit Syntax

Context: A Live relay sentence containing `workflow/operation` was interpreted as a raw GitHub `owner/repository` declaration. Marcus registered a false project, selected it as active conversation context, and prepared approval-gated work against it.

Decision: Auto-register only GitHub URLs, `.git` targets, whole-message `owner/repository` values, or slash targets adjacent to an explicit GitHub/repository label. Exclude archived registry records from ordinary resolution, provider evidence refresh, activity snapshots, focus, and bottlenecks while retaining exact-name historical lookup.

Consequence: Conversational slash phrases cannot silently become projects. The false operation was cancelled without provider execution, the record was archived, and production read-back retained the real Reggie context and the two legitimate demo approvals.

## 2026-08-12: Serialized Settings Writes Recover After Validation Failure

Context: One rejected provider configuration left the shared settings promise chain rejected, causing every later serialized settings write to fail even when the new input was valid.

Decision: Recover the queue before appending each serialized write with `writeLock.catch(() => {}).then(...)`. The request that supplied invalid data still fails; later independent requests are no longer poisoned by that failure. Accept a validated display-name SMTP From value such as `Marcus <marcus@gromore.media>` while extracting and validating its mailbox address.

Consequence: Production accepted and no-send verified the Resend SMTP configuration after an intentionally invalid update, and regression coverage proves a valid write succeeds after a rejected one.

## 2026-08-12: Existing Resend Credential Is Reused Under The Same Approval Boundary

Context: A related production service already held a working Resend credential, and `gromore.media` was verified. Creating another voice/email subsystem or duplicating secrets would add operational work without changing Marcus's send authority.

Decision: Reuse the existing server-side Resend credential for SMTP authentication and send as `Marcus <marcus@gromore.media>`. Keep verification no-send and retain the existing draft -> explicit approval -> provider send -> receipt sequence.

Consequence: At that point, SMTP configuration and authentication passed without exposing the credential, while draft `V8uMUUZjiRz1` remained unsent and `pending_approval`. The later approved attempt and its `550` result are recorded in the following decision.

## 2026-08-12: Provider Verification Is Fingerprint-Preserved And Failed Approved Sends Are Retryable

Context: Saving the full provider form cleared both provider verification records even when only one provider changed. The first approved Resend delivery then proved that SMTP authentication alone did not establish sender-domain authorization: Resend rejected `Marcus <marcus@gromore.media>` with `550`. The failed action kept its approval fields but was hidden from mobile review and could not be retried.

Decision: Compare each provider's effective configuration fingerprint before invalidating its verification record. Preserve unchanged-provider evidence, including Quo when only SMTP changes. Keep a failed action retryable only when the exact draft already has explicit approval, and show that state as `Retry approved message` in Marcus Mobile. A retry never bypasses initial approval and still requires provider receipt evidence before acceptance passes.

Consequence: Email credential rotation no longer erases unrelated Quo evidence. A transient or configuration-related provider failure does not force Mark to approve identical content again, while changed recipient, subject, body, or a new draft still requires a separate exact approval.

## 2026-08-12: Completed Provider Jobs Are Stable Across Restart

Context: Startup recovery included provider jobs with status `completed` even when the bound Codex step was already complete. Every Render replacement reset that step to running, polled the same finished job, and added another no-runnable-step blocker when verification still required review. Marcus Mobile then surfaced the stale blocked operation as active work.

Decision: Reconcile a completed provider job only when its bound step is not complete. Treat an already blocked operation with completed implementation and an active verification blocker as stable. Deduplicate active runner and recovery blockers by type and bound step, and emit the corresponding event only when the blocker is first created.

Consequence: Process replacement no longer rewrites settled implementation state or grows blocker noise. After deployment, two superseded Reggie acceptance operations were cancelled with no external provider action, and the mobile tracker returned to the legitimate pending demo approval.

## 2026-08-12: Mobile Approval Is Exact-Target And Admin-Authenticated

Context: Marcus Mobile could report that a durable operation needed approval, but Mark had to copy operation ids into chat. That made a consequential action difficult to inspect and easy to approve ambiguously from a phone.

Decision: Add a redacted pending-approval descriptor to the Live-token-safe operation summary and an exact-target review dialog to Marcus Mobile. Show the immutable operation target, action, risk, reason, and operation id. Keep approval disabled until an explicit authorization checkbox is selected, require typed strong confirmation for critical actions, and submit approve/reject only through paired durable-admin routes.

Consequence: Mark can review and authorize one exact GitHub or Cloudflare action from the installed PWA without exposing prompts, artifacts, provider input, or credentials to the read-only feed. Production service-worker `marcus-mobile-v17` rendered both pending targets at 390x844; neither was executed during validation, and an unauthenticated approval attempt returned 401.

## 2026-08-12: External Message Approval Requires Durable Authentication

Context: External-action routes were classified as Live-session routes. A short-lived Live token could therefore list full drafts or call draft, approve, send, and reject directly, even though provider administration and operation approval already required the paired durable-admin context. The phone also lacked an exact-draft review surface.

Decision: Remove every external-action route from Live-token authorization and require durable admin authentication before conversational approval can execute. Add an exact-draft review dialog to the paired mobile `Verify` workflow showing recipient, subject, project, reason, body, and draft id. Keep approve-and-send disabled until Mark authorizes the displayed draft; preserve separate approve and provider-send requests behind that one explicit command.

Consequence: A copied ephemeral token cannot inspect or authorize messages. The production service worker returned 401 for direct Live-token draft access and `reauthenticationRequired` for a Live-token-only send phrase. The paired 390x844 dialog rendered draft `V8uMUUZjiRz1` with zero browser errors or warnings before its later explicit approval.

## 2026-08-13: Whole-PC Scope Is A Durable Critical Grant, Not A Startup Flag

Context: Mark wants Marcus to discover and switch among projects anywhere on his PC, create new workspaces, open local tools, and operate from phone instructions. The scheduled relay was limited to three workspace roots, while silently changing an environment flag would provide no exact-target review, durable authorization record, or persisted-policy verification.

Decision: Model whole-PC scope as a two-step critical operation bound to one desktop agent and explicit fixed-drive roots. The first step persists the local policy only after a checkbox plus typed `I understand`; the second reads back runtime and disk state and must match the immutable operation target. Keep credential contents, arbitrary shell execution, and consequential external actions outside this grant.

Consequence: Render serves `marcus-mobile-v20`, and operation `op_1HYqnishgglGZQ` is prepared for `Marks_PC: C:\`. It remains `waiting_for_approval` with zero attempts, so no wider access exists until Mark confirms it. After approval, production must prove the `full_pc` manifest, persisted-policy verification, and representative inventory/search/read behavior before this capability is accepted.

## 2026-08-13: Documentation-Only Changes Do Not Recycle Production

Context: Obsidian evidence updates triggered full Render replacements because every commit to `main` autodeployed `task-tracker`. Marcus briefly returned Render `502` pages during those replacements even though no runtime file changed.

Decision: Keep automatic runtime deployment enabled, but add `buildFilter.ignoredPaths: [docs/**]` to the Blueprint-managed service. Application code, tests, configuration, and mobile assets still trigger deployment; only `docs/` changes are excluded.

Consequence: Blueprint commit `d318950` deployed successfully, and the Render settings dashboard shows `docs/**` as the sole ignored path. Docs-only commit `02fec3d` then produced no replacement: eight checks over two minutes returned HTTP 200 while uptime increased from 191.9 to 297.6 seconds. Obsidian maintenance no longer creates an avoidable Marcus outage.

## 2026-08-13: Verify Is The Complete Approval Queue

Context: Marcus Mobile persisted one tracked operation for progress and transition announcements. When two durable operations simultaneously needed approval, the active-work card could surface only one, forcing Mark to locate or switch operations before completing system acceptance.

Decision: Keep single-operation tracking for progress announcements, but make `Verify` load the complete redacted operation-summary feed and list every pending approval in critical-to-low risk order. Selecting a queue item binds the exact summarized operation to the existing paired-admin review dialog. After approval, reopen `Verify` so the next pending approval and physical acceptance gates remain visible.

Consequence: Production cache `marcus-mobile-v21` exposes the prepared `Marks_PC: C:\` critical grant and `create_repository:markgromer/marcus-pc-bridge-demo (private)` high-risk approval in one acceptance surface. Prompts, artifacts, provider input, patches, and credentials remain absent from the queue. Commit `ad99e29` passed all 144 tests and syntax lint before deployment; production served `v21` over HTTP 200 on 2026-08-13 UTC.

## 2026-08-13: Full-PC Acceptance Requires Returned Evidence

Context: Mark approved the exact `Marks_PC: C:\` critical grant, and policy persistence plus read-back passed. The first production file actions executed successfully, but their search, listing, and read fields were placed at the desktop response top level while the hosted action queue retains only `details`, causing successful responses with `details: null`.

Decision: Normalize every typed PC operator result into an explicit desktop action envelope: `ok` and `error` remain control fields, while bounded evidence and refusal metadata are stored in `details`. Retain the existing secret-path refusal and hosted result-size bound. Add regression coverage for search evidence, bounded file content, and credential-file refusal.

Consequence: Commit `dd0c7e3` passes all 145 tests and syntax lint. After restarting only `MARCUS-DesktopAgent`, production returned the `C:\` inventory, one matching demo `package.json`, twelve directory entries, and an untruncated 545-byte read with SHA-256 `9313db803af3cafb4770014cf1746c528f42bdc88c9b0f45798aab6ee0c68ba2`. Combined production acceptance now passes 12/13 gates; only physical Android voice acceptance remains.
