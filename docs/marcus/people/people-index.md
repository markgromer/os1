# People Notes

Status: manual notes and automatic community profiles are implemented. Broader cross-channel identity resolution remains planned in [[context-memory]].

People notes preserve relationship context, preferences, promises, trust signals, and communication facts that Mark should not have to reconstruct.

## Current Notes

- Manual notes use [[person-note-template]].
- Community scans write deduplicated `community-*.md` profiles into this folder through the desktop relay.
- `GET /api/marcus/community/members` exposes the same durable profile records to MARCUS chat.

## Capture Standard

Each person note should include:

- role and relationship to Mark or a project
- communication preferences and tone expectations
- commitments made by or to that person
- relationship risks, constraints, or sensitivities
- linked clients, projects, money notes, and decisions
- visible community engagement and bounded source-linked activity summaries

Avoid speculation. Community facts and evidence-based inferences are stored separately. Sensitive-trait inference is rejected, and raw post/comment transcripts are not retained in person notes.

## Community Profile Loop

1. `marcus_browser_observe_community` reads bounded rendered Skool activity from the visible MARCUS Chrome profile.
2. `CommunityIntelligenceStore` deduplicates observations and members in the business-scoped data store.
3. The server queues `marcus-community-profile-note` for each changed member.
4. The desktop agent atomically updates the member's Obsidian note in this folder.
5. Repeated scans update the same stable person record instead of creating another note.

This loop observes only content visible in the attended browser session. It does not react, reply, publish, or infer hidden profile facts.

## Template

Use [[person-note-template]].
