# Conversation Notes

Status: manual structure ready, plus automatic live-call summary checkpoints from the OBS sidecar. Broader email, SMS, and general conversation summarization remain planned work in [[context-memory]].

Conversation notes preserve durable facts from calls, texts, emails, meetings, chats, and Marcus conversations. They are summaries, not transcripts.

## Automatic Live Notes

While `/obs-marcus.html` has an active Realtime session, recognized participant speech and MARCUS responses remain in a bounded in-memory buffer. Every five minutes and when the session stops, the sidecar sends at most 20,000 characters to the existing transcript analyzer. The server queues only the derived summary, decisions, commitments, and follow-ups to the local desktop agent. The desktop agent atomically writes or refreshes one session note in this folder. Raw transcript text is not written to the Obsidian vault or the durable desktop queue.

Important commitments remain AI-derived until Mark reviews them. If AI routing is unavailable, the note is marked low confidence and uses the existing heuristic action extractor.

## Connected Sources

- [[quo-sms]] can provide text-message facts after webhook capture. Automatic durable SMS summaries remain planned work.

## Capture Standard

Each conversation note should include:

- who was involved
- date/time or approximate timing
- topic
- decisions, promises, risks, preferences, or relationship signals
- follow-ups and owner
- linked projects, clients, people, money notes, schedule notes, or decisions
- source and confidence

Do not store full private message dumps by default. Sensitive relationship or client facts should be marked as verified, inferred, or uncertain.

## Template

Use [[conversation-note-template]].
