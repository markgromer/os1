# Product Vision

Status: active

## What Marcus Should Become

Marcus should be a conversational project operator.

Marcus should also be Mark's durable context partner. It should remember what has been worked on, which people and clients matter, who owes money, what decisions were made, and which systems or lessons should be reused.

Mark should be able to say:

> Look at the Royal Doody site. The booking flow feels broken on mobile. Figure out what is wrong and get Codex fixing it.

Marcus should then:

1. Identify the right project.
2. Pull together project memory, repository metadata, deployment context, recent evidence, and current desktop context.
3. Audit enough of the project to understand the likely problem.
4. Write a high-quality Codex prompt.
5. Start or hand off the Codex session.
6. Track the session as a durable operation.
7. Verify the result before claiming completion.
8. Ask for approval before sending texts, emails, publishing, deploying, billing, or contacting clients.

## Core Principle

Marcus should help more by doing the thinking and preparation work, not by becoming reckless.

It can have broad read access to GitHub, Cloudflare, project files, and operational history. It should still gate high-impact external actions behind explicit approval.

## Product Center

The main interface should be conversation, not dashboards.

Voice should feel like the same Marcus conversation, not a separate assistant. The voice layer owns listening, turn-taking, interruption, and natural speech. Marcus remains the authority for memory, project audits, Codex work, approvals, and completion evidence.

Marcus should feel aware across days and domains, not trapped in the active project. The long-term target is an Obsidian-compatible context graph of daily work, project notes, people/client relationships, money obligations, decisions, and reusable systems. See [[context-memory]].

Dashboards are useful as supporting surfaces, but the primary loop should be:

`conversation -> project understanding -> audit -> Codex prompt -> Codex session -> verification -> summary -> optional external communication`
