# Repo Root Marcus Note

Status: active
Tags: #workflow #project/marcus

Projects:
- [[marcus]]

## Use When

Use this workflow when Marcus enters a project repository, prepares to report on project state, or finishes meaningful work that future sessions should remember.

## Inputs

- Root-level `marcus.txt`, when present.
- Repository files and docs.
- `git status --short`.
- Project-specific README, package metadata, tests, and local documentation.
- Related Marcus notes, especially [[context-memory]], [[current-system-map]], and [[execution-loop]] for this repo.

## Steps

1. Read `marcus.txt` first if it exists.
2. Inspect the repository enough to understand the current code, docs, scripts, runtime, and active worktree state.
3. Keep secrets out of every note. Refer to environment variable names or provider classes, not credential values.
4. Before reporting, separate verified facts from inference and planned work.
5. After meaningful work, append a dated entry to `marcus.txt` with the change, verification, and unresolved context.
6. If the work changes durable Marcus doctrine, update the linked Obsidian notes instead of leaving the root text file as the only source.

## Approval Boundaries

- A root `marcus.txt` note records context only. It does not grant authority to deploy, publish, merge, send messages, access credentials, or mutate providers.
- Do not append raw transcript dumps, secrets, private customer data, or large source excerpts.

## Verification

- Confirm `marcus.txt` exists at the repo root after adding the convention.
- Run the Obsidian note check when Markdown workflow notes change.
- Verify any implementation, provider, deployment, or test claim before recording it as complete.

## Links

- Projects: [[marcus]]
- Decisions:
- System notes: [[context-memory]], [[current-system-map]], [[execution-loop]]
