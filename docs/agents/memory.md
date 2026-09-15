# Memory Adapter

`session-retro`'s promotion ladder stages a first-cited lesson in **memory**, and promotes it into a **guardrail** on its second citation. The ladder is portable; where memory physically lives is not. This file is the seam: it answers *where entries live, how the index works, how one entry is written*, and *what a guardrail promotion target looks like here* — and nothing else. It is the memory-side twin of `docs/agents/issue-tracker.md`.

Read it *before* a retro classifies its findings, not while writing them. A retro that discovers its substrate mid-write is a retro that lands nothing.

**Do not restate this file's contents inside a skill body.** The seam's known failure mode, inherited verbatim from the tracker adapter, is skills accreting substrate prose back inline; a skill that names a memory path has broken the seam even if the path is correct.

## The substrate

Memory for this repo lives as markdown files in **`docs/agents/memory/`**. It is **inside** the repo, and is readable by any agent or engineer working in this repository.

**Is it a repo change?** Yes — entries ride the same branch and PR as everything else.

## Where entries live

- **Directory**: `docs/agents/memory/`
- **One entry per file**, named `<category>-<kebab-case-slug>.md`
- **Categories**:
  - `workflow`: Process, tool, CLI, or agent interaction lesson
  - `architecture`: Codebase structure, subsystem behavior, or design pattern
  - `defect`: Subtle bug pattern, edge case, or testing trap

## The index

- **Index file**: `docs/agents/memory/INDEX.md`
- **Format**: Bullet list holding a relative markdown link to the entry file, an em dash, and a one-clause summary:
  `- [Slug](./category-slug.md) — Brief summary of the staged lesson`
- **On write**: Append to the end of the list in `docs/agents/memory/INDEX.md`.

## Writing one entry

```markdown
---
title: <title>
category: <workflow | architecture | defect>
date: YYYY-MM-DD
citations: 1
---

## Concrete Context
[The specific moment, tool failure, or test that produced this lesson]

## Lesson & Application
[Why it happened and what to do differently next time]
```

Body: Focus on observable facts and actionable prevention.

**Cross-references**: Use standard Markdown relative links (e.g. `[link text](./other-entry.md)`). Store-local syntax is prohibited.

## Guardrail promotion targets

Ladder rung 3. When a lesson is cited a second time it leaves memory for a **guardrail** — a durable, repo-visible home that every agent and every human reads.

| Class of fact | Guardrail |
| --- | --- |
| Agent instructions & workflows | `AGENTS.md` / `CLAUDE.md` |
| How one skill behaves | That skill's own `SKILL.md` |
| Settled architectural decisions | `docs/adr/` |
| Per-repo conventions | `docs/agents/conventions.md` |
| Tracker / issue configuration | `docs/agents/issue-tracker.md` |

**Rung 4 is not optional.** After promoting, delete the memory entry and remove it from `docs/agents/memory/INDEX.md`, or replace the file content with a pointer:

```markdown
# Promoted
Promoted to `docs/agents/conventions.md` on YYYY-MM-DD.
```
