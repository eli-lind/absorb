# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues on `eli-lind/absorb`. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Ensure `gh repo set-default eli-lind/absorb` is set when an `upstream` remote exists so `gh` targets the fork rather than upstream.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either — resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Grouping an epic's tickets (milestones)

`build-the-frontier` groups one epic's tickets under a **milestone** — GitHub-native, set with `gh issue create --milestone "<name>"` (or `gh issue edit <n> --milestone "<name>"`).

- **Name it** from the epic's ADR slug if one exists — kebab-case, numeric prefix dropped: `0001-native-mqtt-client-for-remote-control` → `native-mqtt-client-for-remote-control`. **If the epic settled with no ADR** (the common case — `domain-modeling` sets a deliberately high bar), kebab-case the spec/feature title instead: `Scheduler registry & trigger` → `scheduler-registry-trigger`. Prefer the ADR slug when both exist.
- **Create-or-reuse** — look the milestone up first (`gh api repos/{owner}/{repo}/milestones --jq '.[].title'`); create it only if absent (`gh api --method POST repos/{owner}/{repo}/milestones -f title="<name>"`).
- **Keep it open** — every child ticket belongs to the milestone; leave the milestone and the design/spec parent issues open while any child is still workable, and close them under the rule below, which is `work-the-frontier`'s job. A milestone is not a frontier: re-derive frontier status from blocking edges, not milestone membership.

**The close rule: no child workable, and at least one shipped.** A child is workable if it is open *and* not `icebox`. A parked child stays open by design, so it holds neither the milestone nor the design/spec parent issue open — both close under the same condition, together — `gh issue close <n>` for the parent, and `gh api --method PATCH repos/{owner}/{repo}/milestones/{number} -f state=closed` for the milestone, which `gh` has no first-class verb for. If every child was parked and nothing shipped, close the parent with `gh issue close <n> --reason "not planned"` (plus the `wontfix` label) rather than as completed — `gh` offers exactly that two-way choice, and the two render differently in the timeline, which is the distinction between an abandoned design and a delivered one.

**Name the parked children in the close comment** (`gh issue close <n> --comment "…"`), alongside the child PRs that shipped. GitHub's *derived* completion surfaces count closed issues, so an epic closed over a park sits permanently short: a sub-issue parent's progress never reaches full, and neither does the milestone bar. Those numbers are correct and permanently short, so the comment is what stops the parent reading as closed by mistake. A plain markdown **task list is the opposite trap** — GitHub does not auto-tick a `- [ ] #123` box when its child closes, so the boxes say whatever someone last ticked by hand; don't read a full checklist as evidence the rule is met, and don't tick a parked child's box to tidy it.

**Don't close an `icebox` child to complete the progress count.** Closing it advances the sub-issue and milestone counts, rendering consciously-parked work as delivered — the overload the `icebox` state role exists to avoid, and the reason ADR 0006 rejected park-as-close. Leave it open, in its milestone, and close the parent over it.

**Blocking edges.** GitHub's **native issue dependencies** are the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by`, counting open blockers only — the live gate. Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.

**A parked ticket keeps its milestone.** An `icebox` ticket stays in the epic that ruled it out — do **not** create an "Icebox"/parking milestone. GitHub allows at most one milestone per issue, so a parking milestone evicts the ticket from the epic that explains it, forcing that provenance back into prose. Nor does grouping enforce anything: milestone membership has no bearing on frontier status (above), whereas the `icebox` state role takes the ticket off the frontier by construction. If this repo groups epics with **GitHub Projects** instead, the eviction argument weakens — an issue can sit on several boards — but the conclusion doesn't: board membership carries no mechanical force either, so a separate "Icebox" project only duplicates the `icebox` label at a second altitude and takes the parked ticket off the board that explains it. A **Parked** column in the epic's own board is fine, since a project's single-select `Status` field is a *rendering* of the state role rather than a second grouping — as long as the label stays authoritative. A **Prospect** simply has no milestone yet.

## Claiming a ticket

The claim is the issue's **assignee**; the frontier is the open, unblocked, **unassigned** tickets. Claiming, release (close at ship), and stale-claim handling are tracker-independent — see `work-the-frontier`'s Claiming. Per-tracker mechanics:

- **Claim**: `gh issue edit <N> --add-assignee @me`, then `gh issue view <N> --json assignees` and confirm you are the **sole assignee** (if two raced, earliest assignment wins). If someone else holds it, pick another.
- **Frontier query**: `gh issue list --state open` scoped to the epic, dropping any with an unclosed blocker or an assignee, and — on an autonomous pass — keeping only those carrying the **AFK-ready role** from [`triage-labels.md`](./triage-labels.md) (pass its label string to `--label`). Admission is by role, not by subtracting the states you don't want: without that last clause an `icebox` or `needs-info` issue that is open, unblocked and unassigned passes this query, which is the whole thing the triage states exist to prevent.

## Convenience scripts (optional — interface, not implementation)

`work-the-frontier` looks for these by name if the project provides them. Whether to actually write them is a per-repo call, not something this setup skill generates — but if you do, match this contract so `work-the-frontier` can find them without extra config:

- **`scripts/ship-ticket.sh <pr-number> <issue-number>`** — merge the PR (`gh pr merge <pr-number> --squash` or your preferred strategy), close the issue referencing it (`gh issue close <issue-number> --comment "..."`), then sync `main` and delete the branch. Fail loud and stop on the first failing step rather than partially cleaning up. Run from the feature branch, not `main`.
- **`scripts/parent-status.sh <parent-issue-number>`** — parse the parent issue's task-list checklist (or its sub-issues, if GitHub's sub-issue feature is enabled), fetch each child's state and labels via `gh issue view <n> --json state,labels`, and answer **"is any child still workable?"** — open *and* not `icebox` — rather than "is every child closed?", with at least one child shipped. An open-but-parked child must not report not-safe-to-close; list it instead, so the caller can name it in the close comment. Exit 0 if safe to close, 1 if not, 2 if the issue has no children. List each remaining open child's own triage label alongside it — `work-the-frontier` and `triage`'s park path reuse that same read to re-derive the parent's own label when it doesn't close.
- **`scripts/frontier-status.sh [--label ready-for-agent]`** — cross-reference open issues against open PRs (`gh pr list --json body,title,number` searched for a `#N` reference) to flag `has-open-pr` candidates (in-flight work) before starting a new implementation.

If none exist, do the merge/close/sync/delete-branch sequence as separate `gh` calls — these scripts are a convenience wrapper, never a requirement.
