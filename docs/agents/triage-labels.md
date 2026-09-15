# Triage Labels

The `triage` skill speaks in terms of canonical triage roles — two **category** roles and six **state** roles. This file maps those roles to the actual label strings used in this repo's issue tracker. Every **open** triaged issue carries exactly one category role and one state role. The invariant describes an issue while it is open: closure is itself the terminal fact, and no state role is asserted to remain accurate afterwards — so a closed issue carrying no state role, or a stale one, is not a defect, and nothing relabels an issue at close.

**Category** — what kind of work this is:

| Canonical role  | Label in our tracker | Meaning                          |
| --------------- | -------------------- | -------------------------------- |
| `bug`           | `bug`                | Something is broken              |
| `enhancement`   | `enhancement`        | New feature or improvement       |

**State** — where it sits in the triage machine:

| Canonical role    | Label in our tracker | Meaning                                  |
| ----------------- | -------------------- | ---------------------------------------- |
| `needs-triage`    | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`      | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent` | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human` | `ready-for-human`    | Requires human implementation            |
| `icebox`          | `icebox`             | Evaluated, kept, not being worked now    |
| `wontfix`         | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

**`ready-for-agent` is admission control, not a description.** `work-the-frontier`'s frontier query admits a ticket that is open, has its blockers closed, is unassigned, and carries this label — and the label is the only one of the four carrying a judgement about what the ticket *contains*, so it alone is what admits a ticket to autonomous work. A ticket whose body poses an **open decision** — options to choose between, a question to answer, or a proposal to evaluate — takes `ready-for-human` instead, with kind **Design**; its next step is `settle-the-design`, not an agent. A spec `build-the-frontier` never decomposed goes the same way but as kind **Manual**, since what it waits on is a person running that skill rather than a decision. Applied wrongly, an autonomous pass claims it, reads a body asking it to choose, and either stalls or ships a design decision as an implementation.

Edit the right-hand column to match whatever vocabulary you actually use.
