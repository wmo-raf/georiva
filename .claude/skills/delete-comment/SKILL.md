---
name: delete-comment
description: Sweep code comments and docstrings out of a part of the codebase and lower the comment ratchet. Use when the user runs /delete-comment, asks to delete or triage comments, or the comment ratchet in `just lint` fails.
argument-hint: "[path or package, e.g. georiva/src/georiva/harvest]"
---

# Delete comments

Comments and docstrings are banned in Python. `scripts/comment_ratchet.py` (run by `just lint`) holds a per-file
baseline in `scripts/comment-baseline.json`. A file may have fewer comment lines than its baseline, never more, and
a new file starts at zero. `# noqa`, `# type:`, `# pragma:`, `# fmt:`, `# ruff:`, a coding line and a shebang
don't count.

## Process

Work on a branch. Take the path from the argument; if there isn't one, take the files with the highest counts in
the baseline. Read every comment and docstring in scope and put each one in exactly one category:

| Category | Test | Action |
| --- | --- | --- |
| **Tautology** | Restates the code: a banner (`# =====`), a section label (`# Status`), a docstring that repeats the name (`"""Get a temporary file path."""`) | Delete |
| **Stale** | Names a symbol, file, behaviour or issue that no longer matches the code (check with grep or `gh issue view`) | Delete |
| **Excuse** | Excuses a shortcut: TODO, "for now", "hack", "workaround", "until …", a stub left in place | Delete and file an issue (below) |
| **Decision Record** | Explains a non-obvious why that the code cannot show | See below |

**Decision Record:**

- If an ADR in `docs/adr/` already states the point, delete the comment, but only if you can quote the ADR
  sentence that says it. "The ADR covers this area" is not enough: a comment that explains why this particular
  line exists (a recursion guard, a cost bound, a quirk of a framework) is usually more specific than any ADR.
- If the point is architectural (it constrains more than this one spot) and no ADR has it, add it to the right ADR
  (or write a new one following the `domain-modeling` skill), then delete the comment.
- Otherwise, if it is local and worth keeping, **leave it**. The ratchet keeps it from growing, and a later sweep
  can reconsider it. Never delete a reason the code can't tell a reader just to lower a number.
- When unsure, keep it. A Decision Record deleted by mistake is lost; one kept by mistake costs a line.
- Documentation that other repositories rely on (test fakes like `FakeUpstream`, plugin base classes, `#:`
  attribute docs) is a contract, not a comment: keep it.

Before deleting a comment, try to make the code say it instead: a better name, an extracted function, a constant,
or a test that pins the behaviour.

## Mechanics

- When you delete a docstring that is a function or class's whole body, put `...` or `pass` in its place, or the
  file won't parse.
- Keep tool directives (`# noqa: E501`, `# type: ignore`); the ratchet doesn't count them.
- Leave comments inside string literals, templates and generated files alone.
- Then run `just format`. It formats the code and lowers the baseline (`just comment-ratchet-update` does only the
  second part). Run `just lint`, then the affected tests with `just test <labels>` (or `just test-worktree`).
- Commit the code and `scripts/comment-baseline.json` together.

## Excuse issues

One GitHub issue per excuse on `wmo-raf/georiva` (see `docs/agents/issue-tracker.md`, labels per
`docs/agents/triage-labels.md`). Show the user the list before filing. Each issue names the file and the function
(not a line number, since the comment is gone), what the shortcut is, and what a proper fix looks like.

## Report

End with: lines shed (the `--update` output), counts per category, issues filed, ADRs touched, and the Decision
Records you kept, with a reason for each.
