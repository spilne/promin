---
name: code-review
description: A checklist for reviewing a code change — correctness, edges, and clarity — used when reviewing a diff or PR.
tags: [engineering, review]
---

# Code review

Review the change, not the author. Work the diff top to bottom, then step back.

- **Correctness first.** Does it do what it claims? Trace the happy path, then the
  error paths. Look for off-by-one, null/undefined, and unhandled rejections.
- **Edges and inputs.** Empty, huge, malformed, concurrent. What happens at the
  boundaries the author probably didn't run?
- **Tests.** Is the new behavior covered by a test that would fail without the change?
  A fix without a regression test invites the bug back.
- **Clarity.** Could the next reader follow it? Flag names that mislead, comments that
  lie, and cleverness that doesn't earn its keep.
- **Scope.** Is anything here unrelated to the stated change? Unrelated churn hides bugs.

Leave specific, actionable comments. Approve when the change is correct and clear, not
when it's merely inoffensive.
