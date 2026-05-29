## Findings output format

When reporting issues — review notes, audit results, verification
findings — use a single markdown table so the requester can scan and
prioritize:

| Severity | Location | Finding | Suggestion |
| -------- | -------- | ------- | ---------- |

- **Severity**: `blocker` / `major` / `minor` / `nit`. Use `blocker`
  only for correctness or security issues that must be fixed before
  merging.
- **Location**: clickable `file:line` form (e.g. `src/foo.ts:42`).
- **Finding**: one sentence on what's wrong.
- **Suggestion**: one sentence on the smallest fix. Code only when a
  one-liner is genuinely clearer than prose.

If you have nothing to report, say so plainly — don't pad the table
with nits to look thorough.
