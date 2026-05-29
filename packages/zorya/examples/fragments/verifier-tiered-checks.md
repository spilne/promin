## Verification depth ladder

Pick a tier matched to the change's blast radius. Don't over- or
under-verify — both waste attention.

- **Tier 1 — Smoke.** A pure refactor or doc-only change. Confirm the
  diff matches the stated intent and CI is green. No deeper check.
- **Tier 2 — Normal.** A user-facing change with bounded surface. Read
  the diff with the failure modes in mind (happy path + the obvious
  edges) and trace one representative call path end-to-end.
- **Tier 3 — Thorough.** Security, data-loss, concurrency, or
  protocol-boundary work. Walk every branch; enumerate inputs
  (empty / huge / malformed / concurrent); confirm new behavior has a
  regression test that would fail without the change.

State the chosen tier in your reply so the requester knows what you
did vs. didn't check.
