// Curated prompt fragments — small, reusable instruction layers role
// recipes compose into their system prompt via `systemPrompt.layers[]`.
// Concatenated at resolve time by `resolveLocalAgent` when a
// FragmentRegistry is wired.
//
// Each entry is operator-curated markdown. Keep them tight (50-100 lines)
// and behavioral: rubrics, checklists, output-format pins. Not "you are X"
// preambles — that's the role recipe's `base`.

export const FRAGMENTS: Record<string, string> = {
  // -------------------------------------------------------------------------
  "verifier-tiered-checks": [
    "## Verification depth ladder",
    "",
    "Pick a tier matched to the change's blast radius. Don't over- or",
    "under-verify — both waste attention.",
    "",
    "- **Tier 1 — Smoke.** A pure refactor or doc-only change. Confirm the",
    "  diff matches the stated intent and CI is green. No deeper check.",
    "- **Tier 2 — Normal.** A user-facing change with bounded surface. Read",
    "  the diff with the failure modes in mind (happy path + the obvious",
    "  edges) and trace one representative call path end-to-end.",
    "- **Tier 3 — Thorough.** Security, data-loss, concurrency, or",
    "  protocol-boundary work. Walk every branch; enumerate inputs",
    "  (empty / huge / malformed / concurrent); confirm new behavior has a",
    "  regression test that would fail without the change.",
    "",
    "State the chosen tier in your reply so the requester knows what you",
    "did vs. didn't check.",
  ].join("\n"),

  // -------------------------------------------------------------------------
  "explorer-time-budget": [
    "## Time-budget ladder",
    "",
    "Pick a budget before you start so you don't drift. Stop at the budget,",
    "report what you found, and let the requester decide whether to go deeper.",
    "",
    "- **5 minutes** — a single file or one targeted question. Goal: a",
    "  pointer + a one-line conclusion.",
    "- **15 minutes** — a small subsystem. Goal: a map of the relevant files",
    "  + the key types/functions + one or two surprising observations.",
    "- **60 minutes** — a whole feature or unfamiliar package. Goal: a",
    "  written summary of the surface area, the seams it sits on, and the",
    "  risks to watch when changing it.",
    "",
    "Open with the chosen budget. End with what you'd do next if granted",
    "more time. Don't keep exploring once you've found enough to answer.",
  ].join("\n"),

  // -------------------------------------------------------------------------
  "findings-table": [
    "## Findings output format",
    "",
    "When reporting issues — review notes, audit results, verification",
    "findings — use a single markdown table so the requester can scan and",
    "prioritize:",
    "",
    "| Severity | Location | Finding | Suggestion |",
    "| --- | --- | --- | --- |",
    "",
    "- **Severity**: `blocker` / `major` / `minor` / `nit`. Use `blocker`",
    "  only for correctness or security issues that must be fixed before",
    "  merging.",
    "- **Location**: clickable `file:line` form (e.g. `src/foo.ts:42`).",
    "- **Finding**: one sentence on what's wrong.",
    "- **Suggestion**: one sentence on the smallest fix. Code only when a",
    "  one-liner is genuinely clearer than prose.",
    "",
    "If you have nothing to report, say so plainly — don't pad the table",
    "with nits to look thorough.",
  ].join("\n"),

  // -------------------------------------------------------------------------
  "decision-rubric": [
    "## Architectural decision rubric",
    "",
    "Frame each recommendation as a deliberate tradeoff, not a preference.",
    "Before you propose a design, work through these four:",
    "",
    "1. **What problem are we actually solving?** State it in one sentence.",
    "   If you can't, the design isn't ready — ask first.",
    "2. **What are the two or three real options?** Including the boring",
    "   one (\"do nothing / extend what exists\"). Don't pretend there's",
    "   only one path.",
    "3. **For each option, what does it cost AND what does it foreclose?**",
    '   The cost answer is the easy half; "what future moves does this',
    '   make harder?" is the half people skip.',
    "4. **Which option, and why this one?** Tie it back to the problem",
    '   statement. If the answer is "it depends," name the variable that',
    "   should decide it and who owns that call.",
    "",
    "Output the four sections explicitly. Brevity matters — bullet form,",
    "not essay.",
  ].join("\n"),
};
