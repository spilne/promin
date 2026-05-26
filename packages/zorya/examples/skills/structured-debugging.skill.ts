import type { RegisterSkillInput } from "@promin/agent";

// A reusable instruction block — NOT a tool. The agent pulls this into
// context via `loadSkill` when a debugging task warrants it. Discovered by
// the SkillScanner from this folder; referenced by an agent recipe's
// `backend.skills` catalog.
export const STRUCTURED_DEBUGGING: RegisterSkillInput = {
  id: "structured-debugging",
  description: "A disciplined, hypothesis-driven loop for tracking down a stubborn bug.",
  whenToUse:
    "The user reports a bug that resists an obvious fix, or asks why something is broken and the cause isn't immediately clear.",
  body: [
    "# Structured debugging",
    "",
    "Work the bug as a loop, not a guess. Narrate each step so the user can follow.",
    "",
    "1. **Reproduce.** Pin down the exact, minimal steps that trigger the failure. If you",
    "   can't reproduce it, your first job is a reliable repro — everything else waits.",
    "2. **Observe.** State what actually happens vs. what was expected. Quote the real",
    "   error / output; don't paraphrase it away.",
    "3. **Bisect.** Halve the search space each step — comment out, git-bisect, binary",
    "   search the input. Each cut should rule out roughly half of the remaining causes.",
    '4. **Hypothesize.** Form ONE falsifiable hypothesis at a time ("X is null because Y',
    "   runs before Z\"). Predict what you'd see if it were true.",
    "5. **Test the hypothesis** with the cheapest possible probe (a log line, a unit test,",
    "   a REPL call). Confirm or kill it before forming the next one.",
    "6. **Fix the cause, not the symptom.** Once confirmed, fix the root cause and add a",
    "   regression test that fails without the fix.",
    "",
    "Anti-patterns: changing several things at once, fixing by coincidence, or declaring",
    "victory without a repro that now passes.",
  ].join("\n"),
  metadata: {
    tags: ["engineering", "debugging"],
  },
};
