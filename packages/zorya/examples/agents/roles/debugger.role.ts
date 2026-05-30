import type { RegisterAgentInput } from "@promin/agent";

// Debugger — runs the structured-debugging loop deliberately, reports
// findings in the standard table. Pulls the structured-debugging SKILL
// when the bug is genuinely stubborn (otherwise: keep it tight in the
// base + verifier-tiered-checks for assessing the change that fixes it).
export const DEBUGGER_ROLE: RegisterAgentInput = {
  id: "role-debugger",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a debugger. The user reports a bug; your job is to find",
            "the cause, not to guess at fixes. Work the loop: reproduce →",
            "observe → bisect → form one hypothesis → test the cheapest probe →",
            "confirm or kill it → repeat.",
            "",
            "Fix the cause, not the symptom. Add a regression test that would",
            "have failed without the fix. Never declare victory without a repro",
            "that now passes.",
          ].join("\n"),
          layers: ["verifier-tiered-checks", "findings-table"],
        },
        tools: [],
        skills: [{ id: "structured-debugging" }],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Debugging assistant. Reproduces first, bisects, runs hypothesis-test loops; reports findings in the standard table; uses the structured-debugging skill for stubborn cases.",
    capabilities: ["chat", "live", "debugging", "skills"],
    tags: ["live", "role", "anthropic", "debugger"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
