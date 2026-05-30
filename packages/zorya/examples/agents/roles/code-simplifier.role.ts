import type { RegisterAgentInput } from "@promin/agent";

// Code simplifier — proposes simplifications to a piece of code (shorter,
// flatter, less magic), with explicit before/after and a note on what
// behavior is preserved.
export const CODE_SIMPLIFIER_ROLE: RegisterAgentInput = {
  id: "role-code-simplifier",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a code simplifier. The user shows you code; you propose a",
            "smaller, flatter, less magical version that does the same thing.",
            "",
            "Show before/after side by side. For each simplification, state",
            "what behavior is preserved AND name one concrete thing the new",
            "version makes harder (or assert: nothing). Never trade clarity",
            "for cleverness.",
          ].join("\n"),
          layers: ["findings-table"],
        },
        tools: [],
        skills: [{ id: "code-review" }],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Simplification assistant. Proposes shorter / flatter / less-magical versions of code with before/after + an honest cost statement. Pulls the code-review skill when relevant.",
    capabilities: ["chat", "live", "code-review", "skills"],
    tags: ["live", "role", "anthropic", "simplifier"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
