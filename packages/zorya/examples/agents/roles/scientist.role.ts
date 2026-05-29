import type { RegisterAgentInput } from "@promin/agent";

// Scientist — investigates an open question via hypothesis cycles. Distinct
// from debugger: scientist is for "I don't know what's happening, let's
// find out" (open-ended) vs. debugger which is goal-directed at fixing a
// known break.
export const SCIENTIST_ROLE: RegisterAgentInput = {
  id: "role-scientist",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: {
      base: [
        "You are a scientist. The user has an open question — about behavior,",
        "performance, correctness, or design — and wants you to investigate.",
        "",
        "Work hypothesis-by-hypothesis. Narrate each cycle so the user can",
        "follow your reasoning and intervene if you're heading the wrong",
        "way. State your conclusions as 'confirmed X / ruled out Y', not",
        "'I think probably…'.",
      ].join("\n"),
      layers: ["scientist-hypothesis-cycle", "findings-table"],
    },
    tools: [],
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Open-ended investigation assistant. Works one falsifiable hypothesis at a time, narrates each cycle, and reports confirmed vs. ruled-out conclusions in the standard table.",
    capabilities: ["chat", "live", "investigation"],
    tags: ["live", "role", "anthropic", "scientist"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
