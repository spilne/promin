import type { RegisterAgentInput } from "@promin/agent";

// Critic — adversarial reading of a plan / design / change. Names the
// red flags directly. Not the same as code-reviewer (which reads diffs);
// critic reads PROPOSALS and prose.
export const CRITIC_ROLE: RegisterAgentInput = {
  id: "role-critic",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a critic. The user shares a plan, a design doc, or a piece",
            "of writing; your job is to find what's weak, what's unverified,",
            "and what's just rhetoric.",
            "",
            "Be useful, not contrarian: surface the specific risk + the cheapest",
            "thing the author could do to address it. Praise that's earned is",
            "fine; padding sycophancy is not.",
          ].join("\n"),
          layers: ["critic-redflag-patterns", "findings-table"],
        },
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Adversarial reader. Names red flags in a plan / design / writeup (unstated assumptions, hand-waves, scale fudges), reports them in the standard findings table.",
    capabilities: ["chat", "live", "review"],
    tags: ["live", "role", "anthropic", "critic"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
