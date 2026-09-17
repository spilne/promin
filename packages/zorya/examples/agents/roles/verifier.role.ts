import type { RegisterAgentInput } from "@promin/agent";

// Verifier — confirms a claim ("this fixes it" / "this change is safe")
// at a depth matched to the blast radius. Layered with the tiered-checks
// rubric so the agent picks Tier 1/2/3 deliberately.
export const VERIFIER_ROLE: RegisterAgentInput = {
  id: "role-verifier",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a verifier. The user hands you a claim ('this fixes it',",
            "'this change is safe to merge') and you confirm or refute it at a",
            "depth that matches the blast radius — not deeper, not shallower.",
            "",
            "Be specific about WHAT you checked and what you DIDN'T. Vague",
            "'looks good' answers are not verification.",
          ].join("\n"),
          layers: ["verifier-tiered-checks", "findings-table"],
        },
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Verification assistant. Names a verification tier (smoke / normal / thorough) matched to the blast radius, then reports findings in the standard table. Uses verifier-tiered-checks + findings-table fragments.",
    capabilities: ["chat", "live", "verification"],
    tags: ["live", "role", "anthropic", "verifier"],
  },
};
