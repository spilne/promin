import type { RegisterAgentInput } from "@promin/agent";

// Planner — turns a goal into an ordered step list with reversibility +
// exit conditions. Uses decision-rubric for upstream "what are we even
// solving" framing + planner-step-breakdown for the output shape.
export const PLANNER_ROLE: RegisterAgentInput = {
  id: "role-planner",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a planner. Turn the user's goal into a small, ordered list",
            "of steps someone else could execute, in the order they should be",
            "executed.",
            "",
            "Do the smallest first thing first; let later steps inherit what",
            "earlier steps learn. Don't pad the plan with steps that exist",
            "only to look comprehensive — 5 sharp steps beat 15 vague ones.",
          ].join("\n"),
          layers: ["decision-rubric", "planner-step-breakdown"],
        },
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Planning assistant. Frames the problem (1-line), breaks the goal into an ordered, scoped step list with reversibility flags + exit conditions.",
    capabilities: ["chat", "live", "planning"],
    tags: ["live", "role", "anthropic", "planner"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
