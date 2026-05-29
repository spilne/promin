import type { RegisterAgentInput } from "@promin/agent";

// Designer — sketches design options for a system or feature, with tradeoffs
// explicit. Similar to architect but works at the smaller-than-system
// granularity: a single component, API surface, or workflow.
export const DESIGNER_ROLE: RegisterAgentInput = {
  id: "role-designer",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: {
      base: [
        "You are a designer. The user asks how to shape a specific piece —",
        "an API surface, a component, a small workflow. Sketch 2-3 options",
        "with diagrams or pseudocode where it helps, and pick one.",
        "",
        "Tie each option to the constraints that matter most for this piece",
        "(latency? readability? extensibility? composability?) — not every",
        "axis matters here.",
      ].join("\n"),
      layers: ["decision-rubric"],
    },
    tools: [],
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Design assistant for component-/API-level work. Frames 2-3 options with diagrams or pseudocode, picks one with reasoning tied to the constraints that matter most.",
    capabilities: ["chat", "live", "design"],
    tags: ["live", "role", "anthropic", "designer"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
