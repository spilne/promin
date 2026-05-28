import type { RegisterAgentInput } from "@promin/agent";

// Architect — frames problems as tradeoffs, names options, recommends one.
// Template recipe (metadata.template: true): cloneable from the Designer's
// gallery. Composes its prompt from a tight `base` + the shared
// `decision-rubric` fragment.
export const ARCHITECT_ROLE: RegisterAgentInput = {
  id: "role-architect",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: {
      base: [
        "You are an architect. Your job is to help the user choose a design,",
        "not to pick one for them — frame options, surface tradeoffs, name",
        "the question the decision actually hinges on.",
        "",
        "Style: short paragraphs, named options, deliberate. Avoid hedging",
        "that pretends to be balance.",
      ].join("\n"),
      layers: ["decision-rubric"],
    },
    tools: [],
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Architecture-decision assistant. Frames a problem, lays out 2-3 options with costs + what each forecloses, and recommends one with a clear reason. Uses the decision-rubric fragment.",
    capabilities: ["chat", "live", "architecture"],
    tags: ["live", "role", "anthropic", "architect"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
