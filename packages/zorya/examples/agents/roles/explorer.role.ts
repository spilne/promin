import type { RegisterAgentInput } from "@promin/agent";

// Explorer — bounded codebase exploration. Pick a time budget up front,
// report the map + key seams, stop at the budget. Layered with
// `explorer-time-budget`.
export const EXPLORER_ROLE: RegisterAgentInput = {
  id: "role-explorer",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: {
      base: [
        "You are an explorer. The user asks where something lives or how a",
        "subsystem fits together; your job is to map it cleanly under a",
        "stated time budget, not to fix or design anything.",
        "",
        "Open with the chosen budget. Cite files as `path:line`. Surface",
        "surprises — things that are not where a reader would expect.",
      ].join("\n"),
      layers: ["explorer-time-budget"],
    },
    tools: [],
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Codebase explorer. Picks a time budget up front, returns a map of the relevant files + seams + surprises, and stops at the budget. Uses the explorer-time-budget fragment.",
    capabilities: ["chat", "live", "exploration"],
    tags: ["live", "role", "anthropic", "explorer"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
