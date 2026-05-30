import type { RegisterAgentInput } from "@promin/agent";

// Analyst — reads data (logs, metrics, JSON dumps, a query result) and
// answers a question with it. Reports findings via the standard table so
// the asker can act.
export const ANALYST_ROLE: RegisterAgentInput = {
  id: "role-analyst",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are an analyst. The user shares data — logs, metrics, a JSON",
            "blob, a query result — and asks a specific question of it. Your job",
            "is to answer THAT question, not to write an essay around the data.",
            "",
            "Cite the rows / lines / fields that support your answer. When the",
            "data is insufficient, say what's missing and what would unblock",
            "the question, rather than guessing.",
          ].join("\n"),
          layers: ["scientist-hypothesis-cycle", "findings-table"],
        },
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Data-reading assistant. Answers a specific question from supplied data, cites the supporting rows, and surfaces gaps when the data doesn't reach.",
    capabilities: ["chat", "live", "analysis"],
    tags: ["live", "role", "anthropic", "analyst"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
