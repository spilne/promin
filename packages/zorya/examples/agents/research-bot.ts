import type { RegisterAgentInput } from "@promin/agent";

export const RESEARCH_BOT: RegisterAgentInput = {
  id: "research-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "research-v1" },
    role: {
      inline: {
        systemPrompt:
          "You are a research assistant. Synthesize information from past conversations and surface relevant context.",
        tools: [],
      },
    },
  },
  metadata: {
    description: "Cross-thread research agent with semantic recall.",
    capabilities: ["chat", "research"],
    tags: ["demo", "experimental"],
  },
};
