import type { RegisterAgentInput } from "@promin/agent";

export const ECHO_BOT: RegisterAgentInput = {
  id: "echo-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "echo-v1" },
    role: {
      inline: {
        systemPrompt:
          "You are a friendly echo bot for Acme support. Repeat what the user said with a short acknowledgement.",
        tools: [],
      },
    },
  },
  metadata: {
    description: "Echoes user messages with a friendly tone.",
    capabilities: ["chat"],
    tags: ["demo", "stable"],
  },
};
