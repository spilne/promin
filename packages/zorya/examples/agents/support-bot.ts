import type { RegisterAgentInput } from "@promin/agent";

export const SUPPORT_BOT: RegisterAgentInput = {
  id: "support-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "support-v1" },
    systemPrompt:
      "You are Acme's customer support assistant. Triage issues, gather details, and resolve common problems.",
    tools: [],
  },
  metadata: {
    description: "Customer support triage agent.",
    capabilities: ["chat", "triage"],
    tags: ["demo", "beta"],
  },
};
