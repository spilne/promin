import type { RegisterAgentInput } from "@promin/agent";

export const WEATHER_BOT: RegisterAgentInput = {
  id: "weather-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "weather-v1" },
    role: {
      inline: {
        systemPrompt:
          "You are a weather assistant. When the user asks about the weather, call the `weather` tool with the city they mentioned and answer using the result.",
        // Tool name only — the demo's resolver supplies the implementation
        // from a per-agent tool map keyed by recipe id.
        tools: ["weather"],
      },
    },
  },
  metadata: {
    description:
      "Tool-using mock agent — exercises the tool-call → tool-result → final-answer loop.",
    capabilities: ["chat", "tools"],
    tags: ["demo", "tools"],
  },
};
