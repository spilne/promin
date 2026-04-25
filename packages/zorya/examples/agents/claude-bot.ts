import type { RegisterAgentInput } from "@promin/agent";

// Live LLM agent — requires ANTHROPIC_API_KEY in the environment. The
// demo's boot wiring filters this recipe out when the key isn't set,
// so it won't surface in the dashboard unless usable.
export const CLAUDE_BOT: RegisterAgentInput = {
  id: "claude-bot",
  backend: {
    type: "local",
    // claude-sonnet-4-6 is the current cheap-fast capable model. Swap to
    // claude-opus-4-7 for harder tasks or claude-haiku-4-5-20251001 for
    // the cheapest-fastest tier — the adapter is the same.
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt:
      "You are Claude, a helpful AI assistant running inside the Promin/Zorya agent gateway demo. " +
      "Be concise and conversational.\n\n" +
      "You have access to four tools:\n" +
      "  - `weather(city)` — current conditions and temperature for a city.\n" +
      "  - `currentTime(timezone?)` — current date/time in an IANA timezone (defaults to UTC).\n" +
      "  - `calculate(a, b, op)` — single arithmetic op (add/subtract/multiply/divide). Chain multiple calls for compound expressions.\n" +
      "  - `listWorkflows(limit?)` — workflows registered on this Zorya server with recent run counts.\n\n" +
      "Call tools whenever they would give a better answer than guessing. If asked what model you are, answer truthfully: claude-sonnet-4-6 via the Anthropic Messages API.",
    tools: ["weather", "currentTime", "calculate", "listWorkflows"],
  },
  metadata: {
    description:
      "Live Claude (sonnet-4-6) with weather, currentTime, calculate, and listWorkflows tools. Streams natively via the Anthropic Messages API; requires ANTHROPIC_API_KEY.",
    capabilities: ["chat", "tools", "live"],
    tags: ["live", "anthropic"],
  },
};
