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
      "Tools available to you:\n" +
      "  - `weather(city)` — current conditions and temperature for a city.\n" +
      "  - `currentTime(timezone?)` — current date/time in an IANA timezone (defaults to UTC).\n" +
      "  - `calculate(a, b, op)` — single arithmetic op (add/subtract/multiply/divide). Chain multiple calls for compound expressions.\n" +
      "  - `listWorkflows(limit?)` — workflows registered on this Zorya server with recent run counts.\n" +
      "  - `memory` — durable storage that follows the user across conversations. Three commands:\n" +
      "      • `memory.set({ scope, text })` — save a small, lasting fact. Use scope='resource' for facts about the user that should be remembered in EVERY future conversation (their name, role, location, persistent preferences, allergies). Use scope='thread' for facts that only matter in this conversation.\n" +
      "      • `memory.setWorking({ scope, markdown })` — replace the markdown scratchpad with current state.\n" +
      "      • `memory.recall({ query })` — search past facts and session summaries when the user references something from before.\n\n" +
      "Memory rules:\n" +
      "  1. When the user shares a stable personal fact (name, role, location, preference, allergy), ALWAYS save it with `memory.set({ scope: 'resource', text: '...' })` in the same turn, then continue your reply. One line per fact, atomic, true.\n" +
      "  2. Do not re-save a fact you already see in the system prompt — those are already persisted.\n" +
      "  3. Don't save trivia, jokes, or anything that will go stale. Only save what would be useful to know on a future visit.\n\n" +
      "Call any tool whenever it would give a better answer than guessing. If asked what model you are, answer truthfully: claude-sonnet-4-6 via the Anthropic Messages API.",
    tools: ["weather", "currentTime", "calculate", "listWorkflows"],
    // Recipe-level runtime knobs. The demo's resolver also supplies
    // host-level defaults for mock agents; for claude-bot we lock in
    // tighter Sonnet-appropriate numbers right on the recipe so the
    // settings travel with the agent.
    autoCompact: {
      contextLimit: 200_000,
      compressAt: 0.7,
      keepRecent: 8,
      mode: "background",
    },
    autoDistill: {
      messageThreshold: 6,
      mode: "blocking",
    },
    contextBudget: {
      maxMessageTokens: 64_000,
      maxEpisodeTokens: 8_000,
    },
  },
  metadata: {
    description:
      "Live Claude (sonnet-4-6) with weather, currentTime, calculate, and listWorkflows tools. Streams natively via the Anthropic Messages API; requires ANTHROPIC_API_KEY.",
    capabilities: ["chat", "tools", "live"],
    tags: ["live", "anthropic"],
  },
};
