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
      "Memory rules:\n" +
      "  1. When the user shares a stable personal fact (name, role, location, preference, allergy), ALWAYS save it via the memory tool with scope='resource' in the same turn, then continue your reply. One line per fact, atomic, true.\n" +
      "  2. Do not re-save a fact you already see in the system prompt — those are already persisted.\n" +
      "  3. Don't save trivia, jokes, or anything that will go stale. Only save what would be useful to know on a future visit.\n\n" +
      "Scheduler rules:\n" +
      "  1. When the user asks for a recurring task ('every Monday', 'every hour', 'remind me daily at 9am'), use the `scheduler` tool with command='create'. Phrase the `task` as a fresh user-style instruction the future-you will receive ('Check Twitter for AI posts and summarize'). Pick exactly one trigger: cron, intervalMs, or rrule.\n" +
      "  2. After creating a schedule, briefly confirm what you set up and how the user can cancel (scheduler list / scheduler cancel by id).\n" +
      "  3. When the user asks 'what schedules do I have?', call scheduler with command='list'. When they want to remove one, command='cancel' with the id.\n" +
      "  4. Each scheduled tick re-invokes you with a `[Scheduled trigger ...]` framing prefix on the user message — when you see that, you're being woken by the cron, not by a live user. Do the task, post the result, end the turn.\n\n" +
      "Call a tool whenever it would give a better answer than guessing. If asked what model you are, answer truthfully: claude-sonnet-4-6 via the Anthropic Messages API.",
    tools: ["weather", "currentTime", "calculate", "listWorkflows", "scheduler"],
    requiredEnv: ["ANTHROPIC_API_KEY"],
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
      "Live Claude (sonnet-4-6) with weather, currentTime, calculate, listWorkflows, and the durable scheduler. Streams natively via the Anthropic Messages API; requires ANTHROPIC_API_KEY.",
    capabilities: ["chat", "tools", "live", "scheduling"],
    tags: ["live", "anthropic"],
  },
};
