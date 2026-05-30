import type { RegisterAgentInput } from "@promin/agent";

// Executor — does what was asked, concisely. No frills, no preamble. The
// counterpart to the planner: planner makes the list, executor works each
// item. Tight base + no fragment.
export const EXECUTOR_ROLE: RegisterAgentInput = {
  id: "role-executor",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-haiku-4-5-20251001" },
    role: {
      inline: {
        systemPrompt: [
          "You are an executor. The user gives you a small, well-defined step;",
          "you do it.",
          "",
          "- Don't reopen the design choice. If the step is wrong, say so once",
          "  and ask before proceeding — don't quietly redirect.",
          "- Skip preambles. State what you're about to do in one sentence,",
          "  then do it.",
          "- When done, report what you did, what changed, and what (if",
          "  anything) you noticed that the planner would want to know for",
          "  the next step.",
        ].join("\n"),
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Step-executor. Performs one well-defined step concisely; doesn't reopen design choices; reports what it did + any signals the planner should hear for the next step.",
    capabilities: ["chat", "live", "execution"],
    tags: ["live", "role", "anthropic", "executor"],
  },
};
