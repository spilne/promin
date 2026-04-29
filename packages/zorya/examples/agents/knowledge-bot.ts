import type { RegisterAgentInput } from "@promin/agent";

// Live LLM agent — answers questions over a small in-memory org
// knowledge base. The tools are wired in demo.ts; this recipe just
// declares the contract. Filtered out of the dashboard when
// ANTHROPIC_API_KEY is missing (see liveOnlyAgentIds in demo.ts).
export const KNOWLEDGE_BOT: RegisterAgentInput = {
  id: "knowledge-bot",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt:
      "You are the Spilne/Promin org-knowledge assistant. Your job is to answer questions about " +
      "company policies, runbooks, RFCs, engineering practices, and other internal documents using " +
      "ONLY the knowledge base — never invent facts.\n\n" +
      "Workflow:\n" +
      "  1. For any factual question, START by searching the knowledge base with 2–4 keywords from " +
      "the user's question. If a hit looks promising but the snippet is incomplete, fetch the full doc.\n" +
      "  2. Cite the doc id in your answer (e.g. `(source: oncall-runbook)`). When multiple docs " +
      "contributed, cite all of them.\n" +
      "  3. If the search returns nothing relevant, SAY SO clearly — do not fall back to general " +
      "knowledge. Suggest the user check with the doc owner or file an RFC.\n" +
      "  4. When the user shares a stable personal fact (their team, role, location, on-call status), " +
      "save it via the memory tool with scope='resource' so it persists across sessions.\n\n" +
      "Tone: concise, factual, link-style citations. Avoid fluff. If the user asks something the KB " +
      "doesn't cover, admit it.",
    tools: ["searchKnowledge", "getDocument"],
    requiredEnv: ["ANTHROPIC_API_KEY"],
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
    // Opts into the agents network so the model can discover and delegate
    // to peers in the same namespace. v1 policy: any peer in the default
    // network is fair game; depth-3 cap stops runaway A→B→A chains.
    network: {
      networks: ["default"],
      canDiscover: true,
      canCall: true,
      maxDepth: 3,
    },
  },
  metadata: {
    description:
      "Live Claude (sonnet-4-6) grounded in an in-memory org KB (engineering handbook, on-call runbook, " +
      "security policy, deploy guide, RFCs). Demonstrates retrieval-augmented chat with searchKnowledge " +
      "+ getDocument tools. Requires ANTHROPIC_API_KEY.",
    capabilities: ["chat", "tools", "live", "rag"],
    tags: ["live", "anthropic", "knowledge-base", "rag"],
  },
};
