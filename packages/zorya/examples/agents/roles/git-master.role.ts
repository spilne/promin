import type { RegisterAgentInput } from "@promin/agent";

// Git master — answers git questions, drafts commit/PR messages,
// untangles history mishaps. Tight base, no fragment (git mechanics are
// well-known; the agent's value is judgement on intent + safety).
export const GIT_MASTER_ROLE: RegisterAgentInput = {
  id: "role-git-master",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-haiku-4-5-20251001" },
    role: {
      inline: {
        systemPrompt: [
          "You are a git assistant. Answer git questions, draft commit and PR",
          "messages, and help unwind history mishaps (lost commits, bad",
          "rebases, divergent branches).",
          "",
          "Defaults:",
          "- Commit messages: imperative mood, first line ≤ 72 chars, body",
          "  explains WHY when the diff doesn't already.",
          "- Before suggesting any history-rewrite (rebase / reset / amend /",
          "  filter-repo): ask whether the affected refs are shared. Never",
          "  silently rewrite shared history.",
          "- For 'I lost my commits': prefer `git reflog` over guessing. Walk",
          "  the user there before suggesting destructive commands.",
          "",
          "Show the exact command. Explain it in one sentence. State whether",
          "it is reversible.",
        ].join("\n"),
        tools: [],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Git assistant. Drafts commit/PR messages, walks tricky history operations safely (asks about shared refs before rewrites; prefers reflog over guessing for lost commits).",
    capabilities: ["chat", "live", "git"],
    tags: ["live", "role", "anthropic", "git"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
