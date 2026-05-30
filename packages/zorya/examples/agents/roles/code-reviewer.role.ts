import type { RegisterAgentInput } from "@promin/agent";

// Code reviewer — reviews a diff/PR. The review checklist lives in the
// `code-review` SKILL (load-on-demand) so the agent can pull it when the
// task warrants; the always-on FRAGMENT here is the findings-table format
// so every review reports issues in a consistent shape.
export const CODE_REVIEWER_ROLE: RegisterAgentInput = {
  id: "role-code-reviewer",
  backend: {
    type: "local",
    // Haiku — review is mostly diff-reading; cheaper model is fine for the
    // first pass. Operators can clone + bump to Sonnet for harder review.
    model: { provider: "anthropic", id: "claude-haiku-4-5-20251001" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a code reviewer. Review the change, not the author. Work",
            "the diff top to bottom and then step back: does it do what it",
            "claims, what does it break at the edges, and is the next reader",
            "going to understand it?",
            "",
            "Leave specific, actionable comments. Approve when the change is",
            "correct and clear — not when it's merely inoffensive. Defer to the",
            "loaded `code-review` skill for the full checklist when needed.",
          ].join("\n"),
          layers: ["findings-table"],
        },
        tools: [],
        skills: [{ id: "code-review" }],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Code-review assistant. Reviews a diff/PR for correctness, edges, tests, clarity. Pulls the code-review skill on demand; always reports findings in the standard table.",
    capabilities: ["chat", "live", "code-review", "skills"],
    tags: ["live", "role", "anthropic", "code-review"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
