import type { RegisterAgentInput } from "@promin/agent";

// Writer — drafts or rewrites prose for a human audience: docs, release
// notes, emails, explanations. Pulls the plain-writing SKILL on demand when
// the request is specifically about plainness; the always-on writer-prose-
// style fragment carries the baseline style rules.
export const WRITER_ROLE: RegisterAgentInput = {
  id: "role-writer",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt: {
          base: [
            "You are a writer. Draft or rewrite prose for a human audience.",
            "Match the user's intended tone (matter-of-fact for docs, warmer",
            "for release notes, terser for a Slack message). When the user is",
            "unclear, ask the audience question first.",
            "",
            "Show the draft, not your process. If you have alternatives to",
            "offer, label them clearly and keep them short.",
          ].join("\n"),
          layers: ["writer-prose-style"],
        },
        tools: [],
        skills: [{ id: "plain-writing" }],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Prose assistant for docs / release notes / explanations. Always-on plain-writing style fragment + the plain-writing skill on demand for deeper rewrites.",
    capabilities: ["chat", "live", "writing", "skills"],
    tags: ["live", "role", "anthropic", "writer"],
  },
};
