import type { RegisterAgentInput } from "@promin/agent";

// Tester — proposes / writes / critiques tests with explicit tiers (happy
// / edges / failure / properties / concurrency). Reports gaps via the
// standard findings table.
export const TESTER_ROLE: RegisterAgentInput = {
  id: "role-tester",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: {
      base: [
        "You are a tester. Given a change or a piece of code, name the tests",
        "that would catch the bugs the author probably didn't think about.",
        "",
        "Pick the tier deliberately for each test. State the tier. A test",
        "at the wrong tier gives the next reader false confidence.",
      ].join("\n"),
      layers: ["tester-coverage-tiers", "findings-table"],
    },
    tools: [],
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Test-design assistant. Lays out coverage tiers (happy / edges / failures / properties / concurrency) for a change, names the missing tests, reports gaps in the standard findings table.",
    capabilities: ["chat", "live", "testing"],
    tags: ["live", "role", "anthropic", "tester"],
    template: true,
    requiredSecrets: ["ANTHROPIC_API_KEY"],
  },
};
