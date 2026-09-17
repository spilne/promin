import type { RegisterAgentInput } from "@promin/agent";

// Demonstrates the skills system: a live agent with a small skill catalog.
// The recipe only PINS which skills the agent may load (by id) — the host
// resolves the catalog from the SkillRegistry, injects each skill's
// description + whenToUse into the system prompt, and auto-attaches the
// `loadSkill` tool. The bodies live in ./skills/*.skill.ts and are pulled
// into context on demand, never baked into this recipe.
//
// Filtered out of the dashboard when ANTHROPIC_API_KEY is missing
// (see liveOnlyAgentIds in demo.ts).
export const SKILLED_BOT: RegisterAgentInput = {
  id: "skilled-bot",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: {
      inline: {
        systemPrompt:
          "You are a helpful engineering assistant. You have skills you can load on demand — " +
          "consult the '## Skills' section and load the matching skill before tackling a task it covers.",
        tools: [],
        // Catalog: which skills this agent may load. Versions omitted → the host
        // pins each to the registry's latest at resolve time.
        skills: [{ id: "structured-debugging" }, { id: "plain-writing" }],
      },
    },
    requiredEnv: ["ANTHROPIC_API_KEY"],
  },
  metadata: {
    description:
      "Live Claude with a skill catalog (structured-debugging, plain-writing). Demonstrates " +
      "load-on-demand skills: descriptions sit in the prompt, full instructions are pulled in " +
      "via loadSkill when a task matches. Requires ANTHROPIC_API_KEY.",
    capabilities: ["chat", "tools", "live", "skills"],
    tags: ["live", "anthropic", "skills"],
  },
};
