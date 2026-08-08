// ---------------------------------------------------------------------------
// reconcileToolReferences — pure-function diff between live catalog
// + recipe tool refs. Pinned cases:
//   1. All-resolved recipe → empty missing list, full resolved list
//   2. Partially-broken recipe → missing names accurate
//   3. Two recipes share a missing tool → orphan groups them
//   4. Non-local backends skipped (no false orphans)
//   5. Empty registry → empty report
//   6. Recipe with empty tools list → in `recipes` but with both lists empty
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { InMemoryAgentRegistry } from "../registry/in-memory-agent-registry.ts";
import { DefaultAgentToolCatalog } from "../tool-catalog.ts";
import { reconcileToolReferences } from "../reconcile-tool-refs.ts";
import { tool } from "../tool.ts";

const search = tool({
  name: "search",
  description: "Web search",
  parameters: z.object({ q: z.string() }),
  execute: async () => "(results)",
});

const memory = tool({
  name: "memory",
  description: "Read memory",
  parameters: z.object({ key: z.string() }),
  execute: async () => "(value)",
});

async function setup(opts?: {
  /** Tools wired into the live catalog (default: search + memory). */
  liveTools?: Record<string, ReturnType<typeof tool>>;
  /** Recipe tool refs to register, keyed by recipe id. */
  recipes?: Record<string, string[]>;
}) {
  const live = opts?.liveTools ?? { search, memory };
  const catalog = new DefaultAgentToolCatalog({ inProcess: live });
  const registry = new InMemoryAgentRegistry();
  for (const [id, tools] of Object.entries(opts?.recipes ?? {})) {
    await registry.register({
      id,
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: null, tools } },
      },
    });
  }
  return { catalog, registry };
}

describe("reconcileToolReferences", () => {
  it("returns all-resolved when every recipe tool ref is in the catalog", async () => {
    const { catalog, registry } = await setup({
      recipes: { "bot-a": ["search"], "bot-b": ["search", "memory"] },
    });
    const report = await reconcileToolReferences({ catalog, registry });
    expect(report.orphans).toEqual([]);
    expect(report.recipes).toHaveLength(2);
    const a = report.recipes.find((r) => r.recipeId === "bot-a");
    const b = report.recipes.find((r) => r.recipeId === "bot-b");
    expect(a?.resolved).toEqual(["search"]);
    expect(a?.missing).toEqual([]);
    expect(b?.resolved.sort()).toEqual(["memory", "search"]);
    expect(b?.missing).toEqual([]);
  });

  it("flags missing tool names per recipe", async () => {
    const { catalog, registry } = await setup({
      recipes: { "bot-a": ["search", "missing-tool"] },
    });
    const report = await reconcileToolReferences({ catalog, registry });
    const a = report.recipes.find((r) => r.recipeId === "bot-a");
    expect(a?.resolved).toEqual(["search"]);
    expect(a?.missing).toEqual(["missing-tool"]);
  });

  it("inverts to per-tool orphan groups when multiple recipes reference the same missing tool", async () => {
    const { catalog, registry } = await setup({
      recipes: {
        "bot-a": ["search", "vanished"],
        "bot-b": ["vanished"],
        "bot-c": ["search"],
      },
    });
    const report = await reconcileToolReferences({ catalog, registry });
    expect(report.orphans).toHaveLength(1);
    expect(report.orphans[0]?.toolName).toBe("vanished");
    expect(report.orphans[0]?.recipes.map((r) => r.id).sort()).toEqual(["bot-a", "bot-b"]);
  });

  it("skips non-local backends (remote / cursor have their own tool model)", async () => {
    const catalog = new DefaultAgentToolCatalog({ inProcess: { search } });
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "remote-bot",
      backend: { type: "remote", endpoint: "https://x", remoteAgentId: "remote-bot" },
    });
    await registry.register({
      id: "cursor-bot",
      backend: { type: "cursor" },
    });
    const report = await reconcileToolReferences({ catalog, registry });
    expect(report.recipes).toEqual([]);
    expect(report.orphans).toEqual([]);
  });

  it("returns empty report when no recipes are registered", async () => {
    const { catalog, registry } = await setup({ recipes: {} });
    const report = await reconcileToolReferences({ catalog, registry });
    expect(report.recipes).toEqual([]);
    expect(report.orphans).toEqual([]);
  });

  it("recipe with empty tools list appears in recipes with both arrays empty", async () => {
    const { catalog, registry } = await setup({ recipes: { "bot-a": [] } });
    const report = await reconcileToolReferences({ catalog, registry });
    const a = report.recipes.find((r) => r.recipeId === "bot-a");
    expect(a?.resolved).toEqual([]);
    expect(a?.missing).toEqual([]);
  });

  it("does not fail when a legacy local recipe has no role binding", async () => {
    const catalog = new DefaultAgentToolCatalog({ inProcess: { search } });
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "legacy-bot",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: undefined,
      } as never,
    });

    await expect(reconcileToolReferences({ catalog, registry })).resolves.toEqual({
      recipes: [],
      orphans: [],
    });
  });

  it("orphan output is stably ordered (alphabetical tool names, then recipe ids)", async () => {
    const { catalog, registry } = await setup({
      recipes: {
        "z-bot": ["zeta-missing", "alpha-missing"],
        "a-bot": ["alpha-missing"],
      },
    });
    const report = await reconcileToolReferences({ catalog, registry });
    expect(report.orphans.map((o) => o.toolName)).toEqual(["alpha-missing", "zeta-missing"]);
    const alpha = report.orphans[0];
    expect(alpha?.recipes.map((r) => r.id)).toEqual(["a-bot", "z-bot"]);
  });
});
