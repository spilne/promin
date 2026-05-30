// ---------------------------------------------------------------------------
// `resolveLocalAgent` — turns a `RegisteredAgent` recipe into a live `LocalAgent`.
//
// Pins:
//   - injects the resolved LLMProvider for the (provider, modelId) pair
//   - filters tools to those listed in the recipe (extras are not added)
//   - throws on unknown tool by default; skips with onUnknownTool: 'skip'
//   - rejects non-local backends with a clear error
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { resolveLocalAgent } from "../resolve-local-agent.ts";
import { InMemoryAgentRegistry } from "../in-memory-agent-registry.ts";
import { LocalAgent } from "../../agent/local-agent.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { RegisteredAgent } from "../types.ts";
import { createElevatedTool, tool } from "../../tool.ts";
import { z } from "zod";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("Mock LLM exhausted");
      return r;
    },
  };
}

function makeRunner() {
  return createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
}

const baseRow = (): RegisteredAgent => ({
  id: "support",
  version: "v1",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: { inline: { systemPrompt: "You are a helpful assistant.", tools: ["search"] } },
  },
  metadata: { description: null, capabilities: [], tags: [] },
  createdAt: 0,
  updatedAt: 0,
});

const searchTool = tool({
  name: "search",
  description: "Search the web",
  parameters: z.object({ query: z.string() }),
  execute: async () => "(stub search result)",
});
const fileTool = tool({
  name: "fileRead",
  description: "Read a file",
  parameters: z.object({ path: z.string() }),
  execute: async () => "(stub file content)",
});

describe("resolveLocalAgent", () => {
  it("instantiates a LocalAgent that runs against the resolved LLM", async () => {
    const llmCalls: Array<{ provider: string; id: string }> = [];
    const agent = resolveLocalAgent(baseRow(), {
      runner: makeRunner(),
      llm: (provider, id) => {
        llmCalls.push({ provider, id });
        return mockLLM([{ content: "ok", finishReason: "stop" }]);
      },
      tools: { search: searchTool },
      namespaceId: "acme",
    });

    expect(llmCalls).toEqual([{ provider: "anthropic", id: "claude-sonnet-4-6" }]);
    const out = await agent.invoke({ task: "hi" });
    expect(await out.text).toBe("ok");
  });

  it("only wires the tools listed in the recipe", async () => {
    let observedTools: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        observedTools = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };

    const agent = resolveLocalAgent(baseRow(), {
      runner: makeRunner(),
      llm: () => llm,
      // Intentionally provide MORE tools than the recipe listed.
      tools: { search: searchTool, fileRead: fileTool },
      namespaceId: "acme",
    });

    await agent.invoke({ task: "hi" });
    expect(observedTools).toEqual(["search"]); // fileRead not selected
  });

  it("throws on unknown tool by default", () => {
    const row = baseRow();
    expect(() =>
      resolveLocalAgent(row, {
        runner: makeRunner(),
        llm: () => mockLLM([]),
        tools: {}, // search missing
        namespaceId: "acme",
      }),
    ).toThrow(/search/);
  });

  it("skips unknown tools when onUnknownTool='skip'", async () => {
    let observedTools: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        observedTools = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };
    const agent = resolveLocalAgent(baseRow(), {
      runner: makeRunner(),
      llm: () => llm,
      tools: {}, // search missing
      onUnknownTool: "skip",
      namespaceId: "acme",
    });
    await agent.invoke({ task: "hi" });
    expect(observedTools).toEqual([]); // silently dropped
  });

  it("recipe-level autoCompact / autoDistill / contextBudget override host defaults", async () => {
    const row: RegisteredAgent = {
      ...baseRow(),
      backend: {
        ...baseRow().backend,
        autoCompact: { messageThreshold: 99, mode: "blocking" },
        autoDistill: { messageThreshold: 7 },
        contextBudget: { maxMessageTokens: 4_000, maxEpisodeTokens: 1_000 },
      } as RegisteredAgent["backend"],
    };
    const agent = resolveLocalAgent(row, {
      runner: makeRunner(),
      llm: () => mockLLM([]),
      tools: { search: searchTool },
      namespaceId: "acme",
      // Host defaults — should be OVERRIDDEN by the recipe's values.
      autoCompact: { messageThreshold: 1, mode: "background" },
      autoDistill: { messageThreshold: 1 },
      contextBudget: { maxMessageTokens: 100 },
    });
    // Inspect the underlying config the agent was built with.
    const cfg = (agent as unknown as { config: Record<string, unknown> }).config;
    expect((cfg.autoCompact as Record<string, unknown>).messageThreshold).toBe(99);
    expect((cfg.autoCompact as Record<string, unknown>).mode).toBe("blocking");
    expect((cfg.autoDistill as Record<string, unknown>).messageThreshold).toBe(7);
    expect((cfg.contextBudget as Record<string, unknown>).maxMessageTokens).toBe(4_000);
    expect((cfg.contextBudget as Record<string, unknown>).maxEpisodeTokens).toBe(1_000);
  });

  it("recipe-level autoCompact: false explicitly disables even when host enables", async () => {
    const row: RegisteredAgent = {
      ...baseRow(),
      backend: {
        ...baseRow().backend,
        autoCompact: false,
      } as RegisteredAgent["backend"],
    };
    const agent = resolveLocalAgent(row, {
      runner: makeRunner(),
      llm: () => mockLLM([]),
      tools: { search: searchTool },
      namespaceId: "acme",
      autoCompact: { messageThreshold: 4, mode: "background" }, // host enables
    });
    const cfg = (agent as unknown as { config: Record<string, unknown> }).config;
    expect(cfg.autoCompact).toBe(false);
  });

  it("recipe inherits host's autoCompact when recipe leaves it unset", async () => {
    const row = baseRow(); // no autoCompact on the recipe
    const agent = resolveLocalAgent(row, {
      runner: makeRunner(),
      llm: () => mockLLM([]),
      tools: { search: searchTool },
      namespaceId: "acme",
      autoCompact: { messageThreshold: 12 },
    });
    const cfg = (agent as unknown as { config: Record<string, unknown> }).config;
    expect((cfg.autoCompact as Record<string, unknown>).messageThreshold).toBe(12);
  });

  it("host's `when` predicate carries through when recipe sets numeric thresholds", async () => {
    const row: RegisteredAgent = {
      ...baseRow(),
      backend: {
        ...baseRow().backend,
        autoCompact: { messageThreshold: 50 }, // recipe numeric only
      } as RegisteredAgent["backend"],
    };
    const hostWhen = () => false;
    const agent = resolveLocalAgent(row, {
      runner: makeRunner(),
      llm: () => mockLLM([]),
      tools: { search: searchTool },
      namespaceId: "acme",
      autoCompact: { when: hostWhen, messageThreshold: 1 }, // host: closure + low threshold
    });
    const cfg = (agent as unknown as { config: Record<string, unknown> }).config;
    const ac = cfg.autoCompact as { messageThreshold?: number; when?: unknown };
    // Recipe wins on the numeric field; host's `when` carries through.
    expect(ac.messageThreshold).toBe(50);
    expect(ac.when).toBe(hostWhen);
  });

  it("rejects non-local backends", () => {
    const row = baseRow();
    // biome-ignore lint/suspicious/noExplicitAny: forging a future backend type
    const futureRow: RegisteredAgent = { ...row, backend: { type: "acp" as any } as any };
    expect(() =>
      resolveLocalAgent(futureRow, {
        runner: makeRunner(),
        llm: () => mockLLM([]),
        tools: {},
        namespaceId: "acme",
      }),
    ).toThrow(/not supported/);
  });
});

// --- elevated-tool capability gate --------------------------------------

const adminTool = createElevatedTool({
  name: "admin_action",
  description: "Run a privileged admin action",
  parameters: z.object({}),
  requires: "admin",
  execute: async (_input, ctx) => {
    ctx.audit({ action: "admin_action" });
    return "ok";
  },
});
const ungatedElevatedTool = createElevatedTool({
  name: "elevated_action",
  description: "An elevated tool that declares no explicit requires",
  parameters: z.object({}),
  execute: async (_input, ctx) => {
    ctx.audit({ action: "elevated_action" });
    return "ok";
  },
});

/** Recipe row with the given capabilities + recipe tool list. */
function recipeWith(capabilities: string[], toolNames: string[]): RegisteredAgent {
  const row = baseRow();
  return {
    ...row,
    backend: {
      ...row.backend,
      role: { inline: { systemPrompt: "You are a helpful assistant.", tools: toolNames } },
    },
    metadata: { description: null, capabilities, tags: [] },
  };
}

/** Resolve + run one turn, return the tool names the LLM was offered. */
async function offeredTools(
  recipe: RegisteredAgent,
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary shapes
  tools: Record<string, any>,
): Promise<string[]> {
  let observed: string[] = [];
  const llm: LLMProvider = {
    chat: async (params) => {
      observed = (params.tools ?? []).map((t) => t.name);
      return { content: "ok", finishReason: "stop" };
    },
  };
  const agent = resolveLocalAgent(recipe, {
    runner: makeRunner(),
    llm: () => llm,
    tools,
    namespaceId: "acme",
  });
  await agent.invoke({ task: "hi" });
  return observed;
}

describe("resolveLocalAgent — elevated-tool capability gate", () => {
  it("hides a requires-gated elevated tool when the recipe lacks the capability", async () => {
    const observed = await offeredTools(recipeWith([], ["search", "admin_action"]), {
      search: searchTool,
      admin_action: adminTool,
    });
    expect(observed).toContain("search");
    expect(observed).not.toContain("admin_action");
  });

  it("exposes a requires-gated elevated tool when the capability is granted", async () => {
    const observed = await offeredTools(recipeWith(["admin"], ["search", "admin_action"]), {
      search: searchTool,
      admin_action: adminTool,
    });
    expect(observed).toContain("admin_action");
  });

  it("an elevated tool with no `requires` needs the implicit 'elevated' capability", async () => {
    const tools = { elevated_action: ungatedElevatedTool };
    expect(await offeredTools(recipeWith([], ["elevated_action"]), tools)).not.toContain(
      "elevated_action",
    );
    expect(await offeredTools(recipeWith(["elevated"], ["elevated_action"]), tools)).toContain(
      "elevated_action",
    );
  });

  it("never gates a bare (non-elevated) tool", async () => {
    const observed = await offeredTools(recipeWith([], ["search"]), { search: searchTool });
    expect(observed).toEqual(["search"]);
  });
});

// --- role binding ---------------------------------------------------------

describe("resolveLocalAgent — role binding", () => {
  /** Resolve + run one turn, return { system, tools } the LLM saw. */
  async function runWith(
    recipe: RegisteredAgent,
    // biome-ignore lint/suspicious/noExplicitAny: tool shapes vary
    tools: Record<string, any>,
    extraDeps: Partial<Parameters<typeof resolveLocalAgent>[1]> = {},
  ): Promise<{ system: string | undefined; tools: string[] }> {
    let system: string | undefined;
    let observed: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        system = params.messages.find((m) => m.role === "system")?.content;
        observed = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };
    const agent = resolveLocalAgent(recipe, {
      runner: makeRunner(),
      llm: () => llm,
      tools,
      namespaceId: "acme",
      ...extraDeps,
    });
    await agent.invoke({ task: "hi" });
    return { system, tools: observed };
  }

  it("an inline role drives the resolved systemPrompt + tools", async () => {
    const row = baseRow();
    const recipe: RegisteredAgent = {
      ...row,
      backend: {
        ...row.backend,
        role: { inline: { systemPrompt: "ROLE PROMPT", tools: ["fileRead"] } },
      },
    };
    const { system, tools } = await runWith(recipe, {
      search: searchTool,
      fileRead: fileTool,
    });
    expect(system).toBe("ROLE PROMPT");
    expect(tools).toEqual(["fileRead"]);
  });

  it("a pre-resolved role (deps.role) is the source of truth for a ref binding", async () => {
    const row = baseRow();
    const recipe: RegisteredAgent = {
      ...row,
      backend: { ...row.backend, role: { ref: { id: "git-master" } } },
    };
    const { system, tools } = await runWith(
      recipe,
      { fileRead: fileTool },
      { role: { systemPrompt: "RESOLVED ROLE", tools: ["fileRead"] } },
    );
    expect(system).toBe("RESOLVED ROLE");
    expect(tools).toEqual(["fileRead"]);
  });

  it("throws when a ref binding is not pre-resolved into deps.role", () => {
    const row = baseRow();
    const recipe: RegisteredAgent = {
      ...row,
      backend: { ...row.backend, role: { ref: { id: "git-master" } } },
    };
    expect(() =>
      resolveLocalAgent(recipe, {
        runner: makeRunner(),
        llm: () => mockLLM([]),
        tools: {},
      }),
    ).toThrow(/role is a ref/);
  });

  it("the role's capabilities gate elevated tools", async () => {
    const row = baseRow();
    const recipe: RegisteredAgent = {
      ...row,
      // recipe metadata has no capabilities; the role grants 'admin'.
      backend: {
        ...row.backend,
        role: { inline: { systemPrompt: null, tools: ["admin_action"], capabilities: ["admin"] } },
      },
    };
    const { tools } = await runWith(recipe, { admin_action: adminTool });
    expect(tools).toContain("admin_action");
  });
});

// --- LocalAgent.fromRegistry ----------------------------------------------

describe("LocalAgent.fromRegistry", () => {
  const okDeps = () => ({
    runner: makeRunner(),
    llm: () => mockLLM([{ content: "ok", finishReason: "stop" }]),
    tools: { search: searchTool },
    namespaceId: "acme",
  });

  it("fetches the recipe by id and returns a working agent", async () => {
    const registry = new InMemoryAgentRegistry();
    const row = baseRow();
    await registry.register({ id: row.id, backend: row.backend, metadata: row.metadata });

    const agent = await LocalAgent.fromRegistry(registry, row.id, okDeps());
    const out = await agent.invoke({ task: "hi" });
    expect(await out.text).toBe("ok");
  });

  it("throws a clear error when the id is unknown", async () => {
    const registry = new InMemoryAgentRegistry();
    await expect(LocalAgent.fromRegistry(registry, "ghost", okDeps())).rejects.toThrow(
      /no agent "ghost"/,
    );
  });

  it("resolves the requested version", async () => {
    const registry = new InMemoryAgentRegistry();
    const row = baseRow();
    await registry.register({
      id: row.id,
      version: "v1",
      backend: { ...row.backend, role: { inline: { systemPrompt: "ALPHA", tools: ["search"] } } },
      metadata: row.metadata,
    });
    await registry.register({
      id: row.id,
      version: "v2",
      backend: { ...row.backend, role: { inline: { systemPrompt: "BETA", tools: ["search"] } } },
      metadata: row.metadata,
    });

    // Capture the system prompt the resolved agent sends to the LLM.
    let seenSystem: string | undefined;
    const capturingLlm: LLMProvider = {
      chat: async (params) => {
        seenSystem = params.messages.find((m) => m.role === "system")?.content;
        return { content: "ok", finishReason: "stop" };
      },
    };
    const agent = await LocalAgent.fromRegistry(
      registry,
      row.id,
      {
        runner: makeRunner(),
        llm: () => capturingLlm,
        tools: { search: searchTool },
        namespaceId: "acme",
      },
      { version: "v1" },
    );
    await agent.invoke({ task: "hi" });
    expect(seenSystem).toBe("ALPHA");
  });

  it("surfaces a clear error for an unknown version", async () => {
    const registry = new InMemoryAgentRegistry();
    const row = baseRow();
    await registry.register({ id: row.id, backend: row.backend, metadata: row.metadata });
    await expect(
      LocalAgent.fromRegistry(registry, row.id, okDeps(), { version: "v99" }),
    ).rejects.toThrow(/version "v99"/);
  });
});
