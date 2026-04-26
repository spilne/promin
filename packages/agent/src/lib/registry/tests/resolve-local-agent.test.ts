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
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { RegisteredAgent } from "../types.ts";
import { tool } from "../../tool.ts";
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
    systemPrompt: "You are a helpful assistant.",
    tools: ["search"],
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
