// ---------------------------------------------------------------------------
// LocalAgent retriever bindings — tool-mode bindings attach a search tool,
// context-mode bindings inject retrieved knowledge as a system message
// before each turn (one-shot and thread), and that context never lands in
// the persisted thread history (including turns resumed after an approval).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryRetriever } from "../../rag/in-memory-retriever.ts";
import type { RetrieveRequest, Retriever } from "../../rag/types.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import { tool } from "../../tool.ts";

function makeRunner() {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

function returnsRetriever(): InMemoryRetriever {
  return new InMemoryRetriever({
    documents: [
      {
        id: "returns",
        title: "Returns",
        uri: "https://example.test/returns",
        text: "Returns allow unopened items within 30 days.",
      },
    ],
  });
}

/** LLM that records what each call saw and replies from a script. */
function recordingLLM(responses: LLMResponse[]): {
  llm: LLMProvider;
  calls: Array<{ tools: string[]; system: string[] }>;
} {
  const calls: Array<{ tools: string[]; system: string[] }> = [];
  let i = 0;
  const llm: LLMProvider = {
    chat: async (params) => {
      calls.push({
        tools: (params.tools ?? []).map((t) => t.name),
        system: params.messages
          .filter((m) => m.role === "system")
          .map((m) => (typeof m.content === "string" ? m.content : "")),
      });
      const resp = responses[i++];
      if (!resp) throw new Error("LLM script exhausted");
      return resp;
    },
  };
  return { llm, calls };
}

describe("LocalAgent — retriever bindings", () => {
  it("attaches a bare retriever as a search tool named after its id", async () => {
    const { runner } = makeRunner();
    const { llm, calls } = recordingLLM([{ content: "ok", finishReason: "stop" }]);
    const agent = new LocalAgent({
      agent: { name: "support", llm },
      runner,
      namespaceId: "acme",
      retrievers: { docs: returnsRetriever(), default: returnsRetriever() },
    });

    const out = await agent.invoke({ task: "How do returns work?" });

    expect(await out.text).toBe("ok");
    expect(calls[0]?.tools.sort()).toEqual(["search_docs", "search_knowledge_base"]);
  });

  it("keeps a host tool that shares the retriever tool's name", async () => {
    const { runner } = makeRunner();
    const { llm, calls } = recordingLLM([{ content: "ok", finishReason: "stop" }]);
    const hostTool = tool({
      name: "search_docs",
      description: "host-owned search",
      parameters: z.object({ q: z.string() }),
      execute: async () => "host",
    });
    const agent = new LocalAgent({
      agent: { name: "support", llm, tools: { search_docs: hostTool } },
      runner,
      namespaceId: "acme",
      retrievers: { docs: returnsRetriever() },
    });

    await agent.invoke({ task: "returns" });

    expect(calls[0]?.tools).toEqual(["search_docs"]);
    // biome-ignore lint/suspicious/noExplicitAny: test introspection of the built tool kit
    const built = (agent as any).buildTools({ namespaceId: "acme" });
    expect(built.search_docs.description).toBe("host-owned search");
  });

  it("injects context-mode results as a system message on one-shot runs", async () => {
    const { runner } = makeRunner();
    const { llm, calls } = recordingLLM([{ content: "grounded", finishReason: "stop" }]);
    const agent = new LocalAgent({
      agent: { name: "support", llm, systemPrompt: "Answer from policy." },
      runner,
      namespaceId: "acme",
      retrievers: { docs: { mode: "context", retriever: returnsRetriever() } },
    });

    await agent.invoke({ task: "How do returns work?" });

    const system = calls[0]!.system.join("\n\n");
    expect(calls[0]?.system[0]).toBe("Answer from policy.");
    expect(system).toContain("Knowledge results from docs");
    expect(system).toContain("Returns allow unopened items");
    expect(system).toContain("source=Returns (https://example.test/returns)");
    expect(calls[0]?.tools).toEqual([]);
  });

  it("passes the binding's topK, filter and chunk cap to context searches", async () => {
    const { runner } = makeRunner();
    const { llm } = recordingLLM([{ content: "ok", finishReason: "stop" }]);
    const seen: RetrieveRequest[] = [];
    const spy: Retriever = {
      retrieve: async (request) => {
        seen.push(request);
        return [];
      },
    };
    const agent = new LocalAgent({
      agent: { name: "support", llm },
      runner,
      namespaceId: "acme",
      retrievers: {
        docs: {
          mode: "tool-and-context",
          retriever: spy,
          topK: 2,
          maxChunkCharacters: 40,
          filter: { tags: ["policy"], metadata: { tenant: "acme" } },
        },
      },
    });

    await agent.invoke({ task: "returns" });

    expect(seen).toEqual([
      {
        query: "returns",
        topK: 2,
        maxChunkCharacters: 40,
        filter: { tags: ["policy"], metadata: { tenant: "acme" } },
      },
    ]);
  });

  it("searches before every thread turn and never persists the knowledge block", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const { llm, calls } = recordingLLM([
      { content: "first", finishReason: "stop" },
      { content: "second", finishReason: "stop" },
    ]);
    const agent = new LocalAgent({
      agent: { name: "support", llm },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      retrievers: { docs: { mode: "tool-and-context", retriever: returnsRetriever() } },
    });

    const thread = await agent.thread("t-rag");
    await thread.send({ task: "How do returns work?" });
    await thread.send({ task: "Any returns for opened items?" });

    for (const call of calls) {
      expect(call.tools).toContain("search_docs");
      expect(call.system.some((s) => s.includes("Knowledge results from docs"))).toBe(true);
    }
    const stored = await memory.getMessages({ namespaceId: "acme", threadId: "t-rag" });
    expect(stored.map((m) => m.role)).toEqual(["user", "assistant", "user", "assistant"]);
    expect(stored.some((m) => String(m.content).includes("Knowledge results"))).toBe(false);
  });

  it("persists a resumed turn once, without re-appending history", async () => {
    const { storage, runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const guarded = tool({
      name: "guarded",
      description: "Requires approval before running",
      parameters: z.object({ x: z.string() }),
      execute: async ({ x }) => `ran: ${x}`,
      requireApproval: true,
    });
    const { llm } = recordingLLM([
      { content: "hello", finishReason: "stop" },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "go" } }],
      },
      { content: "done", finishReason: "stop" },
    ]);
    const agent = new LocalAgent({
      agent: { name: "support", llm, tools: { guarded } },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      retrievers: { docs: { mode: "context", retriever: returnsRetriever() } },
    });
    const thread = await agent.thread("t-resume");
    await thread.send({ task: "hi about returns" });
    await expect(thread.send({ task: "run it for returns" })).rejects.toThrow(/suspended|signal/i);

    // Bounded by iterations, not wall time: the suspended row is written
    // before send() rejects, so this normally passes on the first check.
    for (let i = 0; i < 100 && !(await thread.hasPendingApproval!("tc-1")); i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    expect(await thread.hasPendingApproval!("tc-1")).toBe(true);
    expect(await storage.listWorkflows({ status: "suspended", limit: 10 })).toHaveLength(1);

    const out = await thread.resume!("tc-1", { approved: true });
    expect(await out.text).toBe("done");

    const stored: Message[] = await memory.getMessages({
      namespaceId: "acme",
      threadId: "t-resume",
    });
    expect(stored.map((m) => m.role)).toEqual([
      "user",
      "assistant",
      "user",
      "assistant",
      "tool",
      "assistant",
    ]);
  });
});
