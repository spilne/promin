import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { RetrieveRequest, RetrieveResult, RetrieverCaller } from "../../rag/types.ts";
import type { LocalAgentBackend, RegisteredAgent } from "../types.ts";
import { resolveLocalAgent } from "../resolve-local-agent.ts";

const CALLER: RetrieverCaller = {
  userId: "u1",
  email: "user@example.com",
  accessRoles: ["member"],
  teams: ["support"],
};

const result: RetrieveResult = {
  chunk: {
    id: "open#0",
    text: "The handbook is open.",
    source: { id: "open", title: "Open handbook" },
    index: 0,
  },
  score: 1,
};

function runner() {
  return createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
}

function recipe(ids: ReadonlyArray<string>): RegisteredAgent {
  const backend: LocalAgentBackend = {
    type: "local",
    model: { provider: "test", id: "model" },
    role: { inline: { systemPrompt: "Answer from approved knowledge.", tools: [] } },
    knowledge: ids.map((id) => ({ id })),
  };
  return {
    id: "knowledge-agent",
    version: "v1",
    backend,
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    active: true,
    enabled: true,
    trust: "trusted",
  };
}

function recordingRetriever(requests: RetrieveRequest[] = []) {
  return {
    requests,
    retriever: {
      retrieve: async (request: RetrieveRequest) => {
        requests.push(request);
        return [result];
      },
    },
  };
}

function offeredToolNames() {
  const names: string[] = [];
  const llm: LLMProvider = {
    chat: async (params) => {
      names.push(...(params.tools ?? []).map((tool) => tool.name));
      return { content: "ok", finishReason: "stop" };
    },
  };
  return { llm, names };
}

describe("resolveLocalAgent knowledge access", () => {
  it("leaves a refused binding out and still resolves the agent", async () => {
    const open = recordingRetriever();
    const secret = recordingRetriever();
    const offered = offeredToolNames();
    const agent = await resolveLocalAgent(recipe(["kb-open", "kb-secret"]), {
      runner: runner(),
      llm: () => offered.llm,
      tools: {},
      retrievers: { "kb-open": open.retriever, "kb-secret": secret.retriever },
      knowledgeAccess: ({ id }) => id !== "kb-secret",
    });

    await agent.invoke({ task: "question" });

    expect(offered.names).toContain("search_kb_open");
    expect(offered.names).not.toContain("search_kb_secret");
  });

  it("resolves with no search tools when every binding is refused", async () => {
    const offered = offeredToolNames();
    const agent = await resolveLocalAgent(recipe(["kb-open", "kb-secret"]), {
      runner: runner(),
      llm: () => offered.llm,
      tools: {},
      retrievers: {},
      knowledgeAccess: () => false,
    });

    await agent.invoke({ task: "question" });

    expect(offered.names.filter((name) => name.startsWith("search_"))).toEqual([]);
  });

  it("does not look up a refused unregistered retriever", async () => {
    const agent = await resolveLocalAgent(recipe(["missing"]), {
      runner: runner(),
      llm: () => ({ chat: async () => ({ content: "ok", finishReason: "stop" }) }),
      tools: {},
      retrievers: {},
      knowledgeAccess: () => false,
    });

    const output = await agent.invoke({ task: "question" });
    expect(await output.text).toBe("ok");
  });

  it("keeps the unknown-retriever error when no access check is configured", async () => {
    await expect(
      resolveLocalAgent(recipe(["missing"]), {
        runner: runner(),
        llm: () => ({ chat: async () => ({ content: "ok", finishReason: "stop" }) }),
        tools: {},
        retrievers: {},
      }),
    ).rejects.toThrow(/knowledge retriever "missing"/);
  });

  it("passes the configured caller to the bound retriever", async () => {
    const open = recordingRetriever();
    const responses: LLMResponse[] = [
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "call-1", name: "search_kb_open", input: { query: "handbook" } }],
      },
      { content: "answer", finishReason: "stop" },
    ];
    const llm: LLMProvider = {
      chat: async () => {
        const response = responses.shift();
        if (!response) throw new Error("test LLM exhausted");
        return response;
      },
    };
    const agent = await resolveLocalAgent(recipe(["kb-open"]), {
      runner: runner(),
      llm: () => llm,
      tools: {},
      retrievers: { "kb-open": open.retriever },
      knowledgeCaller: CALLER,
    });

    const output = await agent.invoke({ task: "question" });

    expect(await output.text).toBe("answer");
    expect(open.requests[0]?.caller).toEqual(CALLER);
  });
});
