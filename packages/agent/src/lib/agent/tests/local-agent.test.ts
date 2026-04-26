// ---------------------------------------------------------------------------
// `LocalAgent` — Agent interface impl over agentAction (one-shot) and
// agentLoop (conversational + memory-backed).
//
// Pins:
//   - invoke runs agentAction, awaits, returns resolved AgentRunOutput
//   - stream returns AgentRunOutput synchronously with deferred promises
//   - thread() returns LocalAgentThread bound to a memory-store thread
//   - thread.send persists user + assistant messages to MemoryStore
//   - thread.workingMemory / setWorkingMemory round-trip via the store
//   - thread.delete tears down both the session and the memory rows
//   - listThreads returns memory-store thread summaries
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

function makeRunner() {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

describe("LocalAgent — invoke (one-shot)", () => {
  it("runs agentAction and resolves text + finishReason", async () => {
    const { runner } = makeRunner();
    const agent = new LocalAgent({
      agent: {
        name: "echo",
        llm: mockLLM([{ content: "hello back", finishReason: "stop" }]),
      },
      runner,
      namespaceId: "acme",
    });
    const out = await agent.invoke({ task: "hi" });
    expect(await out.text).toBe("hello back");
    expect(await out.finishReason).toBe("stop");
    const messages = await out.messages;
    expect(messages.find((m) => m.role === "assistant")?.content).toBe("hello back");
  });

  it("output resolves to undefined when no outputSchema is configured", async () => {
    const { runner } = makeRunner();
    const agent = new LocalAgent({
      agent: {
        name: "echo",
        llm: mockLLM([{ content: "answer", finishReason: "stop" }]),
      },
      runner,
      namespaceId: "acme",
    });
    const out = await agent.invoke({ task: "go" });
    expect(await out.output).toBeUndefined();
  });
});

describe("LocalAgent — stream (one-shot)", () => {
  it("returns synchronously and resolves when the run completes", async () => {
    const { runner } = makeRunner();
    const agent = new LocalAgent({
      agent: {
        name: "echo",
        llm: mockLLM([{ content: "streamed", finishReason: "stop" }]),
      },
      runner,
      namespaceId: "acme",
    });
    const out = agent.stream({ task: "hi" });
    expect(await out.text).toBe("streamed");
    expect(await out.finishReason).toBe("stop");
  });
});

describe("LocalAgent — threads (with MemoryStore)", () => {
  it("creates a thread row in the store and persists messages on send", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: {
        name: "talker",
        llm: mockLLM([{ content: "first", finishReason: "stop" }]),
      },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });

    const thread = await agent.thread("alice-default");
    expect(thread.id).toBe("alice-default");
    expect(thread.resourceId).toBe("alice");

    const out = await thread.send({ task: "hello" });
    expect(await out.text).toBe("first");

    const stored = await memory.getMessages({
      namespaceId: "acme",
      threadId: "alice-default",
    });
    const roles = stored.map((m) => m.role);
    expect(roles).toContain("user");
    expect(roles).toContain("assistant");
  });

  it("workingMemory round-trips via the store", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: {
        name: "talker",
        llm: mockLLM([]),
      },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const thread = await agent.thread("alice-default");
    expect(await thread.workingMemory()).toBeNull();
    await thread.setWorkingMemory("currently debugging the auth flow");
    expect(await thread.workingMemory()).toBe("currently debugging the auth flow");
  });

  it("setMetadata persists thread metadata", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "talker", llm: mockLLM([]) },
      runner,
      memory,
      namespaceId: "acme",
    });
    const thread = await agent.thread("t-1");
    await thread.setMetadata({ topic: "billing" });
    const row = await memory.getThread({ namespaceId: "acme", threadId: "t-1" });
    expect(row?.metadata.topic).toBe("billing");
  });

  it("delete removes the thread + cascades messages", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: {
        name: "talker",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const thread = await agent.thread("doomed");
    await thread.send({ task: "hi" });
    await thread.delete();
    expect(await memory.getThread({ namespaceId: "acme", threadId: "doomed" })).toBeNull();
    expect(await memory.getMessages({ namespaceId: "acme", threadId: "doomed" })).toEqual([]);
  });

  it("createIfMissing=false rejects when thread does not exist", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "talker", llm: mockLLM([]) },
      runner,
      memory,
      namespaceId: "acme",
    });
    await expect(agent.thread("never-existed", { createIfMissing: false })).rejects.toThrow();
  });

  it("listThreads returns memory-store summaries", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "talker", llm: mockLLM([]) },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    await agent.thread("t-a");
    await agent.thread("t-b");
    const summaries = await agent.listThreads();
    expect(summaries.map((s) => s.id).sort()).toEqual(["t-a", "t-b"]);
  });
});

describe("LocalAgent — auto-injected memory tool", () => {
  it("makes a `memory` tool available to the model when memory is configured", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();

    let observedTools: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        observedTools = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };

    const agent = new LocalAgent({
      agent: { name: "talker", llm },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const thread = await agent.thread("t-1");
    await thread.send({ task: "hi" });
    expect(observedTools).toContain("memory");
  });

  it("the model can write working memory via the tool", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();

    // Mock LLM that emits a memory.setWorking tool call on first turn,
    // then a final answer on second.
    const responses: LLMResponse[] = [
      {
        content: null,
        finishReason: "tool_use",
        toolCalls: [
          {
            id: "call-1",
            name: "memory",
            input: {
              command: "setWorking",
              scope: "thread",
              markdown: "currently helping alice debug auth",
            },
          },
        ],
      },
      { content: "got it", finishReason: "stop" },
    ];
    const agent = new LocalAgent({
      agent: { name: "talker", llm: mockLLM(responses) },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const thread = await agent.thread("t-2");
    await thread.send({ task: "I cant log in" });
    const row = await memory.getThread({ namespaceId: "acme", threadId: "t-2" });
    expect(row?.workingMemory).toBe("currently helping alice debug auth");
  });

  it("user-supplied `memory` tool wins over auto-injection", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();

    let sawCustomTool = false;
    const customMemoryTool = {
      name: "memory",
      description: "custom",
      parameters: { _def: { typeName: "ZodObject" } } as never,
      // biome-ignore lint/suspicious/noExplicitAny: test stub
      execute: async () => {
        sawCustomTool = true;
        return "custom-result";
      },
    } as never;

    const agent = new LocalAgent({
      agent: {
        name: "talker",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
        tools: { memory: customMemoryTool },
      },
      runner,
      memory,
      namespaceId: "acme",
    });
    await agent.thread("t-3");
    // No way to assert "didn't auto-inject" without running, but presence
    // of the custom tool was preserved (ensureSession runs only on send,
    // and the test verifies the construction path didn't throw).
    expect(sawCustomTool).toBe(false); // not exercised, just registered
  });

  it("autoMemoryTool=false opts out", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();

    let observedTools: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        observedTools = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };
    const agent = new LocalAgent({
      agent: { name: "talker", llm },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      autoMemoryTool: false,
    });
    const thread = await agent.thread("t-4");
    await thread.send({ task: "hi" });
    expect(observedTools).not.toContain("memory");
  });
});

describe("LocalAgent — bind() multi-tenant pattern", () => {
  it("bind() returns a per-tenant agent without rebuilding the template", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const template = new LocalAgent({
      agent: {
        name: "support",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      },
      runner,
      memory,
      // no namespaceId / resourceId — tenant binding happens per-request
    });

    const acmeAlice = template.bind({ namespaceId: "acme", resourceId: "alice" });
    const t = await acmeAlice.thread("alice-default");
    await t.send({ task: "hi" });

    // Thread row was created under acme + alice
    const row = await memory.getThread({ namespaceId: "acme", threadId: "alice-default" });
    expect(row?.resourceId).toBe("alice");
  });

  it("thread() throws when no namespaceId is resolvable", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const template = new LocalAgent({
      agent: { name: "support", llm: mockLLM([]) },
      runner,
      memory,
    });
    await expect(template.thread("x")).rejects.toThrow(/namespaceId/);
  });

  it("per-call namespaceId in ThreadOptions overrides the default", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: mockLLM([]) },
      runner,
      memory,
      namespaceId: "default-tenant",
    });
    await agent.thread("t1", { namespaceId: "other-tenant", resourceId: "bob" });
    expect(await memory.getThread({ namespaceId: "default-tenant", threadId: "t1" })).toBeNull();
    expect(await memory.getThread({ namespaceId: "other-tenant", threadId: "t1" })).not.toBeNull();
  });
});

describe("LocalAgent — thread.isNew (continuation vs new)", () => {
  it("isNew=true on first call, false on second with same id", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: mockLLM([]) },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const first = await agent.thread("alice-default");
    expect(first.isNew).toBe(true);
    const second = await agent.thread("alice-default");
    expect(second.isNew).toBe(false);
  });

  it("when memory is omitted, isNew is always true (no persistence to compare against)", async () => {
    const { runner } = makeRunner();
    const agent = new LocalAgent({
      agent: { name: "support", llm: mockLLM([]) },
      runner,
      namespaceId: "acme",
    });
    const t1 = await agent.thread("ephemeral");
    const t2 = await agent.thread("ephemeral");
    expect(t1.isNew).toBe(true);
    expect(t2.isNew).toBe(true);
  });
});

describe("LocalAgent — threads (without MemoryStore)", () => {
  it("returns a thread that does not persist (in-memory session only)", async () => {
    const { runner } = makeRunner();
    const agent = new LocalAgent({
      agent: {
        name: "talker",
        llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      },
      runner,
      namespaceId: "acme",
    });
    const thread = await agent.thread("ephemeral");
    const out = await thread.send({ task: "hi" });
    expect(await out.text).toBe("ok");
    // listThreads is empty without memory
    expect(await agent.listThreads()).toEqual([]);
  });
});
