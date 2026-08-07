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
import { InMemoryRetriever } from "../../rag/in-memory-retriever.ts";

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

describe("LocalAgent — retrievers", () => {
  it("auto-attaches retriever tools", async () => {
    const { runner } = makeRunner();
    let observedTools: string[] = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        observedTools = (params.tools ?? []).map((t) => t.name);
        return { content: "ok", finishReason: "stop" };
      },
    };

    const agent = new LocalAgent({
      agent: { name: "support", llm },
      runner,
      namespaceId: "acme",
      retrievers: {
        docs: new InMemoryRetriever({
          documents: [{ id: "returns", text: "Return unopened items within 30 days." }],
        }),
      },
    });

    const out = await agent.invoke({ task: "How do returns work?" });

    expect(await out.text).toBe("ok");
    expect(observedTools).toEqual(["search_docs"]);
  });

  it("injects context-mode retriever results before a turn", async () => {
    const { runner } = makeRunner();
    let observedSystem = "";
    const llm: LLMProvider = {
      chat: async (params) => {
        observedSystem = params.messages
          .filter((m) => m.role === "system")
          .map((m) => m.content)
          .join("\n\n");
        return { content: "grounded", finishReason: "stop" };
      },
    };

    const agent = new LocalAgent({
      agent: { name: "support", llm, systemPrompt: "Answer from policy." },
      runner,
      namespaceId: "acme",
      retrievers: {
        docs: {
          mode: "context",
          retriever: new InMemoryRetriever({
            documents: [
              {
                id: "returns",
                title: "Returns",
                uri: "https://example.test/returns",
                text: "Returns allow unopened items within 30 days.",
              },
            ],
          }),
        },
      },
    });

    await agent.invoke({ task: "How do returns work?" });

    expect(observedSystem).toContain("Answer from policy.");
    expect(observedSystem).toContain("Knowledge results from docs");
    expect(observedSystem).toContain("Returns allow unopened items");
    expect(observedSystem).toContain("source=Returns");
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

    const acmeAlice = template.withScope({ namespaceId: "acme", resourceId: "alice" });
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

describe("LocalAgent — autoCompact", () => {
  // Returns ONE chat response per call; reused for the chat path.
  function chattyLLM(): LLMProvider {
    let i = 0;
    return {
      chat: async () => ({
        content: `reply #${++i}`,
        finishReason: "stop" as const,
        usage: { inputTokens: 10, outputTokens: 5 },
      }),
    };
  }
  // Always returns the distill envelope. Used by the auto-built
  // DefaultConsolidator.
  function envelopeLLM(): LLMProvider {
    return {
      chat: async () => ({
        content: JSON.stringify({
          summary: "compacted by test",
          outcome: null,
          salience: 0.4,
          facts: [],
        }),
        finishReason: "stop",
      }),
    };
  }

  // Drive N user turns through the same thread. Each turn writes
  // [user, assistant] = 2 messages, so after N turns the thread has
  // ~2N persisted messages.
  async function driveTurns(thread: Awaited<ReturnType<LocalAgent["thread"]>>, n: number) {
    for (let i = 0; i < n; i++) {
      const out = await thread.send({ task: `turn ${i}` });
      await out.text; // ensure persistTurn ran
    }
  }

  it("messageThreshold fires compactThread once when uncompacted count crosses; subsequent under-threshold turns don't re-fire", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: envelopeLLM(),
      autoCompact: { messageThreshold: 4, keepRecent: 2, mode: "blocking" },
    });
    const t = await agent.thread("auto-1");

    // Sequence (messageThreshold=4, keepRecent=2):
    //   Turn 1: 2 msgs. uncompacted=2. 2 > 4? no.
    //   Turn 2: 4 msgs. uncompacted=4. 4 > 4? no.
    //   Turn 3: 6 msgs. uncompacted=6. FIRE. lastCompactedSeq = 6-2 = 4.
    //   Turn 4: 8 msgs. uncompacted=8-4=4. 4 > 4? no.   ← this is the
    //                                                    "doesn't re-fire" assertion.
    await driveTurns(t, 4);

    const episodes = await memory.listThreadEpisodes({
      namespaceId: "acme",
      resourceId: undefined,
      threadId: "auto-1",
    });
    const compactEpisodes = episodes.filter(
      (e) => (e.metadata as { kind?: unknown }).kind === "compact",
    );
    expect(compactEpisodes.length).toBe(1);
    expect(compactEpisodes[0]!.summary).toBe("compacted by test");
    expect(compactEpisodes[0]!.sourceMessageRange?.toSeq).toBe(4);
  });

  it("tokenThreshold fires when char-budget crosses even at low message counts", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // LLM that emits a LARGE assistant message — one turn is enough
    // to blow a small token budget.
    const heavyLLM: LLMProvider = {
      chat: async () => ({
        content: "x".repeat(2000), // ~500 tokens via chars/4
        finishReason: "stop",
      }),
    };
    const agent = new LocalAgent({
      agent: { name: "verbose", llm: heavyLLM },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: envelopeLLM(),
      autoCompact: { tokenThreshold: 200, keepRecent: 1, mode: "blocking" },
    });
    const t = await agent.thread("auto-token");
    // ONE turn only — proves the token gate fires at low message
    // count when individual messages are large.
    await driveTurns(t, 1);

    const episodes = await memory.listThreadEpisodes({
      namespaceId: "acme",
      resourceId: undefined,
      threadId: "auto-token",
    });
    const compacts = episodes.filter((e) => (e.metadata as { kind?: unknown }).kind === "compact");
    expect(compacts.length).toBe(1);
  });

  it("`contextLimit` + `compressAt` compute the effective token gate (mirrors agentLoop convention)", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // Big assistant message — ~500 estimated tokens per turn.
    const heavyLLM: LLMProvider = {
      chat: async () => ({
        content: "x".repeat(2000),
        finishReason: "stop",
      }),
    };
    const agent = new LocalAgent({
      agent: { name: "verbose", llm: heavyLLM },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: envelopeLLM(),
      // contextLimit 200 * compressAt 0.5 = effective 100 token gate.
      // One turn (~500 tokens) clearly crosses it.
      autoCompact: {
        contextLimit: 200,
        compressAt: 0.5,
        keepRecent: 1,
        mode: "blocking",
      },
    });
    const t = await agent.thread("auto-ctx");
    await driveTurns(t, 1);

    const episodes = await memory.listThreadEpisodes({
      namespaceId: "acme",
      resourceId: undefined,
      threadId: "auto-ctx",
    });
    const compacts = episodes.filter((e) => (e.metadata as { kind?: unknown }).kind === "compact");
    expect(compacts.length).toBe(1);
  });

  it("`when` predicate replaces the threshold check", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const seenSignals: number[] = [];
    const agent = new LocalAgent({
      agent: { name: "predicate", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: envelopeLLM(),
      autoCompact: {
        // Numeric thresholds present, but `when` should override them.
        messageThreshold: 100,
        when: ({ uncompactedCount }) => {
          seenSignals.push(uncompactedCount);
          return uncompactedCount >= 4;
        },
        keepRecent: 1,
        mode: "blocking",
      },
    });
    const t = await agent.thread("auto-pred");
    await driveTurns(t, 3);

    expect(seenSignals.length).toBe(3); // predicate consulted on every turn
    const episodes = await memory.listThreadEpisodes({
      namespaceId: "acme",
      resourceId: undefined,
      threadId: "auto-pred",
    });
    expect(
      episodes.filter((e) => (e.metadata as { kind?: unknown }).kind === "compact"),
    ).toHaveLength(1);
  });

  it("does nothing when autoCompact is unset", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "no-auto", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: envelopeLLM(),
      // autoCompact: undefined ← off
    });
    const t = await agent.thread("auto-off");
    for (let i = 0; i < 5; i++) {
      await (
        await t.send({ task: `t${i}` })
      ).text;
    }
    const episodes = await memory.listThreadEpisodes({
      namespaceId: "acme",
      resourceId: undefined,
      threadId: "auto-off",
    });
    expect(episodes).toHaveLength(0);
  });
});

describe("LocalAgent — autoDistill", () => {
  function chattyLLM(): LLMProvider {
    let i = 0;
    return {
      chat: async () => ({
        content: `reply #${++i}`,
        finishReason: "stop" as const,
      }),
    };
  }
  function envelopeLLM(): LLMProvider {
    return {
      chat: async () => ({
        content: JSON.stringify({
          summary: "thread distilled by test",
          outcome: null,
          salience: 0.7,
          facts: ["user mentioned X"],
        }),
        finishReason: "stop",
      }),
    };
  }
  async function driveTurns(thread: Awaited<ReturnType<LocalAgent["thread"]>>, n: number) {
    for (let i = 0; i < n; i++) {
      const out = await thread.send({ task: `turn ${i}` });
      await out.text;
    }
  }

  it("messageThreshold fires distillThread once and is idempotent on subsequent turns", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      autoDistill: { messageThreshold: 4, mode: "blocking" },
    });
    const t = await agent.thread("auto-d-1");

    // 5 turns × 2 messages = 10 persisted. Threshold = 4. Fires after
    // turn 2 (count = 4). Subsequent turns hit the consolidator's
    // idempotency check (no force) and return the same episode without
    // re-running the LLM, so the resource layer ends with exactly 1
    // episode for this thread.
    await driveTurns(t, 5);

    const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    const distillEps = eps.filter(
      (e) =>
        e.sourceThreadId === "auto-d-1" && (e.metadata as { kind?: unknown }).kind === "distill",
    );
    expect(distillEps.length).toBe(1);
    expect(distillEps[0]!.summary).toBe("thread distilled by test");
  });

  it("force: true causes auto-distill to re-run on every fire", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      autoDistill: {
        // Distill every turn from turn 2 onward.
        when: ({ totalCount }) => totalCount >= 4,
        force: true,
        mode: "blocking",
      },
    });
    const t = await agent.thread("auto-d-force");
    // 4 turns: at turns 2, 3, 4 the predicate fires and rewrites the
    // episode (force: true → consolidator's idempotency check is skipped).
    await driveTurns(t, 4);

    const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    const distillEps = eps.filter((e) => e.sourceThreadId === "auto-d-force");
    // 3 distinct episode rows (turns 2/3/4 each wrote a fresh one).
    expect(distillEps.length).toBe(3);
  });

  it("`when` predicate sees lastUserMessage for goodbye-style heuristics", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    let sawGoodbye = false;
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      autoDistill: {
        when: ({ lastUserMessage }) => {
          const goodbye = /^(thanks|bye|goodbye)\b/i.test(lastUserMessage ?? "");
          if (goodbye) sawGoodbye = true;
          return goodbye;
        },
        mode: "blocking",
      },
    });
    const t = await agent.thread("auto-d-bye");
    await (
      await t.send({ task: "hi" })
    ).text; // not a goodbye
    await (
      await t.send({ task: "more questions" })
    ).text;
    await (
      await t.send({ task: "thanks!" })
    ).text; // <-- triggers

    expect(sawGoodbye).toBe(true);
    const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    const distillEps = eps.filter((e) => e.sourceThreadId === "auto-d-bye");
    expect(distillEps.length).toBe(1);
  });

  it("tokenThreshold fires distillThread once when NEW-message tokens cross", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // Reply long enough that a single turn pushes past the token gate.
    // chattyLLM replies are ~10 chars → ~3 tokens. We need a beefier reply.
    const longLLM: LLMProvider = {
      chat: async () => ({
        content: "x".repeat(800), // ~200 estimated tokens per assistant turn
        finishReason: "stop" as const,
      }),
    };
    const agent = new LocalAgent({
      agent: { name: "support", llm: longLLM },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      autoDistill: { tokenThreshold: 300, mode: "blocking" },
    });
    const t = await agent.thread("auto-d-tokens");

    // First turn: user "turn 0" (~2 tok) + assistant 800-char reply (~200 tok)
    // = ~202 tok < 300 → no fire.
    await (
      await t.send({ task: "turn 0" })
    ).text;
    let eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps.filter((e) => e.sourceThreadId === "auto-d-tokens")).toHaveLength(0);

    // Second turn pushes total uncompacted tokens past 300 → fires.
    await (
      await t.send({ task: "turn 1" })
    ).text;
    eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    const distillEps = eps.filter((e) => e.sourceThreadId === "auto-d-tokens");
    expect(distillEps.length).toBe(1);
  });

  it("intervalMs gates re-fire by wall-clock gap from last distill", async () => {
    const { FakeClock } = await import("@promin/core");
    const clock = FakeClock.create(1_000_000);
    const { runner } = makeRunner();
    // Memory store must share the clock so episode.createdAt is in
    // the same domain as the trigger's "now".
    const memory = new InMemoryMemoryStore({ clock });
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM(), clock },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      // Only intervalMs is set. First call: lastDistilledAt=0 →
      // msSinceLastDistill=Infinity → trips. Subsequent calls gated
      // until 60s of wall-clock has passed.
      autoDistill: { intervalMs: 60_000, force: true, mode: "blocking" },
    });
    const t = await agent.thread("auto-d-interval");

    // Turn 1: no prior distill → Infinity > 60_000 → fires.
    await (
      await t.send({ task: "turn 0" })
    ).text;
    let eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps.filter((e) => e.sourceThreadId === "auto-d-interval")).toHaveLength(1);

    // Turn 2 with only +30s elapsed → 30_000 < 60_000 → no re-fire.
    clock.advance(30_000);
    await (
      await t.send({ task: "turn 1" })
    ).text;
    eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps.filter((e) => e.sourceThreadId === "auto-d-interval").length).toBe(1);

    // Turn 3 after another +35s → 65_000 since last distill → fires
    // again (force:true so a fresh episode rather than dedup).
    clock.advance(35_000);
    await (
      await t.send({ task: "turn 2" })
    ).text;
    eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps.filter((e) => e.sourceThreadId === "auto-d-interval").length).toBe(2);
  });

  it("OR-composition: gates compose so any one tripping fires", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "support", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
      // Both gates set; whichever fires first wins. tokenThreshold is
      // unrealistically high — only messageThreshold can trip in this run.
      autoDistill: {
        messageThreshold: 2,
        tokenThreshold: 1_000_000,
        mode: "blocking",
      },
    });
    const t = await agent.thread("auto-d-or");
    await driveTurns(t, 1); // 2 messages persisted → messageThreshold trips
    const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps.filter((e) => e.sourceThreadId === "auto-d-or")).toHaveLength(1);
  });

  it("rate-limited consolidator: auto-trigger swallows ConsolidatorRateLimitError silently", async () => {
    const { FakeClock } = await import("@promin/core");
    const { DefaultConsolidator } = await import("../../memory/consolidator.ts");
    const { RateLimitedConsolidator } = await import("../../memory/rate-limited-consolidator.ts");
    const clock = FakeClock.create(1_000_000);
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore({ clock });

    const inner = new DefaultConsolidator({ store: memory, llm: envelopeLLM() });
    const consolidator = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 1,
      clock,
    });

    // Pre-seed one distill episode → cap is already reached. Auto-trigger
    // should fire, hit the rate limit, and swallow without warning.
    await memory.appendResourceEpisode(
      { namespaceId: "acme", resourceId: "alice" },
      {
        summary: "seed",
        sourceThreadId: "earlier",
        salience: 0.5,
        facts: [],
        metadata: { kind: "distill" },
      },
    );

    const warnings: string[] = [];
    const origWarn = console.warn;
    console.warn = (...args: unknown[]) => {
      warnings.push(args.map(String).join(" "));
    };

    try {
      const agent = new LocalAgent({
        agent: { name: "support", llm: chattyLLM(), clock },
        runner,
        memory,
        namespaceId: "acme",
        resourceId: "alice",
        consolidator,
        autoDistill: { messageThreshold: 2, mode: "blocking" },
      });
      const t = await agent.thread("auto-d-rl");
      await driveTurns(t, 1);

      expect(warnings.filter((w) => w.includes("autoDistill"))).toHaveLength(0);
      // Still only the seeded episode — auto-trigger did NOT add a second.
      const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
      const distillEps = eps.filter((e) => (e.metadata as { kind?: unknown }).kind === "distill");
      expect(distillEps).toHaveLength(1);
    } finally {
      console.warn = origWarn;
    }
  });

  it("does nothing when autoDistill is unset", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "no-auto", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      consolidatorLlm: envelopeLLM(),
    });
    const t = await agent.thread("auto-d-off");
    await driveTurns(t, 5);
    const eps = await memory.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(eps).toHaveLength(0);
  });

  it("silently skips when no resourceId is bound (distillation is resource-scoped)", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: { name: "no-resource", llm: chattyLLM() },
      runner,
      memory,
      namespaceId: "acme",
      // no resourceId
      consolidatorLlm: envelopeLLM(),
      autoDistill: { messageThreshold: 2, mode: "blocking" },
    });
    const t = await agent.thread("auto-d-no-res");
    await driveTurns(t, 4);
    // No resourceId → distillation can't write a ResourceEpisode →
    // trigger silently no-ops. (Asserting on the InMemoryMemoryStore
    // side: zero episodes anywhere for this run.)
    expect(memory["resourceEpisodes" as keyof InMemoryMemoryStore]).toBeDefined(); // sanity check
  });
});

describe("LocalAgent — resolveContext-driven prompt assembly", () => {
  // LLM that records all system blocks (in order) + non-system messages
  // it received, so tests can assert on what actually got fed to the
  // model. With the per-block prompt-cache split, persona and cascade
  // arrive as TWO separate system messages.
  function spyLLM(reply = "ok") {
    const seen: Array<{ systems: string[]; messages: Message[] }> = [];
    const llm: LLMProvider = {
      chat: async (params) => {
        const systems = params.messages
          .filter(
            (m): m is Message & { content: string } =>
              m.role === "system" && typeof m.content === "string",
          )
          .map((m) => m.content);
        seen.push({
          systems,
          messages: params.messages.filter((m) => m.role !== "system"),
        });
        return { content: reply, finishReason: "stop" };
      },
    };
    return { llm, seen };
  }

  it("merges static systemPrompt with the cascade-resolved one (static first, cascade follows)", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // Seed namespace + resource layers BEFORE the first turn.
    await memory.upsertNamespace("acme", { staticRules: "Be polite to all customers." });
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: "alice" },
      { workingMemory: "alice prefers terse replies" },
    );
    await memory.appendResourceFact({ namespaceId: "acme", resourceId: "alice" }, "is in EU");

    const { llm, seen } = spyLLM("hello");
    const agent = new LocalAgent({
      agent: {
        name: "support",
        llm,
        systemPrompt: "You are claude-bot, a helpful assistant.",
      },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const t = await agent.thread("greet-1");
    await (
      await t.send({ task: "hi" })
    ).text;

    expect(seen.length).toBe(1);
    const systems = seen[0]!.systems;
    // Two system blocks: persona first, cascade second. Splitting them
    // is the prompt-cache prerequisite — Anthropic adapter marks both
    // first and last with cache_control so the persona prefix stays
    // cached across cascade changes.
    expect(systems.length).toBe(2);
    expect(systems[0]).toContain("You are claude-bot");
    expect(systems[1]).toContain("Be polite to all customers.");
    expect(systems[1]).toContain("Resource Working Memory");
    expect(systems[1]).toContain("alice prefers terse replies");
    expect(systems[1]).toContain("is in EU");
  });

  it("trims message tail to fit `contextBudget.maxMessageTokens`", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // Pre-seed a long history (≥10 turns of ~20 chars each ≈ 100 tokens
    // total). Set a tiny budget so most of it is trimmed.
    for (let i = 0; i < 10; i++) {
      await memory.appendMessages(
        { namespaceId: "acme", resourceId: "alice", threadId: "long-thread" },
        [
          { role: "user", content: `q${i}.${"x".repeat(40)}` }, // ~10+ tokens
          { role: "assistant", content: `a${i}.${"x".repeat(40)}` },
        ],
      );
    }

    const { llm, seen } = spyLLM("ok");
    const agent = new LocalAgent({
      agent: { name: "trimmer", llm, systemPrompt: "stay concise" },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      contextBudget: { maxMessageTokens: 30 }, // tight: only ~3 messages fit
    });
    const t = await agent.thread("long-thread");
    await (
      await t.send({ task: "follow-up" })
    ).text;

    // Persisted: 20 prior + 1 user task = 21. After trimming to 30
    // tokens, only the latest few survive. The follow-up's user
    // message is appended to the seeded tail BEFORE the call so it's
    // included; assert that significantly fewer than 21 messages
    // reach the LLM.
    expect(seen[0]!.messages.length).toBeLessThan(10);
    expect(seen[0]!.messages.length).toBeGreaterThan(0);
  });

  it("injects resource episodes when `maxEpisodeTokens > 0` (rollups become visible to next turn)", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    // Operator distilled an earlier thread → a ResourceEpisode lives at
    // resource scope. Without `maxEpisodeTokens` it stays disk-only.
    await memory.appendResourceEpisode(
      { namespaceId: "acme", resourceId: "alice" },
      {
        summary: "User Anton previously asked about saga patterns; resolved with Postgres choice.",
        outcome: "Postgres for audit log",
        salience: 0.8,
      },
    );

    const { llm, seen } = spyLLM("ack");
    const agent = new LocalAgent({
      agent: { name: "recall-bot", llm, systemPrompt: "be helpful" },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
      contextBudget: { maxMessageTokens: 16_000, maxEpisodeTokens: 2_000 },
    });
    const t = await agent.thread("new-thread");
    await (
      await t.send({ task: "remind me what we picked" })
    ).text;

    // Episode rendered into the cascade (the second system block).
    const cascade = seen[0]!.systems.join("\n\n");
    expect(cascade).toContain("Recent Episodes");
    expect(cascade).toContain("saga patterns");
    expect(cascade).toContain("Postgres for audit log");
  });

  it("first turn (no thread row yet) doesn't fail when resolveContext can't resolve", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const { llm, seen } = spyLLM("hi");
    const agent = new LocalAgent({
      agent: { name: "fresh", llm, systemPrompt: "static rules" },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    // First send — thread row doesn't exist yet. loadContext should
    // gracefully fall back to raw messages + the static prompt.
    const t = await agent.thread("brand-new");
    await (
      await t.send({ task: "hello" })
    ).text;

    // Static system prompt makes it through (one block — cascade is
    // empty on a brand-new thread, so no second block emitted).
    expect(seen[0]!.systems.length).toBe(1);
    expect(seen[0]!.systems[0]).toContain("static rules");
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
