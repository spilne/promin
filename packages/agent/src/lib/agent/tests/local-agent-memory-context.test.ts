// ---------------------------------------------------------------------------
// LocalAgent turn context: memory resolution failures fail the turn, a new
// thread's first turn already sees the namespace layers, a thread compaction
// reaches the next turn's prompt, and a failing context-mode retriever only
// costs its own results.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import type { ResolvedContext, ThreadKey, TokenBudget } from "../../memory/types.ts";
import { InMemoryRetriever } from "../../rag/in-memory-retriever.ts";
import type { Retriever, RetrieveResult } from "../../rag/types.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import type { AgentEvent } from "../types.ts";

function makeRunner() {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

/** LLM that records each call's messages and answers `reply(call)`. */
function recordingLLM(reply: (call: number) => string = (n) => `reply ${n}`): {
  llm: LLMProvider;
  calls: Message[][];
} {
  const calls: Message[][] = [];
  return {
    calls,
    llm: {
      chat: async (params): Promise<LLMResponse> => {
        calls.push([...params.messages]);
        return { content: reply(calls.length - 1), finishReason: "stop" };
      },
    },
  };
}

function text(messages: ReadonlyArray<Message>): string {
  return messages.map((m) => (typeof m.content === "string" ? m.content : "")).join("\n");
}

async function events(out: { fullStream: AsyncIterable<AgentEvent> }): Promise<AgentEvent[]> {
  const seen: AgentEvent[] = [];
  for await (const e of out.fullStream) seen.push(e);
  return seen;
}

/** A memory store whose `resolveContext` fails. */
class BrokenContextStore extends InMemoryMemoryStore {
  override async resolveContext(_key: ThreadKey, _budget: TokenBudget): Promise<ResolvedContext> {
    throw new Error("memory backend unavailable");
  }
}

describe("LocalAgent — memory context", () => {
  it("fails the turn when the memory context can't be resolved", async () => {
    const { runner } = makeRunner();
    const memory = new BrokenContextStore();
    await memory.upsertNamespace("acme", { staticRules: "NEVER-DROP-THESE-RULES" });
    const { llm, calls } = recordingLLM();
    const agent = new LocalAgent({
      agent: { name: "a", llm },
      runner,
      memory,
      namespaceId: "acme",
    });
    const thread = await agent.thread("t1");

    await expect(thread.send({ task: "hi" })).rejects.toThrow("memory backend unavailable");
    // No turn ran without its rules.
    expect(calls).toHaveLength(0);
  });

  it("a new thread's first turn already sees the namespace rules and resource facts", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    await memory.upsertNamespace("acme", { staticRules: "NAMESPACE-RULES" });
    await memory.appendResourceFact(
      { namespaceId: "acme", resourceId: "alice" },
      { text: "alice prefers metric units", source: "user" },
    );
    const { llm, calls } = recordingLLM();
    const agent = new LocalAgent({
      agent: { name: "a", llm },
      runner,
      memory,
      namespaceId: "acme",
      resourceId: "alice",
    });
    const thread = await agent.thread("fresh");
    // The row is created lazily: drop it to model a thread whose first
    // turn runs before anything was stored.
    await memory.deleteThread({ namespaceId: "acme", threadId: "fresh" });

    await (
      await thread.send({ task: "hello" })
    ).text;

    const seen = calls[0]!;
    expect(seen.filter((m) => m.role === "system").map((m) => m.content)).toContainEqual(
      expect.stringContaining("NAMESPACE-RULES"),
    );
    expect(text(seen)).toContain("alice prefers metric units");
  });

  it("a thread compaction replaces the folded messages in the next turn's prompt", async () => {
    const { runner } = makeRunner();
    const memory = new InMemoryMemoryStore();
    const chat = recordingLLM((n) => `answer ${n}`);
    const summarizer: LLMProvider = {
      chat: async () => ({ content: "SUMMARY-OF-EARLY-TURNS", finishReason: "stop" }),
    };
    const agent = new LocalAgent({
      agent: { name: "a", llm: chat.llm },
      runner,
      memory,
      namespaceId: "acme",
      consolidatorLlm: summarizer,
      autoCompact: { messageThreshold: 5, keepRecent: 2, mode: "blocking" },
    });
    const thread = await agent.thread("long");
    for (let i = 0; i < 4; i++) await (await thread.send({ task: `question ${i}` })).text;

    const last = chat.calls.at(-1)!;
    expect(text(last)).toContain("SUMMARY-OF-EARLY-TURNS");
    // The folded turns are gone from the prompt; the kept tail is not.
    expect(text(last)).not.toContain("question 0");
    expect(text(last)).toContain("question 2");
    expect(text(last)).toContain("question 3");
  });
});

/** A retriever that rejects every search. */
const failingRetriever: Retriever = {
  retrieve: async () => {
    throw new Error("vector store down");
  },
};

/** A retriever whose search never settles. */
const hangingRetriever: Retriever = {
  retrieve: () => new Promise<RetrieveResult[]>(() => {}),
};

function docs(): InMemoryRetriever {
  return new InMemoryRetriever({
    documents: [{ id: "returns", text: "Returns allow unopened items within 30 days." }],
  });
}

describe("LocalAgent — context-mode retriever failures", () => {
  it("a failing retriever is skipped with an event and a note; the others still inject", async () => {
    const { runner } = makeRunner();
    const { llm, calls } = recordingLLM();
    const agent = new LocalAgent({
      agent: { name: "a", llm },
      runner,
      namespaceId: "acme",
      retrievers: {
        broken: { retriever: failingRetriever, mode: "context" },
        docs: { retriever: docs(), mode: "context" },
      },
    });

    const out = await agent.invoke({ task: "returns policy" });

    expect(await out.text).toBe("reply 0");
    const prompt = text(calls[0]!);
    expect(prompt).toContain("Returns allow unopened items");
    expect(prompt).toContain("broken: the search failed");
    expect(await events(out)).toContainEqual({
      type: "retriever-failed",
      bindingId: "broken",
      error: "vector store down",
      timedOut: false,
    });
  });

  it("a retriever slower than its timeout is skipped on a thread turn", async () => {
    const clock = FakeWallClock.create(0);
    const { runner } = makeRunner();
    const { llm, calls } = recordingLLM();
    const agent = new LocalAgent({
      agent: { name: "a", llm, clock },
      runner,
      memory: new InMemoryMemoryStore({ clock }),
      namespaceId: "acme",
      retrievers: { slow: { retriever: hangingRetriever, mode: "context", timeoutMs: 2_000 } },
    });
    const thread = await agent.thread("t1");

    const sent = thread.stream({ task: "anything" });
    for (let i = 0; i < 1_000 && clock.pendingCount() === 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    clock.advance(2_000);
    const out = await sent;

    expect(await out.text).toBe("reply 0");
    expect(text(calls[0]!)).toContain("slow: the search timed out");
    expect(await events(out)).toContainEqual({
      type: "retriever-failed",
      bindingId: "slow",
      error: "retriever search timed out after 2000ms",
      timedOut: true,
    });
  });
});
