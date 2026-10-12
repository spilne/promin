import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { AgentEngineConfig } from "../agent-action.ts";
import { LocalAgent } from "../agent/local-agent.ts";
import type { LLMChatParams, LLMProvider, LLMStreamChunk } from "../llm-provider.ts";
import { InMemoryAgentMetrics } from "../metrics/types.ts";

// ---------------------------------------------------------------------------
// Streaming + abort concurrency of a thread turn's `stream()`:
//   1. Normal completion — chunks delivered, stream ends cleanly
//   2. Abort mid-stream — the caller's AbortSignal ends the turn
//   3. cancel() mid-stream — the output's own cancel ends the turn
//   4. LLM rejects after chunks have streamed — the error surfaces from
//      the result promises, no hang, and the promises the caller doesn't
//      read don't count as unhandled rejections
// ---------------------------------------------------------------------------

async function makeThread(config: AgentEngineConfig) {
  const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
  const agent = new LocalAgent({ agent: config, runner, namespaceId: "ns" });
  return agent.thread(`t-${Math.random()}`);
}

/** Streams `chunks`, then stalls until the request is aborted. */
function stallingLLM(chunks: ReadonlyArray<string>): LLMProvider {
  return {
    chat: async () => {
      throw new Error("non-streaming chat not used in this test");
    },
    chatStream: async function* (params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      for (const delta of chunks) yield { delta };
      await new Promise<void>((resolve) => {
        if (params.signal?.aborted) return resolve();
        params.signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      // Match real provider behaviour — abort ends the stream.
    },
  };
}

describe("thread stream() — concurrency contract", () => {
  it("delivers all chunks for a normal completion", async () => {
    const thread = await makeThread({
      name: "test",
      llm: {
        chat: async () => {
          throw new Error("non-streaming chat not used in this test");
        },
        chatStream: async function* (): AsyncIterable<LLMStreamChunk> {
          for (const delta of ["hel", "lo, ", "world"]) yield { delta };
          yield { delta: "", finishReason: "stop" };
        },
      },
    });

    const collected: string[] = [];
    const out = thread.stream({ task: "hi" });
    for await (const chunk of out.textStream) collected.push(chunk);
    expect(collected.join("")).toBe("hello, world");
    expect(await out.text).toBe("hello, world");
  });

  it("AbortSignal aborts mid-stream without hanging", async () => {
    // Two chunks, then the LLM stalls until its request is aborted: the
    // stream can only end through the caller's abort, so no timing is
    // involved in telling "abort propagated" from "hung".
    const thread = await makeThread({ name: "test", llm: stallingLLM(["one ", "two "]) });

    const ac = new AbortController();
    const collected: string[] = [];
    const out = thread.stream({ task: "count" }, { signal: ac.signal });
    for await (const chunk of out.textStream) {
      collected.push(chunk);
      if (collected.length === 2) ac.abort();
    }

    expect(collected).toEqual(["one ", "two "]);
    expect(await out.finishReason).toBe("cancelled");
  });

  it("cancel() during streaming ends the iteration without hanging", async () => {
    const thread = await makeThread({ name: "test", llm: stallingLLM(["a "]) });

    const collected: string[] = [];
    const out = thread.stream({ task: "go" });
    for await (const chunk of out.textStream) {
      collected.push(chunk);
      void out.cancel();
    }

    expect(collected).toEqual(["a "]);
    expect(await out.finishReason).toBe("cancelled");
  });

  it("an LLM error after chunks surfaces from the result, with no unhandled rejection", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      const thread = await makeThread({
        name: "test",
        llmRetry: false,
        llm: {
          chat: async () => {
            throw new Error("non-streaming chat not used in this test");
          },
          chatStream: async function* (): AsyncIterable<LLMStreamChunk> {
            yield { delta: "partial " };
            yield { delta: "answer " };
            throw new Error("upstream-fail");
          },
        },
      });

      const collected: string[] = [];
      const out = thread.stream({ task: "fail me" });
      for await (const chunk of out.textStream) collected.push(chunk);

      expect(collected).toEqual(["partial ", "answer "]);
      // The failure shows on fullStream too, not only on the promises.
      const events = [];
      for await (const e of out.fullStream) events.push(e);
      expect(events.at(-1)).toEqual({ type: "error", error: { message: "upstream-fail" } });
      // Only `text` is read; the other result promises reject unobserved.
      await expect(out.text).rejects.toThrow("upstream-fail");
      for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("a run that fails before its turn starts ends fullStream with an error event", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const agent = new LocalAgent({
      agent: { name: "test", llm: { chat: async () => ({ content: "ok", finishReason: "stop" }) } },
      runner,
      namespaceId: "ns",
    });
    await agent.invoke({ task: "first" }, { runId: "same-run" });

    // The run id names another task: refused before any turn runs.
    const out = agent.stream({ task: "second" }, { runId: "same-run" });
    const events = [];
    for await (const e of out.fullStream) events.push(e);

    expect(events).toEqual([{ type: "error", error: { message: expect.any(String) } }]);
    await expect(out.text).rejects.toThrow();
  });

  it("an aborted stream still counts the tokens it reported", async () => {
    const metrics = new InMemoryAgentMetrics();
    const thread = await makeThread({
      name: "test",
      metrics,
      llmProvider: "p",
      llmModel: "m",
      llm: {
        chat: async () => {
          throw new Error("non-streaming chat not used in this test");
        },
        chatStream: async function* (params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
          yield { delta: "partial", usage: { inputTokens: 120, outputTokens: 7 } };
          await new Promise<void>((resolve) => {
            if (params.signal?.aborted) return resolve();
            params.signal?.addEventListener("abort", () => resolve(), { once: true });
          });
        },
      },
    });

    const ac = new AbortController();
    const out = thread.stream({ task: "go" }, { signal: ac.signal });
    for await (const _chunk of out.textStream) ac.abort();

    expect(await out.finishReason).toBe("cancelled");
    expect(await out.usage).toMatchObject({ inputTokens: 120, outputTokens: 7 });
    expect(metrics.counterTotal("llm.tokens.input")).toBe(120);
  });
});
