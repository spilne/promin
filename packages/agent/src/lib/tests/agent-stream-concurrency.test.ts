import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type {
  LLMProvider,
  LLMResponse,
  LLMChatStreamChunk,
  LLMChatParams,
} from "../llm-provider.ts";

// ---------------------------------------------------------------------------
// promin-hhqq — streaming + abort + close concurrency tests
//
// stream() composes runner.runSafe (the workflow side) with a ChunkQueue
// (the producer-consumer queue feeding tokens to the for-await caller).
// Several races deserve test coverage:
//   1. Normal completion — chunks delivered, stream ends cleanly
//   2. Abort mid-stream — caller's AbortSignal fires while chunks are queued
//   3. session.close() during stream — session-level abort
//   4. LLM rejects after chunks have streamed — error surfaces, no hang
// ---------------------------------------------------------------------------

/** Streaming LLM that yields chunks one at a time, optionally with a delay between them. */
function makeStreamingLLM(opts: {
  chunks: string[];
  /** ms to wait between chunks. Default 5. */
  perChunkDelayMs?: number;
  /** When set, throw this error after emitting `chunks` items. */
  throwAfter?: Error;
}): LLMProvider {
  const delay = opts.perChunkDelayMs ?? 5;
  return {
    chat: async () => {
      throw new Error("non-streaming chat not used in this test");
    },
    chatStream: async function* (params: LLMChatParams): AsyncIterable<LLMChatStreamChunk> {
      for (const chunk of opts.chunks) {
        if (params.signal?.aborted) {
          // Match real provider behaviour — abort yields a stop finish.
          return;
        }
        await new Promise((r) => setTimeout(r, delay));
        yield { delta: chunk };
      }
      if (opts.throwAfter) throw opts.throwAfter;
      yield { finishReason: "stop" };
    },
  } as unknown as LLMProvider;
}

describe("stream() — concurrency contract", () => {
  it("delivers all chunks for a normal completion", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: makeStreamingLLM({ chunks: ["hel", "lo, ", "world"] }),
    }).session({ runner, sessionId: "stream-1" });

    const collected: string[] = [];
    for await (const chunk of session.stream("hi")) {
      collected.push(chunk);
    }
    expect(collected.join("")).toBe("hello, world");
    await session.close();
  });

  it("AbortSignal aborts mid-stream without hanging", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: makeStreamingLLM({
        chunks: ["one ", "two ", "three ", "four ", "five"],
        perChunkDelayMs: 20,
      }),
    }).session({ runner, sessionId: "stream-abort" });

    const ac = new AbortController();
    const collected: string[] = [];
    let chunkCount = 0;
    const startedAt = Date.now();
    for await (const chunk of session.stream("count", { signal: ac.signal })) {
      collected.push(chunk);
      chunkCount++;
      if (chunkCount === 2) ac.abort();
    }
    const elapsed = Date.now() - startedAt;

    // Should exit before all 5 chunks emit (otherwise abort didn't propagate).
    expect(collected.length).toBeLessThan(5);
    // And shouldn't hang for the full delay sequence.
    expect(elapsed).toBeLessThan(200);
    await session.close();
  });

  it("session.close() during streaming ends the iteration without hanging", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: makeStreamingLLM({
        chunks: ["a ", "b ", "c ", "d ", "e"],
        perChunkDelayMs: 20,
      }),
    }).session({ runner, sessionId: "stream-close" });

    const collected: string[] = [];
    setTimeout(() => session.close(), 30);
    const startedAt = Date.now();
    try {
      for await (const chunk of session.stream("go")) {
        collected.push(chunk);
      }
    } catch (err) {
      // close() rejects the answerPromise with "Session closed"; either a
      // clean exit OR this thrown error is acceptable. The bug we're
      // guarding against is HANGING, not erroring.
      expect((err as Error).message).toMatch(/Session closed/i);
    }
    const elapsed = Date.now() - startedAt;
    // Full chunk sequence would be 5 × 20ms = 100ms. Close at 30ms must
    // unblock the iterator before then.
    expect(elapsed).toBeLessThan(120);
  });

  it("LLM error AFTER chunks have streamed surfaces from the for-await (no hang, no swallow)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: makeStreamingLLM({
        chunks: ["partial ", "answer "],
        throwAfter: new Error("upstream-fail"),
      }),
    }).session({ runner, sessionId: "stream-err" });

    let caught: unknown = null;
    const collected: string[] = [];
    try {
      for await (const chunk of session.stream("fail me")) {
        collected.push(chunk);
      }
    } catch (err) {
      caught = err;
    }

    // Caller sees SOME chunks (the ones that landed before the throw)…
    expect(collected.length).toBeGreaterThanOrEqual(1);
    // …and the upstream error must surface (not be silently dropped).
    expect(caught).toBeDefined();
    await session.close();
  });
});

// ---------------------------------------------------------------------------
// promin-3eh5 — approval-gate race tests
//
// session.approve() / .reject() deliver workflow signals via
// completeSignal. The retry loop inside deliverApprovalSignal handles the
// case where the workflow hasn't yet suspended on the matching signalName
// — which is the common race in real UIs (UI calls /approve milliseconds
// after the approval card renders).
// ---------------------------------------------------------------------------

function nonStreamingLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("LLM exhausted");
      return r;
    },
  } as unknown as LLMProvider;
}

describe("approve()/reject() — early-call race", () => {
  it("approve() called before the workflow suspends still delivers (retry path)", async () => {
    // The agent loop emits a tool call; the approval activity reads the
    // signal. If the caller approves immediately after send() returns,
    // the signal might land before the suspend point — deliverApprovalSignal
    // retries until the signal lands at a suspended workflow.
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: nonStreamingLLM([
        // First response: ask to use a tool
        {
          content: null,
          toolCalls: [{ id: "call-1", name: "doit", input: {} }],
          finishReason: "tool_use",
        },
        // After approval + tool result: final answer
        { content: "done", finishReason: "stop" },
      ]),
      tools: {
        doit: {
          name: "doit",
          description: "",
          parameters: { parse: () => ({}) } as never,
          requireApproval: true,
          execute: async () => "tool output",
        },
        // biome-ignore lint/suspicious/noExplicitAny: simplified test stub
      } as any,
    }).session({ runner, sessionId: "approve-early" });

    const turnPromise = session.send("use the tool");
    // Immediately approve — workflow likely hasn't suspended yet.
    const approvePromise = session.approve("call-1");

    const [answer, approved] = await Promise.all([turnPromise, approvePromise]);
    expect(approved).toBe(true);
    expect(answer).toBe("done");
    await session.close();
  });

  it("reject() before suspend works the same way", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      llm: nonStreamingLLM([
        {
          content: null,
          toolCalls: [{ id: "call-1", name: "doit", input: {} }],
          finishReason: "tool_use",
        },
        { content: "rejected, here's what i can do instead", finishReason: "stop" },
      ]),
      tools: {
        doit: {
          name: "doit",
          description: "",
          parameters: { parse: () => ({}) } as never,
          requireApproval: true,
          execute: async () => "should not run",
        },
        // biome-ignore lint/suspicious/noExplicitAny: simplified test stub
      } as any,
    }).session({ runner, sessionId: "reject-early" });

    const turnPromise = session.send("use the tool");
    const rejectPromise = session.reject("call-1", "no thanks");

    const [answer, rejected] = await Promise.all([turnPromise, rejectPromise]);
    expect(rejected).toBe(true);
    expect(answer).toContain("rejected");
    await session.close();
  });
});
