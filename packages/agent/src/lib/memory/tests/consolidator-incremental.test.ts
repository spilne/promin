// ---------------------------------------------------------------------------
// DefaultConsolidator: incremental compaction, chunked distillation, and
// LLM calls that are retried and accounted.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { DefaultConsolidator, type ConsolidationLlmCall } from "../consolidator.ts";
import { LLMHttpError } from "../../adapters/llm-http.ts";
import { InMemoryAgentMetrics } from "../../metrics/types.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";

const KEY = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };

/** An LLM that answers with `reply(callIndex)` and records each request's user message. */
function scriptedLLM(reply: (call: number) => string): { llm: LLMProvider; requests: string[] } {
  const requests: string[] = [];
  return {
    requests,
    llm: {
      chat: async (params: { messages: Message[] }): Promise<LLMResponse> => {
        const user = params.messages.find((m) => m.role === "user");
        requests.push(user && typeof user.content === "string" ? user.content : "");
        return {
          content: reply(requests.length - 1),
          finishReason: "stop",
          usage: { inputTokens: 100, outputTokens: 10 },
        };
      },
    },
  };
}

function turns(params: { from: number; count: number; size?: number }): Message[] {
  const out: Message[] = [];
  const pad = "x".repeat(params.size ?? 0);
  for (let i = params.from; i < params.from + params.count; i++) {
    out.push({ role: "user", content: `question-${i} ${pad}` });
    out.push({ role: "assistant", content: `answer-${i}` });
  }
  return out;
}

describe("DefaultConsolidator.compactThread", () => {
  it("is incremental: the next compaction reads the previous summary and only newer messages", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 4 }));
    const { llm, requests } = scriptedLLM((call) => `SUMMARY-${call}`);
    const consolidator = new DefaultConsolidator({ store, llm });

    const first = await consolidator.compactThread(KEY, { keepRecent: 2 });
    expect(first.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 6 });

    await store.appendMessages(KEY, turns({ from: 4, count: 3 }));
    const second = await consolidator.compactThread(KEY, { keepRecent: 2 });

    // The second call saw the first summary and the messages after seq 6 only.
    expect(requests[1]).toContain("SUMMARY-0");
    expect(requests[1]).toContain("question-3");
    expect(requests[1]).not.toContain("question-2");
    expect(second.summary).toBe("SUMMARY-1");
    expect(second.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 12 });
  });

  it("never splits a turn: the kept tail starts at a user message", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, [
      { role: "user", content: "q1" },
      { role: "assistant", content: "a1" },
      { role: "user", content: "q2" },
      { role: "assistant", content: null, toolCalls: [{ id: "c1", name: "t", input: {} }] },
      { role: "tool", toolCallId: "c1", content: "r1" },
      { role: "assistant", content: "a2" },
    ]);
    const { llm } = scriptedLLM(() => "S");
    // keepRecent 2 would cut between the tool call and its result; the cut
    // moves back to the user message that opened the turn.
    const ep = await new DefaultConsolidator({ store, llm }).compactThread(KEY, { keepRecent: 2 });
    expect(ep.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 2 });
  });

  it("caps the stored summary at summaryMaxTokens * 4 characters", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 3 }));
    const long = Array.from({ length: 50 }, (_, i) => `line ${i} ${"y".repeat(30)}`).join("\n");
    const { llm } = scriptedLLM(() => long);
    const ep = await new DefaultConsolidator({ store, llm, summaryMaxTokens: 64 }).compactThread(
      KEY,
      { keepRecent: 2 },
    );
    expect(ep.summary.length).toBeLessThanOrEqual(256);
    expect(ep.summary).toContain("line 49");
  });

  it("is rendered by resolveContext in place of the folded messages", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 4 }));
    const { llm } = scriptedLLM(() => "GIST-OF-EARLIER-TURNS");
    await new DefaultConsolidator({ store, llm }).compactThread(KEY, { keepRecent: 2 });
    const ctx = await store.resolveContext(KEY, { maxMessageTokens: 100_000 });
    expect(ctx.reference).toContain("GIST-OF-EARLIER-TURNS");
    expect(ctx.messages.map((m) => m.seq)).toEqual([7, 8]);
  });
});

describe("DefaultConsolidator.distillThread", () => {
  it("summarises a long thread in chunks, each carrying the running summary", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 30, size: 100 }));
    const { llm, requests } = scriptedLLM((call) =>
      JSON.stringify({
        summary: `PART-${call}`,
        outcome: null,
        salience: 0.4 + call * 0.01,
        facts: [`fact ${call % 2}`],
      }),
    );
    const ep = await new DefaultConsolidator({ store, llm, maxChunkChars: 1_000 }).distillThread(
      KEY,
    );
    expect(requests.length).toBeGreaterThan(3);
    for (const request of requests) expect(request.length).toBeLessThan(2_000);
    expect(requests[1]).toContain("PART-0");
    expect(ep.summary).toBe(`PART-${requests.length - 1}`);
    expect(ep.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 60 });
    const facts = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
    expect(facts.map((f) => f.text).sort()).toEqual(["fact 0", "fact 1"]);
  });

  it("force replaces the thread's previous distill episode", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 2 }));
    const { llm } = scriptedLLM((call) =>
      JSON.stringify({ summary: `v${call}`, outcome: null, salience: 0.5, facts: [] }),
    );
    const consolidator = new DefaultConsolidator({ store, llm });
    await consolidator.distillThread(KEY);
    await consolidator.distillThread(KEY, { force: true });
    const episodes = await store.listResourceEpisodes({ namespaceId: "acme", resourceId: "alice" });
    expect(episodes.map((e) => e.summary)).toEqual(["v1"]);
  });
});

describe("DefaultConsolidator LLM calls", () => {
  it("retries a transient failure per the retry policy", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 2 }));
    let attempts = 0;
    const llm: LLMProvider = {
      chat: async (): Promise<LLMResponse> => {
        attempts += 1;
        if (attempts === 1) {
          throw new LLMHttpError({ status: 503, provider: "test", body: "overloaded" });
        }
        return {
          content: JSON.stringify({ summary: "ok", outcome: null, salience: 0.5, facts: [] }),
          finishReason: "stop",
        };
      },
    };
    const ep = await new DefaultConsolidator({
      store,
      llm,
      retry: { baseDelayMs: 0 },
    }).distillThread(KEY);
    expect(attempts).toBe(2);
    expect(ep.summary).toBe("ok");
  });

  it("fails on a transient error when retries are off", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 2 }));
    const llm: LLMProvider = {
      chat: async (): Promise<LLMResponse> => {
        throw new LLMHttpError({ status: 503, provider: "test", body: "overloaded" });
      },
    };
    await expect(
      new DefaultConsolidator({ store, llm, retry: false }).distillThread(KEY),
    ).rejects.toBeInstanceOf(LLMHttpError);
  });

  it("records metrics, reports each call and stores the usage on the episode", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns({ from: 0, count: 4 }));
    const metrics = new InMemoryAgentMetrics();
    const calls: ConsolidationLlmCall[] = [];
    const { llm } = scriptedLLM(() => "S");
    const ep = await new DefaultConsolidator({
      store,
      llm,
      metrics,
      llmProvider: "anthropic",
      llmModel: "haiku",
      metricLabels: { agent: "bot" },
      onLlmCall: (call) => calls.push(call),
    }).compactThread(KEY, { keepRecent: 2 });

    expect(metrics.counterTotal("llm.tokens.input")).toBe(100);
    expect(metrics.counterSamples("llm.calls")[0]!.labels).toMatchObject({
      provider: "anthropic",
      model: "haiku",
      agent: "bot",
      purpose: "consolidation",
      operation: "compact",
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({ operation: "compact", key: KEY });
    expect(ep.metadata["usage"]).toEqual({ inputTokens: 100, outputTokens: 10 });
  });
});
