import { describe, expect, it } from "bun:test";
import type { Agent, AgentRunOutput, Message } from "@promin/agent";
import { InMemoryAgentMetrics } from "@promin/agent";
import type { Scorer } from "../../types.ts";
import { liveScoredFromRecipe } from "../from-recipe.ts";
import { liveScored } from "../live-scored.ts";
import { inMemoryLiveSink, metricsLiveSink } from "../sinks.ts";

function fakeRun(params: { text: string; settled?: Promise<Message[]> }): AgentRunOutput {
  async function* empty(): AsyncGenerator<never> {}
  return {
    textStream: empty(),
    fullStream: empty(),
    text: Promise.resolve(params.text),
    output: Promise.resolve(undefined),
    toolCalls: Promise.resolve([]),
    toolResults: Promise.resolve([]),
    steps: Promise.resolve([]),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    finishReason: Promise.resolve("stop"),
    messages: params.settled ?? Promise.resolve([]),
    cancel: async () => {},
  };
}

function fakeAgent(params: { reply: string; settled?: () => Promise<Message[]> }): Agent {
  const notImplemented = (): never => {
    throw new Error("not implemented in fake agent");
  };
  const run = (): AgentRunOutput =>
    fakeRun({
      text: params.reply,
      ...(params.settled !== undefined && { settled: params.settled() }),
    });
  return {
    invoke: async () => run(),
    stream: () => run(),
    thread: notImplemented,
    listThreads: notImplemented,
    compactThread: notImplemented,
    distillThread: notImplemented,
    withScope: () => fakeAgent(params),
  };
}

const okScorer: Scorer = {
  id: "ok",
  async score() {
    return { scorerId: "ok", value: 1 };
  },
};

const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 10));

describe("liveScored", () => {
  it("scores a run and records it to the sink", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScored({ agent: fakeAgent({ reply: "hello" }), scorers: [okScorer], sink });
    const run = await scored.invoke({ task: "say hello" });
    expect(await run.text).toBe("hello"); // run surface is untouched
    await sink.waitForCount(1);
    expect(sink.scores[0]?.scores[0]?.scorerId).toBe("ok");
  });

  it("skips scoring on a sampling miss", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScored({
      agent: fakeAgent({ reply: "x" }),
      scorers: [okScorer],
      sink,
      decide: () => false,
    });
    await scored.invoke({ task: "t" });
    await flush();
    expect(sink.scores.length).toBe(0);
  });

  it("refuses a sampling rate outside 0..1 and a non-positive concurrency cap", () => {
    const agent = fakeAgent({ reply: "x" });
    const sink = inMemoryLiveSink();
    for (const rate of [-0.1, 1.5, Number.NaN]) {
      expect(() => liveScored({ agent, scorers: [okScorer], sink, sampling: { rate } })).toThrow(
        RangeError,
      );
    }
    for (const maxConcurrent of [0, 2.5]) {
      expect(() => liveScored({ agent, scorers: [okScorer], sink, maxConcurrent })).toThrow(
        RangeError,
      );
    }
  });

  it("drops sampled runs past the concurrency cap instead of queueing scorer calls", async () => {
    const sink = inMemoryLiveSink();
    const releases: Array<() => void> = [];
    const dropped: string[] = [];
    const scored = liveScored({
      agent: fakeAgent({
        reply: "x",
        settled: () => new Promise<Message[]>((resolve) => releases.push(() => resolve([]))),
      }),
      scorers: [okScorer],
      sink,
      maxConcurrent: 2,
      onDropped: (runId) => dropped.push(runId),
    });
    for (const runId of ["r1", "r2", "r3", "r4"]) await scored.invoke({ task: "t" }, { runId });
    expect(dropped).toEqual(["r3", "r4"]);
    for (const release of releases) release();
    await sink.waitForCount(2);
    expect(sink.scores.map((s) => s.runId).sort()).toEqual(["r1", "r2"]);
    // A slot frees once its scoring finishes.
    await flush();
    await scored.invoke({ task: "t" }, { runId: "r5" });
    releases.at(-1)?.();
    await sink.waitForCount(3);
    expect(dropped).toEqual(["r3", "r4"]);
  });

  it("shares the concurrency cap with withScope views", async () => {
    const sink = inMemoryLiveSink();
    const releases: Array<() => void> = [];
    const dropped: string[] = [];
    const scored = liveScored({
      agent: fakeAgent({
        reply: "x",
        settled: () => new Promise<Message[]>((resolve) => releases.push(() => resolve([]))),
      }),
      scorers: [okScorer],
      sink,
      maxConcurrent: 1,
      onDropped: (runId) => dropped.push(runId),
    });
    await scored.invoke({ task: "t" }, { runId: "a" });
    await scored.withScope({ namespaceId: "ns" }).invoke({ task: "t" }, { runId: "b" });
    expect(dropped).toEqual(["b"]);
    for (const release of releases) release();
    await sink.waitForCount(1);
  });

  it("swallows a throwing scorer without breaking the run", async () => {
    const sink = inMemoryLiveSink();
    const errors: unknown[] = [];
    const boom: Scorer = {
      id: "boom",
      async score() {
        throw new Error("scorer boom");
      },
    };
    const scored = liveScored({
      agent: fakeAgent({ reply: "x" }),
      scorers: [boom, okScorer],
      sink,
      onError: (err) => errors.push(err),
    });
    const run = await scored.invoke({ task: "t" });
    expect(await run.text).toBe("x");
    await sink.waitForCount(1);
    expect(errors.length).toBe(1);
    expect(sink.scores[0]?.scores.map((s) => s.scorerId)).toEqual(["ok"]);
  });

  it("metricsLiveSink emits an eval.score histogram", async () => {
    const metrics = new InMemoryAgentMetrics();
    const scored = liveScored({
      agent: fakeAgent({ reply: "x" }),
      scorers: [okScorer],
      sink: metricsLiveSink(metrics),
    });
    await scored.invoke({ task: "t" });
    await flush();
    expect(metrics.histogramSamples("eval.score").length).toBe(1);
  });

  it("withScope keeps the scoring wrapper", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScored({
      agent: fakeAgent({ reply: "x" }),
      scorers: [okScorer],
      sink,
    }).withScope({});
    await scored.invoke({ task: "t" });
    await sink.waitForCount(1);
    expect(sink.scores.length).toBe(1);
  });
});

describe("liveScoredFromRecipe", () => {
  it("resolves scorer refs against the catalog", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScoredFromRecipe({
      agent: fakeAgent({ reply: "x" }),
      config: { scorerRefs: ["ok"] },
      scorerCatalog: { ok: okScorer },
      sink,
    });
    await scored.invoke({ task: "t" });
    await sink.waitForCount(1);
    expect(sink.scores[0]?.scores[0]?.scorerId).toBe("ok");
  });

  it("throws when a referenced scorer is missing", () => {
    expect(() =>
      liveScoredFromRecipe({
        agent: fakeAgent({ reply: "x" }),
        config: { scorerRefs: ["ghost"] },
        scorerCatalog: {},
        sink: inMemoryLiveSink(),
      }),
    ).toThrow("ghost");
  });

  it("refuses an out-of-range recipe sampling rate", () => {
    expect(() =>
      liveScoredFromRecipe({
        agent: fakeAgent({ reply: "x" }),
        config: { scorerRefs: ["ok"], sampling: { rate: 2 } },
        scorerCatalog: { ok: okScorer },
        sink: inMemoryLiveSink(),
      }),
    ).toThrow(RangeError);
  });
});
