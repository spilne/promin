import { describe, expect, it } from "bun:test";
import type { Agent, AgentRunOutput } from "@promin/agent";
import { InMemoryAgentMetrics } from "@promin/agent";
import type { Scorer } from "../../types.ts";
import { liveScoredFromRecipe } from "../from-recipe.ts";
import { liveScored } from "../live-scored.ts";
import { inMemoryLiveSink, metricsLiveSink } from "../sinks.ts";

function fakeRun(text: string): AgentRunOutput {
  async function* empty(): AsyncGenerator<never> {}
  return {
    textStream: empty(),
    fullStream: empty(),
    text: Promise.resolve(text),
    output: Promise.resolve(undefined),
    toolCalls: Promise.resolve([]),
    toolResults: Promise.resolve([]),
    steps: Promise.resolve([]),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    finishReason: Promise.resolve("stop"),
    messages: Promise.resolve([]),
    cancel: async () => {},
  } as AgentRunOutput;
}

function fakeAgent(reply: string): Agent {
  const notImplemented = (): never => {
    throw new Error("not implemented in fake agent");
  };
  return {
    invoke: async () => fakeRun(reply),
    stream: () => fakeRun(reply),
    thread: notImplemented,
    listThreads: notImplemented,
    compactThread: notImplemented,
    distillThread: notImplemented,
    withScope: () => fakeAgent(reply),
  } as unknown as Agent;
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
    const scored = liveScored(fakeAgent("hello"), { scorers: [okScorer], sink });
    const run = await scored.invoke({ task: "say hello" });
    expect(await run.text).toBe("hello"); // run surface is untouched
    await sink.waitForCount(1);
    expect(sink.scores[0]?.scores[0]?.scorerId).toBe("ok");
  });

  it("skips scoring on a sampling miss", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScored(fakeAgent("x"), {
      scorers: [okScorer],
      sink,
      decide: () => false,
    });
    await scored.invoke({ task: "t" });
    await flush();
    expect(sink.scores.length).toBe(0);
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
    const scored = liveScored(fakeAgent("x"), {
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
    const scored = liveScored(fakeAgent("x"), {
      scorers: [okScorer],
      sink: metricsLiveSink(metrics),
    });
    await scored.invoke({ task: "t" });
    await flush();
    expect(metrics.histogramSamples("eval.score").length).toBe(1);
  });

  it("withScope keeps the scoring wrapper", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScored(fakeAgent("x"), { scorers: [okScorer], sink }).withScope({});
    await scored.invoke({ task: "t" });
    await sink.waitForCount(1);
    expect(sink.scores.length).toBe(1);
  });
});

describe("liveScoredFromRecipe", () => {
  it("resolves scorer refs against the catalog", async () => {
    const sink = inMemoryLiveSink();
    const scored = liveScoredFromRecipe(
      fakeAgent("x"),
      { scorerRefs: ["ok"] },
      { scorerCatalog: { ok: okScorer }, sink },
    );
    await scored.invoke({ task: "t" });
    await sink.waitForCount(1);
    expect(sink.scores[0]?.scores[0]?.scorerId).toBe("ok");
  });

  it("throws when a referenced scorer is missing", () => {
    expect(() =>
      liveScoredFromRecipe(
        fakeAgent("x"),
        { scorerRefs: ["ghost"] },
        { scorerCatalog: {}, sink: inMemoryLiveSink() },
      ),
    ).toThrow("ghost");
  });
});
