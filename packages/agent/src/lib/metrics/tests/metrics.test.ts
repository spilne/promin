// ---------------------------------------------------------------------------
// AgentMetrics — pure tests for the no-op + in-memory adapters and the
// per-call recording helpers. Integration with runLlmCall is covered by
// the agent-loop tests once the metrics field is set.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentMetrics,
  NoopAgentMetrics,
  computeCallCostUsd,
  staticCostRegistry,
  type AgentMetrics,
} from "../types.ts";
import { recordApproval, recordChat, recordTool } from "../instrument.ts";

describe("NoopAgentMetrics", () => {
  it("counters and histograms are no-ops (zero cost)", () => {
    const m: AgentMetrics = NoopAgentMetrics;
    // No throws. No state to inspect — it's a no-op by design.
    m.counter("anything").inc(5);
    m.counter("anything").inc(5, { label: "x" });
    m.histogram("anything").observe(123);
    m.histogram("anything").observe(456, { label: "y" });
  });
});

describe("InMemoryAgentMetrics", () => {
  it("counter accumulates total + samples", () => {
    const m = new InMemoryAgentMetrics();
    m.counter("c").inc(2);
    m.counter("c").inc(3, { x: "1" });
    expect(m.counterTotal("c")).toBe(5);
    expect(m.counterSamples("c")).toEqual([
      { value: 2, labels: {} },
      { value: 3, labels: { x: "1" } },
    ]);
  });

  it("histogram tracks count, sum, and samples", () => {
    const m = new InMemoryAgentMetrics();
    m.histogram("h").observe(100);
    m.histogram("h").observe(200, { tag: "y" });
    expect(m.histogramSum("h")).toBe(300);
    expect(m.histogramSamples("h")).toHaveLength(2);
  });

  it("returns zero / empty for unrecorded metrics", () => {
    const m = new InMemoryAgentMetrics();
    expect(m.counterTotal("ghost")).toBe(0);
    expect(m.counterSamples("ghost")).toEqual([]);
    expect(m.histogramSum("ghost")).toBe(0);
  });

  it("reset clears all state", () => {
    const m = new InMemoryAgentMetrics();
    m.counter("c").inc(1);
    m.histogram("h").observe(1);
    m.reset();
    expect(m.counterTotal("c")).toBe(0);
    expect(m.histogramSum("h")).toBe(0);
  });
});

describe("computeCallCostUsd", () => {
  it("computes total from input + output rates", () => {
    const cost = computeCallCostUsd(
      { inputTokens: 1_000_000, outputTokens: 500_000 },
      { inputPerMillion: 3, outputPerMillion: 15 },
    );
    // 1M @ $3 = $3.00 + 0.5M @ $15 = $7.50 → $10.50
    expect(cost).toBe(10.5);
  });

  it("includes cache rates when set", () => {
    const cost = computeCallCostUsd(
      { inputTokens: 1_000_000, outputTokens: 0, cacheReadTokens: 1_000_000 },
      { inputPerMillion: 3, outputPerMillion: 15, cacheReadPerMillion: 0.3 },
    );
    expect(cost).toBe(3 + 0.3);
  });

  it("returns undefined when rates missing — never fabricates", () => {
    expect(computeCallCostUsd({ inputTokens: 1000, outputTokens: 500 }, undefined)).toBeUndefined();
  });

  it("ignores cache cost dimensions when usage doesn't carry the field", () => {
    const cost = computeCallCostUsd(
      { inputTokens: 1_000_000, outputTokens: 0 },
      { inputPerMillion: 3, cacheReadPerMillion: 100 },
    );
    expect(cost).toBe(3);
  });
});

describe("staticCostRegistry", () => {
  it("returns rates by `${provider}/${model}` key", () => {
    const r = staticCostRegistry({
      "anthropic/claude-sonnet-4-6": { inputPerMillion: 3, outputPerMillion: 15 },
    });
    expect(r.rates("anthropic", "claude-sonnet-4-6")?.inputPerMillion).toBe(3);
    expect(r.rates("anthropic", "claude-haiku-4-5")).toBeUndefined();
    expect(r.rates("openai", "gpt-x")).toBeUndefined();
  });
});

describe("recordChat", () => {
  it("emits llm.calls + llm.latency.ms + llm.tokens.* with provider/model labels", () => {
    const m = new InMemoryAgentMetrics();
    recordChat({
      metrics: m,
      provider: "anthropic",
      model: "claude-sonnet-4-6",
      response: {
        content: "hello",
        finishReason: "stop",
        usage: {
          inputTokens: 100,
          outputTokens: 50,
          cacheReadTokens: 200,
          cacheWriteTokens: 0,
        },
      },
      durationMs: 1234,
    });
    expect(m.counterTotal("llm.calls")).toBe(1);
    expect(m.counterTotal("llm.tokens.input")).toBe(100);
    expect(m.counterTotal("llm.tokens.output")).toBe(50);
    expect(m.counterTotal("llm.tokens.cache.read")).toBe(200);
    // No samples for cache.write since the value was 0 — gates against
    // emitting zero-counter rows.
    expect(m.counterTotal("llm.tokens.cache.write")).toBe(0);
    expect(m.histogramSum("llm.latency.ms")).toBe(1234);

    // Labels carry provider + model on every metric.
    const tokenLabels = m.counterSamples("llm.tokens.input")[0]!.labels;
    expect(tokenLabels).toEqual({ provider: "anthropic", model: "claude-sonnet-4-6" });
    // llm.calls additionally carries finish_reason.
    const callLabels = m.counterSamples("llm.calls")[0]!.labels;
    expect(callLabels).toMatchObject({
      provider: "anthropic",
      model: "claude-sonnet-4-6",
      finish_reason: "stop",
    });
  });

  it("emits llm.cost.usd when costs registry has rates", () => {
    const m = new InMemoryAgentMetrics();
    const costs = staticCostRegistry({
      "anthropic/claude-sonnet-4-6": { inputPerMillion: 3, outputPerMillion: 15 },
    });
    recordChat({
      metrics: m,
      costs,
      provider: "anthropic",
      model: "claude-sonnet-4-6",
      response: {
        content: "x",
        finishReason: "stop",
        usage: { inputTokens: 1_000_000, outputTokens: 500_000 },
      },
      durationMs: 100,
    });
    // 1M @ $3 + 0.5M @ $15 = $10.50
    expect(m.counterTotal("llm.cost.usd")).toBe(10.5);
  });

  it("skips cost emission when model isn't in the registry", () => {
    const m = new InMemoryAgentMetrics();
    const costs = staticCostRegistry({});
    recordChat({
      metrics: m,
      costs,
      provider: "p",
      model: "m",
      response: {
        content: null,
        finishReason: "stop",
        usage: { inputTokens: 100, outputTokens: 50 },
      },
      durationMs: 1,
    });
    expect(m.counterTotal("llm.cost.usd")).toBe(0);
  });

  it("supports extraLabels (agent id, mode, etc.)", () => {
    const m = new InMemoryAgentMetrics();
    recordChat({
      metrics: m,
      provider: "p",
      model: "m",
      response: { content: null, finishReason: "stop" },
      durationMs: 1,
      extraLabels: { agent: "writer", mode: "chat" },
    });
    expect(m.counterSamples("llm.calls")[0]!.labels).toMatchObject({
      agent: "writer",
      mode: "chat",
    });
  });
});

describe("recordTool", () => {
  it("counts calls + observes latency, increments errors when ok=false", () => {
    const m = new InMemoryAgentMetrics();
    recordTool({ metrics: m, tool: "search", durationMs: 100, ok: true });
    recordTool({ metrics: m, tool: "search", durationMs: 50, ok: false });
    expect(m.counterTotal("tool.calls")).toBe(2);
    expect(m.counterTotal("tool.errors")).toBe(1);
    expect(m.histogramSum("tool.latency.ms")).toBe(150);
  });
});

describe("recordApproval", () => {
  it("requested → approval.requests; decision → approval.decisions with outcome label", () => {
    const m = new InMemoryAgentMetrics();
    recordApproval(m, "requested");
    recordApproval(m, "approved");
    recordApproval(m, "rejected");
    recordApproval(m, "timeout");
    expect(m.counterTotal("approval.requests")).toBe(1);
    expect(m.counterTotal("approval.decisions")).toBe(3);
    const outcomes = m.counterSamples("approval.decisions").map((s) => s.labels.outcome);
    expect(outcomes.sort()).toEqual(["approved", "rejected", "timeout"]);
  });
});
