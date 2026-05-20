import { describe, expect, it } from "bun:test";
import { buildAgentTrace } from "@promin/agent";
import type { Message } from "@promin/agent";
import { mockLLM } from "@promin/agent/testing";
import type { EvalOutput } from "../../types.ts";
import { budget } from "../budget.ts";
import { llmJudge } from "../llm-judge.ts";
import { createLLMScorer } from "../llm-scorer.ts";
import { toolCallNames, toolTrajectory } from "../tool-trajectory.ts";

function out(output: unknown): EvalOutput {
  return { output, metrics: { latencyMs: 0 } };
}

/** An EvalOutput carrying a trace built from the given tool-call sequence. */
function traceWith(toolNames: ReadonlyArray<string>): EvalOutput {
  const messages: Message[] = [
    { role: "user", content: "do it" },
    {
      role: "assistant",
      content: null,
      toolCalls: toolNames.map((name, i) => ({ id: `t${i}`, name, input: {} })),
    },
    ...toolNames.map((_name, i) => ({ role: "tool" as const, toolCallId: `t${i}`, content: "ok" })),
  ];
  return { output: "done", metrics: { latencyMs: 0 }, trace: buildAgentTrace(messages) };
}

describe("createLLMScorer", () => {
  it("parses a JSON judgement from the judge reply", async () => {
    const scorer = createLLMScorer({
      id: "j",
      judge: mockLLM([{ content: '{"value":0.9,"reason":"solid"}', finishReason: "stop" }]),
      prompt: () => "rate it",
    });
    const score = await scorer.score({ input: "q", output: out("a") });
    expect(score.value).toBe(0.9);
    expect(score.reason).toBe("solid");
  });

  it("falls back to the first number, scaling an N/10 rating", async () => {
    const scorer = createLLMScorer({
      id: "j",
      judge: mockLLM([{ content: "I'd give this an 8 out of 10.", finishReason: "stop" }]),
      prompt: () => "rate it",
    });
    const score = await scorer.score({ input: "q", output: out("a") });
    expect(score.value).toBe(0.8);
  });

  it("scores 0 on an unparseable reply", async () => {
    const scorer = createLLMScorer({
      id: "j",
      judge: mockLLM([{ content: "no idea", finishReason: "stop" }]),
      prompt: () => "rate it",
    });
    const score = await scorer.score({ input: "q", output: out("a") });
    expect(score.value).toBe(0);
  });

  it("clamps an out-of-range value to 0..1", async () => {
    const scorer = createLLMScorer({
      id: "j",
      judge: mockLLM([{ content: '{"value":4}', finishReason: "stop" }]),
      prompt: () => "x",
    });
    const score = await scorer.score({ input: "q", output: out("a") });
    expect(score.value).toBe(1);
  });
});

describe("llmJudge", () => {
  it("scores via the rubric and judge LLM", async () => {
    const scorer = llmJudge({
      judge: mockLLM([{ content: '{"value":1,"reason":"matches"}', finishReason: "stop" }]),
      rubric: "Is the answer correct?",
    });
    expect(scorer.id).toBe("llmJudge");
    const score = await scorer.score({
      input: "capital of France?",
      expected: "Paris",
      output: out("Paris"),
    });
    expect(score.value).toBe(1);
    expect(score.scorerId).toBe("llmJudge");
  });
});

describe("toolTrajectory", () => {
  it("extracts the tool-call sequence from a trace", () => {
    const traced = traceWith(["search", "fetch"]);
    expect(traced.trace).toBeDefined();
    expect(toolCallNames(traced.trace!)).toEqual(["search", "fetch"]);
  });

  it("scores an exact sequence match", async () => {
    const score = await toolTrajectory({ expected: ["search", "fetch"], mode: "exact" }).score({
      input: "q",
      output: traceWith(["search", "fetch"]),
    });
    expect(score.value).toBe(1);
  });

  it("ordered mode gives partial credit for a subsequence", async () => {
    const score = await toolTrajectory({
      expected: ["search", "rank", "fetch"],
      mode: "ordered",
    }).score({ input: "q", output: traceWith(["search", "fetch"]) });
    expect(score.value).toBeCloseTo(2 / 3);
  });

  it("set mode ignores order", async () => {
    const score = await toolTrajectory({ expected: ["fetch", "search"], mode: "set" }).score({
      input: "q",
      output: traceWith(["search", "fetch"]),
    });
    expect(score.value).toBe(1);
  });

  it("scores 0 when the output has no trace", async () => {
    const score = await toolTrajectory({ expected: ["search"] }).score({
      input: "q",
      output: out("a"),
    });
    expect(score.value).toBe(0);
  });
});

describe("budget", () => {
  function metricsOut(metrics: EvalOutput["metrics"]): EvalOutput {
    return { output: "x", metrics };
  }

  it("scores 1 when within every limit", async () => {
    const score = await budget({ maxLatencyMs: 1000, maxCostUsd: 1 }).score({
      input: "q",
      output: metricsOut({ latencyMs: 200, costUsd: 0.5 }),
    });
    expect(score.value).toBe(1);
  });

  it("scores the fraction of limits met", async () => {
    const score = await budget({ maxLatencyMs: 100, maxCostUsd: 1 }).score({
      input: "q",
      output: metricsOut({ latencyMs: 500, costUsd: 0.5 }),
    });
    expect(score.value).toBe(0.5);
    expect(score.reason).toContain("latency");
  });

  it("skips a limit whose metric is unavailable", async () => {
    const score = await budget({ maxCostUsd: 1 }).score({
      input: "q",
      output: metricsOut({ latencyMs: 10 }),
    });
    expect(score.value).toBe(1);
  });
});
