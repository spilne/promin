import { describe, expect, it } from "bun:test";
import { buildAgentTrace } from "@promin/agent";
import type { Message } from "@promin/agent";
import { mockLLM } from "@promin/agent/testing";
import type { EvalOutput } from "../../types.ts";
import { budget } from "../budget.ts";
import { llmJudge } from "../llm-judge.ts";
import { inlineDataset } from "../../datasets/inline.ts";
import { runEval } from "../../runner.ts";
import { fnTarget } from "../../targets/fn-target.ts";
import { createLLMScorer, JudgeParseError, parseJudgement } from "../llm-scorer.ts";
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

  it("reads a rating with its scale: 85/100, 8 out of 10, 85%, outOf", async () => {
    const valueOf = async (reply: string): Promise<number> =>
      (
        await createLLMScorer({
          id: "j",
          judge: mockLLM([{ content: reply, finishReason: "stop" }]),
          prompt: () => "rate it",
        }).score({ input: "q", output: out("a") })
      ).value;
    // "85/100" used to be read as 85 → 8.5 → clamped to 1.
    expect(await valueOf("Score: 85/100")).toBe(0.85);
    expect(await valueOf("I'd give this an 8 out of 10.")).toBe(0.8);
    expect(await valueOf("about 85% there")).toBe(0.85);
    expect(await valueOf('{"score": 4, "outOf": 5}')).toBe(0.8);
    expect(await valueOf("0.7")).toBe(0.7);
  });
});

describe("parseJudgement", () => {
  it("refuses replies with no score or no scale instead of guessing", () => {
    // A bare "7" used to be read as 7/10, and {"value":4} clamped to 1.
    for (const reply of ["no idea", "Score: 7", '{"value":4}', "12/10", "3 or 0.5"]) {
      expect(() => parseJudgement(reply)).toThrow(JudgeParseError);
    }
  });

  it("an unparsable reply fails the score with its reason instead of a silent 0", async () => {
    const summary = await runEval({
      dataset: inlineDataset({ cases: [{ id: "1", input: "q" }] }),
      target: fnTarget({ fn: () => "a" }),
      scorers: [
        createLLMScorer({
          id: "j",
          judge: mockLLM([{ content: "Score: 7", finishReason: "stop" }]),
          prompt: () => "rate it",
        }),
      ],
    });
    const score = summary.caseResults[0]?.samples[0]?.scores[0];
    expect(score?.passed).toBe(false);
    expect(score?.reason).toContain("unparsable judge reply");
    expect(score?.reason).toContain("Score: 7");
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
