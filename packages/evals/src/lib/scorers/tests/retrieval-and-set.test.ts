import { describe, expect, it } from "bun:test";
import { buildAgentTrace, type Message } from "@promin/agent";
import type { EvalOutput } from "../../types.ts";
import { retrievalRecall } from "../retrieval-recall.ts";
import { setMatch } from "../set-match.ts";

/** A run that called `search` once per entry, each result carrying those sources. */
function retrieved(params: {
  calls: ReadonlyArray<{ name: string; sources: ReadonlyArray<string> }>;
}): EvalOutput {
  const messages: Message[] = [
    { role: "user", content: "question" },
    {
      role: "assistant",
      content: null,
      toolCalls: params.calls.map((call, i) => ({ id: `t${i}`, name: call.name, input: {} })),
    },
    ...params.calls.map((call, i) => ({
      role: "tool" as const,
      toolCallId: `t${i}`,
      content: "chunks",
      metadata: { sources: call.sources.map((id) => ({ id, title: id.toUpperCase() })) },
    })),
    { role: "assistant", content: "answer" },
  ];
  return { output: "answer", metrics: { latencyMs: 0 }, trace: buildAgentTrace(messages) };
}

describe("retrievalRecall", () => {
  it("scores the fraction of expected sources retrieved across calls", async () => {
    const output = retrieved({
      calls: [
        { name: "search", sources: ["oncall-runbook"] },
        { name: "search", sources: ["security-policy", "deploy-pipeline"] },
      ],
    });
    const full = await retrievalRecall().score({
      input: "q",
      output,
      expected: { sources: ["oncall-runbook", "deploy-pipeline"] },
    });
    expect(full.value).toBe(1);
    const half = await retrievalRecall().score({
      input: "q",
      output,
      expected: { sources: ["oncall-runbook", "code-review-guide"] },
    });
    expect(half.value).toBe(0.5);
    expect(half.reason).toContain("code-review-guide");
  });

  it("only counts the named tools", async () => {
    const output = retrieved({
      calls: [
        { name: "search", sources: ["a"] },
        { name: "web", sources: ["b"] },
      ],
    });
    const score = await retrievalRecall({ toolNames: ["web"] }).score({
      input: "q",
      output,
      expected: { sources: ["a", "b"] },
    });
    expect(score.value).toBe(0.5);
  });

  it("needs expected.sources", async () => {
    await expect(
      retrievalRecall().score({ input: "q", output: retrieved({ calls: [] }), expected: "x" }),
    ).rejects.toThrow("sources");
  });
});

describe("setMatch", () => {
  const out = (value: unknown): EvalOutput => ({ output: value, metrics: { latencyMs: 0 } });

  it("computes precision, recall and f1 on normalized items", async () => {
    const output = out(["User lives in Kyiv.", "user is on the SRE team", "User likes cats"]);
    const expected = ["user lives in kyiv", "User is on the SRE team", "User's name is Ana"];
    const precision = await setMatch({ metric: "precision" }).score({
      input: "q",
      output,
      expected,
    });
    expect(precision.value).toBeCloseTo(2 / 3);
    expect(precision.reason).toContain("user likes cats");
    const recall = await setMatch({ metric: "recall" }).score({ input: "q", output, expected });
    expect(recall.value).toBeCloseTo(2 / 3);
    const f1 = await setMatch().score({ input: "q", output, expected });
    expect(f1.value).toBeCloseTo(2 / 3);
  });

  it("counts a repeated item as a precision miss", async () => {
    const score = await setMatch({ metric: "precision" }).score({
      input: "q",
      output: out(["User lives in Kyiv", "user lives in kyiv."]),
      expected: ["User lives in Kyiv"],
    });
    expect(score.value).toBe(0.5);
    const recall = await setMatch({ metric: "recall" }).score({
      input: "q",
      output: out(["User lives in Kyiv", "user lives in kyiv."]),
      expected: ["User lives in Kyiv"],
    });
    expect(recall.value).toBe(1);
  });

  it("an empty output is perfect only against an empty expectation", async () => {
    expect((await setMatch().score({ input: "q", output: out([]), expected: [] })).value).toBe(1);
    expect((await setMatch().score({ input: "q", output: out([]), expected: ["x"] })).value).toBe(
      0,
    );
  });

  it("refuses non-string lists", async () => {
    await expect(setMatch().score({ input: "q", output: out("x"), expected: [] })).rejects.toThrow(
      "output",
    );
  });
});
