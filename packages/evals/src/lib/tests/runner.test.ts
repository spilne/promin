import { describe, expect, it } from "bun:test";
import { FakeClock } from "@promin/core";
import { inlineDataset } from "../datasets/inline.ts";
import { diffRuns } from "../diff.ts";
import { runEval, runMatrix } from "../runner.ts";
import { exactMatch } from "../scorers/exact-match.ts";
import { fnTarget } from "../targets/fn-target.ts";
import type { EvalCase } from "../types.ts";

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const cases: EvalCase[] = [
  { id: "1", input: "a", expected: "A" },
  { id: "2", input: "b", expected: "B" },
];

describe("runEval", () => {
  it("scores every case and stamps ranAt via the clock", async () => {
    const summary = await runEval({
      dataset: inlineDataset(cases, "ds"),
      target: fnTarget((input) => String(input).toUpperCase()),
      scorers: [exactMatch],
      clock: FakeClock.create(5000),
    });
    expect(summary.totalCases).toBe(2);
    expect(summary.passRate).toBe(1);
    expect(summary.ranAt).toBe(5000);
    expect(summary.datasetId).toBe("ds");
    expect(summary.perScorer.exactMatch?.passRate).toBe(1);
  });

  it("fails a case when the threshold-derived verdict fails", async () => {
    const summary = await runEval({
      dataset: inlineDataset([{ id: "1", input: "a", expected: "WRONG" }], "ds"),
      target: fnTarget((input) => String(input)),
      scorers: [exactMatch],
    });
    expect(summary.passRate).toBe(0);
    expect(summary.caseResults[0]?.passed).toBe(false);
  });

  it("reports a per-case pass-rate across samples and honours passThreshold", async () => {
    let calls = 0;
    const target = fnTarget(() => {
      calls += 1;
      return calls % 2 === 0 ? "good" : "bad";
    });
    const dataset = inlineDataset([{ id: "1", input: "x", expected: "good" }], "ds");

    const strict = await runEval({ dataset, target, scorers: [exactMatch], samplesPerCase: 2 });
    expect(strict.caseResults[0]?.passRate).toBe(0.5);
    expect(strict.caseResults[0]?.passed).toBe(false);

    calls = 0;
    const lenient = await runEval({
      dataset,
      target,
      scorers: [exactMatch],
      samplesPerCase: 2,
      passThreshold: 0.5,
    });
    expect(lenient.caseResults[0]?.passed).toBe(true);
  });

  it("bounds concurrency", async () => {
    let active = 0;
    let peak = 0;
    const target = fnTarget(async () => {
      active += 1;
      peak = Math.max(peak, active);
      await sleep(5);
      active -= 1;
      return "x";
    });
    const many = Array.from({ length: 8 }, (_unused, i): EvalCase => ({ id: String(i), input: i }));
    await runEval({ dataset: inlineDataset(many, "ds"), target, scorers: [], concurrency: 3 });
    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBeGreaterThan(1);
  });

  it("scores a failed target run as 0 without throwing", async () => {
    const summary = await runEval({
      dataset: inlineDataset([{ id: "1", input: "a", expected: "A" }], "ds"),
      target: fnTarget(() => {
        throw new Error("target boom");
      }),
      scorers: [exactMatch],
    });
    expect(summary.passRate).toBe(0);
    const score = summary.caseResults[0]?.samples[0]?.scores[0];
    expect(score?.value).toBe(0);
    expect(score?.reason).toContain("target boom");
  });
});

describe("runMatrix + diffRuns", () => {
  it("diffs each target against the first and flags regressions", async () => {
    const { summaries, diffs } = await runMatrix({
      dataset: inlineDataset(cases, "ds"),
      targets: [
        fnTarget((input) => String(input).toUpperCase(), { id: "good" }),
        fnTarget(() => "nope", { id: "bad" }),
      ],
      scorers: [exactMatch],
    });
    expect(summaries.length).toBe(2);
    expect(diffs.length).toBe(1);
    expect(diffs[0]?.passRateDelta).toBe(-1);
    expect([...(diffs[0]?.regressions ?? [])].sort()).toEqual(["1", "2"]);
  });

  it("marks an improved case", async () => {
    const dataset = inlineDataset([{ id: "1", input: "a", expected: "A" }], "ds");
    const baseline = await runEval({
      dataset,
      target: fnTarget(() => "x"),
      scorers: [exactMatch],
    });
    const candidate = await runEval({
      dataset,
      target: fnTarget((input) => String(input).toUpperCase()),
      scorers: [exactMatch],
    });
    const diff = diffRuns(baseline, candidate);
    expect(diff.perCase[0]?.status).toBe("improved");
    expect(diff.regressions).toEqual([]);
  });
});
