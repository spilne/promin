import { describe, expect, it } from "bun:test";
import { assertEvalSummary, evalSummaryFailures } from "../assert-eval-summary.ts";
import type { EvalRunSummary } from "../types.ts";

function summary(over: Partial<EvalRunSummary> = {}): EvalRunSummary {
  return {
    targetId: "t",
    datasetId: "d",
    ranAt: 0,
    totalCases: 1,
    passRate: 1,
    perScorer: {},
    samplesPerCase: 1,
    caseResults: [],
    ...over,
  };
}

describe("assertEvalSummary", () => {
  it("passes when every bar is met", () => {
    expect(() =>
      assertEvalSummary({ summary: summary({ passRate: 0.95 }), assertion: { minPassRate: 0.9 } }),
    ).not.toThrow();
  });

  it("throws when passRate is below the minimum", () => {
    expect(() =>
      assertEvalSummary({ summary: summary({ passRate: 0.5 }), assertion: { minPassRate: 0.9 } }),
    ).toThrow("passRate");
  });

  it("throws on a missing per-scorer result", () => {
    expect(() =>
      assertEvalSummary({ summary: summary(), assertion: { minPerScorer: { exactMatch: 0.8 } } }),
    ).toThrow("exactMatch");
  });

  it("throws on a per-scorer pass-rate below the minimum", () => {
    const withScorer = summary({ perScorer: { exactMatch: { meanValue: 0.4, passRate: 0.4 } } });
    expect(() =>
      assertEvalSummary({ summary: withScorer, assertion: { minPerScorer: { exactMatch: 0.8 } } }),
    ).toThrow("exactMatch");
  });

  it("checks a per-scorer mean value", () => {
    const withScorer = summary({ perScorer: { recall: { meanValue: 0.75, passRate: 1 } } });
    expect(
      evalSummaryFailures({
        summary: withScorer,
        assertion: { minMeanPerScorer: { recall: 0.8 } },
      }),
    ).toEqual(['scorer "recall" meanValue 0.750 < required 0.8']);
    expect(
      evalSummaryFailures({
        summary: withScorer,
        assertion: { minMeanPerScorer: { recall: 0.7 } },
      }),
    ).toEqual([]);
  });

  it("throws on a regression beyond the allowed drop", () => {
    const baseline = summary({ passRate: 1 });
    const candidate = summary({ passRate: 0.7 });
    expect(() =>
      assertEvalSummary({
        summary: candidate,
        assertion: { maxRegression: { baseline, maxPassRateDrop: 0.1 } },
      }),
    ).toThrow("regressed");
  });

  it("allows a regression within tolerance", () => {
    const baseline = summary({ passRate: 1 });
    const candidate = summary({ passRate: 0.95 });
    expect(() =>
      assertEvalSummary({
        summary: candidate,
        assertion: { maxRegression: { baseline, maxPassRateDrop: 0.1 } },
      }),
    ).not.toThrow();
  });
});
