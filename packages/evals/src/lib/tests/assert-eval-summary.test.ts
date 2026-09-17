import { describe, expect, it } from "bun:test";
import { assertEvalSummary } from "../assert-eval-summary.ts";
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
      assertEvalSummary(summary({ passRate: 0.95 }), { minPassRate: 0.9 }),
    ).not.toThrow();
  });

  it("throws when passRate is below the minimum", () => {
    expect(() => assertEvalSummary(summary({ passRate: 0.5 }), { minPassRate: 0.9 })).toThrow(
      "passRate",
    );
  });

  it("throws on a missing per-scorer result", () => {
    expect(() => assertEvalSummary(summary(), { minPerScorer: { exactMatch: 0.8 } })).toThrow(
      "exactMatch",
    );
  });

  it("throws on a per-scorer pass-rate below the minimum", () => {
    const withScorer = summary({ perScorer: { exactMatch: { meanValue: 0.4, passRate: 0.4 } } });
    expect(() => assertEvalSummary(withScorer, { minPerScorer: { exactMatch: 0.8 } })).toThrow(
      "exactMatch",
    );
  });

  it("throws on a regression beyond the allowed drop", () => {
    const baseline = summary({ passRate: 1 });
    const candidate = summary({ passRate: 0.7 });
    expect(() =>
      assertEvalSummary(candidate, { maxRegression: { baseline, maxPassRateDrop: 0.1 } }),
    ).toThrow("regressed");
  });

  it("allows a regression within tolerance", () => {
    const baseline = summary({ passRate: 1 });
    const candidate = summary({ passRate: 0.95 });
    expect(() =>
      assertEvalSummary(candidate, { maxRegression: { baseline, maxPassRateDrop: 0.1 } }),
    ).not.toThrow();
  });
});
