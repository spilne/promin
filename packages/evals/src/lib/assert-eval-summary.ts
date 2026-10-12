// ---------------------------------------------------------------------------
// assertEvalSummary — a CI-gate helper. Throws a readable error when a run
// misses its quality bar; call it inside a `bun:test` block or a CI script.
// ---------------------------------------------------------------------------

import type { EvalRunSummary } from "./types.ts";

export interface EvalAssertion {
  /** Minimum overall case pass-rate. */
  readonly minPassRate?: number;
  /** Per-scorer minimum pass-rate, keyed by scorer id. */
  readonly minPerScorer?: Readonly<Record<string, number>>;
  /** Per-scorer minimum mean value, keyed by scorer id. */
  readonly minMeanPerScorer?: Readonly<Record<string, number>>;
  /** Fail when the run's pass-rate drops more than `maxPassRateDrop` below `baseline`. */
  readonly maxRegression?: {
    readonly baseline: EvalRunSummary;
    readonly maxPassRateDrop: number;
  };
}

/** The bars `summary` misses, one readable line each; empty when it meets them all. */
export function evalSummaryFailures(params: {
  readonly summary: EvalRunSummary;
  readonly assertion: EvalAssertion;
}): string[] {
  const { summary, assertion } = params;
  const failures: string[] = [];

  if (assertion.minPassRate !== undefined && summary.passRate < assertion.minPassRate) {
    failures.push(`passRate ${summary.passRate.toFixed(3)} < required ${assertion.minPassRate}`);
  }

  const perScorerBars: Array<{
    bars: Readonly<Record<string, number>> | undefined;
    field: "passRate" | "meanValue";
  }> = [
    { bars: assertion.minPerScorer, field: "passRate" },
    { bars: assertion.minMeanPerScorer, field: "meanValue" },
  ];
  for (const { bars, field } of perScorerBars) {
    for (const [scorerId, min] of Object.entries(bars ?? {})) {
      const actual = summary.perScorer[scorerId]?.[field];
      if (actual === undefined) {
        failures.push(`scorer "${scorerId}" has no result in this run`);
      } else if (actual < min) {
        failures.push(`scorer "${scorerId}" ${field} ${actual.toFixed(3)} < required ${min}`);
      }
    }
  }

  if (assertion.maxRegression !== undefined) {
    const { baseline, maxPassRateDrop } = assertion.maxRegression;
    const drop = baseline.passRate - summary.passRate;
    if (drop > maxPassRateDrop) {
      failures.push(
        `passRate regressed by ${drop.toFixed(3)} vs baseline (max allowed ${maxPassRateDrop})`,
      );
    }
  }
  return failures;
}

/**
 * Throw when `summary` misses any configured bar. A no-op when every check
 * passes:
 *
 *     it("support-bot meets the bar", async () => {
 *       assertEvalSummary({ summary: await runEval(config), assertion: { minPassRate: 0.9 } });
 *     });
 */
export function assertEvalSummary(params: {
  readonly summary: EvalRunSummary;
  readonly assertion: EvalAssertion;
}): void {
  const failures = evalSummaryFailures(params);
  if (failures.length > 0) {
    throw new Error(`assertEvalSummary failed:\n  - ${failures.join("\n  - ")}`);
  }
}
