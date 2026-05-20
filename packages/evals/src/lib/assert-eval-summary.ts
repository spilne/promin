// ---------------------------------------------------------------------------
// assertEvalSummary — a CI-gate helper. Throws a readable error when a run
// misses its quality bar; designed to be called inside a `bun:test` block.
// ---------------------------------------------------------------------------

import type { EvalRunSummary } from "./types.ts";

export interface EvalAssertion {
  /** Minimum overall case pass-rate. */
  readonly minPassRate?: number;
  /** Per-scorer minimum pass-rate, keyed by scorer id. */
  readonly minPerScorer?: Readonly<Record<string, number>>;
  /** Fail when the run's pass-rate drops more than `maxPassRateDrop` below `baseline`. */
  readonly maxRegression?: {
    readonly baseline: EvalRunSummary;
    readonly maxPassRateDrop: number;
  };
}

/**
 * Throw when `summary` misses any configured bar. A no-op when every check
 * passes. Call it inside a `bun:test` `it(...)` to gate a deploy on eval
 * quality:
 *
 *     it("support-bot meets the bar", async () => {
 *       assertEvalSummary(await runEval(config), { minPassRate: 0.9 });
 *     });
 */
export function assertEvalSummary(summary: EvalRunSummary, assertion: EvalAssertion): void {
  const failures: string[] = [];

  if (assertion.minPassRate !== undefined && summary.passRate < assertion.minPassRate) {
    failures.push(`passRate ${summary.passRate.toFixed(3)} < required ${assertion.minPassRate}`);
  }

  if (assertion.minPerScorer !== undefined) {
    for (const [scorerId, min] of Object.entries(assertion.minPerScorer)) {
      const actual = summary.perScorer[scorerId]?.passRate;
      if (actual === undefined) {
        failures.push(`scorer "${scorerId}" has no result in this run`);
      } else if (actual < min) {
        failures.push(`scorer "${scorerId}" passRate ${actual.toFixed(3)} < required ${min}`);
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

  if (failures.length > 0) {
    throw new Error(`assertEvalSummary failed:\n  - ${failures.join("\n  - ")}`);
  }
}
