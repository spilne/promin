// ---------------------------------------------------------------------------
// defineEval — ergonomic entry over the seams. `data` accepts a bare case
// array; `target` accepts a bare function. Both are coerced to the seam
// types, so getting started is a few lines without losing seam purity.
// ---------------------------------------------------------------------------

import type { Clock } from "@promin/core";
import { inlineDataset } from "./datasets/inline.ts";
import { runEval } from "./runner.ts";
import { fnTarget } from "./targets/fn-target.ts";
import type {
  EvalCase,
  EvalDataset,
  EvalReporter,
  EvalRunSummary,
  EvalTarget,
  Scorer,
} from "./types.ts";

export interface DefineEvalConfig {
  /** A dataset, or a bare array of cases (wrapped via `inlineDataset`). */
  readonly data: EvalDataset | ReadonlyArray<EvalCase>;
  /** A target, or a bare `input -> output` function (wrapped via `fnTarget`). */
  readonly target: EvalTarget | ((input: unknown) => Promise<unknown> | unknown);
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly clock?: Clock;
}

export interface DefinedEval {
  readonly name: string;
  run(): Promise<EvalRunSummary>;
}

/** Assemble a runnable eval from loosely-typed config. */
export function defineEval(name: string, config: DefineEvalConfig): DefinedEval {
  const dataset = isEvalDataset(config.data) ? config.data : inlineDataset(config.data, name);
  const target = isEvalTarget(config.target)
    ? config.target
    : fnTarget(config.target, { id: name });
  return {
    name,
    run: () =>
      runEval({
        dataset,
        target,
        scorers: config.scorers,
        ...(config.reporters !== undefined && { reporters: config.reporters }),
        ...(config.samplesPerCase !== undefined && { samplesPerCase: config.samplesPerCase }),
        ...(config.concurrency !== undefined && { concurrency: config.concurrency }),
        ...(config.passThreshold !== undefined && { passThreshold: config.passThreshold }),
        ...(config.clock !== undefined && { clock: config.clock }),
      }),
  };
}

function isEvalDataset(value: DefineEvalConfig["data"]): value is EvalDataset {
  return !Array.isArray(value) && typeof (value as EvalDataset).cases === "function";
}

function isEvalTarget(value: DefineEvalConfig["target"]): value is EvalTarget {
  return (
    typeof value === "object" && value !== null && typeof (value as EvalTarget).run === "function"
  );
}
