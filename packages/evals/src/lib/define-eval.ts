// ---------------------------------------------------------------------------
// defineEval — ergonomic entry over the seams. `data` accepts a bare case
// array; `target` accepts a bare function. Both are coerced to the seam
// types, so getting started is a few lines without losing seam purity.
// ---------------------------------------------------------------------------

import type { WallClock } from "@promin/workflow";
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
  /** Names the eval; also the dataset / target id when they are bare. */
  readonly name: string;
  /** A dataset, or a bare array of cases (wrapped via `inlineDataset`). */
  readonly data: EvalDataset | ReadonlyArray<EvalCase>;
  /** A target, or a bare `input -> output` function (wrapped via `fnTarget`). */
  readonly target: EvalTarget | ((input: unknown) => Promise<unknown> | unknown);
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly caseTimeoutMs?: number;
  readonly clock?: WallClock;
}

export interface DefinedEval {
  readonly name: string;
  run(opts?: { readonly signal?: AbortSignal }): Promise<EvalRunSummary>;
}

/** Assemble a runnable eval from loosely-typed config. */
export function defineEval(config: DefineEvalConfig): DefinedEval {
  const { name } = config;
  const dataset = isEvalDataset(config.data)
    ? config.data
    : inlineDataset({ cases: config.data, id: name });
  const target = isEvalTarget(config.target)
    ? config.target
    : fnTarget({
        fn: config.target,
        id: name,
        ...(config.clock !== undefined && { clock: config.clock }),
      });
  return {
    name,
    run: (opts) =>
      runEval({
        dataset,
        target,
        scorers: config.scorers,
        ...(config.reporters !== undefined && { reporters: config.reporters }),
        ...(config.samplesPerCase !== undefined && { samplesPerCase: config.samplesPerCase }),
        ...(config.concurrency !== undefined && { concurrency: config.concurrency }),
        ...(config.passThreshold !== undefined && { passThreshold: config.passThreshold }),
        ...(config.caseTimeoutMs !== undefined && { caseTimeoutMs: config.caseTimeoutMs }),
        ...(config.clock !== undefined && { clock: config.clock }),
        ...(opts?.signal !== undefined && { signal: opts.signal }),
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
