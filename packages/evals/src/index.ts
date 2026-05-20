// @promin/evals — agent evaluation framework.
//
// Four pluggable seams — datasets, targets, scorers, reporters — plus the
// orchestration (`runEval` / `runMatrix`) and the ergonomic `defineEval`.

// Core types — the four seams + result shapes.
export type {
  CaseDiff,
  CaseDiffStatus,
  EvalCase,
  EvalCaseResult,
  EvalDataset,
  EvalMetrics,
  EvalOutput,
  EvalReporter,
  EvalRunSummary,
  EvalSample,
  EvalTarget,
  EvalTargetRunOpts,
  PerScorerSummary,
  RunDiff,
  Score,
  ScoredResult,
  Scorer,
  ScorerInput,
} from "./lib/types.ts";

// Datasets.
export { inlineDataset } from "./lib/datasets/inline.ts";
export { jsonlDataset } from "./lib/datasets/jsonl.ts";

// Scorers.
export { exactMatch } from "./lib/scorers/exact-match.ts";
export { regexMatch, type RegexMatchConfig } from "./lib/scorers/regex-match.ts";
export { jsonMatch, type JsonMatchConfig } from "./lib/scorers/json-match.ts";

// Targets.
export { toEvalOutput, type ToEvalOutputOpts } from "./lib/targets/to-eval-output.ts";
export { fnTarget, type FnTargetConfig } from "./lib/targets/fn-target.ts";
export { recipeTarget, type RecipeTargetConfig } from "./lib/targets/recipe-target.ts";

// Reporters.
export { consoleReporter, type ConsoleReporterConfig } from "./lib/reporters/console.ts";
export { jsonReporter } from "./lib/reporters/json.ts";

// Orchestration.
export {
  runEval,
  runMatrix,
  type EvalRunConfig,
  type EvalMatrixConfig,
  type EvalMatrixResult,
} from "./lib/runner.ts";
export { diffRuns } from "./lib/diff.ts";
export { defineEval, type DefineEvalConfig, type DefinedEval } from "./lib/define-eval.ts";
