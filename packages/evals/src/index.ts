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
export { traceDataset, type TraceDatasetConfig } from "./lib/datasets/trace.ts";

// Scorers — deterministic.
export { exactMatch } from "./lib/scorers/exact-match.ts";
export { regexMatch, type RegexMatchConfig } from "./lib/scorers/regex-match.ts";
export { jsonMatch, type JsonMatchConfig } from "./lib/scorers/json-match.ts";

// Scorers — judgement.
export {
  createLLMScorer,
  type JudgePrompt,
  type LLMScorerConfig,
  type ParsedJudgement,
} from "./lib/scorers/llm-scorer.ts";
export { llmJudge, type LLMJudgeConfig } from "./lib/scorers/llm-judge.ts";
export {
  toolTrajectory,
  toolCallNames,
  type ToolTrajectoryConfig,
  type TrajectoryMode,
} from "./lib/scorers/tool-trajectory.ts";
export { budget, type BudgetConfig } from "./lib/scorers/budget.ts";

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

// Storage — persistent run history + datasets.
export type {
  EvalDatasetStore,
  EvalRunQuery,
  EvalRunStore,
  StoredEvalRun,
} from "./lib/storage/types.ts";
export { composeRunId } from "./lib/storage/run-id.ts";
export {
  InMemoryEvalRunStore,
  type InMemoryEvalRunStoreConfig,
} from "./lib/storage/in-memory-run-store.ts";
export { InMemoryEvalDatasetStore } from "./lib/storage/in-memory-dataset-store.ts";
export { storedDataset } from "./lib/storage/stored-dataset.ts";
export { compareRuns } from "./lib/storage/compare-runs.ts";

// CI gate.
export { assertEvalSummary, type EvalAssertion } from "./lib/assert-eval-summary.ts";

// Live scoring — score an agent's own production runs, sampled.
export type { LiveScore, LiveScoreSink, LiveScoringConfig } from "./lib/live/types.ts";
export { liveScored } from "./lib/live/live-scored.ts";
export { metricsLiveSink, inMemoryLiveSink, InMemoryLiveScoreSink } from "./lib/live/sinks.ts";
export {
  liveScoredFromRecipe,
  type LiveScoringRecipeConfig,
  type LiveScoredFromRecipeDeps,
} from "./lib/live/from-recipe.ts";
