// @promin/evals — agent evaluation framework.
//
// Four pluggable seams — datasets, targets, scorers, reporters — plus the
// orchestration (`runEval` / `runMatrix`), the ergonomic `defineEval`,
// persistent run history, versioned eval suites and promotion gates.

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
export {
  traceDataset,
  DEFAULT_TRACE_PAGE_SIZE,
  type TraceDatasetConfig,
} from "./lib/datasets/trace.ts";

// Scorers — deterministic.
export { exactMatch } from "./lib/scorers/exact-match.ts";
export { regexMatch, type RegexMatchConfig } from "./lib/scorers/regex-match.ts";
export { jsonMatch, type JsonMatchConfig } from "./lib/scorers/json-match.ts";
export {
  setMatch,
  normalizeSetItem,
  type SetMatchConfig,
  type SetMatchMetric,
} from "./lib/scorers/set-match.ts";

// Scorers — judgement.
export {
  createLLMScorer,
  parseJudgement,
  JudgeParseError,
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
export {
  retrievalRecall,
  retrievedSourceIds,
  type RetrievalRecallConfig,
} from "./lib/scorers/retrieval-recall.ts";
export { budget, type BudgetConfig } from "./lib/scorers/budget.ts";

// Targets.
export { toEvalOutput, type ToEvalOutputParams } from "./lib/targets/to-eval-output.ts";
export { fnTarget, type FnTargetConfig } from "./lib/targets/fn-target.ts";
export {
  agentTarget,
  defaultAgentInput,
  type AgentTargetConfig,
} from "./lib/targets/agent-target.ts";
export { recipeTarget, type RecipeTargetConfig } from "./lib/targets/recipe-target.ts";

// Reporters.
export { consoleReporter, type ConsoleReporterConfig } from "./lib/reporters/console.ts";
export { jsonReporter } from "./lib/reporters/json.ts";

// Orchestration.
export {
  runEval,
  runMatrix,
  runEvalCase,
  summarizeCases,
  totalsOf,
  resolveEvalOptions,
  EvalRunAbortedError,
  MAX_EVAL_CONCURRENCY,
  MAX_EVAL_SAMPLES_PER_CASE,
  type EvalCaseOptions,
  type EvalRunConfig,
  type EvalRunPersistence,
  type EvalMatrixConfig,
  type EvalMatrixResult,
  type ResolvedEvalOptions,
} from "./lib/runner.ts";
export { diffRuns } from "./lib/diff.ts";
export { defineEval, type DefineEvalConfig, type DefinedEval } from "./lib/define-eval.ts";

// Storage — persistent run history + datasets.
export {
  storedRunSummary,
  type EvalDatasetStore,
  type EvalRunQuery,
  type EvalRunRecord,
  type EvalRunStatus,
  type EvalRunStore,
  type EvalRunTotals,
  type FinishEvalRunParams,
  type StartEvalRunParams,
  type StoredEvalRun,
} from "./lib/storage/types.ts";
export { InMemoryEvalRunStore } from "./lib/storage/in-memory-run-store.ts";
export { InMemoryEvalDatasetStore } from "./lib/storage/in-memory-dataset-store.ts";
export { storedDataset } from "./lib/storage/stored-dataset.ts";
export { compareRuns } from "./lib/storage/compare-runs.ts";

// CI gate.
export {
  assertEvalSummary,
  evalSummaryFailures,
  type EvalAssertion,
} from "./lib/assert-eval-summary.ts";

// Live scoring — score an agent's own production runs, sampled.
export {
  DEFAULT_LIVE_MAX_CONCURRENT,
  type LiveScore,
  type LiveScoreSink,
  type LiveScoringConfig,
} from "./lib/live/types.ts";
export { liveScored, type LiveScoredParams } from "./lib/live/live-scored.ts";
export { metricsLiveSink, inMemoryLiveSink, InMemoryLiveScoreSink } from "./lib/live/sinks.ts";
export {
  liveScoredFromRecipe,
  type LiveScoringRecipeConfig,
  type LiveScoredFromRecipeParams,
} from "./lib/live/from-recipe.ts";

// Eval suites — serializable eval definitions in a VersionedRegistry.
export {
  SCORER_SPEC_KINDS,
  type DatasetSpec,
  type EvalSuite,
  type EvalSuiteFilter,
  type EvalSuiteInput,
  type EvalSuiteMetadata,
  type EvalSuiteRegistry,
  type RegisteredEvalSuite,
  type ScorerSpec,
} from "./lib/suite/types.ts";
export { EVAL_SUITE_KIND } from "./lib/suite/kind.ts";
export {
  resolveEvalSuite,
  runEvalSuite,
  type ResolveEvalSuiteDeps,
  type ResolvedEvalSuite,
} from "./lib/suite/resolve.ts";
export {
  InMemoryEvalSuiteRegistry,
  InMemoryEvalGateRegistry,
  type InMemoryEvalRegistryConfig,
} from "./lib/suite/registries.ts";

// Promotion gates.
export {
  EVAL_GATE_KIND,
  checkEvalGate,
  evalGateScore,
  type EvalGate,
  type EvalGateFilter,
  type EvalGateRegistry,
  type EvalGateVerdict,
  type RegisteredEvalGate,
} from "./lib/suite/gate.ts";
