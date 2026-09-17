// ---------------------------------------------------------------------------
// @promin/evals — core type surface.
//
// Four pluggable seams, each `interface + default impl + documented swap
// point`:
//   - EvalDataset  — where cases come from
//   - EvalTarget   — the subject under test
//   - Scorer       — judges one output
//   - EvalReporter — result sink
//
// Every IO boundary is `unknown` so aggregation never special-cases a
// scorer kind. A Score is a smooth 0..1 `value`; the runner derives the
// boolean `passed` from a per-scorer `threshold` — the scorer never stamps
// pass/fail itself.
// ---------------------------------------------------------------------------

import type { AgentTrace } from "@promin/agent";

/** One test example. `input` / `expected` are `unknown` — a scorer casts locally. */
export interface EvalCase {
  readonly id: string;
  readonly input: unknown;
  readonly expected?: unknown;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Seam 1 — source of cases. */
export interface EvalDataset {
  readonly id: string;
  cases(): AsyncIterable<EvalCase>;
}

/** Token + latency + cost accounting for one target run. */
export interface EvalMetrics {
  readonly latencyMs: number;
  readonly inputTokens?: number;
  readonly outputTokens?: number;
  readonly costUsd?: number;
}

/**
 * What a target produced for one case. A target never throws — a failure
 * lands in `error`, with `output` left undefined.
 */
export interface EvalOutput {
  readonly output: unknown;
  readonly trace?: AgentTrace;
  readonly metrics: EvalMetrics;
  readonly error?: string;
}

/** Per-call options handed to a target. */
export interface EvalTargetRunOpts {
  readonly signal?: AbortSignal;
}

/** Seam 2 — the subject under test. `version` feeds run identity. */
export interface EvalTarget {
  readonly id: string;
  readonly version?: string;
  run(evalCase: EvalCase, opts?: EvalTargetRunOpts): Promise<EvalOutput>;
}

/** What a scorer emits — a smooth 0..1 `value`, never a boolean. */
export interface Score {
  readonly scorerId: string;
  /** 0..1. Uniform across every scorer kind so aggregation stays generic. */
  readonly value: number;
  /** Human-readable rationale — surfaced by reporters and the dashboard. */
  readonly reason?: string;
  /** Short categorical tag (e.g. a judge verdict). */
  readonly label?: string;
}

/**
 * Input handed to `Scorer.score`. Deliberately decoupled from `EvalCase`
 * so the same scorer works in offline eval AND in live production scoring,
 * where there is no dataset case behind the output.
 */
export interface ScorerInput {
  readonly input: unknown;
  readonly expected?: unknown;
  readonly output: EvalOutput;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Seam 3 — judges one output. */
export interface Scorer {
  readonly id: string;
  /** Pass cutoff on `value`. Default 1. The runner derives `passed = value >= threshold`. */
  readonly threshold?: number;
  /** When false, a failing score does not fail the case. Default true. */
  readonly required?: boolean;
  score(input: ScorerInput): Promise<Score>;
}

/** A Score with the runner's derived verdict attached. */
export interface ScoredResult extends Score {
  readonly passed: boolean;
  readonly threshold: number;
  readonly required: boolean;
}

/** One scored run of one case (`samplesPerCase` produces several). */
export interface EvalSample {
  readonly sampleIndex: number;
  readonly output: EvalOutput;
  readonly scores: ReadonlyArray<ScoredResult>;
  /** True when every required scorer passed. */
  readonly passed: boolean;
}

/** All samples of one case, aggregated into a pass-rate. */
export interface EvalCaseResult {
  readonly caseId: string;
  readonly samples: ReadonlyArray<EvalSample>;
  /**
   * Fraction of samples where `passed` is true — the honest unit for
   * nondeterministic LLM output.
   */
  readonly passRate: number;
  /** True when `passRate` is at least the run's `passThreshold`. */
  readonly passed: boolean;
}

/** Per-scorer roll-up across every sample of a run. */
export interface PerScorerSummary {
  readonly meanValue: number;
  readonly passRate: number;
}

/** The result of one `runEval`. */
export interface EvalRunSummary {
  readonly targetId: string;
  readonly targetVersion?: string;
  readonly datasetId: string;
  readonly ranAt: number;
  readonly totalCases: number;
  /** Fraction of cases that passed. */
  readonly passRate: number;
  readonly perScorer: Readonly<Record<string, PerScorerSummary>>;
  readonly samplesPerCase: number;
  readonly caseResults: ReadonlyArray<EvalCaseResult>;
}

/** Seam 4 — result sink. */
export interface EvalReporter {
  onCaseResult?(result: EvalCaseResult): void | Promise<void>;
  onComplete?(summary: EvalRunSummary): void | Promise<void>;
}

/** Per-case verdict shift between two runs. */
export type CaseDiffStatus = "improved" | "regressed" | "unchanged";

export interface CaseDiff {
  readonly caseId: string;
  readonly baselinePassed: boolean;
  readonly candidatePassed: boolean;
  readonly status: CaseDiffStatus;
}

/**
 * A baseline-vs-candidate comparison. One type, two producers: `runMatrix`
 * (ephemeral, in-memory) and `compareRuns` (P3, from persisted runs).
 */
export interface RunDiff {
  readonly baseline: EvalRunSummary;
  readonly candidate: EvalRunSummary;
  readonly passRateDelta: number;
  readonly perScorerDelta: Readonly<Record<string, number>>;
  readonly perCase: ReadonlyArray<CaseDiff>;
  /** Case ids that passed in `baseline` but fail in `candidate`. */
  readonly regressions: ReadonlyArray<string>;
}
