// ---------------------------------------------------------------------------
// Storage seams — persistent run history + dataset storage.
//
// Both follow the promin storage pattern: an interface with InMemory /
// Sqlite / Postgres implementations and a shared conformance suite.
//
// A run is written as it happens: `start` creates it (`running`),
// `recordCase` adds each case result as the case finishes, and `finish`
// ends it with its totals. A reader sees a running run's progress, and a
// run whose process died keeps the cases it had finished.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalCaseResult, EvalRunSummary, PerScorerSummary } from "../types.ts";

/** Lifecycle of a stored run. */
export type EvalRunStatus = "running" | "completed" | "failed" | "cancelled";

/** Roll-up of a finished run's case results. */
export interface EvalRunTotals {
  /** Fraction of cases that passed. */
  readonly passRate: number;
  readonly perScorer: Readonly<Record<string, PerScorerSummary>>;
  /** Cases the totals cover. */
  readonly scoredCases: number;
}

/** A stored run without its case results — what `list` answers. */
export interface EvalRunRecord {
  readonly runId: string;
  readonly status: EvalRunStatus;
  readonly targetId: string;
  readonly targetVersion?: string;
  readonly datasetId: string;
  /** The eval suite the run measured, when it ran one. */
  readonly suiteId?: string;
  readonly suiteVersion?: string;
  /** Tenant namespace the run acted in (hosts authorize readers by it). */
  readonly namespaceId?: string;
  readonly samplesPerCase: number;
  /** Cases in the dataset. */
  readonly totalCases: number;
  /** Cases recorded so far. */
  readonly completedCases: number;
  /** Epoch ms, the writer's clock. */
  readonly startedAt: number;
  readonly finishedAt?: number;
  /** Set by `finish`. */
  readonly totals?: EvalRunTotals;
  /** Why a `failed` run failed. */
  readonly error?: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** A stored run with every case result recorded so far, in dataset order. */
export interface StoredEvalRun extends EvalRunRecord {
  readonly caseResults: ReadonlyArray<EvalCaseResult>;
}

/** Input to `EvalRunStore.start`. */
export interface StartEvalRunParams {
  readonly runId: string;
  readonly targetId: string;
  readonly targetVersion?: string;
  readonly datasetId: string;
  readonly suiteId?: string;
  readonly suiteVersion?: string;
  readonly namespaceId?: string;
  readonly samplesPerCase: number;
  readonly totalCases: number;
  readonly startedAt: number;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Input to `EvalRunStore.finish`. */
export interface FinishEvalRunParams {
  readonly runId: string;
  readonly status: Exclude<EvalRunStatus, "running">;
  readonly finishedAt: number;
  readonly totals?: EvalRunTotals;
  readonly error?: string;
}

/** Filter for `EvalRunStore.list`. Fields AND together. */
export interface EvalRunQuery {
  readonly targetId?: string;
  readonly targetVersion?: string;
  readonly datasetId?: string;
  readonly suiteId?: string;
  readonly suiteVersion?: string;
  readonly namespaceId?: string;
  readonly status?: EvalRunStatus;
  /** Cap on rows returned (newest first). */
  readonly limit?: number;
}

/** Persistent run history, written while a run is in flight. */
export interface EvalRunStore {
  /**
   * Create a `running` run. Idempotent: a run that exists already is left
   * as it is (a resumed run keeps the cases it recorded).
   */
  start(params: StartEvalRunParams): Promise<void>;
  /**
   * Record one finished case at its dataset position. Recording a case
   * again replaces it. A run that has ended is left as it is (a straggler
   * of a cancelled run never lands). Throws when the run does not exist.
   */
  recordCase(params: {
    readonly runId: string;
    readonly caseIndex: number;
    readonly result: EvalCaseResult;
  }): Promise<void>;
  /**
   * End a running run. A run that has ended already is left as it is, so
   * the first ending wins (a cancel is not overwritten by a late
   * completion). Throws when the run does not exist.
   */
  finish(params: FinishEvalRunParams): Promise<void>;
  get(runId: string): Promise<StoredEvalRun | null>;
  /** Stored runs, newest (`startedAt` desc) first, without their case results. */
  list(query?: EvalRunQuery): Promise<EvalRunRecord[]>;
  /** Remove a run and its case results. */
  delete(runId: string): Promise<void>;
}

/** Persistent dataset storage — named, replaceable case lists. */
export interface EvalDatasetStore {
  /** Create or replace a dataset's cases. */
  save(params: {
    readonly datasetId: string;
    readonly cases: ReadonlyArray<EvalCase>;
  }): Promise<void>;
  get(datasetId: string): Promise<EvalCase[] | null>;
  /** All dataset ids, ascending. */
  list(): Promise<string[]>;
  delete(datasetId: string): Promise<void>;
}

/** A stored run as the `EvalRunSummary` `diffRuns` and reporters take. */
export function storedRunSummary(run: StoredEvalRun): EvalRunSummary {
  return {
    runId: run.runId,
    targetId: run.targetId,
    ...(run.targetVersion !== undefined && { targetVersion: run.targetVersion }),
    datasetId: run.datasetId,
    ranAt: run.startedAt,
    totalCases: run.totalCases,
    passRate: run.totals?.passRate ?? 0,
    perScorer: run.totals?.perScorer ?? {},
    samplesPerCase: run.samplesPerCase,
    caseResults: run.caseResults,
  };
}
