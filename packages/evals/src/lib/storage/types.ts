// ---------------------------------------------------------------------------
// Storage seams — persistent run history + dataset storage.
//
// Both follow the promin storage pattern: an interface with InMemory /
// Sqlite / Postgres implementations and a shared conformance suite.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalRunSummary } from "../types.ts";

/** A persisted run — the summary plus storage metadata. */
export interface StoredEvalRun {
  /** Deterministic id — see `composeRunId`. */
  readonly runId: string;
  readonly summary: EvalRunSummary;
  /** Unix-ms timestamp when the run was persisted. */
  readonly savedAt: number;
}

/** Filter for `EvalRunStore.list`. Fields AND together. */
export interface EvalRunQuery {
  readonly targetId?: string;
  readonly targetVersion?: string;
  readonly datasetId?: string;
  /** Cap on rows returned (newest first). */
  readonly limit?: number;
}

/**
 * Persistent run history. A run is keyed on
 * `(targetId, targetVersion, datasetId, ranAt)` via `composeRunId`, so
 * re-saving the same run is idempotent.
 */
export interface EvalRunStore {
  /** Persist a completed run. Returns its `composeRunId`. */
  save(summary: EvalRunSummary): Promise<string>;
  get(runId: string): Promise<StoredEvalRun | null>;
  /** Stored runs, newest (`ranAt` desc) first. */
  list(query?: EvalRunQuery): Promise<StoredEvalRun[]>;
  delete(runId: string): Promise<void>;
}

/** Persistent dataset storage — named, replaceable case lists. */
export interface EvalDatasetStore {
  /** Create or replace a dataset's cases. */
  save(datasetId: string, cases: ReadonlyArray<EvalCase>): Promise<void>;
  get(datasetId: string): Promise<EvalCase[] | null>;
  /** All dataset ids, ascending. */
  list(): Promise<string[]>;
  delete(datasetId: string): Promise<void>;
}
