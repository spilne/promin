// ---------------------------------------------------------------------------
// InMemoryEvalRunStore — the default, process-local EvalRunStore.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { EvalRunSummary } from "../types.ts";
import { composeRunId } from "./run-id.ts";
import type { EvalRunQuery, EvalRunStore, StoredEvalRun } from "./types.ts";

export interface InMemoryEvalRunStoreConfig {
  /** Time source for `savedAt`. Default `SystemClock`. */
  readonly clock?: Clock;
}

export class InMemoryEvalRunStore implements EvalRunStore {
  private readonly runs = new Map<string, StoredEvalRun>();
  private readonly clock: Clock;

  constructor(config: InMemoryEvalRunStoreConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  async save(summary: EvalRunSummary): Promise<string> {
    const runId = composeRunId(summary);
    this.runs.set(runId, { runId, summary, savedAt: this.clock.currentTimeMs() });
    return runId;
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    return this.runs.get(runId) ?? null;
  }

  async list(query: EvalRunQuery = {}): Promise<StoredEvalRun[]> {
    let rows = [...this.runs.values()];
    if (query.targetId !== undefined) {
      rows = rows.filter((r) => r.summary.targetId === query.targetId);
    }
    if (query.targetVersion !== undefined) {
      rows = rows.filter((r) => r.summary.targetVersion === query.targetVersion);
    }
    if (query.datasetId !== undefined) {
      rows = rows.filter((r) => r.summary.datasetId === query.datasetId);
    }
    rows.sort((a, b) => b.summary.ranAt - a.summary.ranAt);
    return query.limit !== undefined ? rows.slice(0, query.limit) : rows;
  }

  async delete(runId: string): Promise<void> {
    this.runs.delete(runId);
  }
}
