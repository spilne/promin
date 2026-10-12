// ---------------------------------------------------------------------------
// InMemoryEvalRunStore — the default, process-local EvalRunStore.
// ---------------------------------------------------------------------------

import type { EvalCaseResult } from "../types.ts";
import type {
  EvalRunQuery,
  EvalRunRecord,
  EvalRunStore,
  FinishEvalRunParams,
  StartEvalRunParams,
  StoredEvalRun,
} from "./types.ts";

interface Row {
  record: Omit<EvalRunRecord, "completedCases">;
  cases: Map<number, EvalCaseResult>;
}

export class InMemoryEvalRunStore implements EvalRunStore {
  private readonly runs = new Map<string, Row>();

  async start(params: StartEvalRunParams): Promise<void> {
    if (this.runs.has(params.runId)) return;
    this.runs.set(params.runId, {
      record: {
        runId: params.runId,
        status: "running",
        targetId: params.targetId,
        ...(params.targetVersion !== undefined && { targetVersion: params.targetVersion }),
        datasetId: params.datasetId,
        ...(params.suiteId !== undefined && { suiteId: params.suiteId }),
        ...(params.suiteVersion !== undefined && { suiteVersion: params.suiteVersion }),
        ...(params.namespaceId !== undefined && { namespaceId: params.namespaceId }),
        samplesPerCase: params.samplesPerCase,
        totalCases: params.totalCases,
        startedAt: params.startedAt,
        ...(params.metadata !== undefined && { metadata: params.metadata }),
      },
      cases: new Map(),
    });
  }

  async recordCase(params: {
    readonly runId: string;
    readonly caseIndex: number;
    readonly result: EvalCaseResult;
  }): Promise<void> {
    const row = this.require(params.runId);
    if (row.record.status !== "running") return;
    row.cases.set(params.caseIndex, params.result);
  }

  async finish(params: FinishEvalRunParams): Promise<void> {
    const row = this.require(params.runId);
    if (row.record.status !== "running") return;
    row.record = {
      ...row.record,
      status: params.status,
      finishedAt: params.finishedAt,
      ...(params.totals !== undefined && { totals: params.totals }),
      ...(params.error !== undefined && { error: params.error }),
    };
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    const row = this.runs.get(runId);
    if (row === undefined) return null;
    const caseResults = [...row.cases.entries()]
      .sort(([a], [b]) => a - b)
      .map(([, result]) => result);
    return { ...recordOf(row), caseResults };
  }

  async list(query: EvalRunQuery = {}): Promise<EvalRunRecord[]> {
    const rows = [...this.runs.values()]
      .map(recordOf)
      .filter(
        (r) =>
          (query.targetId === undefined || r.targetId === query.targetId) &&
          (query.targetVersion === undefined || r.targetVersion === query.targetVersion) &&
          (query.datasetId === undefined || r.datasetId === query.datasetId) &&
          (query.suiteId === undefined || r.suiteId === query.suiteId) &&
          (query.suiteVersion === undefined || r.suiteVersion === query.suiteVersion) &&
          (query.namespaceId === undefined || r.namespaceId === query.namespaceId) &&
          (query.status === undefined || r.status === query.status),
      )
      .sort((a, b) => b.startedAt - a.startedAt || (a.runId < b.runId ? 1 : -1));
    return query.limit !== undefined ? rows.slice(0, query.limit) : rows;
  }

  async delete(runId: string): Promise<void> {
    this.runs.delete(runId);
  }

  private require(runId: string): Row {
    const row = this.runs.get(runId);
    if (row === undefined) throw new Error(`eval run "${runId}" not found`);
    return row;
  }
}

function recordOf(row: Row): EvalRunRecord {
  return { ...row.record, completedCases: row.cases.size };
}
