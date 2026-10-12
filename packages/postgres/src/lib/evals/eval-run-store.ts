// ---------------------------------------------------------------------------
// PostgresEvalRunStore — Postgres-backed `EvalRunStore`.
//
// The durable run history a multi-replica deployment needs. One `eval_run`
// row per run plus one `eval_run_case` row per recorded case (keyed by the
// case's dataset position, cascading on delete), so recording a case is a
// single-row upsert. Matches the SQLite / in-memory semantics.
// ---------------------------------------------------------------------------

import { and, asc, desc, eq, getTableColumns, sql, type SQL } from "drizzle-orm";
import type {
  EvalCaseResult,
  EvalRunQuery,
  EvalRunRecord,
  EvalRunStatus,
  EvalRunStore,
  EvalRunTotals,
  FinishEvalRunParams,
  StartEvalRunParams,
  StoredEvalRun,
} from "@promin/evals";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { evalRun, evalRunCase } from "../schema.ts";

export interface PostgresEvalRunStoreConfig {
  readonly db: DrizzleDb;
}

const completedCases = sql<number>`(SELECT COUNT(*)::int FROM ${evalRunCase} WHERE ${evalRunCase.runId} = ${evalRun.runId})`;

type RunRow = typeof evalRun.$inferSelect & { completedCases: number };

export class PostgresEvalRunStore implements EvalRunStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresEvalRunStoreConfig) {
    this.db = config.db;
  }

  async start(params: StartEvalRunParams): Promise<void> {
    await this.db
      .insert(evalRun)
      .values({
        runId: params.runId,
        status: "running",
        targetId: params.targetId,
        targetVersion: params.targetVersion ?? null,
        datasetId: params.datasetId,
        suiteId: params.suiteId ?? null,
        suiteVersion: params.suiteVersion ?? null,
        namespaceId: params.namespaceId ?? null,
        samplesPerCase: params.samplesPerCase,
        totalCases: params.totalCases,
        startedAt: params.startedAt,
        metadata: params.metadata ?? null,
      })
      .onConflictDoNothing({ target: evalRun.runId });
  }

  async recordCase(params: {
    readonly runId: string;
    readonly caseIndex: number;
    readonly result: EvalCaseResult;
  }): Promise<void> {
    await this.db.transaction(async (tx) => {
      if ((await lockStatus({ tx, runId: params.runId })) !== "running") return;
      await tx
        .insert(evalRunCase)
        .values({ runId: params.runId, caseIndex: params.caseIndex, result: params.result })
        .onConflictDoUpdate({
          target: [evalRunCase.runId, evalRunCase.caseIndex],
          set: { result: params.result },
        });
    });
  }

  async finish(params: FinishEvalRunParams): Promise<void> {
    await this.db.transaction(async (tx) => {
      if ((await lockStatus({ tx, runId: params.runId })) !== "running") return;
      await tx
        .update(evalRun)
        .set({
          status: params.status,
          finishedAt: params.finishedAt,
          ...(params.totals !== undefined && { totals: params.totals }),
          ...(params.error !== undefined && { error: params.error }),
        })
        .where(eq(evalRun.runId, params.runId));
    });
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    const [row] = await this.db
      .select({ ...getTableColumns(evalRun), completedCases })
      .from(evalRun)
      .where(eq(evalRun.runId, runId));
    if (!row) return null;
    const cases = await this.db
      .select({ result: evalRunCase.result })
      .from(evalRunCase)
      .where(eq(evalRunCase.runId, runId))
      .orderBy(asc(evalRunCase.caseIndex));
    return { ...rowToRecord(row), caseResults: cases.map((c) => c.result as EvalCaseResult) };
  }

  async list(query: EvalRunQuery = {}): Promise<EvalRunRecord[]> {
    const conditions: SQL[] = [];
    if (query.targetId !== undefined) conditions.push(eq(evalRun.targetId, query.targetId));
    if (query.targetVersion !== undefined) {
      conditions.push(eq(evalRun.targetVersion, query.targetVersion));
    }
    if (query.datasetId !== undefined) conditions.push(eq(evalRun.datasetId, query.datasetId));
    if (query.suiteId !== undefined) conditions.push(eq(evalRun.suiteId, query.suiteId));
    if (query.suiteVersion !== undefined) {
      conditions.push(eq(evalRun.suiteVersion, query.suiteVersion));
    }
    if (query.namespaceId !== undefined) {
      conditions.push(eq(evalRun.namespaceId, query.namespaceId));
    }
    if (query.status !== undefined) conditions.push(eq(evalRun.status, query.status));

    let q = this.db
      .select({ ...getTableColumns(evalRun), completedCases })
      .from(evalRun)
      .$dynamic();
    if (conditions.length > 0) q = q.where(and(...conditions));
    q = q.orderBy(desc(evalRun.startedAt), desc(evalRun.runId));
    if (query.limit !== undefined) q = q.limit(query.limit);
    const rows = await q;
    return rows.map(rowToRecord);
  }

  async delete(runId: string): Promise<void> {
    // Case rows go with it (ON DELETE CASCADE).
    await this.db.delete(evalRun).where(eq(evalRun.runId, runId));
  }
}

type Tx = Parameters<Parameters<DrizzleDb["transaction"]>[0]>[0];

/** The run's status, row-locked for the transaction; throws when the run does not exist. */
async function lockStatus(params: { tx: Tx; runId: string }): Promise<EvalRunStatus> {
  const [row] = await params.tx
    .select({ status: evalRun.status })
    .from(evalRun)
    .where(eq(evalRun.runId, params.runId))
    .for("update");
  if (!row) throw new Error(`eval run "${params.runId}" not found`);
  return row.status as EvalRunStatus;
}

function rowToRecord(row: RunRow): EvalRunRecord {
  return {
    runId: row.runId,
    status: row.status as EvalRunStatus,
    targetId: row.targetId,
    ...(row.targetVersion !== null && { targetVersion: row.targetVersion }),
    datasetId: row.datasetId,
    ...(row.suiteId !== null && { suiteId: row.suiteId }),
    ...(row.suiteVersion !== null && { suiteVersion: row.suiteVersion }),
    ...(row.namespaceId !== null && { namespaceId: row.namespaceId }),
    samplesPerCase: row.samplesPerCase,
    totalCases: row.totalCases,
    completedCases: row.completedCases,
    startedAt: row.startedAt,
    ...(row.finishedAt !== null && { finishedAt: row.finishedAt }),
    ...(row.totals !== null && { totals: row.totals as EvalRunTotals }),
    ...(row.error !== null && { error: row.error }),
    ...(row.metadata !== null && {
      metadata: row.metadata as Readonly<Record<string, unknown>>,
    }),
  };
}
