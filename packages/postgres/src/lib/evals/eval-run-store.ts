// ---------------------------------------------------------------------------
// PostgresEvalRunStore — Postgres-backed `EvalRunStore`.
//
// The durable run history a multi-replica deployment needs. One row per
// run, keyed on the deterministic `composeRunId` so re-saving the same run
// upserts; the full `EvalRunSummary` rides in the `summary` jsonb column.
// Matches the SQLite / in-memory `EvalRunStore` semantics.
// ---------------------------------------------------------------------------

import { and, desc, eq } from "drizzle-orm";
import { composeRunId } from "@promin/evals";
import type { EvalRunQuery, EvalRunStore, EvalRunSummary, StoredEvalRun } from "@promin/evals";
import type { DrizzleDb } from "../drizzle-db.ts";
import { evalRun } from "../schema.ts";

export interface PostgresEvalRunStoreConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresEvalRunStore implements EvalRunStore {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresEvalRunStoreConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async save(summary: EvalRunSummary): Promise<string> {
    const runId = composeRunId(summary);
    const savedAt = this.clock();
    await this.db
      .insert(evalRun)
      .values({
        runId,
        targetId: summary.targetId,
        targetVersion: summary.targetVersion ?? null,
        datasetId: summary.datasetId,
        ranAt: summary.ranAt,
        summary,
        savedAt,
      })
      .onConflictDoUpdate({ target: evalRun.runId, set: { summary, savedAt } });
    return runId;
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    const [row] = await this.db.select().from(evalRun).where(eq(evalRun.runId, runId));
    return row ? rowToStoredRun(row) : null;
  }

  async list(query: EvalRunQuery = {}): Promise<StoredEvalRun[]> {
    const conditions = [];
    if (query.targetId !== undefined) conditions.push(eq(evalRun.targetId, query.targetId));
    if (query.targetVersion !== undefined) {
      conditions.push(eq(evalRun.targetVersion, query.targetVersion));
    }
    if (query.datasetId !== undefined) conditions.push(eq(evalRun.datasetId, query.datasetId));

    let q = this.db.select().from(evalRun).$dynamic();
    if (conditions.length > 0) q = q.where(and(...conditions));
    q = q.orderBy(desc(evalRun.ranAt));
    if (query.limit !== undefined) q = q.limit(query.limit);

    const rows = await q;
    return rows.map(rowToStoredRun);
  }

  async delete(runId: string): Promise<void> {
    await this.db.delete(evalRun).where(eq(evalRun.runId, runId));
  }
}

function rowToStoredRun(row: typeof evalRun.$inferSelect): StoredEvalRun {
  return {
    runId: row.runId,
    summary: row.summary as EvalRunSummary,
    savedAt: row.savedAt,
  };
}
