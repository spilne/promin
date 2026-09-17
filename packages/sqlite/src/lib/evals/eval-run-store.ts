// ---------------------------------------------------------------------------
// SqliteEvalRunStore — `EvalRunStore` over SQLite.
//
// One row per run, keyed on the deterministic `composeRunId`, so re-saving
// the same run upserts rather than duplicates. The full `EvalRunSummary`
// rides in the `summary` JSON column; identity columns are projected out
// for indexed filtering.
// ---------------------------------------------------------------------------

import { composeRunId } from "@promin/evals";
import type { EvalRunQuery, EvalRunStore, EvalRunSummary, StoredEvalRun } from "@promin/evals";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteEvalRunStoreConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_eval_run`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

interface Row {
  run_id: string;
  summary: string;
  saved_at: number;
}

export class SqliteEvalRunStore implements EvalRunStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteEvalRunStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_eval_run";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteEvalRunStoreConfig): SqliteEvalRunStore {
    return new SqliteEvalRunStore(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        run_id         TEXT PRIMARY KEY,
        target_id      TEXT NOT NULL,
        target_version TEXT,
        dataset_id     TEXT NOT NULL,
        ran_at         INTEGER NOT NULL,
        summary        TEXT NOT NULL,
        saved_at       INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_target_ran ON ${this.table} (target_id, ran_at)`,
    );
    this.db.run(`CREATE INDEX IF NOT EXISTS ${this.table}_dataset ON ${this.table} (dataset_id)`);
  }

  async save(summary: EvalRunSummary): Promise<string> {
    const runId = composeRunId(summary);
    this.db
      .query(
        `INSERT INTO ${this.table}
           (run_id, target_id, target_version, dataset_id, ran_at, summary, saved_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(run_id) DO UPDATE SET
           summary = excluded.summary,
           saved_at = excluded.saved_at`,
      )
      .run(
        runId,
        summary.targetId,
        summary.targetVersion ?? null,
        summary.datasetId,
        summary.ranAt,
        JSON.stringify(summary),
        this.clock(),
      );
    return runId;
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    const row = this.db
      .query<Row>(`SELECT run_id, summary, saved_at FROM ${this.table} WHERE run_id = ?`)
      .get(runId);
    return row ? rowToStoredRun(row) : null;
  }

  async list(query: EvalRunQuery = {}): Promise<StoredEvalRun[]> {
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (query.targetId !== undefined) {
      where.push("target_id = ?");
      args.push(query.targetId);
    }
    if (query.targetVersion !== undefined) {
      where.push("target_version = ?");
      args.push(query.targetVersion);
    }
    if (query.datasetId !== undefined) {
      where.push("dataset_id = ?");
      args.push(query.datasetId);
    }
    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    const limitClause = query.limit !== undefined ? "LIMIT ?" : "";
    if (query.limit !== undefined) args.push(query.limit);

    const rows = this.db
      .query<Row>(
        `SELECT run_id, summary, saved_at FROM ${this.table} ` +
          `${whereClause} ORDER BY ran_at DESC ${limitClause}`,
      )
      .all(...args);
    return rows.map(rowToStoredRun);
  }

  async delete(runId: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.table} WHERE run_id = ?`).run(runId);
  }
}

function rowToStoredRun(row: Row): StoredEvalRun {
  return {
    runId: row.run_id,
    summary: JSON.parse(row.summary) as EvalRunSummary,
    savedAt: row.saved_at,
  };
}
