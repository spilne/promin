// ---------------------------------------------------------------------------
// SqliteEvalRunStore — `EvalRunStore` over SQLite.
//
// One row per run (identity columns projected out for indexed filtering,
// totals and metadata as JSON) plus one row per recorded case in
// `<table>_case`, keyed by the case's dataset position, so recording a
// case is a single-row upsert however large the run grows.
// ---------------------------------------------------------------------------

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
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteEvalRunStoreConfig {
  readonly db: SqliteDatabase;
  /** Override the run table name (default: `promin_eval_run`); cases go to `<table>_case`. */
  readonly table?: string;
}

interface RunRow {
  run_id: string;
  status: string;
  target_id: string;
  target_version: string | null;
  dataset_id: string;
  suite_id: string | null;
  suite_version: string | null;
  namespace_id: string | null;
  samples_per_case: number;
  total_cases: number;
  started_at: number;
  finished_at: number | null;
  totals: string | null;
  error: string | null;
  metadata: string | null;
  completed_cases: number;
}

export class SqliteEvalRunStore implements EvalRunStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly caseTable: string;

  private constructor(config: SqliteEvalRunStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_eval_run";
    this.caseTable = `${this.table}_case`;
    this.setup();
  }

  static make(config: SqliteEvalRunStoreConfig): SqliteEvalRunStore {
    return new SqliteEvalRunStore(config);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        run_id           TEXT PRIMARY KEY,
        status           TEXT NOT NULL,
        target_id        TEXT NOT NULL,
        target_version   TEXT,
        dataset_id       TEXT NOT NULL,
        suite_id         TEXT,
        suite_version    TEXT,
        namespace_id     TEXT,
        samples_per_case INTEGER NOT NULL,
        total_cases      INTEGER NOT NULL,
        started_at       INTEGER NOT NULL,
        finished_at      INTEGER,
        totals           TEXT,
        error            TEXT,
        metadata         TEXT
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.caseTable} (
        run_id     TEXT NOT NULL,
        case_index INTEGER NOT NULL,
        result     TEXT NOT NULL,
        PRIMARY KEY (run_id, case_index)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_target_started ON ${this.table} (target_id, started_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_suite_started ON ${this.table} (suite_id, started_at)`,
    );
  }

  async start(params: StartEvalRunParams): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table}
           (run_id, status, target_id, target_version, dataset_id, suite_id, suite_version,
            namespace_id, samples_per_case, total_cases, started_at, metadata)
         VALUES (?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(run_id) DO NOTHING`,
      )
      .run(
        params.runId,
        params.targetId,
        params.targetVersion ?? null,
        params.datasetId,
        params.suiteId ?? null,
        params.suiteVersion ?? null,
        params.namespaceId ?? null,
        params.samplesPerCase,
        params.totalCases,
        params.startedAt,
        params.metadata !== undefined ? JSON.stringify(params.metadata) : null,
      );
  }

  async recordCase(params: {
    readonly runId: string;
    readonly caseIndex: number;
    readonly result: EvalCaseResult;
  }): Promise<void> {
    this.db.transaction(() => {
      if (this.statusOf(params.runId) !== "running") return;
      this.db
        .query(
          `INSERT INTO ${this.caseTable} (run_id, case_index, result) VALUES (?, ?, ?)
           ON CONFLICT(run_id, case_index) DO UPDATE SET result = excluded.result`,
        )
        .run(params.runId, params.caseIndex, JSON.stringify(params.result));
    })();
  }

  async finish(params: FinishEvalRunParams): Promise<void> {
    this.db.transaction(() => {
      if (this.statusOf(params.runId) !== "running") return;
      this.db
        .query(
          `UPDATE ${this.table}
              SET status = ?, finished_at = ?,
                  totals = COALESCE(?, totals), error = COALESCE(?, error)
            WHERE run_id = ?`,
        )
        .run(
          params.status,
          params.finishedAt,
          params.totals !== undefined ? JSON.stringify(params.totals) : null,
          params.error ?? null,
          params.runId,
        );
    })();
  }

  async get(runId: string): Promise<StoredEvalRun | null> {
    const row = this.db.query<RunRow>(`${this.selectRuns()} WHERE r.run_id = ?`).get(runId);
    if (!row) return null;
    const cases = this.db
      .query<{ result: string }>(
        `SELECT result FROM ${this.caseTable} WHERE run_id = ? ORDER BY case_index ASC`,
      )
      .all(runId);
    return {
      ...rowToRecord(row),
      caseResults: cases.map((c) => JSON.parse(c.result) as EvalCaseResult),
    };
  }

  async list(query: EvalRunQuery = {}): Promise<EvalRunRecord[]> {
    const where: string[] = [];
    const args: Array<string | number> = [];
    const filters: Array<[string, string | undefined]> = [
      ["r.target_id", query.targetId],
      ["r.target_version", query.targetVersion],
      ["r.dataset_id", query.datasetId],
      ["r.suite_id", query.suiteId],
      ["r.suite_version", query.suiteVersion],
      ["r.namespace_id", query.namespaceId],
      ["r.status", query.status],
    ];
    for (const [column, value] of filters) {
      if (value === undefined) continue;
      where.push(`${column} = ?`);
      args.push(value);
    }
    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    let limitClause = "";
    if (query.limit !== undefined) {
      limitClause = "LIMIT ?";
      args.push(query.limit);
    }
    const rows = this.db
      .query<RunRow>(
        `${this.selectRuns()} ${whereClause} ORDER BY r.started_at DESC, r.run_id DESC ${limitClause}`,
      )
      .all(...args);
    return rows.map(rowToRecord);
  }

  async delete(runId: string): Promise<void> {
    this.db.transaction(() => {
      this.db.query(`DELETE FROM ${this.caseTable} WHERE run_id = ?`).run(runId);
      this.db.query(`DELETE FROM ${this.table} WHERE run_id = ?`).run(runId);
    })();
  }

  private selectRuns(): string {
    return `SELECT r.*, (SELECT COUNT(*) FROM ${this.caseTable} c WHERE c.run_id = r.run_id) AS completed_cases
              FROM ${this.table} r`;
  }

  /** The run's status; throws when the run does not exist. */
  private statusOf(runId: string): EvalRunStatus {
    const row = this.db
      .query<{ status: string }>(`SELECT status FROM ${this.table} WHERE run_id = ?`)
      .get(runId);
    if (!row) throw new Error(`eval run "${runId}" not found`);
    return row.status as EvalRunStatus;
  }
}

function rowToRecord(row: RunRow): EvalRunRecord {
  return {
    runId: row.run_id,
    status: row.status as EvalRunStatus,
    targetId: row.target_id,
    ...(row.target_version !== null && { targetVersion: row.target_version }),
    datasetId: row.dataset_id,
    ...(row.suite_id !== null && { suiteId: row.suite_id }),
    ...(row.suite_version !== null && { suiteVersion: row.suite_version }),
    ...(row.namespace_id !== null && { namespaceId: row.namespace_id }),
    samplesPerCase: row.samples_per_case,
    totalCases: row.total_cases,
    completedCases: row.completed_cases,
    startedAt: row.started_at,
    ...(row.finished_at !== null && { finishedAt: row.finished_at }),
    ...(row.totals !== null && { totals: JSON.parse(row.totals) as EvalRunTotals }),
    ...(row.error !== null && { error: row.error }),
    ...(row.metadata !== null && {
      metadata: JSON.parse(row.metadata) as Readonly<Record<string, unknown>>,
    }),
  };
}
