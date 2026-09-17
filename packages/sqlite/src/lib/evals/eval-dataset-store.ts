// ---------------------------------------------------------------------------
// SqliteEvalDatasetStore — `EvalDatasetStore` over SQLite.
//
// One row per dataset; the case list rides in the `cases` JSON column.
// `save` upserts, so re-saving a dataset replaces its cases wholesale.
// ---------------------------------------------------------------------------

import type { EvalCase, EvalDatasetStore } from "@promin/evals";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteEvalDatasetStoreConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_eval_dataset`). */
  readonly table?: string;
}

interface Row {
  dataset_id: string;
  cases: string;
}

export class SqliteEvalDatasetStore implements EvalDatasetStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;

  private constructor(config: SqliteEvalDatasetStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_eval_dataset";
    this._setup();
  }

  static make(config: SqliteEvalDatasetStoreConfig): SqliteEvalDatasetStore {
    return new SqliteEvalDatasetStore(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        dataset_id TEXT PRIMARY KEY,
        cases      TEXT NOT NULL
      )
    `);
  }

  async save(datasetId: string, cases: ReadonlyArray<EvalCase>): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table} (dataset_id, cases)
         VALUES (?, ?)
         ON CONFLICT(dataset_id) DO UPDATE SET cases = excluded.cases`,
      )
      .run(datasetId, JSON.stringify(cases));
  }

  async get(datasetId: string): Promise<EvalCase[] | null> {
    const row = this.db
      .query<Row>(`SELECT dataset_id, cases FROM ${this.table} WHERE dataset_id = ?`)
      .get(datasetId);
    return row ? (JSON.parse(row.cases) as EvalCase[]) : null;
  }

  async list(): Promise<string[]> {
    const rows = this.db
      .query<{ dataset_id: string }>(`SELECT dataset_id FROM ${this.table} ORDER BY dataset_id ASC`)
      .all();
    return rows.map((row) => row.dataset_id);
  }

  async delete(datasetId: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.table} WHERE dataset_id = ?`).run(datasetId);
  }
}
