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

  async save(params: {
    readonly datasetId: string;
    readonly cases: ReadonlyArray<EvalCase>;
  }): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table} (dataset_id, cases)
         VALUES (?, ?)
         ON CONFLICT(dataset_id) DO UPDATE SET cases = excluded.cases`,
      )
      .run(params.datasetId, JSON.stringify(params.cases));
  }

  async get(datasetId: string): Promise<EvalCase[] | null> {
    const row = this.db
      .query<Row>(`SELECT dataset_id, cases FROM ${this.table} WHERE dataset_id = ?`)
      .get(datasetId);
    return row ? (JSON.parse(row.cases) as EvalCase[]) : null;
  }

  async appendCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    // Each statement is atomic, so concurrent writers cannot lose a case or
    // insert the same id twice.
    const value = JSON.stringify(evalCase);
    this.db
      .query(
        `INSERT OR IGNORE INTO ${this.table} (dataset_id, cases) VALUES (?, json_array(json(?)))`,
      )
      .run(datasetId, value);
    if (this.changes() > 0) return true;
    this.db
      .query(
        `UPDATE ${this.table} SET cases = json_insert(cases, '$[#]', json(?))
         WHERE dataset_id = ? AND NOT EXISTS (
           SELECT 1 FROM json_each(${this.table}.cases) WHERE json_extract(value, '$.id') = ?)`,
      )
      .run(value, datasetId, evalCase.id);
    return this.changes() > 0;
  }

  async replaceCase(datasetId: string, evalCase: EvalCase): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this.table} SET cases = (
           SELECT json_group_array(json(CASE WHEN json_extract(value, '$.id') = ? THEN json(?) ELSE value END))
           FROM json_each(${this.table}.cases))
         WHERE dataset_id = ? AND EXISTS (
           SELECT 1 FROM json_each(${this.table}.cases) WHERE json_extract(value, '$.id') = ?)`,
      )
      .run(evalCase.id, JSON.stringify(evalCase), datasetId, evalCase.id);
    return this.changes() > 0;
  }

  async deleteCase(datasetId: string, caseId: string): Promise<boolean> {
    this.db
      .query(
        `UPDATE ${this.table} SET cases = (
           SELECT json_group_array(json(value)) FROM json_each(${this.table}.cases)
           WHERE json_extract(value, '$.id') <> ?)
         WHERE dataset_id = ? AND EXISTS (
           SELECT 1 FROM json_each(${this.table}.cases) WHERE json_extract(value, '$.id') = ?)`,
      )
      .run(caseId, datasetId, caseId);
    return this.changes() > 0;
  }

  /** Rows touched by this connection's last statement (callers run it synchronously after). */
  private changes(): number {
    return this.db.query<{ n: number }>("SELECT changes() AS n").get()?.n ?? 0;
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
