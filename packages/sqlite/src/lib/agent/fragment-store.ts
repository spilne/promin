// ---------------------------------------------------------------------------
// `SqliteFragmentStore` — `FragmentStore` over SQLite. Sibling of
// `SqliteSkillRegistry`. Operator-authored prompt fragments persist here;
// file-scanned ones (handled by `FragmentScanner`) deliberately do NOT.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_fragment_store (
//     key        TEXT PRIMARY KEY,
//     content    TEXT NOT NULL,
//     updated_at INTEGER NOT NULL
//   )
// ---------------------------------------------------------------------------

import type { FragmentStore } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteFragmentStoreConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_fragment_store`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class SqliteFragmentStore implements FragmentStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteFragmentStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_fragment_store";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteFragmentStoreConfig): SqliteFragmentStore {
    return new SqliteFragmentStore(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        key        TEXT PRIMARY KEY,
        content    TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
  }

  async loadAll(): Promise<ReadonlyArray<{ readonly key: string; readonly content: string }>> {
    const rows = this.db
      .query<{ key: string; content: string }>(`SELECT key, content FROM ${this.table}`)
      .all();
    return rows.map((r) => ({ key: r.key, content: r.content }));
  }

  async set(key: string, content: string): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table} (key, content, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           content = excluded.content,
           updated_at = excluded.updated_at`,
      )
      .run(key, content, this.clock());
  }

  async delete(key: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.table} WHERE key = ?`).run(key);
  }
}
