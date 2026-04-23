import type { AsyncQueue } from "@promin/core";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent FIFO job queue backed by SQLite.
 *
 * Items are stored as JSON and dequeued in insertion order.
 * `takeAsync` polls the DB until an item is available (SQLite has no
 * server-push, so polling is the correct approach).
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_queue (
 *     id      INTEGER PRIMARY KEY AUTOINCREMENT,
 *     name    TEXT NOT NULL,
 *     payload TEXT NOT NULL
 *   )
 *
 * Multiple named queues can share one table via distinct `name` values.
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("jobs.db");
 * const queue = SqliteQueue.make<{ url: string }>({ db, name: "crawl" });
 * await queue.offerAsync({ url: "https://example.com" });
 * const job = await queue.takeAsync();
 * ```
 */
export class SqliteQueue<T> implements AsyncQueue<T> {
  private _shutdown = false;
  private readonly _table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    private readonly name: string,
    private readonly pollIntervalMs: number,
    table: string,
  ) {
    this._table = table;
    this._setup();
  }

  static make<T>(params: {
    db: SqliteDatabase;
    /** Logical queue name — multiple queues can share one table via distinct names. */
    name: string;
    /** How often to poll for new items in `takeAsync`. Default: 50 ms. */
    pollIntervalMs?: number;
    /** Override the table name (default: `promin_queue`). */
    table?: string;
  }): SqliteQueue<T> {
    return new SqliteQueue<T>(
      params.db,
      params.name,
      params.pollIntervalMs ?? 50,
      params.table ?? "promin_queue",
    );
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this._table} (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT    NOT NULL,
        payload TEXT    NOT NULL
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${this._table}_name_id ON ${this._table} (name, id)`);
  }

  async offerAsync(item: T): Promise<void> {
    if (this._shutdown) throw new Error("Queue is shut down");
    this.db
      .query(`INSERT INTO ${this._table} (name, payload) VALUES (?, ?)`)
      .run(this.name, JSON.stringify(item));
  }

  async takeAsync(): Promise<T> {
    const { db, _table: t, name } = this;
    const dequeue = db.transaction((): { id: number; payload: string } | null => {
      const row = db
        .query<{ id: number; payload: string }>(
          `SELECT id, payload FROM ${t} WHERE name = ? ORDER BY id ASC LIMIT 1`,
        )
        .get(name);
      if (!row) return null;
      db.query(`DELETE FROM ${t} WHERE id = ?`).run(row.id);
      return row;
    });

    while (true) {
      if (this._shutdown) throw new Error("Queue is shut down");
      const row = dequeue();
      if (row) return JSON.parse(row.payload) as T;
      await new Promise<void>((r) => setTimeout(r, this.pollIntervalMs));
    }
  }

  async shutdownAsync(): Promise<void> {
    this._shutdown = true;
  }

  async sizeAsync(): Promise<number> {
    const row = this.db
      .query<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM ${this._table} WHERE name = ?`)
      .get(this.name);
    return row?.cnt ?? 0;
  }
}
