import type { Throttle } from "@promin/core";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent sliding-window throttle backed by SQLite.
 *
 * Like `SqliteRateLimiter` but blocks instead of throwing — callers wait
 * until a permit slot opens in the current window.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_throttle (key TEXT, ts INTEGER)
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("state.db");
 * const throttle = SqliteThrottle.make({ db, key: "outbound", permits: 10, windowMs: 1_000 });
 * await throttle.withPermitAsync(() => fetch("/api/data"));
 * ```
 */
export class SqliteThrottle implements Throttle {
  private readonly _table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    private readonly key: string,
    private readonly permits: number,
    private readonly windowMs: number,
    table: string,
  ) {
    this._table = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    key: string;
    permits: number;
    windowMs: number;
    /** Override the table name (default: `promin_throttle`). */
    table?: string;
  }): SqliteThrottle {
    return new SqliteThrottle(
      params.db,
      params.key,
      params.permits,
      params.windowMs,
      params.table ?? "promin_throttle",
    );
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this._table} (
        key TEXT NOT NULL,
        ts  INTEGER NOT NULL
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${this._table}_key_ts ON ${this._table} (key, ts)`);
  }

  private _resolveKey(resource?: string): string {
    return resource ? `${this.key}:${resource}` : this.key;
  }

  /**
   * Try to claim a permit. Returns 0 if acquired, or the ms to wait until
   * the oldest active permit expires and a slot opens.
   */
  private _tryAcquireInternal(key: string, now: number): number {
    const { db, _table: t, permits, windowMs } = this;
    return db.transaction((): number => {
      db.query(`DELETE FROM ${t} WHERE key = ? AND ts <= ?`).run(key, now - windowMs);

      const row = db
        .query<{ cnt: number; oldest: number | null }>(
          `SELECT COUNT(*) AS cnt, MIN(ts) AS oldest FROM ${t} WHERE key = ?`,
        )
        .get(key);

      const count = row?.cnt ?? 0;
      const oldest = row?.oldest ?? null;

      if (count < permits) {
        db.query(`INSERT INTO ${t} (key, ts) VALUES (?, ?)`).run(key, now);
        return 0;
      }

      return oldest !== null ? Math.max(1, oldest + windowMs - now) : 1;
    })();
  }

  async acquireAsync(resource?: string): Promise<void> {
    const key = this._resolveKey(resource);
    while (true) {
      const waitMs = this._tryAcquireInternal(key, Date.now());
      if (waitMs === 0) return;
      await new Promise<void>((r) => setTimeout(r, waitMs));
    }
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    return this._tryAcquireInternal(this._resolveKey(resource), Date.now()) === 0;
  }

  async withPermitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }
}
