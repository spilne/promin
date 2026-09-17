import { RateLimitExceeded, type RateLimiter } from "@promin/core";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent sliding-window rate limiter backed by SQLite.
 *
 * State survives process restarts — useful for per-user API quotas, outbound
 * call budgets, or any rate limit that must hold across server restarts without
 * needing Redis or Postgres.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_rate_limit (key TEXT, ts INTEGER)
 *   INDEX on (key, ts) for fast range deletes
 *
 * All reads and writes are wrapped in a transaction so concurrent callers in
 * the same process see a consistent window.
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("state.db");
 * const limiter = SqliteRateLimiter.make({ db, key: "openai", limit: 60, windowMs: 60_000 });
 * await limiter.acquireAsync();
 * ```
 */
export class SqliteRateLimiter implements RateLimiter {
  private readonly _table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    private readonly key: string,
    private readonly limit: number,
    private readonly windowMs: number,
    table: string,
  ) {
    this._table = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    key: string;
    limit: number;
    windowMs: number;
    /** Override the table name (default: `promin_rate_limit`). */
    table?: string;
  }): SqliteRateLimiter {
    return new SqliteRateLimiter(
      params.db,
      params.key,
      params.limit,
      params.windowMs,
      params.table ?? "promin_rate_limit",
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

  private _runAcquire(key: string, now: number): { ok: boolean; retryAfterMs: number } {
    const { db, _table: t, limit, windowMs } = this;
    return db.transaction((): { ok: boolean; retryAfterMs: number } => {
      db.query(`DELETE FROM ${t} WHERE key = ? AND ts <= ?`).run(key, now - windowMs);

      const row = db
        .query<{ cnt: number; oldest: number | null }>(
          `SELECT COUNT(*) AS cnt, MIN(ts) AS oldest FROM ${t} WHERE key = ?`,
        )
        .get(key);

      const count = row?.cnt ?? 0;
      const oldest = row?.oldest ?? null;

      if (count < limit) {
        db.query(`INSERT INTO ${t} (key, ts) VALUES (?, ?)`).run(key, now);
        return { ok: true, retryAfterMs: 0 };
      }

      return {
        ok: false,
        retryAfterMs: oldest !== null ? Math.max(1, oldest + windowMs - now) : 1,
      };
    })();
  }

  async acquireAsync(resource?: string): Promise<void> {
    const result = this._runAcquire(this._resolveKey(resource), Date.now());
    if (!result.ok) throw new RateLimitExceeded({ retryAfterMs: result.retryAfterMs });
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    return this._runAcquire(this._resolveKey(resource), Date.now()).ok;
  }

  async withLimitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }

  async remainingAsync(resource?: string): Promise<number> {
    const key = this._resolveKey(resource);
    const cutoff = Date.now() - this.windowMs;
    const row = this.db
      .query<{ cnt: number }>(`SELECT COUNT(*) AS cnt FROM ${this._table} WHERE key = ? AND ts > ?`)
      .get(key, cutoff);
    return Math.max(0, this.limit - (row?.cnt ?? 0));
  }
}
