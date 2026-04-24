// ---------------------------------------------------------------------------
// PgRateLimiter — Postgres-backed sliding-window rate limiter
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import { RateLimitExceeded, type RateLimiter } from "@promin/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgRateLimiterConfig {
  db: DrizzleDb;
  key: string;
  limit: number;
  windowMs: number;
  table?: string;
}

export class PgRateLimiter implements RateLimiter {
  private readonly db: DrizzleDb;
  private readonly key: string;
  private readonly limit: number;
  private readonly windowMs: number;
  private readonly table: string;
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgRateLimiterConfig) {
    this.db = config.db;
    this.key = config.key;
    this.limit = config.limit;
    this.windowMs = config.windowMs;
    this.table = config.table ?? "promin_rate_limit";
    this.setupPromise = this._setup();
  }

  private async _setup(): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          key TEXT NOT NULL,
          ts BIGINT NOT NULL
        )
      `),
    );
    await this.db.execute(
      sql.raw(`
        CREATE INDEX IF NOT EXISTS ${this.table}_key_ts_idx ON ${this.table} (key, ts)
      `),
    );
  }

  private async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  /** Try to acquire for a given resource key. Returns wait ms or 0 if granted. */
  private async _tryAcquireOnce(resourceKey: string): Promise<0 | number> {
    const table = this.table;
    const limit = this.limit;
    const windowMs = this.windowMs;

    let granted = false;
    let retryAfterMs = 0;

    await this.db.transaction(async (tx) => {
      const db = tx as DrizzleDb;

      // Delete expired entries
      await db.execute(
        sql.raw(
          `DELETE FROM ${table} WHERE key = '${resourceKey}' AND ts <= (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT - ${windowMs}`,
        ),
      );

      // Count active entries
      const countRows = await execRaw(
        db,
        sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(table)} WHERE key = ${resourceKey}`,
      );
      const count = Number(countRows[0]?.cnt ?? 0);

      if (count < limit) {
        await execRaw(
          db,
          sql`INSERT INTO ${sql.raw(table)} (key, ts) VALUES (${resourceKey}, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT)`,
        );
        granted = true;
      } else {
        // Find oldest timestamp to compute retryAfterMs
        const oldestRows = await execRaw(
          db,
          sql`SELECT MIN(ts) AS oldest FROM ${sql.raw(table)} WHERE key = ${resourceKey}`,
        );
        const oldest = Number(oldestRows[0]?.oldest ?? 0);
        const nowVal = Date.now();
        retryAfterMs = Math.max(1, oldest + windowMs - nowVal);
        granted = false;
      }
    });

    return granted ? 0 : retryAfterMs;
  }

  async acquireAsync(resource?: string): Promise<void> {
    await this._ensureReady();
    const resourceKey = `${this.key}:${resource ?? ""}`;
    const waitMs = await this._tryAcquireOnce(resourceKey);
    if (waitMs !== 0) {
      throw new RateLimitExceeded({ retryAfterMs: waitMs });
    }
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    await this._ensureReady();
    const resourceKey = `${this.key}:${resource ?? ""}`;
    const waitMs = await this._tryAcquireOnce(resourceKey);
    return waitMs === 0;
  }

  async withLimitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }

  async remainingAsync(resource?: string): Promise<number> {
    await this._ensureReady();
    const resourceKey = `${this.key}:${resource ?? ""}`;
    const table = this.table;
    const windowMs = this.windowMs;

    const rows = await execRaw(
      this.db,
      sql`SELECT COUNT(*) AS cnt FROM ${sql.raw(table)} WHERE key = ${resourceKey} AND ts > (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT - ${windowMs}`,
    );
    const count = Number(rows[0]?.cnt ?? 0);
    return Math.max(0, this.limit - count);
  }
}
