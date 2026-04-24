// ---------------------------------------------------------------------------
// PgThrottle — Postgres-backed sliding-window throttle (polls until slot open)
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { Throttle } from "@promin/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgThrottleConfig {
  db: DrizzleDb;
  key: string;
  permits: number;
  windowMs: number;
  pollIntervalMs?: number;
  table?: string;
}

export class PgThrottle implements Throttle {
  private readonly db: DrizzleDb;
  private readonly key: string;
  private readonly permits: number;
  private readonly windowMs: number;
  private readonly table: string;
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgThrottleConfig) {
    this.db = config.db;
    this.key = config.key;
    this.permits = config.permits;
    this.windowMs = config.windowMs;
    this.table = config.table ?? "promin_throttle";
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

  async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  /**
   * Try to acquire once.
   * Returns 0 if granted, or ms to wait until a slot opens.
   */
  private async _tryAcquireOnce(resourceKey: string): Promise<number> {
    const table = this.table;
    const permits = this.permits;
    const windowMs = this.windowMs;

    let waitMs = 0;
    let granted = false;

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

      if (count < permits) {
        await execRaw(
          db,
          sql`INSERT INTO ${sql.raw(table)} (key, ts) VALUES (${resourceKey}, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT)`,
        );
        granted = true;
      } else {
        // Find oldest to compute wait
        const oldestRows = await execRaw(
          db,
          sql`SELECT MIN(ts) AS oldest FROM ${sql.raw(table)} WHERE key = ${resourceKey}`,
        );
        const oldest = Number(oldestRows[0]?.oldest ?? 0);
        const nowVal = Date.now();
        waitMs = Math.max(1, oldest + windowMs - nowVal);
        granted = false;
      }
    });

    return granted ? 0 : waitMs;
  }

  async acquireAsync(resource?: string): Promise<void> {
    await this._ensureReady();
    const resourceKey = `${this.key}:${resource ?? ""}`;
    while (true) {
      const waitMs = await this._tryAcquireOnce(resourceKey);
      if (waitMs === 0) return;
      await new Promise<void>((r) => setTimeout(r, waitMs));
    }
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    await this._ensureReady();
    const resourceKey = `${this.key}:${resource ?? ""}`;
    const waitMs = await this._tryAcquireOnce(resourceKey);
    return waitMs === 0;
  }

  async withPermitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }
}
