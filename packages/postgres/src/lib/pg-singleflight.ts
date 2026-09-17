// ---------------------------------------------------------------------------
// PgSingleflight — Postgres-backed distributed singleflight (request dedup)
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { Singleflight } from "@promin/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgSingleflightConfig {
  db: DrizzleDb;
  pollIntervalMs?: number;
  table?: string;
}

type FlightStatus = "running" | "done" | "error";

interface FlightRow {
  key: string;
  gen: number;
  status: FlightStatus;
  result: string | null;
  error: string | null;
}

export class PgSingleflight implements Singleflight {
  private readonly db: DrizzleDb;
  private readonly pollIntervalMs: number;
  private readonly table: string;
  private readonly _localFlights = new Map<string, Promise<unknown>>();
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgSingleflightConfig) {
    this.db = config.db;
    this.pollIntervalMs = config.pollIntervalMs ?? 50;
    this.table = config.table ?? "promin_singleflight";
    this.setupPromise = this._setup();
  }

  static async make(config: PgSingleflightConfig): Promise<PgSingleflight> {
    const sf = new PgSingleflight(config);
    await sf._ensureReady();
    return sf;
  }

  private async _setup(): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          key TEXT PRIMARY KEY,
          gen INTEGER NOT NULL DEFAULT 1,
          status TEXT NOT NULL DEFAULT 'running',
          result TEXT,
          error TEXT
        )
      `),
    );
  }

  private async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  async doAsync<T>(key: string, fn: () => Promise<T>): Promise<T> {
    await this._ensureReady();

    // In-process dedup: if we already have a local promise, join it
    const existing = this._localFlights.get(key);
    if (existing) return existing as Promise<T>;

    const p = this._doRemote(key, fn);
    this._localFlights.set(key, p as Promise<unknown>);
    try {
      const result = await p;
      return result;
    } finally {
      this._localFlights.delete(key);
    }
  }

  private async _doRemote<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const table = this.table;
    const MAX_RETRIES = 10;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      // Try to become winner or join as loser
      let isWinner = false;
      let loserGen = 0;

      await this.db.transaction(async (tx) => {
        const db = tx as DrizzleDb;

        // Lock the row
        const rows = await execRaw(
          db,
          sql`SELECT key, gen, status, result, error FROM ${sql.raw(table)} WHERE key = ${key} FOR UPDATE`,
        );

        if (rows.length === 0) {
          // No row — we are the winner, insert
          await execRaw(
            db,
            sql`INSERT INTO ${sql.raw(table)} (key, gen, status, result, error) VALUES (${key}, 1, 'running', NULL, NULL)`,
          );
          isWinner = true;
        } else {
          const row = rows[0] as FlightRow;
          if (row.status === "running") {
            // Another process is running — be a loser
            isWinner = false;
            loserGen = row.gen;
          } else {
            // Previous run finished (done or error) — start a new generation
            await execRaw(
              db,
              sql`UPDATE ${sql.raw(table)} SET gen = gen + 1, status = 'running', result = NULL, error = NULL WHERE key = ${key}`,
            );
            isWinner = true;
          }
        }
      });

      if (isWinner) {
        // Run fn and store result
        try {
          const result = await fn();
          await execRaw(
            this.db,
            sql`UPDATE ${sql.raw(table)} SET status = 'done', result = ${JSON.stringify(result)} WHERE key = ${key}`,
          );
          // Clean up row after a short delay (don't await)
          setTimeout(() => {
            execRaw(
              this.db,
              sql`DELETE FROM ${sql.raw(table)} WHERE key = ${key} AND status != 'running'`,
            ).catch(() => {});
          }, 1000);
          return result;
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : String(err);
          await execRaw(
            this.db,
            sql`UPDATE ${sql.raw(table)} SET status = 'error', error = ${errMsg} WHERE key = ${key}`,
          );
          // Clean up row after a short delay (don't await)
          setTimeout(() => {
            execRaw(
              this.db,
              sql`DELETE FROM ${sql.raw(table)} WHERE key = ${key} AND status != 'running'`,
            ).catch(() => {});
          }, 1000);
          throw err;
        }
      } else {
        // Poll until the flight completes or the generation changes
        while (true) {
          await new Promise<void>((r) => setTimeout(r, this.pollIntervalMs));

          const rows = await execRaw(
            this.db,
            sql`SELECT key, gen, status, result, error FROM ${sql.raw(table)} WHERE key = ${key}`,
          );

          if (rows.length === 0) {
            // Row was deleted — might have succeeded, try outer loop
            break;
          }

          const row = rows[0] as FlightRow;

          if (row.gen !== loserGen) {
            // Generation changed — restart outer loop
            break;
          }

          if (row.status === "done") {
            return JSON.parse(row.result!) as T;
          }

          if (row.status === "error") {
            throw new Error(row.error ?? "Unknown error");
          }

          // status === "running" — keep polling
        }
        // Loop back to outer for retry
      }
    }

    throw new Error(`PgSingleflight: max retries exceeded for key "${key}"`);
  }
}
