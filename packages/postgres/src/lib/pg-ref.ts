// ---------------------------------------------------------------------------
// PgRef<T> — Postgres-backed atomic reference (JSON value stored as TEXT)
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { AtomicRef } from "@promin/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

export interface PgRefConfig<T> {
  db: DrizzleDb;
  name: string;
  initial: T;
  table?: string;
}

export class PgRef<T> implements AtomicRef<T> {
  private readonly db: DrizzleDb;
  private readonly name: string;
  private readonly table: string;
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgRefConfig<T>) {
    this.db = config.db;
    this.name = config.name;
    this.table = config.table ?? "promin_ref";
    this.setupPromise = this._setup(config.initial);
  }

  static async make<T>(config: PgRefConfig<T>): Promise<PgRef<T>> {
    const ref = new PgRef(config);
    await ref._ensureReady();
    return ref;
  }

  private async _setup(initial: T): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          name TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
      `),
    );
    // Insert initial value only if row doesn't exist
    await execRaw(
      this.db,
      sql`INSERT INTO ${sql.raw(this.table)} (name, value) VALUES (${this.name}, ${JSON.stringify(initial)}) ON CONFLICT (name) DO NOTHING`,
    );
  }

  private async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  async getAsync(): Promise<T> {
    await this._ensureReady();
    const rows = await execRaw(
      this.db,
      sql`SELECT value FROM ${sql.raw(this.table)} WHERE name = ${this.name}`,
    );
    if (rows.length === 0) throw new Error(`PgRef: row not found for name "${this.name}"`);
    return JSON.parse(rows[0].value as string) as T;
  }

  async setAsync(value: T): Promise<void> {
    await this._ensureReady();
    await execRaw(
      this.db,
      sql`UPDATE ${sql.raw(this.table)} SET value = ${JSON.stringify(value)} WHERE name = ${this.name}`,
    );
  }

  async updateAsync(fn: (current: T) => T): Promise<void> {
    await this._ensureReady();
    const table = this.table;
    const name = this.name;

    await this.db.transaction(async (tx) => {
      const db = tx as DrizzleDb;
      const rows = await execRaw(
        db,
        sql`SELECT value FROM ${sql.raw(table)} WHERE name = ${name} FOR UPDATE`,
      );
      if (rows.length === 0) throw new Error(`PgRef: row not found for name "${name}"`);
      const current = JSON.parse(rows[0].value as string) as T;
      const next = fn(current);
      await execRaw(
        db,
        sql`UPDATE ${sql.raw(table)} SET value = ${JSON.stringify(next)} WHERE name = ${name}`,
      );
    });
  }
}
