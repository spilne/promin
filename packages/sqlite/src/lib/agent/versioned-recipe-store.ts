// ---------------------------------------------------------------------------
// VersionedRecipeStore — shared SQL plumbing for any recipe-like type
// (agent recipes, dag recipes, future kinds) that wants:
//
//   - immutable rows keyed on (id, version)
//   - createdAt-preserve / updatedAt-advance upsert semantics
//   - get-latest-by-updatedAt (no version arg)
//   - get-specific-version
//   - list (newest-per-id, optionally filtered)
//   - versions(id) — full history
//   - unregister(id, version?) — drop one or all
//
// Public registry interfaces (AgentRegistry, DagRegistry) stay
// type-specific — strongly-typed payload shapes, validation rules, list
// filters that match the kind. This factory just shares the SQL.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE <table> (
//     id         TEXT NOT NULL,
//     version    TEXT NOT NULL,
//     body       TEXT NOT NULL,    -- JSON blob, kind-specific
//     created_at INTEGER NOT NULL,
//     updated_at INTEGER NOT NULL,
//     PRIMARY KEY (id, version)
//   )
// ---------------------------------------------------------------------------

import type { SqliteDatabase } from "../sqlite-database.ts";

export interface VersionedRecipeStoreConfig<TStored, TInput> {
  readonly db: SqliteDatabase;
  /** SQLite table name. Required — caller chooses the namespace. */
  readonly table: string;
  /** Default version when caller's input.version is omitted. */
  readonly defaultVersion: string;
  /**
   * Decode a JSON body (already-parsed) + identity into the typed
   * stored shape. Receives `(id, version, body, createdAt, updatedAt)`.
   */
  readonly fromRow: (args: {
    id: string;
    version: string;
    body: unknown;
    createdAt: number;
    updatedAt: number;
  }) => TStored;
  /**
   * Build the JSON body to persist from the caller's typed input plus
   * the assigned version. The id + version + timestamps are stored in
   * dedicated columns; everything else lives in the body blob.
   */
  readonly toBody: (input: TInput, assignedVersion: string) => unknown;
  /**
   * Optional validator — runs at register time. Throw to reject. Bad
   * recipes never enter the store. Use for shape checks, cycle
   * detection on graphs, etc.
   */
  readonly validate?: (input: TInput, assignedVersion: string) => void;
  /** Optional clock for tests. Default: `Date.now()`. */
  readonly now?: () => number;
}

export interface VersionedRecipeStore<TStored, TInput extends { id: string; version?: string }> {
  register(input: TInput): Promise<TStored>;
  get(id: string, version?: string): Promise<TStored | null>;
  /**
   * Newest version per id. Returns plain rows; consumers can layer
   * type-specific filters (capability, tag, kind) on top in JS — those
   * live inside the body blob and SQL indices won't help.
   */
  list(params?: { limit?: number; cursor?: string }): Promise<TStored[]>;
  versions(id: string): Promise<TStored[]>;
  unregister(id: string, version?: string): Promise<void>;
}

interface DbRow {
  id: string;
  version: string;
  body: string;
  created_at: number;
  updated_at: number;
}

export function createVersionedRecipeStore<
  TStored,
  TInput extends { id: string; version?: string },
>(config: VersionedRecipeStoreConfig<TStored, TInput>): VersionedRecipeStore<TStored, TInput> {
  const { db, table, defaultVersion, fromRow, toBody, validate, now } = config;
  const clock = now ?? (() => Date.now());

  // Schema setup — idempotent on re-open.
  db.run(`
    CREATE TABLE IF NOT EXISTS ${table} (
      id         TEXT NOT NULL,
      version    TEXT NOT NULL,
      body       TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (id, version)
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS ${table}_id_updated ON ${table} (id, updated_at)`);

  const decode = (r: DbRow): TStored =>
    fromRow({
      id: r.id,
      version: r.version,
      body: JSON.parse(r.body),
      createdAt: r.created_at,
      updatedAt: r.updated_at,
    });

  return {
    async register(input) {
      const version = input.version ?? defaultVersion;
      validate?.(input, version);

      // Look up existing row to preserve createdAt on upsert.
      const existing = db
        .query<DbRow>(`SELECT * FROM ${table} WHERE id = ? AND version = ?`)
        .get(input.id, version);
      const ts = clock();
      const createdAt = existing?.created_at ?? ts;
      const body = JSON.stringify(toBody(input, version));

      db.query(
        `INSERT INTO ${table} (id, version, body, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id, version) DO UPDATE SET
           body = excluded.body,
           updated_at = excluded.updated_at`,
      ).run(input.id, version, body, createdAt, ts);

      return decode({
        id: input.id,
        version,
        body,
        created_at: createdAt,
        updated_at: ts,
      });
    },

    async get(id, version) {
      if (version !== undefined) {
        const row = db
          .query<DbRow>(`SELECT * FROM ${table} WHERE id = ? AND version = ?`)
          .get(id, version);
        return row ? decode(row) : null;
      }
      const row = db
        .query<DbRow>(
          `SELECT * FROM ${table} WHERE id = ?
           ORDER BY updated_at DESC, rowid DESC LIMIT 1`,
        )
        .get(id);
      return row ? decode(row) : null;
    },

    async list(params = {}) {
      // Newest-per-id by updated_at. Window function would be cleaner
      // but SQLite's older versions don't all have it; the GROUP BY
      // sub-select works everywhere.
      const rows = db
        .query<DbRow>(
          `SELECT t.* FROM ${table} t
           INNER JOIN (
             SELECT id, MAX(updated_at) AS max_u FROM ${table} GROUP BY id
           ) latest
             ON latest.id = t.id AND latest.max_u = t.updated_at
           ORDER BY t.created_at ASC, t.rowid ASC`,
        )
        .all();
      const all = rows.map(decode);
      const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
      const limit = params.limit ?? all.length;
      return all.slice(offset, offset + limit);
    },

    async versions(id) {
      const rows = db
        .query<DbRow>(`SELECT * FROM ${table} WHERE id = ? ORDER BY created_at ASC, rowid ASC`)
        .all(id);
      return rows.map(decode);
    },

    async unregister(id, version) {
      if (version !== undefined) {
        db.query(`DELETE FROM ${table} WHERE id = ? AND version = ?`).run(id, version);
        return;
      }
      db.query(`DELETE FROM ${table} WHERE id = ?`).run(id);
    },
  };
}
