/**
 * Driver-agnostic synchronous SQLite interface.
 *
 * Satisfied directly by `bun:sqlite`'s `Database` class.
 * Compatible with `better-sqlite3` (same `.query()` / `.transaction()` shape).
 *
 * @example bun:sqlite (zero adapter needed)
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db: SqliteDatabase = new Database(":memory:");
 * ```
 *
 * @example better-sqlite3 (thin shim — prepare → query alias)
 * ```ts
 * import Db from "better-sqlite3";
 * const raw = new Db(":memory:");
 * const db: SqliteDatabase = { ...raw, query: (sql) => raw.prepare(sql) };
 * ```
 */
export interface SqliteStatement<T = Record<string, unknown>> {
  /** Return the first matching row, or `null` / `undefined` when no rows match. */
  get(...params: unknown[]): T | null | undefined;
  /** Return all matching rows. */
  all(...params: unknown[]): T[];
  /** Execute the statement (INSERT / UPDATE / DELETE / DDL). */
  run(...params: unknown[]): void;
}

export interface SqliteDatabase {
  /** Execute a statement directly (no return value needed). */
  run(sql: string, ...params: unknown[]): void;
  /** Prepare (or retrieve cached) statement for repeated use. */
  query<T = Record<string, unknown>>(sql: string): SqliteStatement<T>;
  /**
   * Wrap a function in a single atomic transaction.
   * Returns a callable wrapper — invoke the wrapper to execute.
   *
   * ```ts
   * const result = db.transaction(() => { ... })();
   * ```
   */
  transaction<T>(fn: () => T): () => T;
}
