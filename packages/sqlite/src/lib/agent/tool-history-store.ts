// ---------------------------------------------------------------------------
// SqliteToolHistoryStore — durable backend for the tool catalog history,
// for embedded / single-process deployments.
//
// Mirror of PostgresToolHistoryStore: `recordSnapshot` upserts each
// observation keyed by the (name, source_kind, source_detail,
// schema_hash) identity tuple — a new tuple inserts with first/last seen
// at now, an existing one advances last_seen_at and refreshes the
// description.
//
// first/last_seen_at come from an injectable `Clock` (default
// `SystemClock`). SQLite is in-process, so the app clock is the database
// clock; the `Clock` keeps it `FakeClock`-testable.
//
// Schema (auto-created on first use): one row per identity tuple.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type {
  ToolHistoryQuery,
  ToolHistoryRecord,
  ToolHistoryStore,
  ToolObservation,
} from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteToolHistoryStoreConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `agent_tool_history`). */
  readonly table?: string;
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

interface DbRow {
  name: string;
  source_kind: string;
  source_detail: string;
  schema_hash: string;
  description: string;
  first_seen_at: number;
  last_seen_at: number;
}

export class SqliteToolHistoryStore implements ToolHistoryStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: Clock;

  private constructor(config: SqliteToolHistoryStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "agent_tool_history";
    this.clock = config.clock ?? SystemClock;
    this._setup();
  }

  static make(config: SqliteToolHistoryStoreConfig): SqliteToolHistoryStore {
    return new SqliteToolHistoryStore(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        name          TEXT NOT NULL,
        source_kind   TEXT NOT NULL,
        source_detail TEXT NOT NULL DEFAULT '',
        schema_hash   TEXT NOT NULL,
        description   TEXT NOT NULL,
        first_seen_at INTEGER NOT NULL,
        last_seen_at  INTEGER NOT NULL,
        PRIMARY KEY (name, source_kind, source_detail, schema_hash)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_name_idx ON ${this.table} (name, last_seen_at)`,
    );
  }

  async recordSnapshot(observations: ReadonlyArray<ToolObservation>): Promise<void> {
    const now = this.clock.currentTimeMs();
    const stmt = this.db.query(
      `INSERT INTO ${this.table}
         (name, source_kind, source_detail, schema_hash, description, first_seen_at, last_seen_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(name, source_kind, source_detail, schema_hash) DO UPDATE SET
         last_seen_at = excluded.last_seen_at,
         description = excluded.description`,
    );
    for (const o of observations) {
      stmt.run(o.name, o.sourceKind, o.sourceDetail, o.schemaHash, o.description, now, now);
    }
  }

  async list(query: ToolHistoryQuery = {}): Promise<ToolHistoryRecord[]> {
    const where: string[] = [];
    const params: unknown[] = [];
    if (query.name !== undefined) {
      where.push("name = ?");
      params.push(query.name);
    }
    if (query.sourceKind !== undefined) {
      where.push("source_kind = ?");
      params.push(query.sourceKind);
    }
    if (query.since !== undefined) {
      where.push("last_seen_at >= ?");
      params.push(query.since);
    }

    let sql = `SELECT * FROM ${this.table}`;
    if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`;
    sql += " ORDER BY last_seen_at DESC, name ASC";
    if (query.limit !== undefined) {
      sql += " LIMIT ?";
      params.push(query.limit);
    }

    return this.db
      .query<DbRow>(sql)
      .all(...params)
      .map((row) => ({
        name: row.name,
        sourceKind: row.source_kind as ToolObservation["sourceKind"],
        sourceDetail: row.source_detail,
        schemaHash: row.schema_hash,
        description: row.description,
        firstSeenAt: row.first_seen_at,
        lastSeenAt: row.last_seen_at,
      }));
  }
}
