// ---------------------------------------------------------------------------
// SqliteAuditLogger — durable backend for the elevated-tool audit log.
//
// Mirror of PostgresAuditLogger for embedded / single-process deployments
// — without it, InMemoryAuditLogger is the only non-Postgres option and a
// SQLite-backed host has no audit trail surviving a restart.
//
// `createElevatedTool` enforces that every elevated tool calls
// `ctx.audit()` per invocation and emits one `record()` per call; this
// turns each into an append-only row a security review can read.
//
// `recorded_at` is logger-assigned, never carried on the `AuditEntry` —
// an audit trail must not be back-datable by the caller. SQLite is
// in-process, so the app clock *is* the database clock (no client/server
// skew to defend against, unlike Postgres); it stays a `Clock` so tests
// can drive it with a `FakeClock`.
//
// Schema (auto-created on first use): one append-only table, INTEGER
// PRIMARY KEY for a strictly monotonic id, `meta` JSON-encoded as TEXT
// (SQLite has no jsonb).
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { AuditEntry, AuditLogger, AuditRecord } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteAuditLoggerConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `agent_audit_log`). */
  readonly table?: string;
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

/** Filters for the audit-log read path. All fields are optional. */
export interface AuditLogQuery {
  /** Restrict to one caller namespace. */
  readonly namespaceId?: string;
  /** Restrict to one caller resource. */
  readonly resourceId?: string;
  /** Only entries recorded at or after this epoch-ms timestamp. */
  readonly since?: number;
  /** Only entries recorded at or before this epoch-ms timestamp. */
  readonly until?: number;
  /** Cap on rows returned. */
  readonly limit?: number;
}

interface DbRow {
  id: number;
  namespace_id: string;
  resource_id: string;
  agent_id: string | null;
  tool_name: string;
  action: string;
  target: string | null;
  meta: string | null;
  recorded_at: number;
}

export class SqliteAuditLogger implements AuditLogger {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: Clock;

  private constructor(config: SqliteAuditLoggerConfig) {
    this.db = config.db;
    this.table = config.table ?? "agent_audit_log";
    this.clock = config.clock ?? SystemClock;
    this._setup();
  }

  static make(config: SqliteAuditLoggerConfig): SqliteAuditLogger {
    return new SqliteAuditLogger(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        namespace_id TEXT NOT NULL,
        resource_id  TEXT NOT NULL,
        agent_id     TEXT,
        tool_name    TEXT NOT NULL,
        action       TEXT NOT NULL,
        target       TEXT,
        meta         TEXT,
        recorded_at  INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_ns_time_idx ON ${this.table} (namespace_id, recorded_at)`,
    );
  }

  async record(entry: AuditEntry): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table}
           (namespace_id, resource_id, agent_id, tool_name, action, target, meta, recorded_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        entry.namespaceId,
        entry.resourceId,
        entry.agentId ?? null,
        entry.toolName,
        entry.action,
        entry.target ?? null,
        entry.meta !== undefined ? JSON.stringify(entry.meta) : null,
        this.clock.currentTimeMs(),
      );
  }

  /**
   * Read recorded entries, newest first. Inspection / review affordance —
   * the `AuditLogger` interface itself is write-only.
   */
  async list(query: AuditLogQuery = {}): Promise<AuditRecord[]> {
    const where: string[] = [];
    const params: unknown[] = [];
    if (query.namespaceId !== undefined) {
      where.push("namespace_id = ?");
      params.push(query.namespaceId);
    }
    if (query.resourceId !== undefined) {
      where.push("resource_id = ?");
      params.push(query.resourceId);
    }
    if (query.since !== undefined) {
      where.push("recorded_at >= ?");
      params.push(query.since);
    }
    if (query.until !== undefined) {
      where.push("recorded_at <= ?");
      params.push(query.until);
    }

    let sql = `SELECT * FROM ${this.table}`;
    if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`;
    sql += " ORDER BY recorded_at DESC, id DESC";
    if (query.limit !== undefined) {
      sql += " LIMIT ?";
      params.push(query.limit);
    }

    return this.db
      .query<DbRow>(sql)
      .all(...params)
      .map(rowToRecord);
  }
}

function rowToRecord(row: DbRow): AuditRecord {
  return {
    namespaceId: row.namespace_id,
    resourceId: row.resource_id,
    ...(row.agent_id !== null ? { agentId: row.agent_id } : {}),
    toolName: row.tool_name,
    action: row.action,
    ...(row.target !== null ? { target: row.target } : {}),
    ...(row.meta !== null
      ? { meta: JSON.parse(row.meta) as Readonly<Record<string, unknown>> }
      : {}),
    timestamp: row.recorded_at,
  };
}
