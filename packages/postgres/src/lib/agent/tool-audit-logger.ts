// ---------------------------------------------------------------------------
// PostgresToolAuditLogger — durable backend for the elevated-tool audit log.
//
// `createElevatedTool` already enforces that every elevated tool calls
// `ctx.audit()` per invocation and emits one `ToolAuditLogger.record()` per
// call. The in-memory logger covers tests and single-process use; this
// one persists to Postgres so a compliance review survives a restart
// and spans every replica.
//
// `recorded_at` is set by the database clock (server-side NOW()), never
// the caller — an audit trail must not be back-datable by a skewed or
// hostile client. See `agent_audit_log` in `schema.ts` for the DDL.
// ---------------------------------------------------------------------------

import { and, desc, eq, gte, lte, sql } from "drizzle-orm";
import type { ToolAuditEntry, ToolAuditLogger, ToolAuditRecord } from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { agentAuditLog } from "../schema.ts";

export interface PostgresToolAuditLoggerConfig {
  readonly db: DrizzleDb;
}

/** Filters for the audit-log read path. All fields are optional. */
export interface ToolAuditLogQuery {
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

export class PostgresToolAuditLogger implements ToolAuditLogger {
  private readonly db: DrizzleDb;

  constructor(config: PostgresToolAuditLoggerConfig) {
    this.db = config.db;
  }

  async record(entry: ToolAuditEntry): Promise<void> {
    await this.db.insert(agentAuditLog).values({
      namespaceId: entry.namespaceId,
      resourceId: entry.resourceId,
      agentId: entry.agentId ?? null,
      toolName: entry.toolName,
      action: entry.action,
      target: entry.target ?? null,
      meta: entry.meta ?? null,
      // Server clock owns the timestamp — `extract(epoch ...)` yields
      // seconds, ×1000 lands the epoch-ms BIGINT the column expects.
      recordedAt: sql`(extract(epoch from now()) * 1000)::bigint`,
    });
  }

  /**
   * Read recorded entries, newest first. Inspection / review affordance —
   * the `ToolAuditLogger` interface itself is write-only.
   */
  async list(query: ToolAuditLogQuery = {}): Promise<ToolAuditRecord[]> {
    const conditions = [];
    if (query.namespaceId !== undefined) {
      conditions.push(eq(agentAuditLog.namespaceId, query.namespaceId));
    }
    if (query.resourceId !== undefined) {
      conditions.push(eq(agentAuditLog.resourceId, query.resourceId));
    }
    if (query.since !== undefined) {
      conditions.push(gte(agentAuditLog.recordedAt, query.since));
    }
    if (query.until !== undefined) {
      conditions.push(lte(agentAuditLog.recordedAt, query.until));
    }

    let select = this.db
      .select()
      .from(agentAuditLog)
      .where(conditions.length > 0 ? and(...conditions) : undefined)
      .orderBy(desc(agentAuditLog.recordedAt), desc(agentAuditLog.id))
      .$dynamic();
    if (query.limit !== undefined) {
      select = select.limit(query.limit);
    }

    const rows = await select;
    return rows.map(rowToRecord);
  }
}

function rowToRecord(row: typeof agentAuditLog.$inferSelect): ToolAuditRecord {
  return {
    namespaceId: row.namespaceId,
    resourceId: row.resourceId,
    ...(row.agentId !== null ? { agentId: row.agentId } : {}),
    toolName: row.toolName,
    action: row.action,
    ...(row.target !== null ? { target: row.target } : {}),
    ...(row.meta !== null ? { meta: row.meta as Readonly<Record<string, unknown>> } : {}),
    timestamp: row.recordedAt,
  };
}
