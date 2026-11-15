// ---------------------------------------------------------------------------
// `SqlitePendingApprovalStore` — `PendingApprovalStore` backed by SQLite:
// the durable index of tool-call approvals thread turns are suspended on.
//
// Schema (auto-created on first use):
//
//   promin_pending_approval
//     UNIQUE (namespace_id, agent_id, resource_id, thread_id, tool_call_id)
//
// Time (requested_at / expires_at / decided_at) comes from the injected
// WallClock. SQLite is in-process, so the app clock is the database clock;
// the WallClock keeps it `FakeWallClock`-testable. `record` runs its
// read-check-write in one synchronous transaction; `decide` is one
// conditional `UPDATE ... RETURNING`, so of concurrent deciders exactly one
// gets the row back.
// ---------------------------------------------------------------------------

import type {
  DecidePendingApprovalParams,
  DecidePendingApprovalResult,
  ListPendingApprovalsParams,
  PendingApproval,
  PendingApprovalKey,
  PendingApprovalStatus,
  PendingApprovalStore,
  RecordPendingApprovalParams,
} from "@promin/agent";
import {
  decodeRunSecurityContext,
  type RunPrincipalRef,
  SystemWallClock,
  type WallClock,
} from "@promin/workflow";
import { normalizeRunSecurityContext, sameRunSecurityContext } from "@promin/workflow/storage-kit";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqlitePendingApprovalStoreConfig {
  readonly db: SqliteDatabase;
  /** Table name. Default: `promin_pending_approval`. */
  readonly tableName?: string;
  /** Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock`. */
  readonly clock?: WallClock;
}

interface DbRow {
  namespace_id: string;
  thread_id: string;
  tool_call_id: string;
  resource_id: string | null;
  agent_id: string | null;
  workflow_id: string;
  tool_name: string;
  input: string;
  elevated: number | null;
  request_context: string | null;
  requested_at: number;
  expires_at: number | null;
  status: string;
  decided_by: string | null;
  decided_by_principal: string | null;
  decided_at: number | null;
  reason: string | null;
  delivered_at: number | null;
}

export class SqlitePendingApprovalStore implements PendingApprovalStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: WallClock;

  private constructor(config: SqlitePendingApprovalStoreConfig) {
    this.db = config.db;
    this.table = config.tableName ?? "promin_pending_approval";
    this.clock = config.clock ?? SystemWallClock;
    this.setup();
  }

  static make(config: SqlitePendingApprovalStoreConfig): SqlitePendingApprovalStore {
    return new SqlitePendingApprovalStore(config);
  }

  private setup(): void {
    const t = this.table;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        namespace_id TEXT NOT NULL,
        thread_id    TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        resource_id  TEXT,
        agent_id     TEXT,
        workflow_id  TEXT NOT NULL,
        tool_name    TEXT NOT NULL,
        input        TEXT NOT NULL,
        elevated     INTEGER,
        request_context TEXT,
        requested_at INTEGER NOT NULL,
        expires_at   INTEGER,
        status       TEXT NOT NULL,
        decided_by   TEXT,
        decided_by_principal TEXT,
        decided_at   INTEGER,
        reason       TEXT,
        delivered_at INTEGER
      )
    `);
    let columns = this.db.query<{ name: string; pk: number }>(`PRAGMA table_info(${t})`).all();
    if (!columns.some((column) => column.name === "elevated")) {
      this.db.run(`ALTER TABLE ${t} ADD COLUMN elevated INTEGER`);
    }
    if (!columns.some((column) => column.name === "request_context")) {
      this.db.run(`ALTER TABLE ${t} ADD COLUMN request_context TEXT`);
    }
    if (!columns.some((column) => column.name === "decided_by_principal")) {
      this.db.run(`ALTER TABLE ${t} ADD COLUMN decided_by_principal TEXT`);
    }
    columns = this.db.query<{ name: string; pk: number }>(`PRAGMA table_info(${t})`).all();
    if (columns.some((column) => column.pk > 0)) {
      const migrationTable = `${t}_scope_key_migration`;
      this.db.transaction(() => {
        this.db.run(`DROP TABLE IF EXISTS ${migrationTable}`);
        this.db.run(`CREATE TABLE ${migrationTable} (
          namespace_id TEXT NOT NULL,
          thread_id TEXT NOT NULL,
          tool_call_id TEXT NOT NULL,
          resource_id TEXT,
          agent_id TEXT,
          workflow_id TEXT NOT NULL,
          tool_name TEXT NOT NULL,
          input TEXT NOT NULL,
          elevated INTEGER,
          request_context TEXT,
          requested_at INTEGER NOT NULL,
          expires_at INTEGER,
          status TEXT NOT NULL,
          decided_by TEXT,
          decided_by_principal TEXT,
          decided_at INTEGER,
          reason TEXT,
          delivered_at INTEGER
        )`);
        this.db.run(`INSERT INTO ${migrationTable}
          (namespace_id, thread_id, tool_call_id, resource_id, agent_id, workflow_id,
           tool_name, input, elevated, request_context, requested_at, expires_at, status,
           decided_by, decided_by_principal, decided_at, reason, delivered_at)
          SELECT namespace_id, thread_id, tool_call_id, resource_id, agent_id, workflow_id,
           tool_name, input, elevated, request_context, requested_at, expires_at, status,
           decided_by, decided_by_principal, decided_at, reason, delivered_at FROM ${t}`);
        this.db.run(`DROP TABLE ${t}`);
        this.db.run(`ALTER TABLE ${migrationTable} RENAME TO ${t}`);
      })();
    }
    // SQLite UNIQUE treats NULL values as distinct. Four partial indexes
    // preserve exact nullable-key semantics for scoped and legacy rows.
    this.db.run(`DROP INDEX IF EXISTS ${t}_scope_key`);
    this.db.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${t}_scope_key ON ${t}
      (namespace_id, agent_id, resource_id, thread_id, tool_call_id)
      WHERE agent_id IS NOT NULL AND resource_id IS NOT NULL`);
    this.db.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${t}_scope_key_no_resource ON ${t}
      (namespace_id, agent_id, thread_id, tool_call_id)
      WHERE agent_id IS NOT NULL AND resource_id IS NULL`);
    this.db.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${t}_scope_key_no_agent ON ${t}
      (namespace_id, resource_id, thread_id, tool_call_id)
      WHERE agent_id IS NULL AND resource_id IS NOT NULL`);
    this.db.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${t}_scope_key_unscoped ON ${t}
      (namespace_id, thread_id, tool_call_id)
      WHERE agent_id IS NULL AND resource_id IS NULL`);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_ns_status ON ${t}
       (namespace_id, agent_id, resource_id, status, requested_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_thread_status ON ${t}
       (namespace_id, agent_id, resource_id, thread_id, status)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_thread_open ON ${t}
       (namespace_id, agent_id, resource_id, thread_id, delivered_at)`,
    );
  }

  async record(params: RecordPendingApprovalParams): Promise<PendingApproval> {
    return this.db.transaction(() => {
      const existing = this.read(params);
      if (
        existing &&
        (existing.workflowId === params.workflowId || existing.deliveredAt === null)
      ) {
        return existing;
      }
      if (existing) this.deleteExact(params);
      const now = this.clock.currentTimeMs();
      this.db
        .query(
          `INSERT INTO ${this.table}
             (namespace_id, thread_id, tool_call_id, resource_id, agent_id, workflow_id,
              tool_name, input, elevated, request_context, requested_at, expires_at, status,
              decided_by, decided_by_principal, decided_at, reason, delivered_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, NULL, NULL)`,
        )
        .run(
          params.namespaceId,
          params.threadId,
          params.toolCallId,
          params.resourceId,
          params.agentId,
          params.workflowId,
          params.toolName,
          JSON.stringify(params.input ?? null),
          params.elevated ? 1 : 0,
          params.requestContext === null
            ? null
            : JSON.stringify(normalizeRunSecurityContext(params.requestContext)),
          now,
          params.ttlMs !== undefined ? now + params.ttlMs : null,
        );
      return this.read(params)!;
    })();
  }

  async get(key: PendingApprovalKey): Promise<PendingApproval | null> {
    return this.read(key);
  }

  async list(params: ListPendingApprovalsParams): Promise<PendingApproval[]> {
    const where = ["namespace_id = ?", "agent_id IS ?", "resource_id IS ?"];
    const args: unknown[] = [params.namespaceId, params.agentId, params.resourceId];
    if (params.open) {
      where.push("delivered_at IS NULL");
    } else {
      where.push("status = ?");
      args.push(params.status ?? "pending");
    }
    if (params.threadId !== undefined) {
      where.push("thread_id = ?");
      args.push(params.threadId);
    }
    args.push(params.limit ?? 100);
    return this.db
      .query<DbRow>(
        `SELECT * FROM ${this.table} WHERE ${where.join(" AND ")}
         ORDER BY requested_at ASC, thread_id ASC, tool_call_id ASC LIMIT ?`,
      )
      .all(...args)
      .map(toApproval);
  }

  async decide(params: DecidePendingApprovalParams): Promise<DecidePendingApprovalResult> {
    const expected = snapshotExpected(params.expected);
    const now = this.clock.currentTimeMs();
    const expired = "(expires_at IS NOT NULL AND expires_at <= ?)";
    const conditions = [
      "namespace_id = ?",
      "agent_id IS ?",
      "resource_id IS ?",
      "thread_id = ?",
      "tool_call_id = ?",
      "workflow_id = ?",
      "elevated IS ?",
      "request_context IS ?",
      "status = 'pending'",
    ];
    const args: unknown[] = [
      expected.namespaceId,
      expected.agentId,
      expected.resourceId,
      expected.threadId,
      expected.toolCallId,
      expected.workflowId,
      expected.elevated === null ? null : expected.elevated ? 1 : 0,
      expected.requestContext === null ? null : JSON.stringify(expected.requestContext),
    ];
    if (params.approved) {
      conditions.push(`NOT ${expired}`);
      args.push(now);
    }
    if (params.onlyIfExpired) {
      conditions.push(expired);
      args.push(now);
    }
    // The UPDATE's own result decides the outcome: a row back means this
    // call recorded the decision.
    const updated = this.db
      .query<DbRow>(
        `UPDATE ${this.table}
            SET status = CASE WHEN ${expired} THEN 'expired' ELSE ? END,
                decided_by = ?, decided_by_principal = ?, decided_at = ?, reason = ?
          WHERE ${conditions.join(" AND ")}
          RETURNING *`,
      )
      .get(
        now,
        params.approved ? "approved" : "rejected",
        params.decidedBy ?? params.decidedByPrincipal?.id ?? null,
        params.decidedByPrincipal === undefined
          ? null
          : JSON.stringify(normalizeRunSecurityContext({ actor: params.decidedByPrincipal }).actor),
        now,
        params.reason ?? null,
        ...args,
      );
    if (updated) return { outcome: "decided", approval: toApproval(updated) };
    const row = this.read(expected);
    if (!row) return { outcome: "not_found" };
    if (!matchesExpected(row, expected)) return { outcome: "conflict", approval: row };
    if (row.status !== "pending") return { outcome: "already_decided", approval: row };
    if (params.onlyIfExpired) return { outcome: "not_expired", approval: row };
    return { outcome: "expired", approval: row };
  }

  async markDelivered(key: PendingApprovalKey): Promise<void> {
    this.db
      .query(
        `UPDATE ${this.table} SET delivered_at = ?
          WHERE namespace_id = ? AND agent_id IS ? AND resource_id IS ?
            AND thread_id = ? AND tool_call_id = ? AND delivered_at IS NULL`,
      )
      .run(
        this.clock.currentTimeMs(),
        key.namespaceId,
        key.agentId,
        key.resourceId,
        key.threadId,
        key.toolCallId,
      );
  }

  async deleteThread(params: {
    namespaceId: string;
    agentId: string | null;
    resourceId: string | null;
    threadId: string;
  }): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.table}
          WHERE namespace_id = ? AND agent_id IS ? AND resource_id IS ? AND thread_id = ?`,
      )
      .run(params.namespaceId, params.agentId, params.resourceId, params.threadId);
  }

  private read(key: PendingApprovalKey): PendingApproval | null {
    const row = this.db
      .query<DbRow>(
        `SELECT * FROM ${this.table}
          WHERE namespace_id = ? AND agent_id IS ? AND resource_id IS ?
            AND thread_id = ? AND tool_call_id = ?`,
      )
      .get(key.namespaceId, key.agentId, key.resourceId, key.threadId, key.toolCallId);
    return row ? toApproval(row) : null;
  }

  private deleteExact(key: PendingApprovalKey): void {
    this.db
      .query(
        `DELETE FROM ${this.table}
          WHERE namespace_id = ? AND agent_id IS ? AND resource_id IS ?
            AND thread_id = ? AND tool_call_id = ?`,
      )
      .run(key.namespaceId, key.agentId, key.resourceId, key.threadId, key.toolCallId);
  }
}

function toApproval(row: DbRow): PendingApproval {
  return {
    namespaceId: row.namespace_id,
    threadId: row.thread_id,
    toolCallId: row.tool_call_id,
    resourceId: row.resource_id,
    agentId: row.agent_id,
    workflowId: row.workflow_id,
    toolName: row.tool_name,
    input: JSON.parse(row.input) as unknown,
    elevated: row.elevated === null ? null : row.elevated === 1,
    requestContext:
      row.request_context === null
        ? null
        : (decodeRunSecurityContext(JSON.parse(row.request_context) as unknown) ?? null),
    requestedAt: row.requested_at,
    expiresAt: row.expires_at,
    status: row.status as PendingApprovalStatus,
    decidedBy: row.decided_by,
    decidedByPrincipal: decodePrincipal(row.decided_by_principal),
    decidedAt: row.decided_at,
    reason: row.reason,
    deliveredAt: row.delivered_at,
  };
}

function decodePrincipal(value: string | null): RunPrincipalRef | null {
  if (value === null) return null;
  try {
    return decodeRunSecurityContext({ actor: JSON.parse(value) })?.actor ?? null;
  } catch {
    return null;
  }
}

function snapshotExpected(expected: DecidePendingApprovalParams["expected"]) {
  return Object.freeze({
    namespaceId: expected.namespaceId,
    agentId: expected.agentId,
    resourceId: expected.resourceId,
    threadId: expected.threadId,
    toolCallId: expected.toolCallId,
    workflowId: expected.workflowId,
    elevated: expected.elevated,
    requestContext:
      expected.requestContext === null
        ? null
        : normalizeRunSecurityContext(expected.requestContext),
  });
}

function matchesExpected(
  row: PendingApproval,
  expected: DecidePendingApprovalParams["expected"],
): boolean {
  return (
    row.workflowId === expected.workflowId &&
    row.elevated === expected.elevated &&
    sameRunSecurityContext(row.requestContext ?? undefined, expected.requestContext ?? undefined)
  );
}
