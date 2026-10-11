// ---------------------------------------------------------------------------
// `SqlitePendingApprovalStore` — `PendingApprovalStore` backed by SQLite:
// the durable index of tool-call approvals thread turns are suspended on.
//
// Schema (auto-created on first use):
//
//   promin_pending_approval ((namespace_id, thread_id, tool_call_id) PK)
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
import { SystemWallClock, type WallClock } from "@promin/workflow";
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
  requested_at: number;
  expires_at: number | null;
  status: string;
  decided_by: string | null;
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
        requested_at INTEGER NOT NULL,
        expires_at   INTEGER,
        status       TEXT NOT NULL,
        decided_by   TEXT,
        decided_at   INTEGER,
        reason       TEXT,
        delivered_at INTEGER,
        PRIMARY KEY (namespace_id, thread_id, tool_call_id)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_ns_status ON ${t} (namespace_id, status, requested_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_thread_status ON ${t} (namespace_id, thread_id, status)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_thread_open ON ${t} (namespace_id, thread_id, delivered_at)`,
    );
  }

  async record(params: RecordPendingApprovalParams): Promise<PendingApproval> {
    return this.db.transaction(() => {
      const existing = this.read(params);
      if (existing && existing.workflowId === params.workflowId) return existing;
      const now = this.clock.currentTimeMs();
      this.db
        .query(
          `INSERT OR REPLACE INTO ${this.table}
             (namespace_id, thread_id, tool_call_id, resource_id, agent_id, workflow_id,
              tool_name, input, requested_at, expires_at, status, decided_by, decided_at, reason,
              delivered_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, NULL)`,
        )
        .run(
          params.namespaceId,
          params.threadId,
          params.toolCallId,
          params.resourceId ?? null,
          params.agentId ?? null,
          params.workflowId,
          params.toolName,
          JSON.stringify(params.input ?? null),
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
    const where = ["namespace_id = ?"];
    const args: unknown[] = [params.namespaceId];
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
    const now = this.clock.currentTimeMs();
    const expired = "(expires_at IS NOT NULL AND expires_at <= ?)";
    const conditions = [
      "namespace_id = ?",
      "thread_id = ?",
      "tool_call_id = ?",
      "status = 'pending'",
    ];
    const args: unknown[] = [params.namespaceId, params.threadId, params.toolCallId];
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
                decided_by = ?, decided_at = ?, reason = ?
          WHERE ${conditions.join(" AND ")}
          RETURNING *`,
      )
      .get(
        now,
        params.approved ? "approved" : "rejected",
        params.decidedBy ?? null,
        now,
        params.reason ?? null,
        ...args,
      );
    if (updated) return { outcome: "decided", approval: toApproval(updated) };
    const row = this.read(params);
    if (!row) return { outcome: "not_found" };
    if (row.status !== "pending") return { outcome: "already_decided", approval: row };
    if (params.onlyIfExpired) return { outcome: "not_expired", approval: row };
    return { outcome: "expired", approval: row };
  }

  async markDelivered(key: PendingApprovalKey): Promise<void> {
    this.db
      .query(
        `UPDATE ${this.table} SET delivered_at = ?
          WHERE namespace_id = ? AND thread_id = ? AND tool_call_id = ? AND delivered_at IS NULL`,
      )
      .run(this.clock.currentTimeMs(), key.namespaceId, key.threadId, key.toolCallId);
  }

  async deleteThread(params: { namespaceId: string; threadId: string }): Promise<void> {
    this.db
      .query(`DELETE FROM ${this.table} WHERE namespace_id = ? AND thread_id = ?`)
      .run(params.namespaceId, params.threadId);
  }

  private read(key: PendingApprovalKey): PendingApproval | null {
    const row = this.db
      .query<DbRow>(
        `SELECT * FROM ${this.table}
          WHERE namespace_id = ? AND thread_id = ? AND tool_call_id = ?`,
      )
      .get(key.namespaceId, key.threadId, key.toolCallId);
    return row ? toApproval(row) : null;
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
    requestedAt: row.requested_at,
    expiresAt: row.expires_at,
    status: row.status as PendingApprovalStatus,
    decidedBy: row.decided_by,
    decidedAt: row.decided_at,
    reason: row.reason,
    deliveredAt: row.delivered_at,
  };
}
