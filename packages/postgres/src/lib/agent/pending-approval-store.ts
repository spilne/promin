// ---------------------------------------------------------------------------
// PostgresPendingApprovalStore — `PendingApprovalStore` over the
// `agent_pending_approval` table: the durable index of tool-call approvals
// thread turns are suspended on, shared by every replica on the database.
//
// Time comes from the database server, never the app: requested_at,
// expires_at and decided_at are stamped with `statement_timestamp()` (as
// epoch ms) and expiry is compared against it, so every process sharing
// the store agrees on whether an approval has expired.
//
//   record — INSERT ... ON CONFLICT DO UPDATE ... WHERE the existing row
//            belongs to another workflow (a re-record from the same
//            workflow is a no-op), then read the row back.
//   decide — one conditional UPDATE ... WHERE status = 'pending' (and, for
//            an approval, not expired; with onlyIfExpired, expired)
//            RETURNING; of concurrent deciders exactly one gets a row back.
//            Rejecting an expired row records status 'expired'.
// ---------------------------------------------------------------------------

import { and, asc, eq, isNull, sql } from "drizzle-orm";
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
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { agentPendingApproval as t } from "../schema.ts";

export interface PostgresPendingApprovalStoreConfig {
  readonly db: DrizzleDb;
}

/** The database server's time as epoch ms, fixed for one statement. */
const DB_NOW_MS = sql<number>`(EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint`;

type Row = typeof t.$inferSelect;

export class PostgresPendingApprovalStore implements PendingApprovalStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresPendingApprovalStoreConfig) {
    this.db = config.db;
  }

  async record(params: RecordPendingApprovalParams): Promise<PendingApproval> {
    const expiresAt =
      params.ttlMs !== undefined ? sql<number>`${DB_NOW_MS} + ${params.ttlMs}` : null;
    await this.db
      .insert(t)
      .values({
        namespaceId: params.namespaceId,
        threadId: params.threadId,
        toolCallId: params.toolCallId,
        resourceId: params.resourceId ?? null,
        agentId: params.agentId ?? null,
        workflowId: params.workflowId,
        toolName: params.toolName,
        input: params.input ?? null,
        requestedAt: DB_NOW_MS,
        expiresAt,
        status: "pending",
      })
      .onConflictDoUpdate({
        target: [t.namespaceId, t.threadId, t.toolCallId],
        set: {
          resourceId: sql`excluded.resource_id`,
          agentId: sql`excluded.agent_id`,
          workflowId: sql`excluded.workflow_id`,
          toolName: sql`excluded.tool_name`,
          input: sql`excluded.input`,
          requestedAt: sql`excluded.requested_at`,
          expiresAt: sql`excluded.expires_at`,
          status: "pending",
          decidedBy: null,
          decidedAt: null,
          reason: null,
          deliveredAt: null,
        },
        setWhere: sql`${t.workflowId} <> excluded.workflow_id`,
      });
    const row = await this.get(params);
    if (!row) throw new Error("PostgresPendingApprovalStore.record: row vanished after upsert");
    return row;
  }

  async get(key: PendingApprovalKey): Promise<PendingApproval | null> {
    const rows = await this.db.select().from(t).where(keyWhere(key)).limit(1);
    return rows[0] ? toApproval(rows[0]) : null;
  }

  async list(params: ListPendingApprovalsParams): Promise<PendingApproval[]> {
    const conditions = [
      eq(t.namespaceId, params.namespaceId),
      params.open ? isNull(t.deliveredAt) : eq(t.status, params.status ?? "pending"),
    ];
    if (params.threadId !== undefined) conditions.push(eq(t.threadId, params.threadId));
    const rows = await this.db
      .select()
      .from(t)
      .where(and(...conditions))
      .orderBy(asc(t.requestedAt), asc(t.threadId), asc(t.toolCallId))
      .limit(params.limit ?? 100);
    return rows.map(toApproval);
  }

  async decide(params: DecidePendingApprovalParams): Promise<DecidePendingApprovalResult> {
    const isExpired = sql`(${t.expiresAt} IS NOT NULL AND ${t.expiresAt} <= ${DB_NOW_MS})`;
    const notExpired = params.approved ? sql`NOT ${isExpired}` : sql`TRUE`;
    const onlyExpired = params.onlyIfExpired ? isExpired : sql`TRUE`;
    const updated = await this.db
      .update(t)
      .set({
        status: sql`CASE WHEN ${isExpired} THEN 'expired' ELSE ${
          params.approved ? "approved" : "rejected"
        } END`,
        decidedBy: params.decidedBy ?? null,
        decidedAt: DB_NOW_MS,
        reason: params.reason ?? null,
      })
      .where(and(keyWhere(params), eq(t.status, "pending"), notExpired, onlyExpired))
      .returning();
    if (updated[0]) return { outcome: "decided", approval: toApproval(updated[0]) };

    const row = await this.get(params);
    if (!row) return { outcome: "not_found" };
    if (row.status !== "pending") return { outcome: "already_decided", approval: row };
    // Still pending, yet the update matched nothing: a live row under
    // onlyIfExpired, or an approval of an expired one.
    if (params.onlyIfExpired) return { outcome: "not_expired", approval: row };
    return { outcome: "expired", approval: row };
  }

  async markDelivered(key: PendingApprovalKey): Promise<void> {
    await this.db
      .update(t)
      .set({ deliveredAt: DB_NOW_MS })
      .where(and(keyWhere(key), isNull(t.deliveredAt)));
  }

  async deleteThread(params: { namespaceId: string; threadId: string }): Promise<void> {
    await this.db
      .delete(t)
      .where(and(eq(t.namespaceId, params.namespaceId), eq(t.threadId, params.threadId)));
  }
}

function keyWhere(key: PendingApprovalKey) {
  return and(
    eq(t.namespaceId, key.namespaceId),
    eq(t.threadId, key.threadId),
    eq(t.toolCallId, key.toolCallId),
  );
}

function toApproval(row: Row): PendingApproval {
  return {
    namespaceId: row.namespaceId,
    threadId: row.threadId,
    toolCallId: row.toolCallId,
    resourceId: row.resourceId,
    agentId: row.agentId,
    workflowId: row.workflowId,
    toolName: row.toolName,
    input: row.input,
    requestedAt: Number(row.requestedAt),
    expiresAt: row.expiresAt === null ? null : Number(row.expiresAt),
    status: row.status as PendingApprovalStatus,
    decidedBy: row.decidedBy,
    decidedAt: row.decidedAt === null ? null : Number(row.decidedAt),
    reason: row.reason,
    deliveredAt: row.deliveredAt === null ? null : Number(row.deliveredAt),
  };
}
