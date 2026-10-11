// ---------------------------------------------------------------------------
// InMemoryPendingApprovalStore — single-process `PendingApprovalStore` for
// tests, dev and single-process hosts. Time comes from the injected
// WallClock. Passes `pendingApprovalStoreTestSuite`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import type {
  DecidePendingApprovalParams,
  DecidePendingApprovalResult,
  ListPendingApprovalsParams,
  PendingApproval,
  PendingApprovalKey,
  PendingApprovalStore,
  RecordPendingApprovalParams,
} from "./pending-approval-store.ts";

export interface InMemoryPendingApprovalStoreConfig {
  /** Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryPendingApprovalStore implements PendingApprovalStore {
  private readonly clock: WallClock;
  private readonly rows = new Map<string, PendingApproval>();

  constructor(config: InMemoryPendingApprovalStoreConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async record(params: RecordPendingApprovalParams): Promise<PendingApproval> {
    const k = encodeKey(params);
    const existing = this.rows.get(k);
    if (existing && existing.workflowId === params.workflowId) return existing;
    const now = this.clock.currentTimeMs();
    const row: PendingApproval = {
      namespaceId: params.namespaceId,
      threadId: params.threadId,
      toolCallId: params.toolCallId,
      resourceId: params.resourceId ?? null,
      agentId: params.agentId ?? null,
      workflowId: params.workflowId,
      toolName: params.toolName,
      input: params.input,
      requestedAt: now,
      expiresAt: params.ttlMs !== undefined ? now + params.ttlMs : null,
      status: "pending",
      decidedBy: null,
      decidedAt: null,
      reason: null,
      deliveredAt: null,
    };
    this.rows.set(k, row);
    return row;
  }

  async get(key: PendingApprovalKey): Promise<PendingApproval | null> {
    return this.rows.get(encodeKey(key)) ?? null;
  }

  async list(params: ListPendingApprovalsParams): Promise<PendingApproval[]> {
    const status = params.status ?? "pending";
    const limit = params.limit ?? 100;
    return [...this.rows.values()]
      .filter(
        (r) =>
          r.namespaceId === params.namespaceId &&
          (params.threadId === undefined || r.threadId === params.threadId) &&
          (params.open ? r.deliveredAt === null : r.status === status),
      )
      .sort((a, b) => a.requestedAt - b.requestedAt)
      .slice(0, limit);
  }

  async decide(params: DecidePendingApprovalParams): Promise<DecidePendingApprovalResult> {
    const k = encodeKey(params);
    const row = this.rows.get(k);
    if (!row) return { outcome: "not_found" };
    if (row.status !== "pending") return { outcome: "already_decided", approval: row };
    const now = this.clock.currentTimeMs();
    const expired = row.expiresAt !== null && row.expiresAt <= now;
    if (params.onlyIfExpired && !expired) return { outcome: "not_expired", approval: row };
    if (params.approved && expired) return { outcome: "expired", approval: row };
    const decided: PendingApproval = {
      ...row,
      status: expired ? "expired" : params.approved ? "approved" : "rejected",
      decidedBy: params.decidedBy ?? null,
      decidedAt: now,
      reason: params.reason ?? null,
    };
    this.rows.set(k, decided);
    return { outcome: "decided", approval: decided };
  }

  async markDelivered(key: PendingApprovalKey): Promise<void> {
    const k = encodeKey(key);
    const row = this.rows.get(k);
    if (row && row.deliveredAt === null) {
      this.rows.set(k, { ...row, deliveredAt: this.clock.currentTimeMs() });
    }
  }

  async deleteThread(params: { namespaceId: string; threadId: string }): Promise<void> {
    for (const [k, r] of this.rows) {
      if (r.namespaceId === params.namespaceId && r.threadId === params.threadId) {
        this.rows.delete(k);
      }
    }
  }
}

function encodeKey(key: PendingApprovalKey): string {
  return JSON.stringify([key.namespaceId, key.threadId, key.toolCallId]);
}
