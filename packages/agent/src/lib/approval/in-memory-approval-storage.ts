// ---------------------------------------------------------------------------
// `InMemoryApprovalStorage` — reference impl. Holds every request in a
// Map keyed by requestId. Cheap, deterministic, and the conformance
// baseline persistent backends must match.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  ApprovalDecisionConflictError,
  type ApprovalRequest,
  type ApprovalStorage,
  type CreateApprovalRequestInput,
  type DecideApprovalInput,
  type ListApprovalsParams,
} from "./types.ts";

export interface InMemoryApprovalStorageConfig {
  readonly clock?: Clock;
}

export class InMemoryApprovalStorage implements ApprovalStorage {
  private readonly clock: Clock;
  private readonly rows = new Map<string, ApprovalRequest>();

  constructor(config: InMemoryApprovalStorageConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  async create(input: CreateApprovalRequestInput): Promise<ApprovalRequest> {
    if (!input.requestId) throw new Error("create: requestId required");
    const existing = this.rows.get(input.requestId);
    // Idempotent on same requestId — re-create just returns the prior row.
    // The bus can fire approval.requested more than once on rare paths
    // (replay edge cases), and we don't want to clobber a row that may
    // already have a decision.
    if (existing) return existing;
    const row: ApprovalRequest = {
      requestId: input.requestId,
      workflowId: input.workflowId,
      signalName: input.signalName,
      namespaceId: input.namespaceId,
      resourceId: input.resourceId ?? null,
      toolName: input.toolName,
      toolInput: input.toolInput,
      summary: input.summary,
      status: "pending",
      createdAt: this.clock.currentTimeMs(),
      decidedAt: null,
      decidedBy: null,
      decisionReason: null,
      expiresAt: input.expiresAt ?? null,
      metadata: { ...input.metadata },
    };
    this.rows.set(row.requestId, row);
    return row;
  }

  async decide(input: DecideApprovalInput): Promise<ApprovalRequest> {
    const row = this.rows.get(input.requestId);
    if (!row) {
      throw new Error(`decide: no row for requestId "${input.requestId}"`);
    }
    if (row.status === input.decision) {
      // Idempotent retry — no-op.
      return row;
    }
    if (row.status !== "pending") {
      throw new ApprovalDecisionConflictError(input.requestId, row.status, input.decision);
    }
    const next: ApprovalRequest = {
      ...row,
      status: input.decision,
      decidedAt: input.decidedAt ?? this.clock.currentTimeMs(),
      decidedBy: input.decidedBy,
      decisionReason: input.reason ?? null,
    };
    this.rows.set(row.requestId, next);
    return next;
  }

  async get(requestId: string): Promise<ApprovalRequest | null> {
    return this.rows.get(requestId) ?? null;
  }

  async list(params: ListApprovalsParams = {}): Promise<ApprovalRequest[]> {
    const filtered = Array.from(this.rows.values()).filter((r) => {
      if (params.namespaceId !== undefined && r.namespaceId !== params.namespaceId) return false;
      if (params.status !== undefined && r.status !== params.status) return false;
      if (params.toolName !== undefined && r.toolName !== params.toolName) return false;
      return true;
    });
    const order = params.order ?? "createdDesc";
    filtered.sort((a, b) =>
      order === "createdAsc" ? a.createdAt - b.createdAt : b.createdAt - a.createdAt,
    );
    return params.limit !== undefined ? filtered.slice(0, params.limit) : filtered;
  }

  async expirePending(now: number): Promise<number> {
    let expired = 0;
    for (const [id, row] of this.rows) {
      if (row.status !== "pending") continue;
      if (row.expiresAt === null) continue;
      if (row.expiresAt > now) continue;
      this.rows.set(id, { ...row, status: "expired", decidedAt: now });
      expired += 1;
    }
    return expired;
  }
}
