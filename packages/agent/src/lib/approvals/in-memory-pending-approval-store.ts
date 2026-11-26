// ---------------------------------------------------------------------------
// InMemoryPendingApprovalStore — single-process `PendingApprovalStore` for
// tests, dev and single-process hosts. Time comes from the injected
// WallClock. Passes `pendingApprovalStoreTestSuite`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import { normalizeRunSecurityContext, sameRunSecurityContext } from "@promin/workflow/storage-kit";
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
    const request = snapshotRecordRequest(params);
    const k = encodeKey(request);
    const existing = this.rows.get(k);
    if (existing && (existing.workflowId === request.workflowId || existing.deliveredAt === null)) {
      return detached(existing);
    }
    const now = this.clock.currentTimeMs();
    const row = immutable({
      namespaceId: request.namespaceId,
      threadId: request.threadId,
      toolCallId: request.toolCallId,
      resourceId: request.resourceId ?? null,
      agentId: request.agentId ?? null,
      workflowId: request.workflowId,
      toolName: request.toolName,
      input: request.input,
      elevated: request.elevated,
      requestContext: request.requestContext,
      requestedAt: now,
      expiresAt: request.ttlMs !== undefined ? now + request.ttlMs : null,
      status: "pending",
      decidedBy: null,
      decidedByPrincipal: null,
      decidedAt: null,
      reason: null,
      deliveredAt: null,
    });
    this.rows.set(k, row);
    return detached(row);
  }

  async get(key: PendingApprovalKey): Promise<PendingApproval | null> {
    const row = this.rows.get(encodeKey(key));
    return row ? detached(row) : null;
  }

  async list(params: ListPendingApprovalsParams): Promise<PendingApproval[]> {
    const status = params.status ?? "pending";
    const limit = params.limit ?? 100;
    return [...this.rows.values()]
      .filter(
        (r) =>
          r.namespaceId === params.namespaceId &&
          r.agentId === params.agentId &&
          r.resourceId === params.resourceId &&
          (params.threadId === undefined || r.threadId === params.threadId) &&
          (params.open ? r.deliveredAt === null : r.status === status),
      )
      .sort((a, b) => a.requestedAt - b.requestedAt)
      .slice(0, limit)
      .map(detached);
  }

  async decide(params: DecidePendingApprovalParams): Promise<DecidePendingApprovalResult> {
    const expected = snapshotExpected(params.expected);
    const k = encodeKey(expected);
    const row = this.rows.get(k);
    if (!row) return { outcome: "not_found" };
    if (!matchesExpected(row, expected)) return { outcome: "conflict", approval: detached(row) };
    if (row.status !== "pending") return { outcome: "already_decided", approval: detached(row) };
    const now = this.clock.currentTimeMs();
    const expired = row.expiresAt !== null && row.expiresAt <= now;
    if (params.onlyIfExpired && !expired)
      return { outcome: "not_expired", approval: detached(row) };
    if (params.approved && expired) return { outcome: "expired", approval: detached(row) };
    const decided = immutable({
      ...row,
      status: expired ? "expired" : params.approved ? "approved" : "rejected",
      decidedBy: params.decidedBy ?? params.decidedByPrincipal?.id ?? null,
      decidedByPrincipal:
        params.decidedByPrincipal === undefined
          ? null
          : normalizeRunSecurityContext({ actor: params.decidedByPrincipal }).actor,
      decidedAt: now,
      reason: params.reason ?? null,
    });
    this.rows.set(k, decided);
    return { outcome: "decided", approval: detached(decided) };
  }

  async markDelivered(key: PendingApprovalKey): Promise<void> {
    const k = encodeKey(key);
    const row = this.rows.get(k);
    if (row && row.deliveredAt === null) {
      this.rows.set(k, immutable({ ...row, deliveredAt: this.clock.currentTimeMs() }));
    }
  }

  async deleteThread(params: {
    namespaceId: string;
    agentId: string | null;
    resourceId: string | null;
    threadId: string;
  }): Promise<void> {
    for (const [k, r] of this.rows) {
      if (
        r.namespaceId === params.namespaceId &&
        r.agentId === params.agentId &&
        r.resourceId === params.resourceId &&
        r.threadId === params.threadId
      ) {
        this.rows.delete(k);
      }
    }
  }
}

function encodeKey(key: PendingApprovalKey): string {
  return JSON.stringify([
    key.namespaceId,
    key.agentId,
    key.resourceId,
    key.threadId,
    key.toolCallId,
  ]);
}

function snapshotRecordRequest(params: RecordPendingApprovalParams): RecordPendingApprovalParams {
  const namespaceId = params.namespaceId;
  const threadId = params.threadId;
  const toolCallId = params.toolCallId;
  const resourceId = params.resourceId;
  const agentId = params.agentId;
  const workflowId = params.workflowId;
  const toolName = params.toolName;
  const input = params.input;
  const elevated = params.elevated;
  const requestContext = params.requestContext;
  const ttlMs = params.ttlMs;
  return Object.freeze({
    namespaceId,
    threadId,
    toolCallId,
    resourceId,
    agentId,
    workflowId,
    toolName,
    input: cloneAndFreeze(input),
    elevated,
    requestContext: requestContext === null ? null : normalizeRunSecurityContext(requestContext),
    ...(ttlMs !== undefined && { ttlMs }),
  });
}

/** Copy the exact security boundary and never expose the stored object. */
function detached(row: PendingApproval): PendingApproval {
  return immutable({
    ...row,
    input: cloneAndFreeze(row.input),
    requestContext:
      row.requestContext === null ? null : normalizeRunSecurityContext(row.requestContext),
    decidedByPrincipal:
      row.decidedByPrincipal === null
        ? null
        : normalizeRunSecurityContext({ actor: row.decidedByPrincipal }).actor,
  });
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

function matchesExpected(row: PendingApproval, expected: DecidePendingApprovalParams["expected"]) {
  return (
    row.workflowId === expected.workflowId &&
    row.elevated === expected.elevated &&
    sameRunSecurityContext(row.requestContext ?? undefined, expected.requestContext ?? undefined)
  );
}

function immutable(row: PendingApproval): PendingApproval {
  return Object.freeze(row);
}

function cloneAndFreeze<T>(value: T): T {
  return deepFreeze(structuredClone(value));
}

function deepFreeze<T>(value: T, seen: WeakSet<object> = new WeakSet()): T {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) return value;
  if (seen.has(value)) return value;
  seen.add(value);
  for (const nested of Object.values(value)) deepFreeze(nested, seen);
  return Object.freeze(value);
}
