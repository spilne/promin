// ---------------------------------------------------------------------------
// PendingApprovalStore — the durable index of tool-call approvals that
// thread turns are suspended on.
//
// When a thread turn reaches a tool that needs approval, the engine records
// `{ namespaceId, threadId, toolCallId } → { workflowId, toolName, input,
// requestedAt, expiresAt }` here before the turn suspends on
// `approve:<toolCallId>`. Approve / reject looks the row up by that exact
// key, so finding the suspended turn never scans workflow storage, never
// matches a thread id by prefix, and never crosses namespaces.
//
// The decision is recorded on the row (approved / rejected / expired, who
// decided, when, why), so the index doubles as the approval audit trail.
// A row stays *open* (`deliveredAt` null) until its decision reached the
// turn, or the turn ended without it: open rows block new turns on the
// thread, so a decision recorded by a resume that then crashed is still
// finished before anything else happens on the thread.
//
// Why a store of its own (and not a MemoryStore table): approvals belong to
// the turn engine, not to the conversation content. A LocalAgent without a
// MemoryStore still suspends on approvals, and an approval has a lifecycle
// (pending → decided, optional expiry) that thread memory doesn't.
//
// Implementations: `InMemoryPendingApprovalStore` (here),
// `SqlitePendingApprovalStore` (@promin/sqlite) and
// `PostgresPendingApprovalStore` (@promin/postgres). All pass
// `pendingApprovalStoreTestSuite`.
//
// Time: `requestedAt`, `expiresAt` and `decidedAt` come from the store's
// time source — the injected WallClock for the single-host stores, the
// database server's clock for Postgres — so every process sharing a store
// agrees on whether an approval has expired.
// ---------------------------------------------------------------------------

/** Identifies one approval: the tool call a thread turn is waiting on. */
export interface PendingApprovalKey {
  readonly namespaceId: string;
  readonly threadId: string;
  readonly toolCallId: string;
}

/**
 * `expired`: the approval passed its `expiresAt` and was closed as a
 * rejection (by an explicit reject, or automatically when encountered).
 */
export type PendingApprovalStatus = "pending" | "approved" | "rejected" | "expired";

/** One approval row. */
export interface PendingApproval extends PendingApprovalKey {
  /** Resource that owns the thread, when the turn ran under one. */
  readonly resourceId: string | null;
  /** Agent (recipe id) that ran the turn. */
  readonly agentId: string | null;
  /** The suspended turn's workflow id. */
  readonly workflowId: string;
  readonly toolName: string;
  readonly input: unknown;
  /** Unix ms, store time. */
  readonly requestedAt: number;
  /** Unix ms, store time. Null: never expires. */
  readonly expiresAt: number | null;
  readonly status: PendingApprovalStatus;
  /** Who decided — the identity the approving caller was authorized as. */
  readonly decidedBy: string | null;
  /** Unix ms, store time. Null while pending. */
  readonly decidedAt: number | null;
  /** The decision's reason, when one was given. */
  readonly reason: string | null;
  /**
   * Unix ms, store time: when the decision reached the turn (or the row
   * was closed because the turn ended without it). Null while open.
   */
  readonly deliveredAt: number | null;
}

/** What `PendingApprovalStore.record` takes. */
export interface RecordPendingApprovalParams extends PendingApprovalKey {
  readonly resourceId?: string;
  readonly agentId?: string;
  readonly workflowId: string;
  readonly toolName: string;
  readonly input: unknown;
  /** Expire the approval this long after it is recorded. Omitted: never. */
  readonly ttlMs?: number;
}

/** What `PendingApprovalStore.decide` takes. */
export interface DecidePendingApprovalParams extends PendingApprovalKey {
  readonly approved: boolean;
  readonly decidedBy?: string;
  readonly reason?: string;
  /**
   * Decide only when the approval has expired (answers `not_expired`
   * otherwise) — closes expired approvals without racing a live one.
   */
  readonly onlyIfExpired?: boolean;
}

/**
 * Outcome of `decide`. `decided` means this call recorded the decision;
 * every other outcome changed nothing.
 */
export type DecidePendingApprovalResult =
  | { readonly outcome: "decided"; readonly approval: PendingApproval }
  | { readonly outcome: "not_found" }
  | { readonly outcome: "already_decided"; readonly approval: PendingApproval }
  | { readonly outcome: "expired"; readonly approval: PendingApproval }
  | { readonly outcome: "not_expired"; readonly approval: PendingApproval };

/** What `PendingApprovalStore.list` takes. */
export interface ListPendingApprovalsParams {
  readonly namespaceId: string;
  /** Exact thread id (never a prefix). Omitted: every thread of the namespace. */
  readonly threadId?: string;
  /** Default `"pending"`. Ignored with `open`. */
  readonly status?: PendingApprovalStatus;
  /** Only open rows (`deliveredAt` null), whatever their status. */
  readonly open?: boolean;
  /** Default 100. */
  readonly limit?: number;
}

export interface PendingApprovalStore {
  /**
   * Record an approval request. Idempotent for one workflow: recording the
   * same key and `workflowId` again returns the existing row unchanged (a
   * crash re-runs the recording activity). A row of the same key from a
   * different workflow is replaced.
   */
  record(params: RecordPendingApprovalParams): Promise<PendingApproval>;
  /** The row for `key`, whatever its status; null when absent. */
  get(key: PendingApprovalKey): Promise<PendingApproval | null>;
  /** Rows of one namespace (optionally one thread), oldest request first. */
  list(params: ListPendingApprovalsParams): Promise<PendingApproval[]>;
  /**
   * Record a decision on a pending row, atomically: of concurrent calls
   * for one key, one gets `decided`, the others `already_decided`. An
   * approval past its `expiresAt` cannot be approved (`expired`, nothing
   * changes); rejecting it records status `expired`.
   */
  decide(params: DecidePendingApprovalParams): Promise<DecidePendingApprovalResult>;
  /** Close the row: its decision reached the turn, or the turn ended. Idempotent. */
  markDelivered(key: PendingApprovalKey): Promise<void>;
  /** Drop every row of a thread (the thread was deleted). */
  deleteThread(params: { readonly namespaceId: string; readonly threadId: string }): Promise<void>;
}

/** Thrown when approve / reject names an approval the thread is not waiting on. */
export class ApprovalNotFoundError extends Error {
  readonly _tag = "ApprovalNotFoundError";
  constructor(key: PendingApprovalKey) {
    super(
      `No pending approval for tool call "${key.toolCallId}" on thread "${key.threadId}" ` +
        `in namespace "${key.namespaceId}".`,
    );
  }
}

/** Thrown when the approval was already decided (e.g. by a concurrent caller). */
export class ApprovalAlreadyDecidedError extends Error {
  readonly _tag = "ApprovalAlreadyDecidedError";
  constructor(readonly approval: PendingApproval) {
    super(`Approval for tool call "${approval.toolCallId}" was already ${approval.status}.`);
  }
}

/**
 * Thrown when an approval arrives while its turn is still on its way to the
 * suspension (or another process is running the turn): retry shortly.
 */
export class ApprovalNotReadyError extends Error {
  readonly _tag = "ApprovalNotReadyError";
  constructor(key: PendingApprovalKey) {
    super(
      `The turn waiting for approval of tool call "${key.toolCallId}" is not ready for a ` +
        `decision yet; retry shortly.`,
    );
  }
}

/** Thrown when an expired approval is approved. Rejecting it still works. */
export class ApprovalExpiredError extends Error {
  readonly _tag = "ApprovalExpiredError";
  constructor(readonly approval: PendingApproval) {
    super(`Approval for tool call "${approval.toolCallId}" expired; it can only be rejected.`);
  }
}

/**
 * Thrown when a new turn is sent on a thread whose previous turn is
 * suspended on an approval. Decide the approval first: the suspended
 * turn's output must land in the thread before the next turn's input.
 */
export class ThreadAwaitingApprovalError extends Error {
  readonly _tag = "ThreadAwaitingApprovalError";
  constructor(readonly pending: ReadonlyArray<PendingApproval>) {
    const first = pending[0];
    super(
      `Thread "${first?.threadId}" has a turn waiting for approval of tool call ` +
        `"${first?.toolCallId}"; approve or reject it before sending a new message.`,
    );
  }
}
