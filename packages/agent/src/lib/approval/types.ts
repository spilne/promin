// ---------------------------------------------------------------------------
// ApprovalStorage — queryable inbox + audit trail for tool approvals.
//
// Why this exists separately from the workflow journal
// ---------------------------------------------------------
// The workflow journal already records each approval activity
// (`approval-${callId}`) with the journaled decision as the activity's
// return value — that's what makes the signal-based approval flow
// replay-safe. But the journal is keyed by workflow + step name, not
// indexed by status / namespace / age, so two operator-facing needs
// can't be served from it directly:
//
//   1. "List all pending approvals across the fleet right now."
//      The journal would force a scan of every active workflow.
//   2. "Compliance: who approved what, when, on which tool."
//      The journal mixes approvals with every other activity; pulling
//      the audit subset means filtering across all journals.
//
// `ApprovalStorage` is a side-index populated from the bus events
// `approval.requested` and `approval.decision`. It's not in the
// critical path — the workflow journal stays the source of truth for
// replay correctness. Storage is queryable, exportable, and survives
// independently of any single workflow's lifecycle.
//
// Replication semantics
// ---------------------
// Hosts that want the inbox view across multiple pods need a storage
// implementation that's itself multi-pod safe (Postgres, Redis, etc.).
// The InMemory and Sqlite-per-process impls are useful for tests + the
// demo; production deployments should layer this on a shared backend
// alongside their `WorkflowStorage` of the same flavor.
// ---------------------------------------------------------------------------

export type ApprovalStatus = "pending" | "approved" | "rejected" | "expired";

export interface ApprovalRequest {
  /** Stable id — generated at create() time. Unique across all rows. */
  readonly requestId: string;
  /** Workflow that suspended on the approve:<callId> signal. */
  readonly workflowId: string;
  /** Signal name the workflow is waiting on (e.g. "approve:call-123"). */
  readonly signalName: string;
  /** Tenant. Indexed for the inbox-by-namespace query. */
  readonly namespaceId: string;
  /** Optional resource (user / agent instance) the request is on behalf of. */
  readonly resourceId: string | null;
  /** Tool the agent wants to call. */
  readonly toolName: string;
  /** Tool input (the parsed payload the agent passed in). */
  readonly toolInput: unknown;
  /**
   * Human-readable summary of what the agent wants to do. Operator-facing —
   * the model can fill this in to make the approval queue self-explanatory
   * ("send invoice #4421 to alice@acme.com").
   */
  readonly summary: string;
  readonly status: ApprovalStatus;
  readonly createdAt: number;
  readonly decidedAt: number | null;
  /** Operator id / name when status is approved | rejected. */
  readonly decidedBy: string | null;
  /** Reason given by the operator (if any). */
  readonly decisionReason: string | null;
  /** When the request becomes stale and is eligible for `expirePending`. */
  readonly expiresAt: number | null;
  /** Free-form labels for filtering in the UI (e.g. agent id, urgency). */
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface CreateApprovalRequestInput {
  readonly requestId: string;
  readonly workflowId: string;
  readonly signalName: string;
  readonly namespaceId: string;
  readonly resourceId?: string | null;
  readonly toolName: string;
  readonly toolInput: unknown;
  readonly summary: string;
  readonly expiresAt?: number | null;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface DecideApprovalInput {
  readonly requestId: string;
  readonly decision: "approved" | "rejected";
  readonly decidedBy: string;
  readonly reason?: string | null;
  /** Override the system clock — useful for tests + replay correctness. */
  readonly decidedAt?: number;
}

export interface ListApprovalsParams {
  readonly namespaceId?: string;
  readonly status?: ApprovalStatus;
  readonly toolName?: string;
  readonly limit?: number;
  /** Defaults to "createdDesc". */
  readonly order?: "createdAsc" | "createdDesc";
}

/**
 * Storage primitive for the approval inbox + audit trail.
 *
 * Implementations: `InMemoryApprovalStorage` (reference), `SqliteApprovalStorage`
 * (single-process persistent), and any host-supplied adapter. All must
 * pass the same conformance tests.
 */
export interface ApprovalStorage {
  /**
   * Persist a new approval request. Called when the bus emits an
   * `approval.requested` event (typically wired via `attachApprovalStorage`).
   * Returns the persisted row including the assigned `createdAt`.
   */
  create(input: CreateApprovalRequestInput): Promise<ApprovalRequest>;

  /**
   * Update a pending request to approved | rejected. Idempotent on the
   * same `(requestId, decision)` pair — calling twice with the same
   * decision is a no-op rather than a write race. Calling with a
   * different decision after the row is already decided throws.
   */
  decide(input: DecideApprovalInput): Promise<ApprovalRequest>;

  get(requestId: string): Promise<ApprovalRequest | null>;

  list(params?: ListApprovalsParams): Promise<ApprovalRequest[]>;

  /**
   * Mark all pending requests with `expiresAt < now` as expired.
   * Returns the count expired. Designed to be called from a periodic
   * sweeper (the existing scheduler-loop pattern).
   */
  expirePending(now: number): Promise<number>;
}

export class ApprovalDecisionConflictError extends Error {
  readonly code = "approval_decision_conflict";
  constructor(
    readonly requestId: string,
    readonly currentStatus: ApprovalStatus,
    readonly attempted: "approved" | "rejected",
  ) {
    super(
      `decide(${requestId}): row already in status "${currentStatus}"; cannot transition to "${attempted}"`,
    );
  }
}
