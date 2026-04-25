// ---------------------------------------------------------------------------
// Workflow & Step state types
// ---------------------------------------------------------------------------

export type WorkflowStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "suspended"
  | "compensating"
  | "tripwire";

export type CompensationStatus = "none" | "compensating" | "compensated" | "partial";

export type StepStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "sleeping"
  | "waiting_for_signal"
  | "compensated"
  | "compensation_failed";

export type StepType = "single" | "map" | "sleep" | "signal";

export interface WorkflowState<Input = unknown, Result = unknown> {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly workflowType?: string;
  readonly parentWorkflowId?: string;
  readonly namespace?: string;
  readonly status: WorkflowStatus;
  readonly version?: string;
  readonly run: number;
  readonly input: Input;
  readonly result?: Result;
  readonly error?: string;
  /**
   * Structured reason attached when the workflow ended via a `.tripwire()`
   * step. Present only when `status === "tripwire"`. Opaque payload — the
   * shape is whatever the tripwire step's `reason(prev)` returned.
   */
  readonly tripwire?: unknown;
  readonly metadata?: Record<string, unknown>;
  readonly steps: Record<string, StepState>;
  readonly workflowAttempt?: number;
  readonly compensationStatus?: CompensationStatus;
  readonly createdAt: Date;
  readonly startedAt?: Date;
  readonly updatedAt: Date;
  readonly completedAt?: Date;
}

/** Summary of a single workflow run — used by loadRunHistory. */
export interface WorkflowRunSummary {
  readonly run: number;
  readonly version?: string;
  readonly status: WorkflowStatus;
  readonly result?: unknown;
  readonly error?: string;
  /** Tripwire reason — present only when `status === "tripwire"`. */
  readonly tripwire?: unknown;
  readonly steps: Record<string, StepState>;
  readonly createdAt: Date;
  readonly startedAt?: Date;
  readonly completedAt?: Date;
}

export interface StepState {
  readonly stepName: string;
  readonly run: number;
  readonly status: StepStatus;
  readonly dependsOn: string[];
  readonly stepType: StepType;
  readonly result?: unknown;
  readonly error?: string;
  readonly startedAt?: Date;
  readonly completedAt?: Date;
  readonly durationMs?: number;
  readonly attempt: number;
  readonly tasks?: StepTaskState[];
  readonly wakeAt?: Date;
  readonly signalName?: string;
  readonly signalTimeoutAt?: Date;
  readonly compensationStatus?: "pending" | "compensated" | "compensation_failed";
  readonly compensationError?: string;
  readonly compensatedAt?: Date;
  /**
   * Step-kind-specific audit data written at execution time and
   * queryable directly from storage. Today:
   *   - `.match()` writes `{ matchCase, matchMode }` so prod debugging
   *     ("why did this workflow route to express?") doesn't require
   *     re-running the selector against `prev`.
   * Future step kinds (subworkflow child id, branch direction, guard
   * failure label, state-machine transition) will extend the shape.
   * Stays `undefined` for steps that don't produce audit data.
   */
  readonly metadata?: Record<string, unknown>;
}

export interface StepTaskState {
  readonly taskIndex: number;
  readonly status: StepStatus;
  readonly input?: unknown;
  readonly result?: unknown;
  readonly error?: string;
  readonly startedAt?: Date;
  readonly completedAt?: Date;
  readonly attempt: number;
}

export interface SignalState {
  readonly signalName: string;
  readonly payload: unknown;
  readonly deliveredAt: Date;
}

// ---------------------------------------------------------------------------
// Step attempt history
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Dead letter queue record
// ---------------------------------------------------------------------------

export interface FailedWorkflowRecord {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly error: string;
  readonly failedAt: Date;
  readonly steps: Record<string, StepState>;
  readonly compensatedSteps: string[];
  readonly failedCompensations: { stepName: string; error: string }[];
  readonly metadata?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Step attempt history
// ---------------------------------------------------------------------------

export type StepAttemptType = "execution" | "compensation";

export interface StepAttemptRecord {
  readonly workflowId: string;
  readonly stepName: string;
  readonly attempt: number;
  readonly type: StepAttemptType;
  readonly status: "completed" | "failed";
  readonly result?: unknown;
  readonly error?: string;
  readonly durationMs: number;
  readonly startedAt: Date;
  readonly completedAt: Date;
  /**
   * ID of the worker that processed this attempt, when known. Set by the
   * distributed worker from its own workerId. In-process runs
   * (`wf.run(...)`) leave this undefined — there is no distinct worker.
   * Use for operational queries like "which worker handled the failed
   * retry of order-123's charge step?" or for per-worker error rates
   * across a rolling deploy.
   */
  readonly workerId?: string;
}
