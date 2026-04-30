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

/**
 * Why this workflow run was started. Stored on the workflow row so the
 * dashboard can filter by source (e.g., "show only manual triggers") and
 * link runs back to whatever produced them without parsing workflow ids
 * or chasing through metadata. `runSourceId` is the id of the producer:
 *  - `"schedule"`  → the `scheduleId`
 *  - `"parent"`    → the parent workflow id
 *  - `"webhook"`   → the webhook name
 *  - `"agent"`     → the agent or thread id
 *  - `"manual"` / `"api"` → no source id (or an operator-supplied tag)
 *
 * String for ergonomics in app code; backends encode as a small int via
 * `RUN_SOURCE_CODES` so the on-disk footprint and index density match a
 * native enum column.
 */
export type RunSource = "manual" | "schedule" | "api" | "webhook" | "parent" | "agent";

export const RUN_SOURCE_CODES = {
  manual: 0,
  schedule: 1,
  api: 2,
  webhook: 3,
  parent: 4,
  agent: 5,
} as const satisfies Record<RunSource, number>;

const RUN_SOURCE_BY_CODE_MAP = new Map<number, RunSource>(
  Object.entries(RUN_SOURCE_CODES).map(([k, v]) => [v, k as RunSource]),
);

export function encodeRunSource(source: RunSource | undefined): number | null {
  if (source === undefined) return null;
  return RUN_SOURCE_CODES[source];
}

export function decodeRunSource(code: number | null | undefined): RunSource | undefined {
  if (code == null) return undefined;
  return RUN_SOURCE_BY_CODE_MAP.get(code);
}

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
  /**
   * What kicked this run off (`"schedule"`, `"manual"`, …). See `RunSource`.
   * Stored as a small int on disk for index density.
   */
  readonly runSource?: RunSource;
  /** Producer id corresponding to `runSource`. See `RunSource` for shape. */
  readonly runSourceId?: string;
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
// Run-scoped workflow events
// ---------------------------------------------------------------------------

/**
 * Event emitted by `WorkflowStorage.subscribeToWorkflow()` for a specific
 * workflow run. Subscribers receive these as they happen so UIs / CLIs can
 * stream live progress without polling `loadWorkflow`.
 *
 * Emission points:
 *   - `notifyStepStarted` → `step-started` (runner calls this before each
 *     local step body runs; storages without the method emit nothing for
 *     this event)
 *   - `saveStepResult` → `step-completed`
 *   - `saveStepFailure` → `step-failed`
 *   - `completeWorkflow` → `workflow-completed` (terminal — stream closes)
 *   - `failWorkflow` → `workflow-failed` (terminal — stream closes)
 *   - `tripwireWorkflow` → `workflow-tripwire` (terminal — stream closes)
 *
 * The runner's polling fallback (used when the storage lacks native push)
 * can only synthesize completion events from step-row transitions, so
 * `step-started` is only observable on the push path.
 */
export type WorkflowRunEvent =
  | { readonly type: "step-started"; readonly stepName: string; readonly at: Date }
  | {
      readonly type: "step-completed";
      readonly stepName: string;
      readonly result: unknown;
      readonly durationMs: number;
      readonly at: Date;
    }
  | {
      readonly type: "step-failed";
      readonly stepName: string;
      readonly error: string;
      readonly at: Date;
    }
  | { readonly type: "workflow-completed"; readonly result: unknown; readonly at: Date }
  | { readonly type: "workflow-failed"; readonly error: string; readonly at: Date }
  | {
      readonly type: "workflow-tripwire";
      readonly stepName: string;
      readonly reason: unknown;
      readonly at: Date;
    };

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
