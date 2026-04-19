// ---------------------------------------------------------------------------
// WorkflowStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type {
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  SignalState,
  StepAttemptRecord,
} from "./workflow-state.ts";

export interface WorkflowStorage {
  /** Load the full workflow state. Returns null if workflow doesn't exist. */
  loadWorkflow(workflowId: string): Promise<WorkflowState | null>;

  /** List workflows, optionally filtered by status, name, type, or namespace. */
  listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]>;

  /** Cancel a running or suspended workflow. With cascade, also cancels children. */
  cancelWorkflow(workflowId: string, options?: { cascade?: boolean }): Promise<void>;

  /** Create a new workflow record. Returns `{ created: false, existing }` on conflict. */
  createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    parentWorkflowId?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }>;

  /** Save a completed step result. */
  saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
    /**
     * Step-kind-specific audit data (e.g. `.match()` writes the chosen
     * case). Stored verbatim on the step row. Omitted / `undefined` for
     * steps that don't produce audit data, which must round-trip as
     * `undefined` on load — not an empty object.
     */
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  /**
   * Save many step results in a single round trip. Exists for the HTTP /
   * remote-storage path, where looping over `saveStepResult` turns into
   * one network call per step and dominates wall-clock for workflows with
   * tall map steps or fan-out DAGs.
   *
   * **Atomicity.** Backends SHOULD persist the batch atomically (Postgres
   * via `INSERT ... VALUES (...), (...), ...`; Redis via a pipeline/Lua
   * script; in-memory trivially). On partial failure the backend MAY
   * roll back — callers MUST NOT assume any subset survived.
   *
   * **Cross-workflow batches.** Each record carries its own `workflowId`,
   * so callers can batch across workflows. In practice the engine always
   * sends records for a single workflow at a time, but the signature stays
   * generic so cron / migration tooling can reuse the same primitive.
   *
   * Default is provided via the `batchSaveStepResultsDefault` helper —
   * custom storages that only override `saveStepResult` can point this
   * at it and opt into the interface without a perf gain.
   */
  batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
  ): Promise<void>;

  /** Mark a step as failed. */
  saveStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    durationMs: number;
    startedAt: Date;
    /**
     * Step-kind-specific audit data set before the step ran. Preserved on
     * the failure row so ops can still see "which case fired" when a
     * `.match()` branch threw.
     */
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  /** Save a completed task result within a map step. */
  saveTaskResult(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    result: unknown;
  }): Promise<void>;

  /** Mark a task within a map step as failed. */
  saveTaskFailure(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    error: string;
  }): Promise<void>;

  /** Mark the entire workflow as completed. */
  completeWorkflow(workflowId: string, result: unknown): Promise<void>;

  /** Mark the entire workflow as failed. */
  failWorkflow(workflowId: string, error: string): Promise<void>;

  /** Suspend the workflow (sleeping or waiting for signal). */
  suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
  ): Promise<void>;

  /** Deliver a signal to a workflow. */
  deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void>;

  /** Load signals delivered to a workflow. */
  loadSignals(workflowId: string): Promise<SignalState[]>;

  /** Acquire a lock on a workflow. Returns false if already locked. */
  tryLock(workflowId: string, lockDurationMs: number): Promise<boolean>;

  /**
   * Acquire the lock AND load the current workflow state in one round trip.
   *
   * Exists for the HTTP / remote-storage path, where the typical
   * `tryLock` → check → `loadWorkflow` sequence is two network calls per
   * step invocation. Backends SHOULD persist this atomically
   * (Postgres: same transaction; Redis: Lua script) so the load reflects
   * the state as-of the moment the lock was acquired — no window where
   * another actor commits writes between the two observations.
   *
   * Return contract:
   * - `locked: true`  — caller holds the lock; `state` is the current
   *   state or null if the workflow record doesn't exist yet.
   * - `locked: false` — someone else holds the lock; `state` is still
   *   returned for diagnostic use (idempotency joins, "already running"
   *   branches), or null if absent.
   *
   * Default is provided via `tryLockAndLoadDefault` — custom storages
   * that can't do an atomic read-lock can point this at it and still
   * satisfy the interface.
   */
  tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; state: WorkflowState | null }>;

  /** Release a workflow lock. */
  releaseLock(workflowId: string): Promise<void>;

  /** Heartbeat to extend a lock (for long-running steps). */
  heartbeat(workflowId: string, lockDurationMs: number): Promise<void>;

  /**
   * Reset a completed workflow for a fresh re-execution.
   * Increments the `run` counter, resets status to `running`, clears result/error.
   * Old step results (from previous runs) remain in storage for history.
   * Returns the new run number.
   */
  startFreshRun(workflowId: string): Promise<number>;

  /**
   * Load run history for a workflow — all runs with their step results.
   * Ordered by run number descending (newest first).
   */
  loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]>;

  /**
   * Delete completed/failed workflows matching the given time window.
   * Returns the number of workflows deleted.
   * Never deletes running or suspended workflows.
   *
   * Overloads:
   * - `olderThanMs` — relative: delete workflows completed more than N ms ago
   * - `from` / `to` — absolute: delete workflows with completedAt in [from, to)
   */
  purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number>;
}

/**
 * Fallback `tryLockAndLoad` that sequences tryLock + loadWorkflow.
 * Atomic across the pair only if the underlying storage serializes both
 * calls against the same transaction — most local-process backends
 * (in-memory, same-connection Postgres) are fine, but a distributed
 * storage with no coupling between the two primitives may see another
 * writer commit between them. Backends that can do better should
 * override.
 */
export async function tryLockAndLoadDefault(
  storage: Pick<WorkflowStorage, "tryLock" | "loadWorkflow">,
  workflowId: string,
  lockDurationMs: number,
): Promise<{ locked: boolean; state: import("./workflow-state.ts").WorkflowState | null }> {
  const locked = await storage.tryLock(workflowId, lockDurationMs);
  const state = await storage.loadWorkflow(workflowId);
  return { locked, state };
}

/**
 * Fallback `batchSaveStepResults` implementation for backends that can't do
 * a real multi-row write. Just loops `saveStepResult` in order. Intentionally
 * not concurrent — callers rely on the batch staying ordered so that step
 * rows created later in the batch sort after earlier ones on the receiving
 * side's `created_at` / insertion order.
 */
export async function batchSaveStepResultsDefault(
  storage: Pick<WorkflowStorage, "saveStepResult">,
  records: ReadonlyArray<{
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
  }>,
): Promise<void> {
  for (const r of records) {
    await storage.saveStepResult(r);
  }
}

// ---------------------------------------------------------------------------
// StepAttemptStorage — optional interface for recording attempt history
// ---------------------------------------------------------------------------

/**
 * Optional storage extension for recording step attempt history.
 * Implementations that support this append a record for every execution
 * and compensation attempt, enabling audit trails and retry analysis.
 *
 * The engine detects this at runtime via `isStepAttemptStorage()`.
 */
export interface StepAttemptStorage {
  /** Append a step attempt record (execution or compensation). */
  saveStepAttempt(record: StepAttemptRecord): Promise<void>;

  /** Load attempt history for a workflow, optionally filtered by step name. */
  loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]>;
}

/** Runtime check for whether a storage implementation supports attempt history. */
export function isStepAttemptStorage(
  storage: WorkflowStorage,
): storage is WorkflowStorage & StepAttemptStorage {
  return "saveStepAttempt" in storage && typeof (storage as any).saveStepAttempt === "function";
}
