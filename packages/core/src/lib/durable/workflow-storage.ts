// ---------------------------------------------------------------------------
// WorkflowStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type {
  WorkflowState,
  WorkflowStatus,
  SignalState,
  StepAttemptRecord,
} from "./workflow-state.ts";

export interface WorkflowStorage {
  /** Load the full workflow state. Returns null if workflow doesn't exist. */
  loadWorkflow(workflowId: string): Promise<WorkflowState | null>;

  /** List workflows, optionally filtered by status, name, or type. */
  listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]>;

  /** Cancel a running or suspended workflow. With cascade, also cancels children. */
  cancelWorkflow(workflowId: string, options?: { cascade?: boolean }): Promise<void>;

  /** Create a new workflow record. */
  createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    parentWorkflowId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  /** Save a completed step result. */
  saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
  }): Promise<void>;

  /** Mark a step as failed. */
  saveStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    durationMs: number;
    startedAt: Date;
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

  /** Release a workflow lock. */
  releaseLock(workflowId: string): Promise<void>;

  /** Heartbeat to extend a lock (for long-running steps). */
  heartbeat(workflowId: string, lockDurationMs: number): Promise<void>;
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
