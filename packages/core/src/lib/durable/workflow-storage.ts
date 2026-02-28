// ---------------------------------------------------------------------------
// WorkflowStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type { WorkflowState, WorkflowStatus, SignalState } from "./workflow-state.ts";

export interface WorkflowStorage {
  /** Load the full workflow state. Returns null if workflow doesn't exist. */
  loadWorkflow(workflowId: string): Promise<WorkflowState | null>;

  /** List workflows, optionally filtered by status and/or name. */
  listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]>;

  /** Cancel a running or suspended workflow. */
  cancelWorkflow(workflowId: string): Promise<void>;

  /** Create a new workflow record. */
  createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
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
