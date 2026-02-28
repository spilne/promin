// ---------------------------------------------------------------------------
// Workflow & Step state types
// ---------------------------------------------------------------------------

export type WorkflowStatus = "running" | "completed" | "failed" | "suspended";

export type StepStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "sleeping"
  | "waiting_for_signal";

export type StepType = "single" | "map" | "sleep" | "signal";

/**
 * Full workflow state. Generic params default to `unknown` for storage layer
 * compatibility — the builder narrows them via its own type params.
 */
export interface WorkflowState<Input = unknown, Result = unknown> {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly status: WorkflowStatus;
  readonly input: Input;
  readonly result?: Result;
  readonly error?: string;
  readonly steps: Record<string, StepState>;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly completedAt?: Date;
}

export interface StepState {
  readonly stepName: string;
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
  /** For sleep steps: when to wake up. */
  readonly wakeAt?: Date;
  /** For signal steps: the signal name to wait for. */
  readonly signalName?: string;
  /** For signal steps: timeout deadline. */
  readonly signalTimeoutAt?: Date;
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

/** A signal delivered to a workflow. */
export interface SignalState {
  readonly signalName: string;
  readonly payload: unknown;
  readonly deliveredAt: Date;
}
