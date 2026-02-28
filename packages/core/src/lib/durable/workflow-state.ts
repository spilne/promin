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

export interface WorkflowState<Input = unknown, Result = unknown> {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly workflowType?: string;
  readonly parentWorkflowId?: string;
  readonly status: WorkflowStatus;
  readonly input: Input;
  readonly result?: Result;
  readonly error?: string;
  readonly metadata?: Record<string, unknown>;
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
  readonly wakeAt?: Date;
  readonly signalName?: string;
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

export interface SignalState {
  readonly signalName: string;
  readonly payload: unknown;
  readonly deliveredAt: Date;
}
