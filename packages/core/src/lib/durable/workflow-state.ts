// ---------------------------------------------------------------------------
// Workflow & Step state types
// ---------------------------------------------------------------------------

export type WorkflowStatus = "running" | "completed" | "failed" | "suspended" | "compensating";

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
  readonly run: number;
  readonly input: Input;
  readonly result?: Result;
  readonly error?: string;
  readonly metadata?: Record<string, unknown>;
  readonly steps: Record<string, StepState>;
  readonly workflowAttempt?: number;
  readonly compensationStatus?: CompensationStatus;
  readonly createdAt: Date;
  readonly updatedAt: Date;
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
}
