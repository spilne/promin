// ---------------------------------------------------------------------------
// API types — DTOs shared between server and UI.
//
// The server serialises WorkflowState / StepState to plain JSON (Date → ISO
// string) and the UI parses them back. These interfaces describe that wire
// format.
// ---------------------------------------------------------------------------

import type { WorkflowStatus, StepStatus, StepType } from "@promin/workflow";

export interface StepTaskDto {
  taskIndex: number;
  status: StepStatus;
  input?: unknown;
  result?: unknown;
  error?: string;
  startedAt?: string;
  completedAt?: string;
  attempt: number;
}

export interface StepDto {
  stepName: string;
  run: number;
  status: StepStatus;
  stepType: StepType;
  dependsOn: string[];
  result?: unknown;
  error?: string;
  /** ISO timestamp. */
  startedAt?: string;
  /** ISO timestamp. */
  completedAt?: string;
  durationMs?: number;
  attempt: number;
  /** ISO timestamp. */
  wakeAt?: string;
  signalName?: string;
  /** ISO timestamp. */
  signalTimeoutAt?: string;
  metadata?: Record<string, unknown>;
  /**
   * True when this step hasn't been saved to storage yet — synthesised from
   * the workflow definition so the UI can show "not-yet-executed" steps on
   * the timeline. All other fields on such a row are default values
   * (status: "pending", attempt: 0, etc.).
   */
  isPlanned?: boolean;
  /** Fan-out (mapOver) sub-tasks. Present for map steps. */
  tasks?: StepTaskDto[];
  /** Saga compensation status. Present when compensation ran. */
  compensationStatus?: "pending" | "compensated" | "compensation_failed";
  compensationError?: string;
  /** ISO timestamp. */
  compensatedAt?: string;
}

export interface RunDto {
  workflowId: string;
  workflowName: string;
  workflowType?: string;
  namespace?: string;
  status: WorkflowStatus;
  version?: string;
  run: number;
  input: unknown;
  result?: unknown;
  error?: string;
  metadata?: Record<string, unknown>;
  steps: StepDto[];
  /** Parent workflow id, when this run was spawned by another workflow. */
  parentWorkflowId?: string;
  /** ISO timestamp. */
  createdAt: string;
  /** ISO timestamp. */
  startedAt?: string;
  /** ISO timestamp. */
  updatedAt: string;
  /** ISO timestamp. */
  completedAt?: string;
}

export interface RunSummaryDto {
  workflowId: string;
  workflowName: string;
  workflowType?: string;
  namespace?: string;
  status: WorkflowStatus;
  run: number;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  updatedAt: string;
  /** Wall-clock total: completedAt - createdAt (or null if not complete). */
  totalMs?: number;
}

export interface RunListResponse {
  runs: RunSummaryDto[];
  total?: number;
}

export interface RunListQuery {
  status?: WorkflowStatus;
  name?: string;
  type?: string;
  namespace?: string;
  limit?: number;
  offset?: number;
}

export interface MetricsDto {
  total: number;
  byStatus: Record<WorkflowStatus, number>;
  avgDurationMs?: number;
  p95DurationMs?: number;
  p99DurationMs?: number;
}

export interface WorkerDto {
  workerId: string;
  status: "online" | "offline";
  queue?: string;
  labels?: Record<string, string>;
  activeTasks: number;
  completedToday: number;
  /** ISO timestamp of last heartbeat. */
  lastHeartbeatAt?: string;
}

export interface WorkersResponse {
  workers: WorkerDto[];
}

export interface TriggerRunRequest {
  input?: unknown;
  workflowType?: string;
  namespace?: string;
  metadata?: Record<string, unknown>;
  /** Explicit id. Defaults to a generated UUID. */
  workflowId?: string;
}

export interface TriggerRunResponse {
  workflowId: string;
}

export interface SignalRequest {
  signalName: string;
  payload?: unknown;
}

export interface ApiError {
  error: string;
  message?: string;
}

/** SSE event payloads sent on /api/runs/:id/events. */
export type RunEvent =
  | { type: "snapshot"; run: RunDto }
  | { type: "step"; stepName: string; step: StepDto }
  | { type: "status"; status: WorkflowStatus }
  | { type: "end" };
