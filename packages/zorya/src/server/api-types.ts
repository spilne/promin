// ---------------------------------------------------------------------------
// API types — DTOs shared between server and UI.
//
// The server serialises WorkflowState / StepState to plain JSON (Date → ISO
// string) and the UI parses them back. These interfaces describe that wire
// format.
// ---------------------------------------------------------------------------

import type { WorkflowStatus, WorkflowOrderBy, StepStatus, StepType } from "@promin/workflow";

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

/**
 * Client-derived extension of StepStatus. The engine only stores the types
 * in @promin/workflow — `upstream_failed` is synthesised by the server
 * when a `pending` step has a dependency that already failed, so the UI
 * can colour "blocked because of an upstream problem" differently from
 * "this step actually failed". Not persisted anywhere.
 */
export type ExtendedStepStatus = StepStatus | "upstream_failed";

export interface StepDto {
  stepName: string;
  run: number;
  status: StepStatus;
  /** Only set when different from `status` (currently: upstream_failed). */
  effectiveStatus?: ExtendedStepStatus;
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
  version?: string;
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
  /** Workflow version. Filtered post-fetch — uses with `name` for accuracy. */
  version?: string;
  /**
   * Search-attributes filter. JSON-encoded `Record<string, unknown>` on the
   * URL (`?metadata=%7B%22userId%22%3A%22u_42%22%7D`) so nested values can
   * round-trip. Server forwards to `WorkflowStorage.listWorkflows({ metadata })`.
   */
  metadata?: Record<string, unknown>;
  limit?: number;
  offset?: number;
  /** Server-side sort column. Default: `createdAt`. */
  orderBy?: WorkflowOrderBy;
  /** Sort direction. Default: `desc`. */
  orderDir?: "asc" | "desc";
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
  // Richer fields populated when the worker protocol is wired. Stay
  // optional so the existing mock WorkersProvider still satisfies the type.
  capabilities?: readonly string[];
  /** Names of workflows this worker advertised on register. */
  workflowNames?: readonly string[];
  workflowVersions?: readonly string[];
  concurrency?: number;
  version?: string;
  hostname?: string;
  runtime?: string;
  /** ISO timestamp of when the worker process started. */
  startedAt?: string;
  /** Runs currently executing on this worker. */
  activeRuns?: ReadonlyArray<{
    workflowId: string;
    workflowName: string;
    /** ISO timestamp. */
    startedAt: string;
  }>;
  completedCount?: number;
  failedCount?: number;
  /** Last ~20 terminal runs this worker executed. */
  recentRuns?: ReadonlyArray<{
    workflowId: string;
    workflowName: string;
    status: "completed" | "failed";
    durationMs: number;
    /** ISO timestamp. */
    at: string;
  }>;
  namespaces?: readonly string[];
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
  /**
   * Workflow version this run should execute under. Recorded on the
   * storage row and routed to a worker advertising that version. Defaults
   * to whatever version a connected worker advertises.
   */
  version?: string;
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
