// ---------------------------------------------------------------------------
// API types — DTOs shared between server and UI.
//
// The server serialises WorkflowState / StepState to plain JSON (Date → ISO
// string) and the UI parses them back. These interfaces describe that wire
// format. Request bodies and query strings are Zod schemas; their types are
// inferred from them, and the routes reject anything that does not parse
// with `400 { error: "invalid_request", message, issues }`.
//
// Conventions every route follows:
//   - errors:   `{ error, message?, issues? }` (see `ApiError`)
//   - creates:  201 + the created resource; 409 when it already exists
//   - starts:   202 + `{ workflowId }` and a `Location` header for work that
//               runs asynchronously (trigger, rerun, schedule emit, signal
//               delivery)
//   - actions:  200 + the resulting state
//   - deletes:  204; 404 when there was nothing to delete
//   - lists:    `?limit=&offset=` paging, `limit` capped at 500
// ---------------------------------------------------------------------------

import { z } from "zod";
import {
  WORKFLOW_STATUSES,
  type RunSource,
  type WorkflowStatus,
  type WorkflowOrderBy,
  type StepStatus,
  type StepType,
} from "@promin/workflow";

/** Every `RunSource`, for request validation. */
export const RUN_SOURCES = [
  "manual",
  "schedule",
  "api",
  "webhook",
  "parent",
  "agent",
] as const satisfies ReadonlyArray<RunSource>;

/** Sortable run-list columns (`?sort=<column>:<asc|desc>`). */
export const RUN_ORDER_BY = [
  "createdAt",
  "startedAt",
  "completedAt",
  "duration",
  "status",
  "name",
] as const satisfies ReadonlyArray<WorkflowOrderBy>;

/** Largest `limit` any list route accepts. */
export const MAX_LIST_LIMIT = 500;

/** A name: workflow, signal, version, namespace. */
export const NameSchema = z.string().min(1).max(256);
const Name = NameSchema;
/** An id: run, schedule, token, stream. */
export const IdSchema = z.string().min(1).max(512);
const Id = IdSchema;

/** `""` counts as absent on the query string. */
function optionalQuery<T extends z.ZodTypeAny>(schema: T) {
  return z.preprocess((v) => (v === "" ? undefined : v), schema.optional());
}

/** `?limit=` — an integer in `[1, max]`, `fallback` when absent. */
export function limitQuery(params: { fallback: number; max?: number }) {
  return z.preprocess(
    (v) => (v === undefined || v === "" ? params.fallback : v),
    z.coerce
      .number()
      .int()
      .min(1)
      .max(params.max ?? MAX_LIST_LIMIT),
  );
}

/** `?offset=` — a non-negative integer, 0 when absent. */
export const OffsetQuery = z.preprocess(
  (v) => (v === undefined || v === "" ? 0 : v),
  z.coerce.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
);

/** `?flag=true|false|1|0`; absent or empty is `undefined`. */
export const BooleanQuery = optionalQuery(
  z.enum(["true", "false", "1", "0"]).transform((v) => v === "true" || v === "1"),
);

/** `?metadata=<JSON object>` — a containment filter; absent or empty is `undefined`. */
export const MetadataQuery = optionalQuery(
  z.string().transform((raw, ctx) => {
    try {
      const parsed: unknown = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      // reported below
    }
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "must be a JSON-encoded object" });
    return z.NEVER;
  }),
);

/** Query string of `GET /api/agents`. */
export const AgentListQuerySchema = z.object({
  /** Agents whose active version claims this capability. */
  capability: optionalQuery(Name),
  /** Agents whose active version carries this tag. */
  tag: optionalQuery(Name),
  /** Page size, 1–500. Default 100. */
  limit: limitQuery({ fallback: 100 }),
  /** Agents to skip. Default 0. */
  offset: OffsetQuery,
});

/** `?namespace=` alone. */
export const NamespaceQuerySchema = z.object({ namespace: optionalQuery(Name) });

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
  /** What kicked this run off (`"schedule"`, `"manual"`, …). */
  runSource?: import("@promin/workflow").RunSource;
  /** Producer id corresponding to `runSource` (e.g. scheduleId). */
  runSourceId?: string;
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
  /** What kicked this run off — surfaces the typed source link. */
  runSource?: import("@promin/workflow").RunSource;
  runSourceId?: string;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  updatedAt: string;
  /** Wall-clock total: completedAt - createdAt (or null if not complete). */
  totalMs?: number;
}

export interface RunListResponse {
  runs: RunSummaryDto[];
  /**
   * Runs matching the filters across all pages. Present when the storage
   * can count (`countWorkflows` capability).
   */
  total?: number;
}

/** Query string of `GET /api/runs`. */
export const RunListQuerySchema = z.object({
  status: optionalQuery(
    z.enum(WORKFLOW_STATUSES as unknown as [WorkflowStatus, ...WorkflowStatus[]]),
  ),
  name: optionalQuery(Name),
  type: optionalQuery(Name),
  namespace: optionalQuery(Name),
  /** Workflow version the run was created with. */
  version: optionalQuery(Name),
  /** What kicked the run off (`"schedule"`, `"manual"`, `"api"`, …). */
  runSource: optionalQuery(z.enum(RUN_SOURCES)),
  /** Producer id paired with `runSource` (e.g. scheduleId). */
  runSourceId: optionalQuery(Id),
  /**
   * Search-attributes filter, JSON-encoded (`?metadata=%7B%22userId%22%3A%22u_42%22%7D`)
   * so nested values round-trip. A run matches when its metadata contains
   * every key with a deep-equal value.
   */
  metadata: MetadataQuery,
  /** Page size, 1–500. Default 50. */
  limit: limitQuery({ fallback: 50 }),
  /** Rows to skip. Default 0. */
  offset: OffsetQuery,
  /** `<column>[:asc|desc]`. Default `startedAt:desc` (most recently started first). */
  sort: optionalQuery(
    z
      .string()
      .regex(new RegExp(`^(${RUN_ORDER_BY.join("|")})(:(asc|desc))?$`), {
        message: `must be <${RUN_ORDER_BY.join("|")}>[:asc|desc]`,
      })
      .transform((raw) => {
        const [orderBy, orderDir] = raw.split(":") as [WorkflowOrderBy, "asc" | "desc" | undefined];
        return { orderBy, orderDir: orderDir ?? ("desc" as const) };
      }),
  ),
});

/** Run-list filters as callers build them; `RunListQuerySchema` is the wire form. */
export interface RunListQuery {
  status?: WorkflowStatus;
  name?: string;
  type?: string;
  namespace?: string;
  /** Workflow version the run was created with. */
  version?: string;
  /** Filter by trigger source (`"schedule"`, `"manual"`, …). */
  runSource?: RunSource;
  /** Producer id paired with `runSource` (e.g. scheduleId). */
  runSourceId?: string;
  /** Search-attributes containment filter. */
  metadata?: Record<string, unknown>;
  /** Page size, 1–500. Default 50. */
  limit?: number;
  offset?: number;
  /** Server-side sort column. Default: `startedAt`. */
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
  /**
   * `online` / `offline` are derived from heartbeat freshness; `retired`
   * is a graceful stop (the registry kept the row for forensics).
   */
  status: "online" | "offline" | "retired";
  queue?: string;
  labels?: Record<string, string>;
  activeTasks: number;
  completedToday: number;
  /** ISO timestamp of last heartbeat. */
  lastHeartbeatAt?: string;
  /** ISO timestamp the worker retired (graceful stop). Set only when `status` is `retired`. */
  retiredAt?: string;
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

/** Body of `POST /api/workflows/:name/runs`. Optional: no body starts a run with no input. */
export const TriggerRunRequestSchema = z
  .object({
    input: z.unknown().optional(),
    workflowType: Name.optional(),
    namespace: Name.optional(),
    metadata: z.record(z.unknown()).optional(),
    /**
     * Explicit run id. Default: a generated UUID. When a run with this id
     * already exists nothing starts: the response names it (`existing:
     * true`), or answers 409 when it belongs to another workflow.
     */
    workflowId: Id.optional(),
    /**
     * Workflow version this run should execute under. Recorded on the
     * storage row and routed to a worker advertising that version.
     */
    version: Name.optional(),
    /**
     * Provenance tag. Default `"manual"` (the dashboard's trigger button);
     * SDK clients send `"api"`, webhook ingestors `"webhook"`.
     */
    runSource: z.enum(RUN_SOURCES).optional(),
    /** Producer id paired with `runSource`. */
    runSourceId: Id.optional(),
  })
  .strict();

export type TriggerRunRequest = z.infer<typeof TriggerRunRequestSchema>;

/**
 * `202` + `Location: /api/runs/:id` when a run was started. `200` with
 * `existing: true` when a run with the requested `workflowId` already
 * existed: nothing was started and the response names that run. Re-run an
 * ended run with `POST /api/runs/:id/rerun`.
 */
export interface TriggerRunResponse {
  workflowId: string;
  existing?: true;
}

/** `202` from `POST /api/runs/:id/rerun`: a fresh run started under the same id. */
export interface RerunResponse {
  workflowId: string;
}

/** Body of `POST /api/runs/:id/signal`. */
export const SignalRequestSchema = z
  .object({
    signalName: Name,
    payload: z.unknown().optional(),
    /**
     * Refuse with 409 `signal_already_delivered` when a value for this
     * signal is already waiting to be consumed. Without it delivery is
     * last-wins: a second value replaces the first until the run consumes it.
     */
    ifAbsent: z.boolean().optional(),
  })
  .strict();

export type SignalRequest = z.infer<typeof SignalRequestSchema>;

/**
 * `202` from `POST /api/runs/:id/signal`: the value is stored and the run
 * resumes when it next consumes the signal (the signal scanner, within its
 * poll interval). Delivery is last-wins: a later delivery of the same name
 * before the run consumes it replaces this one, unless it sends `ifAbsent`.
 */
export interface SignalDeliveryResponse {
  workflowId: string;
  signalName: string;
}

/** Body of `POST /api/runs/:id/mark-failed`. Optional. */
export const MarkFailedRequestSchema = z
  .object({ reason: z.string().min(1).max(4096).optional() })
  .strict();

/** `200` from cancel / mark-success / mark-failed: the run's status after the action. */
export interface RunActionResponse {
  workflowId: string;
  status: WorkflowStatus;
}

/** Body of `POST /api/workflows/:name/rollback`. */
export const RollbackRequestSchema = z.object({ toVersion: Name }).strict();

export type RollbackRequest = z.infer<typeof RollbackRequestSchema>;

/** Longest `timeoutMs` a run query may ask for (ms). */
export const MAX_QUERY_TIMEOUT_MS = 30_000;

/** Body of `POST /api/runs/:id/query`. */
export const QueryRunRequestSchema = z
  .object({
    name: Name,
    args: z.unknown().optional(),
    /** Default 5000, at most 30000. */
    timeoutMs: z.number().int().min(1).max(MAX_QUERY_TIMEOUT_MS).optional(),
  })
  .strict();

/** Body of `POST /api/runs/:id/streams/:streamId`. `payload` is required (it may be `null`). */
export const StreamAppendRequestSchema = z
  .object({ payload: z.unknown() })
  .strict()
  .refine((b) => "payload" in b, { message: "Required", path: ["payload"] });

/** Body of `POST /api/signal-tokens/:tokenId/complete`. `value` is required (it may be `null`). */
export const CompleteSignalTokenRequestSchema = z
  .object({ value: z.unknown() })
  .strict()
  .refine((b) => "value" in b, { message: "Required", path: ["value"] });

export type CompleteSignalTokenRequest = z.infer<typeof CompleteSignalTokenRequestSchema>;

/** One field-level validation problem in an `invalid_request` error. */
export interface ApiIssue {
  path: string;
  message: string;
}

/** The one error envelope every route answers with. */
export interface ApiError {
  error: string;
  message?: string;
  /** Present on `invalid_request`: which fields failed and why. */
  issues?: ApiIssue[];
}

/** SSE event payloads sent on /api/runs/:id/events. */
export type RunEvent =
  | { type: "snapshot"; run: RunDto }
  | { type: "step"; stepName: string; step: StepDto }
  | { type: "status"; status: WorkflowStatus }
  | { type: "error"; error: string }
  | { type: "end" };
