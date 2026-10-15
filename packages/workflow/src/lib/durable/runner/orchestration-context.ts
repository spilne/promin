// ---------------------------------------------------------------------------
// Orchestration context — the full per-run state the orchestration loop
// needs: the workflow definition's knobs plus the runtime (storage, clock,
// step executor, executor id).
// ---------------------------------------------------------------------------

import type { Sinkable } from "../../shared/streamable.ts";
import type { TaggedError } from "../../shared/tagged-error.ts";
import type { RetryPolicy } from "../../shared/retry-policy.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import type {
  CompensateConfig,
  DispatchConfig,
  IdempotencyConfig,
  StepDefinition,
  Workflow,
  WorkflowHooks,
  WorkflowQueueConfig,
} from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { FailedWorkflowRecord } from "../workflow-state.ts";
import type { StepExecutor } from "./step-executor.ts";

/**
 * Full workflow-runtime state the orchestration loop needs. A superset
 * of DagExecutionContext — adds the lock / retry / compensation / DLQ
 * / idempotency / version knobs that live outside the DAG executor.
 * Built once per `run()` from the bound workflow's builder state.
 */
export interface WorkflowOrchestrationContext {
  readonly storage: WorkflowStorage;
  readonly name: string;
  readonly version?: string;
  readonly type?: string;
  readonly metadata?: Record<string, unknown>;
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly retry?: RetryPolicy<TaggedError>;
  readonly compensateConfig?: CompensateConfig;
  readonly dlq?: Sinkable<FailedWorkflowRecord>;
  readonly dispatch?: DispatchConfig;
  readonly idempotency?: IdempotencyConfig;
  readonly timeoutMs?: number;
  readonly onVersionMismatch: "strict" | "drain";
  readonly previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  readonly hooks?: WorkflowHooks;
  /**
   * Workflow-level queue concurrency cap. Stamped onto every dispatched
   * step task; step-level `StepDefinition.queue` overrides for that step.
   */
  readonly queue?: WorkflowQueueConfig<unknown>;
  /**
   * Pluggable step executor. Threaded through to `DagExecutionContext` so
   * the DAG loop delegates step bodies to the configured executor.
   */
  readonly stepExecutor?: StepExecutor;
  /**
   * Time source. Drives workflow start/deadline math, idempotency TTL
   * comparisons, step duration tracking, retry/compensation backoff sleeps,
   * and the `withLock` heartbeat interval. Defaults to `SystemWallClock` when
   * omitted — callers building contexts by hand should only override it
   * for tests.
   */
  readonly clock?: WallClock;
  /**
   * Identifier of the executor running this orchestration (worker id,
   * process id, "in-process", etc.). Stamped on each StepAttemptRecord
   * so the audit trail attributes the attempt to who actually ran it.
   */
  readonly executorId?: string;
}
