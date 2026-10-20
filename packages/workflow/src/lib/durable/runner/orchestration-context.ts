// ---------------------------------------------------------------------------
// Orchestration context — the full per-run state the orchestration loop
// needs: the workflow definition's knobs plus the runtime (storage, clock,
// step executor, executor id, hooks), and the one factory that builds it.
// ---------------------------------------------------------------------------

import type { Sinkable } from "../../shared/streamable.ts";
import type { WorkflowRetryPolicy } from "../../shared/retry-policy.ts";
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
 * Built by `orchestrationContextFor()` for every run, drain and child.
 */
export interface WorkflowOrchestrationContext {
  readonly storage: WorkflowStorage;
  readonly name: string;
  readonly version?: string;
  readonly type?: string;
  readonly metadata?: Record<string, unknown>;
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly retry?: WorkflowRetryPolicy;
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
  /**
   * Patch names active in the definition driving this run. Handed to every
   * step as `ExecuteParams.patches`.
   */
  readonly patches?: readonly string[];
  /**
   * Runner-level hooks (`WorkflowRunnerConfig.hooks`), kept apart from the
   * resolved `hooks` so child runs resolve them against their own
   * definition's hooks.
   */
  readonly runnerHooks?: WorkflowHooks;
}

/**
 * What a run inherits from whoever drives it: the runner, the run a version
 * drain takes over, or a parent run launching a child.
 */
export interface OrchestrationRuntime {
  readonly storage: WorkflowStorage;
  readonly clock?: WallClock;
  /**
   * Step executor. A definition-bound executor (one implementing
   * `forWorkflow`) is rebound to the workflow the context is built for.
   */
  readonly stepExecutor?: StepExecutor;
  readonly executorId?: string;
  /** Runner-level hooks. Win over the definition's own hooks. */
  readonly hooks?: WorkflowHooks;
}

/**
 * Build the orchestration context for running `workflow` on `runtime`. The
 * one place a definition's knobs and the runtime are combined: the runner's
 * `run`, a version drain and every child run go through it, so all of them
 * see the same clock, step executor, executor id and hooks.
 *
 * `hooks` replaces the resolved hooks outright (a version drain keeps the
 * hooks of the run it takes over).
 */
export function orchestrationContextFor(params: {
  readonly workflow: Workflow<unknown, unknown>;
  readonly runtime: OrchestrationRuntime;
  readonly hooks?: WorkflowHooks;
}): WorkflowOrchestrationContext {
  const { workflow, runtime } = params;
  const def = workflow._definition;
  const stepExecutor = runtime.stepExecutor?.forWorkflow?.(workflow) ?? runtime.stepExecutor;
  return {
    storage: runtime.storage,
    name: workflow.name,
    version: workflow.version,
    idempotency: workflow.idempotency,
    type: def.type,
    metadata: def.metadata,
    steps: def.steps,
    retry: def.retry,
    compensateConfig: def.compensateConfig,
    dlq: def.dlq,
    dispatch: def.dispatch,
    timeoutMs: def.timeoutMs,
    onVersionMismatch: def.onVersionMismatch,
    previousVersions: def.previousVersions,
    hooks: params.hooks ?? runtime.hooks ?? def.hooks,
    queue: def.queue,
    patches: def.patches,
    ...(stepExecutor !== undefined && { stepExecutor }),
    ...(runtime.clock !== undefined && { clock: runtime.clock }),
    ...(runtime.executorId !== undefined && { executorId: runtime.executorId }),
    ...(runtime.hooks !== undefined && { runnerHooks: runtime.hooks }),
  };
}

/** The runtime a context hands on to the runs it starts (drains, children). */
export function runtimeOf(ctx: WorkflowOrchestrationContext): OrchestrationRuntime {
  return {
    storage: ctx.storage,
    ...(ctx.clock !== undefined && { clock: ctx.clock }),
    ...(ctx.stepExecutor !== undefined && { stepExecutor: ctx.stepExecutor }),
    ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
    ...(ctx.runnerHooks !== undefined && { hooks: ctx.runnerHooks }),
  };
}
