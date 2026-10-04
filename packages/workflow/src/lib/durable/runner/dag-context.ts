// ---------------------------------------------------------------------------
// DAG execution types — the context the DAG executor and its waves run
// against, the settled wave, and the executor's result union, plus
// the per-step runtime both waves hand to step bodies.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import type {
  DispatchConfig,
  RunChildWorkflow,
  StepDefinition,
  StepRuntime,
  WorkflowHooks,
  WorkflowQueueConfig,
} from "../durable-pipeline.ts";
import type { StepState } from "../workflow-state.ts";
import type { FenceGuard, WorkflowStorage } from "../workflow-storage.ts";
import type { StepOutcome } from "./step-checkpoint.ts";
import type { StepExecutor } from "./step-executor.ts";

/**
 * Slice of the workflow-runtime state the DAG executor needs. A strict
 * subset of the broader orchestration context so callers that only want
 * to drive the DAG (without the surrounding lock / retry / compensation
 * loop) don't need to construct unused fields.
 */
export interface DagExecutionContext {
  readonly storage: WorkflowStorage;
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly hooks?: WorkflowHooks;
  readonly timeoutMs?: number;
  readonly dispatch?: DispatchConfig;
  /**
   * Workflow name + workflow-level queue config — needed to derive a
   * task's `(concurrencyScope, concurrencyKey, concurrencyLimit)` triple
   * at enqueue time. The DAG context is the lowest layer that still has
   * both the input under run and the workflow-level config in scope.
   */
  readonly workflowName?: string;
  readonly workflowQueue?: WorkflowQueueConfig<unknown>;
  /**
   * Fence guard for mutating writes. Captured by the orchestration loop
   * after `tryLock` and threaded into every `saveStepResult` /
   * `saveStepFailure` / `saveStepAttempt` call so a stale holder that
   * wakes up past lock expiry is rejected by the backend. `undefined`
   * on backends without fencing or when the caller is running a
   * sub-DAG outside a lock.
   */
  readonly guard?: FenceGuard;
  /**
   * Pluggable step executor. When set, step bodies run through this executor
   * instead of the inline Eff execution. `undefined` preserves the existing
   * in-process path.
   */
  readonly stepExecutor?: StepExecutor;
  /** Time source. Drives deadline checks, step durations, dispatch poll waits. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /**
   * Identifier of the executor running this DAG execution. Stamped on each
   * `StepAttemptRecord` so the audit trail attributes the attempt to who
   * actually ran it (worker id, in-process pid, etc.). Threaded down from
   * `WorkflowOrchestrationContext.executorId`.
   */
  readonly executorId?: string;
  /**
   * Version of the definition driving this run. Handed to steps
   * (`ExecuteParams.workflowVersion`) and stamped on executor requests
   * (`StepExecutionRequest.version`) so version-pinned workers filter on it.
   */
  readonly workflowVersion?: string;
  /** Patch names active in the definition driving this run (`ExecuteParams.patches`). */
  readonly patches?: readonly string[];
  /** Runs a child workflow of this run on its runtime (`ExecuteParams.runChild`). */
  readonly runChild?: RunChildWorkflow;
}

/**
 * The runtime fields of `ExecuteParams` for one step of a wave: the run's
 * clock, fence guard, child runner, version and patches, plus the step's
 * stored row as of the last load of the run.
 */
export function stepRuntimeFor(params: {
  readonly ctx: DagExecutionContext;
  readonly clock: WallClock;
  readonly stepStates: Readonly<Record<string, StepState>>;
  readonly stepName: string;
}): StepRuntime {
  const { ctx, clock } = params;
  return {
    clock,
    stepState: params.stepStates[params.stepName] ?? null,
    ...(ctx.guard !== undefined && { guard: ctx.guard }),
    ...(ctx.runChild !== undefined && { runChild: ctx.runChild }),
    ...(ctx.workflowVersion !== undefined && { workflowVersion: ctx.workflowVersion }),
    ...(ctx.patches !== undefined && { patches: ctx.patches }),
  };
}

/**
 * A settled wave: one checkpointed outcome per ready step, in ready order.
 * Every step settles before the wave returns; a failing step never cuts
 * its siblings short.
 */
export interface WaveOutcome {
  readonly outcomes: StepOutcome[];
}

/** Inputs shared by both wave implementations. */
export interface WaveParams {
  readonly ctx: DagExecutionContext;
  readonly workflowId: string;
  readonly input: unknown;
  readonly readySteps: StepDefinition[];
  /** Decoded results of completed steps, keyed by step name. */
  readonly results: Record<string, unknown>;
  /** Tracks attempt numbers per step — shared across workflow retries so counters keep incrementing. */
  readonly stepAttempts: Map<string, number>;
  readonly clock: WallClock;
  /** Stored step rows as of the run's last load, keyed by step name. */
  readonly stepStates: Readonly<Record<string, StepState>>;
}

/** Failure outcome of `executeWorkflowDag` (suspension and continue-as-new included). */
export type DagExecutionFailure = {
  success: false;
  error: unknown;
  suspension: boolean;
  continueAsNew?: boolean;
};

/** Outcome of `executeWorkflowDag`. */
export type DagExecutionResult =
  | { success: true; result: unknown }
  | DagExecutionFailure
  | { success: false; tripwire: true; stepName: string; reason: unknown };
