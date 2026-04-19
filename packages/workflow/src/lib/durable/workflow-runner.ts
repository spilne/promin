// ---------------------------------------------------------------------------
// WorkflowRunner — orchestration engine, pluggable step execution.
//
// Eventually this owns the entirety of run-loop orchestration: lock /
// heartbeat / state CRUD, ready-set computation, workflow-level retry,
// compensation, version drain, idempotency TTL, DLQ publish, journaled-step
// replay, suspend / resume, deadlines. Step bodies themselves are run via
// a pluggable `StepExecutor` so in-process TS handlers, remote gRPC workers,
// and queue-dispatched workers all share one orchestration codepath.
//
// Phase 1 (this module) ships the public interfaces + a default runner
// that delegates to the existing `RunnableWorkflow.run()` method so
// downstream packages can code against the runner shape today without
// waiting for the full extraction. Phases 2-3 (promin-e0hd) pull the
// orchestration body out of `WorkflowBuilder.run` into this class and
// formalize `StepExecutor` as the in-process vs remote vs queue seam.
// ---------------------------------------------------------------------------

import { Pipeline, type TaggedError } from "@promin/core";
import type { RunnableWorkflow, Workflow, CompensateConfig } from "./durable-pipeline.ts";
import type { WorkflowHooks, IdempotencyConfig } from "./durable-pipeline.ts";
import { isStepAttemptStorage, type WorkflowStorage } from "./workflow-storage.ts";
import type { DagNode } from "./workflow-dag.ts";
import type {
  StepError,
  WorkflowError,
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  StepTimeoutError,
  WorkflowDeadlineError,
} from "./durable-pipeline-error.ts";

// ---------------------------------------------------------------------------
// StepExecutor — pluggable "how to run a single step body" boundary.
//
// Not yet threaded through the runner in phase 1 — defined here so downstream
// modules (distributed worker, future gRPC executor) can code against the
// shape, and phase 2 has a fixed surface to extract onto. The shape
// intentionally stays request/response; streaming (logs, backpressure) lands
// in phase 4 once promin-m7c's gRPC semantics are concrete.
// ---------------------------------------------------------------------------

export interface StepExecutionRequest {
  readonly workflowId: string;
  readonly stepName: string;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  /** Capabilities the step requires — forwarded to capability-aware workers. */
  readonly needs?: readonly string[];
  readonly priority?: number;
  readonly version?: string;
}

export type StepExecutionResult =
  | { readonly ok: true; readonly result: unknown; readonly metadata?: Record<string, unknown> }
  | { readonly ok: false; readonly error: string };

export interface StepExecutor {
  /**
   * Run a single step body and return its terminal result. Implementations
   * are responsible for applying step-level retry (since the policy is owned
   * by the step definition), per-step timeout, and onFailure strategy —
   * the runner only decides *when* to invoke `executeStep` based on the DAG
   * ready-set, workflow-level retry, compensation, etc.
   *
   * Errors should be surfaced as `{ ok: false, error }` rather than thrown,
   * so the runner's state machine can branch uniformly across in-process
   * and remote executors.
   */
  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult>;
}

// ---------------------------------------------------------------------------
// WorkflowRunner — orchestration engine.
// ---------------------------------------------------------------------------

export interface WorkflowRunnerExecuteParams<Input> {
  /**
   * The workflow to run. Phase 1 accepts a `RunnableWorkflow` (already bound
   * to storage) because the default runner delegates to its `.run()`. Phase
   * 2 will narrow this to pure `Workflow` + an explicit `storage` param +
   * `stepExecutor` — at which point the runner owns the orchestration.
   */
  readonly workflow: RunnableWorkflow<Input, unknown>;
  readonly workflowId: string;
  readonly input: Input;
  readonly force?: boolean;
  /** Optional hooks — mirrors what `WorkflowBuilder.run` exposes today. */
  readonly hooks?: WorkflowHooks;
}

export type WorkflowRunSafeError =
  | Error
  | WorkflowError
  | StepError
  | WorkflowLockError
  | WorkflowSuspendedError
  | WorkflowTimeoutError
  | StepTimeoutError
  | WorkflowDeadlineError
  | TaggedError;

/**
 * Runs a workflow end-to-end. Holds orchestration (DAG ready-set, lock,
 * heartbeat, retry, compensation, idempotency) and delegates step execution
 * to a `StepExecutor`.
 *
 * Phase 1 ships the interface; the default runner here just calls through
 * to `workflow.run()` so downstream packages can adopt the runner-shaped
 * API today. Phase 2 pulls the orchestration body in.
 */
export interface WorkflowRunner {
  /** Execute and throw on failure. */
  execute<Input, Output>(params: WorkflowRunnerExecuteParams<Input>): Promise<Output>;
  /** Execute and return `{ data, error }` instead of throwing. */
  executeSafe<Input, Output>(
    params: WorkflowRunnerExecuteParams<Input>,
  ): Promise<{ data: Output; error: null } | { data: null; error: WorkflowRunSafeError }>;
}

/**
 * Default implementation. Phase 1: delegates to the bound workflow's own
 * `.run()` / `.runSafe()`, which still runs the existing orchestration body
 * inside `WorkflowBuilder`. Phase 2 replaces this body with a direct
 * orchestration loop driven by a `StepExecutor`.
 *
 * Intentionally a class (not a bare function) so phase 2 can add config
 * (default step executor, observability hooks, tracing span factory)
 * without breaking callers.
 */
export class DefaultWorkflowRunner implements WorkflowRunner {
  async execute<Input, Output>(params: WorkflowRunnerExecuteParams<Input>): Promise<Output> {
    const { workflow, workflowId, input, force } = params;
    return workflow.run({ workflowId, input, force }) as Promise<Output>;
  }

  async executeSafe<Input, Output>(
    params: WorkflowRunnerExecuteParams<Input>,
  ): Promise<{ data: Output; error: null } | { data: null; error: WorkflowRunSafeError }> {
    const { workflow, workflowId, input, force } = params;
    return workflow.runSafe({ workflowId, input, force }) as Promise<
      { data: Output; error: null } | { data: null; error: WorkflowRunSafeError }
    >;
  }
}

/**
 * Convenience factory — `createWorkflowRunner()` mirrors how other
 * promin components construct their default implementation.
 */
export function createWorkflowRunner(): WorkflowRunner {
  return new DefaultWorkflowRunner();
}

// ---------------------------------------------------------------------------
// Orchestration helpers — pure, context-taking versions of the utilities
// that used to be private methods on WorkflowBuilder. They live here so
// phase 1b/1c can incrementally move orchestration out of the builder
// without the runner needing access to private class state.
// ---------------------------------------------------------------------------

/**
 * A step definition's view as needed by compensation — just the name and
 * the rollback function. A full `StepDefinition` is a superset and passes
 * this check via structural typing; keeps compensation decoupled from the
 * rest of the step shape (execute / codec / retry / etc.).
 */
export interface CompensatableStep {
  readonly name: string;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, TaggedError> | Promise<void>;
}

/**
 * Run the saga rollback for a workflow — reverses completed steps that
 * carry a `compensate` function, in last-completed-first order, honoring
 * the compensate config's per-step retry policy. Records an attempt row
 * per try when the storage supports `StepAttemptStorage`.
 *
 * Formerly `WorkflowBuilder._compensate`. Pure function now — takes the
 * storage + step list + retry config directly so the runner can drive
 * compensation without holding a builder reference.
 */
export async function compensateWorkflow(params: {
  storage: WorkflowStorage;
  steps: ReadonlyArray<CompensatableStep>;
  compensateConfig?: CompensateConfig;
  workflowId: string;
  input: unknown;
  dagNodes: DagNode[];
}): Promise<{
  compensated: string[];
  failed: { stepName: string; error: unknown }[];
}> {
  const { storage, steps, compensateConfig, workflowId, input } = params;
  const compensated: string[] = [];
  const failed: { stepName: string; error: unknown }[] = [];

  const state = await storage.loadWorkflow(workflowId);
  if (!state) return { compensated, failed };

  // Reverse order so last-completed is compensated first — saga semantics.
  const stepsToCompensate: { stepDef: CompensatableStep; result: unknown }[] = [];
  for (let i = steps.length - 1; i >= 0; i--) {
    const stepDef = steps[i]!;
    const stepState = state.steps[stepDef.name];
    if (stepState?.status === "completed" && stepDef.compensate) {
      stepsToCompensate.push({ stepDef, result: stepState.result });
    }
  }

  const retryConfig = compensateConfig?.retry;
  const maxCompRetries = retryConfig?.maxRetries ?? 0;
  const compRetryDelayMs = retryConfig?.baseDelayMs ?? 500;

  const recordsAttempts = isStepAttemptStorage(storage);

  for (const { stepDef, result } of stepsToCompensate) {
    for (let attempt = 0; attempt <= maxCompRetries; attempt++) {
      const compStartedAt = new Date();
      try {
        if (attempt > 0) {
          await new Promise((r) => setTimeout(r, compRetryDelayMs * Math.pow(2, attempt - 1)));
        }
        const compensateResult = stepDef.compensate!({ result, input, workflowId });
        if (compensateResult instanceof Pipeline) {
          await compensateResult.runPromise();
        } else if (
          compensateResult &&
          typeof (compensateResult as Promise<void>).then === "function"
        ) {
          await compensateResult;
        }
        compensated.push(stepDef.name);
        if (recordsAttempts) {
          await (storage as any).saveStepAttempt({
            workflowId,
            stepName: stepDef.name,
            attempt: attempt + 1,
            type: "compensation",
            status: "completed",
            durationMs: Date.now() - compStartedAt.getTime(),
            startedAt: compStartedAt,
            completedAt: new Date(),
          });
        }
        break;
      } catch (err) {
        if (recordsAttempts) {
          await (storage as any).saveStepAttempt({
            workflowId,
            stepName: stepDef.name,
            attempt: attempt + 1,
            type: "compensation",
            status: "failed",
            error: err instanceof Error ? err.message : String(err),
            durationMs: Date.now() - compStartedAt.getTime(),
            startedAt: compStartedAt,
            completedAt: new Date(),
          });
        }
        if (attempt === maxCompRetries) {
          failed.push({ stepName: stepDef.name, error: err });
        }
      }
    }
  }

  return { compensated, failed };
}

/**
 * Resolve the idempotency TTL for a given terminal status.
 * Returns `undefined` when no idempotency config is active or no TTL is
 * configured for the passed status. Formerly a private method on
 * WorkflowBuilder (`_getIdempotencyTtl`); extracted here so the runner
 * can make the caching decision without holding a reference to the
 * builder instance.
 */
export function getIdempotencyTtl(
  idempotency: IdempotencyConfig | undefined,
  status: string,
): number | undefined {
  if (!idempotency) return undefined;
  const ttl = idempotency.ttl;
  if (typeof ttl === "number") return ttl;
  if (status === "completed") return ttl.success;
  if (status === "failed") return ttl.failure;
  return undefined;
}

// ---------------------------------------------------------------------------
// InProcessStepExecutor — placeholder for phase 2.
//
// Will extract step-body execution from `WorkflowBuilder` and wrap it here.
// Exported as a constructor that takes a workflow + hooks so downstream
// modules (distributed worker) can already reference the shape — phase 2
// fills in the real body.
// ---------------------------------------------------------------------------

/**
 * Placeholder for phase 2. The in-process executor will wrap the step
 * handlers attached to a workflow's builder and run them locally.
 */
export class InProcessStepExecutor implements StepExecutor {
  constructor(
    private readonly workflow: Workflow<unknown, unknown> | RunnableWorkflow<unknown, unknown>,
    private readonly hooks?: WorkflowHooks,
  ) {
    void this.workflow;
    void this.hooks;
  }

  async executeStep(_req: StepExecutionRequest): Promise<StepExecutionResult> {
    // Phase 2: this replaces the per-step body in `WorkflowBuilder.run`.
    // Until then the `DefaultWorkflowRunner` skips this seam and delegates
    // to the bound workflow's `.run()`. Callers that instantiate an
    // InProcessStepExecutor directly are building against the future
    // shape — they'll get the real implementation once phase 2 lands.
    throw new Error(
      "InProcessStepExecutor.executeStep is not implemented yet — wait for promin-e0hd phase 2. " +
        "Use RunnableWorkflow.run / createWorkflowRunner().execute() for now.",
    );
  }
}
