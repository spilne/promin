// ---------------------------------------------------------------------------
// StepExecutor — the pluggable "run a single step body" boundary, plus the
// in-process implementation. When a runner is configured with an executor,
// the DAG executor hands every ready step to it (in-process, queue-
// dispatched, remote worker) instead of running the body inline. The shape
// is request/response.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { StepRuntime, Workflow } from "../durable-pipeline.ts";
import type {
  WorkflowContinueAsNewError,
  WorkflowSuspendedError,
} from "../durable-pipeline-error.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { OrchestrationRuntime } from "./orchestration-context.ts";
import { runChildWorkflow } from "./orchestrate.ts";
import { errorMessage, runStepBody, type StepAttemptFailure } from "./step-body.ts";

export interface StepExecutionRequest {
  readonly workflowId: string;
  readonly stepName: string;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  /** Capabilities the step requires — forwarded to capability-aware workers. */
  readonly needs?: readonly string[];
  readonly priority?: number;
  /**
   * Version of the definition driving the run. Forwarded to the step queue
   * so version-pinned workers (`supportedVersions`) only claim their own.
   */
  readonly version?: string;
  /**
   * Resolved per-task concurrency cap, computed by the coordinator from
   * the workflow / step queue config (step-level wins over workflow-level).
   * Forwarded to the step queue so `claim()` can enforce against currently-
   * running tasks with the same `(scope, key)` tuple.
   */
  readonly concurrencyKey?: string;
  readonly concurrencyScope?: string;
  readonly concurrencyLimit?: number;
  /**
   * In-process runtime of the run that issued the request: clock, fence
   * guard, child runner, stored step row, version and patches. Set by the
   * runner; never serialized, so queue-backed executors ignore it and
   * `InProcessStepExecutor` falls back to its own config without it.
   */
  readonly runtime?: StepRuntime;
  /**
   * Aborted when another step of the same wave fails. Advisory: the runner
   * still waits for this step's result and records whatever it reports,
   * so an executor that stops early reports the step as failed. The bundled
   * executors run the step to completion (an in-process body has no
   * cancellation point, and a queued task cannot be recalled).
   */
  readonly signal?: AbortSignal;
}

/**
 * Fields an executor reports for a step that settled (completed or
 * failed). All optional: a minimal executor reports none of them.
 */
interface StepExecutionReport {
  readonly metadata?: Record<string, unknown>;
  /**
   * The executor already persisted this step (its row and its attempt
   * rows), so the runner writes nothing for it.
   */
  readonly storageAlreadyCheckpointed?: boolean;
  /** Attempt number of the last invocation. Default: the request's `attempt`. */
  readonly attempt?: number;
  /** Attempts that failed before the last one (or including it, when `onFailure` absorbed it), oldest first. */
  readonly failedAttempts?: readonly StepAttemptFailure[];
}

/**
 * How a step settled, as reported by a `StepExecutor`. `kind` tells a
 * failure apart from control flow: `"suspended"` (the step is sleeping or
 * waiting for a signal) and `"continue-as-new"` unwind the run without
 * failing it. A failure without `kind` is a failure.
 */
export type StepExecutionResult =
  | (StepExecutionReport & {
      readonly ok: true;
      readonly kind?: "completed";
      /** Codec-encoded result. */
      readonly result: unknown;
    })
  | (StepExecutionReport & {
      readonly ok: false;
      readonly kind?: "failed";
      /** Error message. */
      readonly error: string;
      /** `_tag` of the original error, when it had one. */
      readonly errorTag?: string;
      /**
       * The original error, from executors that run in-process. Never
       * serialized; when set, the run fails with it exactly as the inline
       * path would.
       */
      readonly cause?: unknown;
    })
  | {
      readonly ok: false;
      readonly kind: "suspended";
      readonly reason: "sleep" | "signal";
      readonly message?: string;
      /** The original `WorkflowSuspendedError`, from in-process executors. */
      readonly cause?: unknown;
    }
  | {
      readonly ok: false;
      readonly kind: "continue-as-new";
      /** Input of the next run. */
      readonly nextInput: unknown;
      readonly message?: string;
      /** The original `WorkflowContinueAsNewError`, from in-process executors. */
      readonly cause?: unknown;
    };

export interface StepExecutor {
  /**
   * Run a single step body and return its terminal result. Implementations
   * are responsible for applying step-level retry (since the policy is owned
   * by the step definition), per-step timeout, and onFailure strategy —
   * the runner only decides *when* to invoke `executeStep` based on the DAG
   * ready-set, workflow-level retry, compensation, etc.
   *
   * Errors should be surfaced as `{ ok: false, error }` rather than thrown,
   * and suspension / continue-as-new as `{ ok: false, kind }`, so the
   * runner's state machine can branch uniformly across in-process and
   * remote executors. A thrown error is treated as the step's failure
   * (a thrown suspension or continue-as-new keeps its meaning).
   */
  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult>;
  /**
   * Executors bound to one definition (like `InProcessStepExecutor`)
   * return an executor for `workflow` with the same configuration. The
   * runner calls it when the executor drives a different definition: a
   * version-drained run or a child workflow. Definition-agnostic
   * executors (queue-backed) leave it out and are reused as-is.
   */
  forWorkflow?(workflow: Workflow<unknown, unknown>): StepExecutor;
}

/**
 * Runs a single step body in-process: applies per-step timeout, retry, and
 * onFailure strategy, then returns the encoded result. Used by the
 * distributed worker so each worker node executes only the steps it claims
 * from the queue, without running the full orchestration loop.
 *
 * Step bodies get the request's `runtime` when the runner supplies one.
 * Without it (a worker running a claimed task), they get this executor's
 * clock, the bound definition's version and patches, and a child runner on
 * this executor's storage and clock; storage writes are then unfenced.
 *
 * Never throws for a step's own outcome: suspension and continue-as-new are
 * reported by `kind`, and failures (defects included) carry the original
 * error as `cause` plus its `errorTag`.
 */
export class InProcessStepExecutor implements StepExecutor {
  private readonly workflow: Workflow<unknown, unknown>;
  private readonly storage: WorkflowStorage;
  private readonly clock: WallClock;

  constructor(
    workflow: Workflow<unknown, unknown>,
    config: {
      storage: WorkflowStorage;
      /** Time source for step retry backoff. Default: `SystemWallClock`. */
      clock?: WallClock;
    },
  ) {
    this.workflow = workflow;
    this.storage = config.storage;
    this.clock = config.clock ?? SystemWallClock;
  }

  forWorkflow(workflow: Workflow<unknown, unknown>): StepExecutor {
    if (workflow === this.workflow) return this;
    return new InProcessStepExecutor(workflow, { storage: this.storage, clock: this.clock });
  }

  async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    const stepDef = this.workflow._definition.steps.find((s) => s.name === req.stepName);
    if (!stepDef) {
      return {
        ok: false,
        kind: "failed",
        error: `Step "${req.stepName}" not found in workflow "${this.workflow.name}"`,
      };
    }

    const runtime = this.runtimeFor(req);
    const clock = runtime.clock ?? this.clock;
    const body = await runStepBody({
      stepDef,
      workflowId: req.workflowId,
      clock,
      firstAttempt: req.attempt,
      execute: ({ attempt, metadataRef }) =>
        stepDef.execute({
          ...runtime,
          clock,
          input: req.input,
          results: req.prevResults,
          workflowId: req.workflowId,
          storage: this.storage,
          attemptRef: { current: attempt },
          metadataRef,
        }),
    });

    switch (body.kind) {
      case "completed":
        return {
          ok: true,
          result: body.result,
          metadata: body.metadata,
          attempt: body.attempt,
          failedAttempts: body.failedAttempts,
        };
      case "failed": {
        const errorTag = (body.error as { _tag?: unknown } | null | undefined)?._tag;
        return {
          ok: false,
          kind: "failed",
          error: errorMessage(body.error),
          ...(typeof errorTag === "string" && { errorTag }),
          cause: body.error,
          metadata: body.metadata,
          attempt: body.attempt,
          failedAttempts: body.failedAttempts,
        };
      }
      case "suspended": {
        const suspended = body.error as WorkflowSuspendedError;
        return {
          ok: false,
          kind: "suspended",
          reason: suspended.reason,
          message: suspended.message,
          cause: body.error,
        };
      }
      case "continue-as-new": {
        const next = body.error as WorkflowContinueAsNewError;
        return {
          ok: false,
          kind: "continue-as-new",
          nextInput: next.nextInput,
          message: next.message,
          cause: body.error,
        };
      }
    }
  }

  /** The request's runtime, or this executor's own when the runner sent none. */
  private runtimeFor(req: StepExecutionRequest): StepRuntime {
    if (req.runtime) return req.runtime;
    const def = this.workflow._definition;
    const runtime: OrchestrationRuntime = { storage: this.storage, clock: this.clock };
    return {
      clock: this.clock,
      ...(this.workflow.version !== undefined && { workflowVersion: this.workflow.version }),
      ...(def.patches !== undefined && { patches: def.patches }),
      runChild: (child) =>
        runChildWorkflow({ runtime, parentWorkflowId: req.workflowId, ...child }),
    };
  }
}
