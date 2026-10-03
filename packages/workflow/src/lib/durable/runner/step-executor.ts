// ---------------------------------------------------------------------------
// StepExecutor — the pluggable "run a single step body" boundary, plus the
// in-process implementation. When a runner is configured with an executor,
// the DAG executor hands every ready step to it (in-process, queue-
// dispatched, remote worker) instead of running the body inline. The shape
// is request/response.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../../shared/tagged-error.ts";
import { runEffSafe } from "../../shared/eff.ts";
import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { StepRuntime, Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { OrchestrationRuntime } from "./orchestration-context.ts";
import { runChildWorkflow } from "./orchestrate.ts";
import { applyStepPolicies } from "./step-policies.ts";

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
}

export type StepExecutionResult =
  | {
      readonly ok: true;
      readonly result: unknown;
      readonly metadata?: Record<string, unknown>;
      /** When true, the executor already persisted this step — runner skips saveStepResult. */
      readonly storageAlreadyCheckpointed?: boolean;
    }
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
 * Re-throws `WorkflowSuspendedError` directly rather than wrapping it in
 * `{ ok: false }` so the caller can distinguish suspension from failure.
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
        error: `Step "${req.stepName}" not found in workflow "${this.workflow.name}"`,
      };
    }

    const attemptRef = { current: req.attempt };
    const metadataRef: { current?: Record<string, unknown> } = { current: undefined };

    const runtime = this.runtimeFor(req);
    const clock = runtime.clock ?? this.clock;
    const raw = applyStepPolicies({
      stepDef,
      workflowId: req.workflowId,
      clock,
      invoke: () => {
        const currentAttempt = attemptRef.current;
        attemptRef.current = currentAttempt + 1;
        return stepDef.execute({
          ...runtime,
          clock,
          input: req.input,
          results: req.prevResults,
          workflowId: req.workflowId,
          storage: this.storage,
          attemptRef: { current: currentAttempt },
          metadataRef,
        });
      },
    });

    // Defects (a rejected `.stepAsync()` body, a synchronous throw) reject
    // here rather than landing in `{ ok: false }`.
    const { data, error } = await runEffSafe(raw.map((result) => stepDef.codec.encode(result)));

    if (error) {
      if ((error as TaggedError)._tag === "WorkflowSuspendedError") {
        throw error;
      }
      const errorMsg = error instanceof globalThis.Error ? error.message : String(error);
      return { ok: false, error: errorMsg };
    }

    return { ok: true, result: data!, metadata: metadataRef.current };
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
