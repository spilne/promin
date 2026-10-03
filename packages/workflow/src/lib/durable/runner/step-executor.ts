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
import type { Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
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
}

/**
 * Runs a single step body in-process: applies per-step timeout, retry, and
 * onFailure strategy, then returns the encoded result. Used by the
 * distributed worker so each worker node executes only the steps it claims
 * from the queue, without running the full orchestration loop.
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

    const raw = applyStepPolicies({
      stepDef,
      workflowId: req.workflowId,
      clock: this.clock,
      invoke: () => {
        const currentAttempt = attemptRef.current;
        attemptRef.current = currentAttempt + 1;
        return stepDef.execute({
          input: req.input,
          results: req.prevResults,
          workflowId: req.workflowId,
          storage: this.storage,
          attemptRef: { current: currentAttempt },
          metadataRef,
          clock: this.clock,
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
}
