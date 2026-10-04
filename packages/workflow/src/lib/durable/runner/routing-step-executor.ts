// ---------------------------------------------------------------------------
// Routing step executor — sends a fixed set of steps to one executor (a
// queue of remote workers, typically) and every other step to another (the
// in-process executor by default). Replaces the workflow-level
// `dispatch.remoteSteps` option, which the runner now maps onto it.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../../shared/wall-clock.ts";
import type { Workflow } from "../durable-pipeline.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import {
  InProcessStepExecutor,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepExecutor,
} from "./step-executor.ts";

/** Config of `RoutingStepExecutor`. */
export interface RoutingStepExecutorConfig {
  /** Executor for the steps named in `remoteSteps` (e.g. a `StepQueueExecutor`). */
  readonly remote: StepExecutor;
  /** Names of the steps `remote` runs. Every other step runs on `local`. */
  readonly remoteSteps: readonly string[];
  /**
   * Executor for every other step. Default: an `InProcessStepExecutor`
   * for the workflow the runner binds this executor to (`forWorkflow`) on
   * `storage` and `clock`.
   */
  readonly local?: StepExecutor;
  /** Storage of the default local executor. Required without `local`. */
  readonly storage?: WorkflowStorage;
  /** Time source of the default local executor. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/**
 * A `StepExecutor` that routes each step by name: steps listed in
 * `remoteSteps` go to `remote`, the rest to `local`. Each step settles
 * through the runner's executor wave, so routed steps run concurrently
 * with the rest of their wave, share its attempt numbering, concurrency
 * keys and version stamping, and have their results decoded and
 * compensated like any other step.
 *
 * ```ts
 * const runner = createWorkflowRunner({
 *   storage,
 *   stepExecutor: new RoutingStepExecutor({
 *     remote: new StepQueueExecutor({ stepQueue, storage }),
 *     remoteSteps: ["transcribe", "train-model"],
 *     storage,
 *   }),
 * });
 * ```
 *
 * The runner calls `forWorkflow` for every definition it drives (a run, a
 * version drain, a child workflow), which binds both executors to that
 * definition. Children inherit the routing, so a child step with a listed
 * name goes to `remote` too.
 */
export class RoutingStepExecutor implements StepExecutor {
  private readonly config: RoutingStepExecutorConfig;
  private readonly remoteSteps: ReadonlySet<string>;

  constructor(config: RoutingStepExecutorConfig) {
    if (config.local === undefined && config.storage === undefined) {
      throw new Error("RoutingStepExecutor: pass `local`, or `storage` for the default one");
    }
    this.config = config;
    this.remoteSteps = new Set(config.remoteSteps);
  }

  forWorkflow(workflow: Workflow<unknown, unknown>): StepExecutor {
    const { remote, local, storage, clock } = this.config;
    return new RoutingStepExecutor({
      ...this.config,
      remote: remote.forWorkflow?.(workflow) ?? remote,
      local:
        local?.forWorkflow?.(workflow) ??
        local ??
        new InProcessStepExecutor(workflow, {
          storage: storage!,
          clock: clock ?? SystemWallClock,
        }),
    });
  }

  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    if (this.remoteSteps.has(req.stepName)) return this.config.remote.executeStep(req);
    const local = this.config.local;
    if (local === undefined) {
      return Promise.resolve({
        ok: false,
        kind: "failed",
        error:
          `RoutingStepExecutor: no local executor for step "${req.stepName}" — ` +
          `pass \`local\`, or let the runner bind it to a workflow (forWorkflow)`,
      });
    }
    return local.executeStep(req);
  }
}
