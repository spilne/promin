// ---------------------------------------------------------------------------
// WorkflowWorker — polls step queue, executes steps, checkpoints results
//
// Workers register step implementations via StepRegistry, then poll
// their assigned queues for tasks. Each claimed task is executed and
// the result is checkpointed to WorkflowStorage.
// ---------------------------------------------------------------------------

import { Pipeline } from "../pipeline.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { StepRegistry, StepContext } from "./step-registry.ts";
import type { StepQueue, StepTask } from "./step-queue.ts";
import type { WorkerMiddleware } from "./middleware.ts";

// ---------------------------------------------------------------------------
// Hooks — simple lifecycle callbacks
// ---------------------------------------------------------------------------

export interface WorkerHooks {
  beforeStep?: (task: StepTask) => void | Promise<void>;
  afterStep?: (task: StepTask, result: unknown, durationMs: number) => void | Promise<void>;
  onError?: (task: StepTask, error: unknown, durationMs: number) => void | Promise<void>;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface WorkerConfig {
  storage: WorkflowStorage;
  stepQueue: StepQueue;
  registry: StepRegistry;
  queues?: string[];
  concurrency?: number;
  pollIntervalMs?: number;
  workerId?: string;
  /** Simple lifecycle hooks — run at fixed points. */
  hooks?: WorkerHooks;
  /** Composable middleware — wraps step execution. Runs inside hooks. */
  middleware?: WorkerMiddleware[];
}

// ---------------------------------------------------------------------------
// WorkflowWorker interface
// ---------------------------------------------------------------------------

export interface WorkflowWorker {
  start(): Promise<void>;
  stop(): Promise<void>;
  readonly workerId: string;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export class DefaultWorker implements WorkflowWorker {
  readonly workerId: string;
  private readonly storage: WorkflowStorage;
  private readonly stepQueue: StepQueue;
  private readonly registry: StepRegistry;
  private readonly queues: string[];
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly hooks: WorkerHooks;
  private readonly middleware: WorkerMiddleware[];
  private running = false;
  private activeCount = 0;

  constructor(config: WorkerConfig) {
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.registry = config.registry;
    this.queues = config.queues ?? ["default"];
    this.concurrency = config.concurrency ?? 1;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.hooks = config.hooks ?? {};
    this.middleware = config.middleware ?? [];
  }

  async start(): Promise<void> {
    this.running = true;
    while (this.running) {
      if (this.activeCount < this.concurrency) {
        const claimCount = this.concurrency - this.activeCount;
        const tasks = await this.stepQueue.claim({ queues: this.queues, limit: claimCount });

        for (const task of tasks) {
          this.activeCount++;
          this.executeTask(task).finally(() => {
            this.activeCount--;
          });
        }
      }
      await new Promise((r) => setTimeout(r, this.pollIntervalMs));
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    while (this.activeCount > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  private async executeTask(task: StepTask): Promise<void> {
    const startTime = Date.now();
    const handler = this.registry.resolve(task.stepName);

    if (!handler) {
      const error = `Step "${task.stepName}" not found in registry. Available: ${this.registry.list().join(", ")}`;
      await this.stepQueue.fail({ taskId: task.id, error, durationMs: 0 });
      await this.storage.saveStepFailure({
        workflowId: task.workflowId,
        stepName: task.stepName,
        error,
        durationMs: 0,
        startedAt: new Date(startTime),
      });
      await this.hooks.onError?.(task, new Error(error), 0);
      return;
    }

    const ctx: StepContext = {
      input: task.input,
      prev: this.computePrev(task),
      deps: task.prevResults,
      workflowId: task.workflowId,
      stepName: task.stepName,
      attempt: task.attempt,
    };

    try {
      // hooks.beforeStep
      await this.hooks.beforeStep?.(task);

      // Build execution chain: middleware → handler
      const execute = this.buildChain(task, handler);
      const value = await execute(ctx);

      const durationMs = Date.now() - startTime;

      // Checkpoint
      await this.stepQueue.complete({ taskId: task.id, result: value, durationMs });
      await this.storage.saveStepResult({
        workflowId: task.workflowId,
        stepName: task.stepName,
        result: value,
        durationMs,
        startedAt: new Date(startTime),
      });

      // hooks.afterStep
      await this.hooks.afterStep?.(task, value, durationMs);
    } catch (err) {
      const durationMs = Date.now() - startTime;
      const error = err instanceof Error ? err.message : String(err);

      await this.stepQueue.fail({ taskId: task.id, error, durationMs });
      await this.storage.saveStepFailure({
        workflowId: task.workflowId,
        stepName: task.stepName,
        error,
        durationMs,
        startedAt: new Date(startTime),
      });

      // hooks.onError
      await this.hooks.onError?.(task, err, durationMs);
    }
  }

  private buildChain(
    task: StepTask,
    handler: (ctx: StepContext) => Pipeline<unknown, any> | Promise<unknown>,
  ): (ctx: StepContext) => Promise<unknown> {
    // Base: resolve handler result (Pipeline or Promise)
    const base = async (ctx: StepContext): Promise<unknown> => {
      const result = handler(ctx);
      if (result instanceof Pipeline) return result.runPromise();
      if (result && typeof (result as Promise<unknown>).then === "function") return result;
      return result;
    };

    // Wrap with middleware (right to left)
    return this.middleware.reduceRight<(ctx: StepContext) => Promise<unknown>>(
      (next, mw) => (ctx) => mw({ task, ctx, next }),
      base,
    );
  }

  private computePrev(task: StepTask): unknown {
    const results = task.prevResults;
    const keys = Object.keys(results);
    if (keys.length === 1) return results[keys[0]!];
    if (keys.length === 0) return task.input;
    return results;
  }
}

export function createWorker(config: WorkerConfig): WorkflowWorker {
  return new DefaultWorker(config);
}
