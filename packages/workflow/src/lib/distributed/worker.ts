// ---------------------------------------------------------------------------
// WorkflowWorker — polls step queue, executes steps, checkpoints results
//
// Supports both:
// - Per-step options (retry, onFailure, compensate) via StepRegistry
// - Global middleware + hooks on the worker itself
// ---------------------------------------------------------------------------

import { Pipeline, type TaggedError } from "@promin/core";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import { isStepAttemptStorage } from "../durable/workflow-storage.ts";
import type { StepRegistry, StepContext, StepRegistration } from "./step-registry.ts";
import type { StepQueue, StepTask } from "./step-queue.ts";
import type { WorkerMiddleware } from "./middleware.ts";
import type { WorkerRegistry } from "./worker-registry.ts";

// ---------------------------------------------------------------------------
// Hooks
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
  hooks?: WorkerHooks;
  middleware?: WorkerMiddleware[];
  /** Worker registry for health monitoring. Optional — without it, no heartbeat/registration. */
  workerRegistry?: WorkerRegistry;
  /** Heartbeat interval in ms. Default: 5000. */
  heartbeatIntervalMs?: number;
  /** Worker metadata (hostname, labels, etc). */
  metadata?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Interface
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
  private readonly workerRegistry?: WorkerRegistry;
  private readonly heartbeatIntervalMs: number;
  private readonly workerMetadata?: Record<string, unknown>;
  private running = false;
  private activeCount = 0;
  private heartbeatTimer?: ReturnType<typeof setInterval>;

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
    this.workerRegistry = config.workerRegistry;
    this.heartbeatIntervalMs = config.heartbeatIntervalMs ?? 5000;
    this.workerMetadata = config.metadata;
  }

  async start(): Promise<void> {
    this.running = true;

    // Register with worker registry
    if (this.workerRegistry) {
      await this.workerRegistry.register({
        workerId: this.workerId,
        queues: this.queues,
        concurrency: this.concurrency,
        metadata: this.workerMetadata,
      });
      this.heartbeatTimer = setInterval(async () => {
        await this.workerRegistry!.heartbeat(this.workerId);
      }, this.heartbeatIntervalMs);
    }

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

    // Mark as draining, then wait for active tasks
    if (this.workerRegistry) {
      await this.workerRegistry.drain(this.workerId);
    }

    while (this.activeCount > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }

    // Deregister and stop heartbeat
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
    if (this.workerRegistry) {
      await this.workerRegistry.deregister(this.workerId);
    }
  }

  private async executeTask(task: StepTask): Promise<void> {
    const startTime = Date.now();
    const registration = this.registry.resolve(task.stepName);

    if (!registration) {
      const error = `Step "${task.stepName}" not found in registry. Available: ${this.registry.list().join(", ")}`;
      await this.failTask(task, error, startTime);
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
      await this.hooks.beforeStep?.(task);

      const chain = this.buildChain(task, registration);
      const value = await chain(ctx);

      const durationMs = Date.now() - startTime;

      await this.stepQueue.complete({ taskId: task.id, result: value, durationMs });
      await this.storage.saveStepResult({
        workflowId: task.workflowId,
        stepName: task.stepName,
        result: value,
        durationMs,
        startedAt: new Date(startTime),
      });

      if (isStepAttemptStorage(this.storage)) {
        await this.storage.saveStepAttempt({
          workflowId: task.workflowId,
          stepName: task.stepName,
          attempt: task.attempt,
          type: "execution",
          status: "completed",
          result: value,
          durationMs,
          startedAt: new Date(startTime),
          completedAt: new Date(),
        });
      }

      await this.hooks.afterStep?.(task, value, durationMs);
    } catch (err) {
      const durationMs = Date.now() - startTime;

      // Apply onFailure strategy
      const strategy = registration.options?.onFailure ?? "fail";
      if (strategy === "skip") {
        await this.stepQueue.complete({ taskId: task.id, result: undefined, durationMs });
        await this.storage.saveStepResult({
          workflowId: task.workflowId,
          stepName: task.stepName,
          result: undefined,
          durationMs,
          startedAt: new Date(startTime),
        });
        await this.hooks.afterStep?.(task, undefined, durationMs);
        return;
      }

      if (typeof strategy === "object" && "fallback" in strategy) {
        const fallbackValue = strategy.fallback(err);
        await this.stepQueue.complete({ taskId: task.id, result: fallbackValue, durationMs });
        await this.storage.saveStepResult({
          workflowId: task.workflowId,
          stepName: task.stepName,
          result: fallbackValue,
          durationMs,
          startedAt: new Date(startTime),
        });
        await this.hooks.afterStep?.(task, fallbackValue, durationMs);
        return;
      }

      // Default: fail
      await this.failTask(task, err instanceof Error ? err.message : String(err), startTime);
    }
  }

  private buildChain(
    task: StepTask,
    registration: StepRegistration,
  ): (ctx: StepContext) => Promise<unknown> {
    const { handler, options } = registration;

    // Base: resolve handler result + apply step-level retry
    let base = async (ctx: StepContext): Promise<unknown> => {
      const result = handler(ctx);
      if (result instanceof Pipeline) return result.runPromise();
      if (result && typeof (result as Promise<unknown>).then === "function") return result;
      return result;
    };

    // Wrap with step-level retry (from StepOptions)
    if (options?.retry) {
      const retryPolicy = options.retry;
      const innerBase = base;
      base = async (ctx: StepContext): Promise<unknown> => {
        const maxRetries = retryPolicy.maxRetries ?? 3;
        const baseDelayMs = retryPolicy.baseDelayMs ?? 250;
        const when = retryPolicy.when;
        let lastError: unknown;

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            if (attempt > 0) {
              await new Promise((r) => setTimeout(r, baseDelayMs * Math.pow(2, attempt - 1)));
            }
            return await innerBase({ ...ctx, attempt: ctx.attempt + attempt });
          } catch (err) {
            lastError = err;
            if (when && !when(err as TaggedError)) throw err;
          }
        }
        throw lastError;
      };
    }

    // Wrap with global middleware (right to left)
    return this.middleware.reduceRight<(ctx: StepContext) => Promise<unknown>>(
      (next, mw) => (ctx) => mw({ task, ctx, next }),
      base,
    );
  }

  private async failTask(task: StepTask, error: string, startTime: number): Promise<void> {
    const durationMs = Date.now() - startTime;

    await this.stepQueue.fail({ taskId: task.id, error, durationMs });
    await this.storage.saveStepFailure({
      workflowId: task.workflowId,
      stepName: task.stepName,
      error,
      durationMs,
      startedAt: new Date(startTime),
    });

    if (isStepAttemptStorage(this.storage)) {
      await this.storage.saveStepAttempt({
        workflowId: task.workflowId,
        stepName: task.stepName,
        attempt: task.attempt,
        type: "execution",
        status: "failed",
        error,
        durationMs,
        startedAt: new Date(startTime),
        completedAt: new Date(),
      });
    }

    await this.hooks.onError?.(task, new Error(error), durationMs);
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
