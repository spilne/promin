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

export interface WorkerConfig {
  /** Workflow storage for checkpointing step results. */
  storage: WorkflowStorage;
  /** Step queue to poll for tasks. */
  stepQueue: StepQueue;
  /** Registry of step implementations this worker can execute. */
  registry: StepRegistry;
  /** Queue names this worker polls. Default: ["default"]. */
  queues?: string[];
  /** Max concurrent step executions. Default: 1. */
  concurrency?: number;
  /** How often to poll for tasks (ms). Default: 1000. */
  pollIntervalMs?: number;
  /** Worker ID for logging/debugging. Default: random UUID. */
  workerId?: string;
  /** Called when a step completes. */
  onStepComplete?: (params: { workflowId: string; stepName: string; durationMs: number }) => void;
  /** Called when a step fails. */
  onStepFailure?: (params: { workflowId: string; stepName: string; error: string }) => void;
}

export interface WorkflowWorker {
  /** Begin polling and executing steps. */
  start(): Promise<void>;
  /** Signal graceful shutdown — finish current work, then stop. */
  stop(): Promise<void>;
  /** Worker ID. */
  readonly workerId: string;
}

export class DefaultWorker implements WorkflowWorker {
  readonly workerId: string;
  private readonly storage: WorkflowStorage;
  private readonly stepQueue: StepQueue;
  private readonly registry: StepRegistry;
  private readonly queues: string[];
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly onStepComplete?: WorkerConfig["onStepComplete"];
  private readonly onStepFailure?: WorkerConfig["onStepFailure"];
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
    this.onStepComplete = config.onStepComplete;
    this.onStepFailure = config.onStepFailure;
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
    // Wait for active tasks to finish
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
      this.onStepFailure?.({ workflowId: task.workflowId, stepName: task.stepName, error });
      return;
    }

    try {
      const ctx: StepContext = {
        input: task.input,
        prev: this.computePrev(task),
        deps: task.prevResults,
        workflowId: task.workflowId,
        stepName: task.stepName,
        attempt: task.attempt,
      };

      const result = handler(ctx);
      let value: unknown;

      if (result instanceof Pipeline) {
        value = await result.runPromise();
      } else if (result && typeof (result as Promise<unknown>).then === "function") {
        value = await result;
      } else {
        value = result;
      }

      const durationMs = Date.now() - startTime;

      await this.stepQueue.complete({ taskId: task.id, result: value, durationMs });
      await this.storage.saveStepResult({
        workflowId: task.workflowId,
        stepName: task.stepName,
        result: value,
        durationMs,
        startedAt: new Date(startTime),
      });
      this.onStepComplete?.({ workflowId: task.workflowId, stepName: task.stepName, durationMs });
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
      this.onStepFailure?.({ workflowId: task.workflowId, stepName: task.stepName, error });
    }
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
