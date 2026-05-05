import { SystemClock, type Clock } from "@promin/core";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type {
  StepExecutor,
  StepExecutionRequest,
  StepExecutionResult,
} from "../durable/workflow-runner.ts";
import type { StepQueue } from "./step-queue.ts";

/**
 * Executes a step by enqueuing it on the step queue and polling storage
 * until a worker checkpoints the result. The worker writes both the queue
 * task and workflow storage, so the runner can skip its own `saveStepResult`
 * call — indicated by `storageAlreadyCheckpointed: true` on the returned
 * success value.
 */
export class StepQueueExecutor implements StepExecutor {
  private readonly stepQueue: StepQueue;
  private readonly storage: WorkflowStorage;
  private readonly pollIntervalMs: number;
  private readonly staleTimeoutMs: number;
  private readonly clock: Clock;

  constructor(config: {
    stepQueue: StepQueue;
    storage: WorkflowStorage;
    /** How often to poll storage for step completion. Default: 500ms. */
    pollIntervalMs?: number;
    /** Re-enqueue tasks stuck in 'running' longer than this. Default: 30000ms. */
    staleTimeoutMs?: number;
    clock?: Clock;
  }) {
    this.stepQueue = config.stepQueue;
    this.storage = config.storage;
    this.pollIntervalMs = config.pollIntervalMs ?? 500;
    this.staleTimeoutMs = config.staleTimeoutMs ?? 30_000;
    this.clock = config.clock ?? SystemClock;
  }

  async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    await this.stepQueue.enqueue({
      workflowId: req.workflowId,
      stepName: req.stepName,
      input: req.input,
      prevResults: req.prevResults,
      needs: req.needs,
      priority: req.priority,
      version: req.version,
      ...(req.concurrencyKey !== undefined && { concurrencyKey: req.concurrencyKey }),
      ...(req.concurrencyScope !== undefined && { concurrencyScope: req.concurrencyScope }),
      ...(req.concurrencyLimit !== undefined && { concurrencyLimit: req.concurrencyLimit }),
    });

    const requeueEveryNPolls = Math.ceil(this.staleTimeoutMs / this.pollIntervalMs);
    let polls = 0;

    while (true) {
      await new Promise<void>((r) => this.clock.setTimeout(() => r(), this.pollIntervalMs));
      polls++;

      if (polls % requeueEveryNPolls === 0) {
        await this.stepQueue.requeueStuck({ staleTimeoutMs: this.staleTimeoutMs });
      }

      const state = await this.storage.loadWorkflow(req.workflowId);
      const stepState = state?.steps[req.stepName];

      if (stepState?.status === "completed") {
        return { ok: true, result: stepState.result, storageAlreadyCheckpointed: true };
      }
      if (stepState?.status === "failed") {
        return { ok: false, error: stepState.error ?? "step failed" };
      }
    }
  }
}
