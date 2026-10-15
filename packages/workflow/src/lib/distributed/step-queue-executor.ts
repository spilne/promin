import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { PollLoop, type PollLoopErrorInfo } from "../shared/poll-loop.ts";
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
  private readonly clock: WallClock;
  private readonly onError?: (error: unknown, info: PollLoopErrorInfo) => void;

  constructor(config: {
    stepQueue: StepQueue;
    storage: WorkflowStorage;
    /** How often to poll storage for step completion. Default: 500ms. */
    pollIntervalMs?: number;
    /** Re-enqueue tasks stuck in 'running' longer than this. Default: 30000ms. */
    staleTimeoutMs?: number;
    clock?: WallClock;
    /**
     * Called when a storage / queue read fails while waiting for a step.
     * The wait retries with backoff. Default: `console.error`.
     */
    onError?: (error: unknown, info: PollLoopErrorInfo) => void;
  }) {
    this.stepQueue = config.stepQueue;
    this.storage = config.storage;
    this.pollIntervalMs = config.pollIntervalMs ?? 500;
    this.staleTimeoutMs = config.staleTimeoutMs ?? 30_000;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError;
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

    const requeueEveryNPolls = Math.max(1, Math.ceil(this.staleTimeoutMs / this.pollIntervalMs));
    let polls = -1;
    let outcome: StepExecutionResult | undefined;

    // Every storage check comes `pollIntervalMs` after the previous one,
    // the first one a full interval after the enqueue (a retried step's row
    // still carries the previous attempt's outcome until a worker picks the
    // new task up). A failed check (storage blip) is reported and retried
    // with backoff instead of failing the step.
    const wait = new PollLoop({
      name: "step-queue-executor",
      intervalMs: this.pollIntervalMs,
      clock: this.clock,
      onError: this.onError,
      tick: async () => {
        polls++;
        if (polls === 0) return "idle";
        if (polls % requeueEveryNPolls === 0) {
          await this.stepQueue.requeueStuck({ staleTimeoutMs: this.staleTimeoutMs });
        }

        const state = await this.storage.loadWorkflow(req.workflowId);
        const stepState = state?.steps[req.stepName];

        if (stepState?.status === "completed") {
          outcome = { ok: true, result: stepState.result, storageAlreadyCheckpointed: true };
          return "stop";
        }
        if (stepState?.status === "failed") {
          outcome = { ok: false, error: stepState.error ?? "step failed" };
          return "stop";
        }
        return "idle";
      },
    });
    await wait.start();
    return outcome!;
  }
}
