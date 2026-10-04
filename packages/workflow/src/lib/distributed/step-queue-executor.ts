import { TaggedError } from "@spilne/perfect-core";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { PollLoop, type PollLoopErrorInfo } from "../shared/poll-loop.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import { isTerminalWorkflowStatus, type WorkflowStatus } from "../durable/workflow-state.ts";
import type {
  StepExecutor,
  StepExecutionRequest,
  StepExecutionResult,
} from "../durable/workflow-runner.ts";
import type { StepQueue } from "./step-queue.ts";

/** Default `stepWaitTimeoutMs`: 24 hours. */
export const DEFAULT_STEP_WAIT_TIMEOUT_MS = 24 * 60 * 60 * 1000;

/**
 * A queued step settled neither way within `stepWaitTimeoutMs` of being
 * enqueued: no worker picked it up, or the one running it never wrote its
 * outcome. The step fails with this error.
 */
export class StepWaitTimeoutError extends TaggedError("StepWaitTimeoutError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly taskId: string;
  readonly timeoutMs: number;
  readonly message: string;
}>() {}

/**
 * The run a queued step belongs to was deleted (`reason: "deleted"`) or
 * reached a terminal status (`reason: "terminal"`) while the step was
 * waiting, so the wait gave up. The step fails with this error.
 */
export class StepWaitAbandonedError extends TaggedError("StepWaitAbandonedError")<{
  readonly workflowId: string;
  readonly stepName: string;
  readonly taskId: string;
  readonly reason: "deleted" | "terminal";
  /** The run's status, for `reason: "terminal"`. */
  readonly status?: WorkflowStatus;
  readonly message: string;
}>() {}

/** Configuration for `StepQueueExecutor`. */
export interface StepQueueExecutorConfig {
  stepQueue: StepQueue;
  storage: WorkflowStorage;
  /** How often to poll storage for step completion. Default: 500ms. */
  pollIntervalMs?: number;
  /** Re-enqueue tasks stuck in 'running' longer than this. Default: 30000ms. */
  staleTimeoutMs?: number;
  /**
   * How long to wait for a step's outcome after enqueuing it before
   * failing the step with `StepWaitTimeoutError`. `Infinity` waits
   * forever. Default: `DEFAULT_STEP_WAIT_TIMEOUT_MS` (24 hours).
   */
  stepWaitTimeoutMs?: number;
  clock?: WallClock;
  /**
   * Called when a storage / queue read fails while waiting for a step.
   * The wait retries with backoff. Default: `console.error`.
   */
  onError?: (error: unknown, info: PollLoopErrorInfo) => void;
}

/**
 * Executes a step by enqueuing it on the step queue and polling storage
 * until a worker checkpoints the result. The worker writes both the queue
 * task and workflow storage, so the runner can skip its own `saveStepResult`
 * call — indicated by `storageAlreadyCheckpointed: true` on the returned
 * value, for a completed step and for a failed one. A task dead-lettered
 * without a storage write is reported as a failure the runner records.
 *
 * The wait is bounded. It gives up, failing the step, when:
 * - `stepWaitTimeoutMs` passes after the enqueue without an outcome
 *   (`StepWaitTimeoutError`). A task still `running` then is failed in the
 *   queue, so its worker loses the claim and skips its commit; a task still
 *   `pending` stays queued (the queue has no cancel for it).
 * - the run is deleted, or reaches a terminal status, while the step waits
 *   (`StepWaitAbandonedError`).
 *
 * The request's `signal` is not observed: a claimed task cannot be
 * recalled, so the executor always waits for its outcome rather than let a
 * later attempt run beside it.
 */
export class StepQueueExecutor implements StepExecutor {
  private readonly stepQueue: StepQueue;
  private readonly storage: WorkflowStorage;
  private readonly pollIntervalMs: number;
  private readonly staleTimeoutMs: number;
  private readonly stepWaitTimeoutMs: number;
  private readonly clock: WallClock;
  private readonly onError?: (error: unknown, info: PollLoopErrorInfo) => void;

  constructor(config: StepQueueExecutorConfig) {
    this.stepQueue = config.stepQueue;
    this.storage = config.storage;
    this.pollIntervalMs = config.pollIntervalMs ?? 500;
    this.staleTimeoutMs = config.staleTimeoutMs ?? 30_000;
    this.stepWaitTimeoutMs = config.stepWaitTimeoutMs ?? DEFAULT_STEP_WAIT_TIMEOUT_MS;
    if (!(this.stepWaitTimeoutMs > 0)) {
      throw new Error(
        `StepQueueExecutor: stepWaitTimeoutMs must be positive, got ${this.stepWaitTimeoutMs}`,
      );
    }
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError;
  }

  async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    const taskId = await this.stepQueue.enqueue({
      workflowId: req.workflowId,
      stepName: req.stepName,
      input: req.input,
      prevResults: req.prevResults,
      needs: req.needs,
      priority: req.priority,
      version: req.version,
      attempt: req.attempt,
      ...(req.concurrencyKey !== undefined && { concurrencyKey: req.concurrencyKey }),
      ...(req.concurrencyScope !== undefined && { concurrencyScope: req.concurrencyScope }),
      ...(req.concurrencyLimit !== undefined && { concurrencyLimit: req.concurrencyLimit }),
    });

    const deadline = this.clock.currentTimeMs() + this.stepWaitTimeoutMs;
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
        const sweep = polls % requeueEveryNPolls === 0;
        if (sweep) {
          await this.stepQueue.requeueStuck({ mode: "stale", olderThanMs: this.staleTimeoutMs });
        }

        const state = await this.storage.loadWorkflow(req.workflowId);
        const stepState = state?.steps[req.stepName];

        if (stepState?.status === "completed") {
          outcome = { ok: true, result: stepState.result, storageAlreadyCheckpointed: true };
          return "stop";
        }
        if (stepState?.status === "failed") {
          // The worker wrote the failure row and its attempt row.
          outcome = {
            ok: false,
            kind: "failed",
            error: stepState.error ?? "step failed",
            storageAlreadyCheckpointed: true,
          };
          return "stop";
        }
        // Nobody will record an outcome for a run that is gone or over.
        if (state === null || isTerminalWorkflowStatus(state.status)) {
          outcome = failedWith(
            new StepWaitAbandonedError({
              workflowId: req.workflowId,
              stepName: req.stepName,
              taskId,
              reason: state === null ? "deleted" : "terminal",
              ...(state !== null && { status: state.status }),
              message:
                state === null
                  ? `step "${req.stepName}" stopped waiting: workflow "${req.workflowId}" was deleted`
                  : `step "${req.stepName}" stopped waiting: workflow "${req.workflowId}" is ${state.status}`,
            }),
          );
          return "stop";
        }
        // A task that failed in the queue without a storage write was
        // dead-lettered (it used up its deliveries); workers always write
        // storage before settling the task, so nothing else will.
        if (sweep) {
          const task = await this.stepQueue.get(taskId);
          if (task?.status === "failed") {
            outcome = { ok: false, kind: "failed", error: task.error ?? "step task failed" };
            return "stop";
          }
        }
        if (this.clock.currentTimeMs() >= deadline) {
          const error = new StepWaitTimeoutError({
            workflowId: req.workflowId,
            stepName: req.stepName,
            taskId,
            timeoutMs: this.stepWaitTimeoutMs,
            message:
              `step "${req.stepName}" of workflow "${req.workflowId}" got no outcome ` +
              `within ${this.stepWaitTimeoutMs}ms of being queued (task ${taskId})`,
          });
          // Take the claim away from a worker still running it, so it skips
          // its commit. A no-op for a task that is still pending.
          await this.stepQueue.fail({ taskId, error: error.message, durationMs: 0 });
          outcome = failedWith(error);
          return "stop";
        }
        return "idle";
      },
    });
    await wait.start();
    return outcome!;
  }
}

function failedWith(error: StepWaitTimeoutError | StepWaitAbandonedError): StepExecutionResult {
  return { ok: false, kind: "failed", error: error.message, errorTag: error._tag, cause: error };
}
