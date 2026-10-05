import { die, fail, succeed, TaggedError, type Eff, type Throws } from "@spilne/perfect-core";
import type { TaggedError as TaggedErrorShape } from "../shared/tagged-error.ts";
import { promiseOrEff, runEffSafe } from "../shared/eff.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { PollLoop, type PollLoopErrorInfo } from "../shared/poll-loop.ts";
import { StepTimeoutError } from "../durable/durable-pipeline-error.ts";
import type { StepDefinition } from "../durable/step-definition.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import { isTerminalWorkflowStatus, type WorkflowStatus } from "../durable/workflow-state.ts";
import type {
  StepExecutor,
  StepExecutionRequest,
  StepExecutionResult,
} from "../durable/workflow-runner.ts";
import { errorMessage, errorTagOf, type StepAttemptFailure } from "../durable/runner/step-body.ts";
import { applyStepPolicies } from "../durable/runner/step-policies.ts";
import type { StepQueue } from "./step-queue.ts";

/** Default `stepWaitTimeoutMs`: 24 hours. */
export const DEFAULT_STEP_WAIT_TIMEOUT_MS = 24 * 60 * 60 * 1000;

/**
 * A queued step settled neither way within `stepWaitTimeoutMs` of being
 * enqueued: no worker picked it up, or the one running it never settled
 * the task. The step fails with this error.
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
  /** How often to check a queued task for its outcome. Default: 500ms. */
  pollIntervalMs?: number;
  /** Re-enqueue tasks stuck in 'running' longer than this. Default: 30000ms. */
  staleTimeoutMs?: number;
  /**
   * How long to wait for an attempt's outcome after enqueuing it before
   * failing the step with `StepWaitTimeoutError`. `Infinity` waits
   * forever. Default: `DEFAULT_STEP_WAIT_TIMEOUT_MS` (24 hours).
   */
  stepWaitTimeoutMs?: number;
  /** Time source for polling, wait deadlines and retry backoff. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * Called when a storage / queue read fails while waiting for a step.
   * The wait retries with backoff. Default: `console.error`.
   */
  onError?: (error: unknown, info: PollLoopErrorInfo) => void;
}

/**
 * A queued attempt's typed failure, as the worker reported it. The error
 * object itself can't cross the queue, so this carries its `message` and
 * its `_tag`: `retry.when`, `onFailure.fallback`, the step row and the run's
 * failure see the same tag and message they would on the inline path.
 */
export class QueuedStepError extends Error {
  readonly _tag: string;
  readonly workflowId: string;
  readonly stepName: string;

  constructor(params: {
    readonly workflowId: string;
    readonly stepName: string;
    readonly message: string;
    /** The original error's `_tag`. */
    readonly errorTag: string;
  }) {
    super(params.message);
    this.name = params.errorTag;
    this._tag = params.errorTag;
    this.workflowId = params.workflowId;
    this.stepName = params.stepName;
  }
}

/** How one queued attempt settled. */
type AttemptOutcome =
  | {
      readonly kind: "completed";
      /** The task's attempt number (a predecessor's, for an adopted task). */
      readonly attempt: number;
      readonly result: unknown;
      readonly metadata?: Record<string, unknown>;
      readonly executorId?: string;
    }
  | {
      readonly kind: "failed";
      readonly attempt: number;
      /**
       * The attempt's failure: typed (`QueuedStepError`, `StepTimeoutError`)
       * when the worker's error had a `_tag`, else a defect.
       */
      readonly error: Error;
      readonly typed: boolean;
      readonly metadata?: Record<string, unknown>;
      readonly executorId?: string;
      readonly durationMs?: number;
    }
  | { readonly kind: "gave-up"; readonly error: StepWaitTimeoutError | StepWaitAbandonedError };

/** A worker's result, already in its stored (encoded) form. */
class QueuedResult {
  constructor(readonly encoded: unknown) {}
}

/**
 * Runs a step on the step queue, with the step definition's policies
 * applied exactly as the inline runner applies them:
 *
 * - **One task per attempt.** Each attempt is enqueued with the runner's
 *   attempt number, the step's declared dependencies (`deps` + `dependsOn`
 *   — not the run's whole results) and the definition's `timeoutMs`, which
 *   the worker enforces on the attempt.
 * - **`retry` and `onFailure` run here**, around the attempts, with the
 *   same combinators as inline, on this executor's clock. As inline, they
 *   act on typed failures: an attempt whose error had a `_tag` (an `Eff`
 *   failure, a tagged throw, the worker's `StepTimeoutError`) fails with a
 *   `QueuedStepError` carrying that tag and message (`StepTimeoutError`
 *   itself for a timeout); an untagged failure (a plain throw or rejection,
 *   a dead-lettered task) is a defect and fails the step as it is. Every
 *   typed failed attempt is reported in `failedAttempts`, with the worker
 *   that ran it as `executorId`.
 * - **Adoption reuses a settled outcome.** The queue keeps a task's
 *   `(workflowId, stepName)` slot until its outcome is consumed, so when
 *   this executor dispatches a step whose task a crashed coordinator left
 *   behind, `enqueue` hands that task back. Settled, it is taken as this
 *   attempt's outcome (with its attempt number) instead of running the
 *   step again; still pending or running, it is waited on. A settled task
 *   of an earlier run (`run`, before a `startFreshRun`) is consumed and the
 *   step dispatched afresh. Every outcome is consumed as it is read, before
 *   the runner records it, so the next attempt of a retry always gets a
 *   new task; a crash between the consume and the step-row write is the
 *   one window in which the step is dispatched again.
 * - **The worker never writes storage.** The outcome is read back from the
 *   queue task, whose `complete` / `fail` are fenced by the claim token in
 *   one atomic operation, and returned to the runner, which writes the step
 *   row and attempt rows fenced by its run lock. A worker that lost its
 *   claim cannot settle the task, so its result never reaches a row.
 *
 * The wait for one attempt is bounded. It gives up, failing the step
 * without retry or `onFailure`, when:
 * - `stepWaitTimeoutMs` passes after the enqueue without an outcome
 *   (`StepWaitTimeoutError`). A task still `running` then is failed in the
 *   queue, so its worker loses the claim; a task still `pending` stays
 *   queued (the queue has no cancel for it).
 * - the run is deleted, or reaches a terminal status, while the step waits
 *   (`StepWaitAbandonedError`).
 *
 * A request without `definition` (built by hand) ships every entry of
 * `prevResults` as a dependency and applies no step policies.
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

  /** Consume the steps' settled tasks, so a reset step is dispatched afresh. */
  async discardSettled(params: {
    readonly workflowId: string;
    readonly stepNames: readonly string[];
  }): Promise<void> {
    await this.stepQueue.consumeSettled(params);
  }

  async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    const definition = req.definition;
    const dependsOn = definition ? definition.dependsOn : Object.keys(req.prevResults);
    const deps: Record<string, unknown> = {};
    for (const name of dependsOn) deps[name] = req.prevResults[name];

    const clock = this.clock;
    const failedAttempts: StepAttemptFailure[] = [];
    let attempt = req.attempt - 1;
    let last: AttemptOutcome | undefined;

    // The worker enforces `timeoutMs` on each attempt; retry and `onFailure`
    // apply here, around the attempts, through the inline combinators.
    const policy: StepDefinition = {
      name: req.stepName,
      dependsOn: [...dependsOn],
      kind: "normal",
      execute: () => succeed(undefined),
      codec: definition?.codec ?? IDENTITY_CODEC,
      ...(definition?.retry !== undefined && { retry: definition.retry }),
      ...(definition?.onFailure !== undefined && { onFailure: definition.onFailure }),
    };

    const raw = applyStepPolicies({
      stepDef: policy,
      workflowId: req.workflowId,
      clock,
      invoke: () =>
        promiseOrEff(async (): Promise<Eff<unknown, Throws<TaggedErrorShape>>> => {
          attempt++;
          const startedAt = clock.now();
          const outcome = await this.runAttempt({ req, attempt, deps, dependsOn });
          last = outcome;
          // An adopted task carries its own attempt number; count on from it.
          if (outcome.kind !== "gave-up") attempt = outcome.attempt;
          if (outcome.kind === "completed") return succeed(new QueuedResult(outcome.result));
          if (outcome.kind === "gave-up" || !outcome.typed) return die(outcome.error);
          failedAttempts.push({
            attempt,
            error: outcome.error.message,
            startedAt,
            durationMs: outcome.durationMs ?? clock.currentTimeMs() - startedAt.getTime(),
            ...(outcome.executorId !== undefined && { executorId: outcome.executorId }),
          });
          return fail(outcome.error as Error & TaggedErrorShape);
        }) as Eff<unknown, Throws<TaggedErrorShape>>,
    });

    const { data, error } = await runEffSafe(
      raw.map((value) =>
        value instanceof QueuedResult ? value.encoded : policy.codec.encode(value),
      ),
      { catchDefects: true },
    );
    const settled = last as AttemptOutcome | undefined;
    const report = {
      attempt: Math.max(attempt, req.attempt),
      failedAttempts,
      ...(settled?.kind !== "gave-up" &&
        settled?.metadata !== undefined && { metadata: settled.metadata }),
      ...(settled?.kind !== "gave-up" &&
        settled?.executorId !== undefined && { executorId: settled.executorId }),
    };

    if (error === null) return { ok: true, result: data, ...report };
    const storedTag = errorTagOf(error);
    return {
      ok: false,
      kind: "failed",
      error: errorMessage(error),
      ...(storedTag !== undefined && { errorTag: storedTag }),
      cause: error,
      ...report,
    };
  }

  /**
   * Enqueue one attempt and wait for it to settle: the queue task's outcome,
   * or a reason to stop waiting (deadline, run deleted or over).
   */
  private async runAttempt(params: {
    readonly req: StepExecutionRequest;
    readonly attempt: number;
    readonly deps: Record<string, unknown>;
    readonly dependsOn: readonly string[];
  }): Promise<AttemptOutcome> {
    const { req, attempt } = params;
    const timeoutMs = req.definition?.timeoutMs;
    const run = req.runtime?.run ?? 1;
    const enqueue = () =>
      this.stepQueue.enqueue({
        workflowId: req.workflowId,
        stepName: req.stepName,
        input: req.input,
        deps: params.deps,
        dependsOn: params.dependsOn,
        ...(timeoutMs !== undefined && { timeoutMs }),
        needs: req.needs,
        priority: req.priority,
        version: req.version,
        attempt,
        run,
        ...(req.concurrencyKey !== undefined && { concurrencyKey: req.concurrencyKey }),
        ...(req.concurrencyScope !== undefined && { concurrencyScope: req.concurrencyScope }),
        ...(req.concurrencyLimit !== undefined && { concurrencyLimit: req.concurrencyLimit }),
      });
    // Idempotent: a task the step already has (pending, running, or settled
    // and not yet consumed) comes back instead of a new one.
    let taskId = await enqueue();

    const deadline = this.clock.currentTimeMs() + this.stepWaitTimeoutMs;
    const requeueEveryNPolls = Math.max(1, Math.ceil(this.staleTimeoutMs / this.pollIntervalMs));
    let polls = -1;
    let outcome: AttemptOutcome | undefined;
    const gaveUp = (error: StepWaitTimeoutError | StepWaitAbandonedError): "stop" => {
      outcome = { kind: "gave-up", error };
      return "stop";
    };

    // Every check comes `pollIntervalMs` after the previous one, the first
    // one a full interval after the enqueue. A failed check (storage or
    // queue blip) is reported and retried with backoff instead of failing
    // the step.
    const wait = new PollLoop({
      name: "step-queue-executor",
      intervalMs: this.pollIntervalMs,
      clock: this.clock,
      onError: this.onError,
      tick: async () => {
        polls++;
        if (polls === 0) return "idle";
        if (polls % requeueEveryNPolls === 0) {
          await this.stepQueue.requeueStuck({ mode: "stale", olderThanMs: this.staleTimeoutMs });
        }

        // The task's outcome is the claim-fenced one: only the claim that
        // held the task when it settled could write it.
        const task = await this.stepQueue.get(taskId);
        const settled = task?.status === "completed" || task?.status === "failed";
        if (settled && task.run !== run) {
          // An earlier run's leftover: not this run's outcome.
          await this.stepQueue.consume({ taskId });
          taskId = await enqueue();
          return "idle";
        }
        // Take the outcome: from here the next enqueue for the step makes a
        // new task. Consumed before the runner writes the step row.
        if (settled) await this.stepQueue.consume({ taskId });
        if (task?.status === "completed") {
          outcome = {
            kind: "completed",
            attempt: task.attempt,
            result: task.result,
            ...(task.stepMetadata !== undefined && { metadata: task.stepMetadata }),
            ...(task.claimedBy !== undefined && { executorId: task.claimedBy }),
          };
          return "stop";
        }
        if (task?.status === "failed") {
          // A worker's failure, or a task dead-lettered after
          // `maxDeliveries` (its error says so; untagged, so a defect).
          const message = task.error ?? "step task failed";
          outcome = {
            kind: "failed",
            attempt: task.attempt,
            error: attemptError({
              workflowId: req.workflowId,
              stepName: req.stepName,
              message,
              errorTag: task.errorTag,
              timeoutMs,
            }),
            typed: task.errorTag !== undefined,
            ...(task.stepMetadata !== undefined && { metadata: task.stepMetadata }),
            ...(task.claimedBy !== undefined && { executorId: task.claimedBy }),
            ...(task.durationMs !== undefined && { durationMs: task.durationMs }),
          };
          return "stop";
        }

        // Nobody will settle a step for a run that is gone or over.
        const state = await this.storage.loadWorkflow(req.workflowId);
        if (state === null || isTerminalWorkflowStatus(state.status)) {
          return gaveUp(
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
          // Take the claim away from a worker still running it, so its
          // late outcome is rejected, and consume the failure we just wrote.
          // A no-op for a task still pending.
          if (await this.stepQueue.fail({ taskId, error: error.message, durationMs: 0 })) {
            await this.stepQueue.consume({ taskId });
          }
          return gaveUp(error);
        }
        return "idle";
      },
    });
    await wait.start();
    return outcome!;
  }
}

/** The error an attempt failed with, rebuilt from what the worker reported. */
function attemptError(params: {
  readonly workflowId: string;
  readonly stepName: string;
  readonly message: string;
  readonly errorTag: string | undefined;
  readonly timeoutMs: number | undefined;
}): Error {
  const { workflowId, stepName, message, errorTag } = params;
  if (errorTag === undefined) return new Error(message);
  if (errorTag === "StepTimeoutError" && params.timeoutMs !== undefined) {
    return new StepTimeoutError({ workflowId, stepName, timeoutMs: params.timeoutMs, message });
  }
  return new QueuedStepError({ workflowId, stepName, message, errorTag });
}

/** Codec for a request without a definition: results pass through as stored. */
const IDENTITY_CODEC: StepDefinition["codec"] = {
  encode: (value: unknown) => value,
  decode: (value: unknown) => value,
} as StepDefinition["codec"];
