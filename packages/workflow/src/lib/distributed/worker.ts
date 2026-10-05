// ---------------------------------------------------------------------------
// WorkflowWorker — claims step tasks, runs one attempt of each (middleware +
// handler, under the step's `timeoutMs`) and settles the task with a
// claim-fenced `complete` / `fail`. Workers never write workflow storage:
// the coordinator writes step rows from the settled task, under its run lock.
// ---------------------------------------------------------------------------

import { runHookValue } from "../shared/eff.ts";
import { SystemWallClock, type WallClock, type TimerHandle } from "../shared/wall-clock.ts";
import { PollLoop, type PollTickResult } from "../shared/poll-loop.ts";
import { StepTimeoutError } from "../durable/durable-pipeline-error.ts";
import type { StepRegistry, WorkerStepContext, StepRegistration } from "./step-registry.ts";
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
  /**
   * The worker lost its claim on a running task: a heartbeat found it
   * reclaimed (this worker stalled past the stale timeout) or gone. The
   * handler's `ctx.signal` is aborted and its outcome is dropped.
   */
  onLeaseLost?: (task: StepTask) => void | Promise<void>;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface WorkerConfig {
  /**
   * The queue to claim from. The worker settles tasks there and writes
   * nothing else: it needs no workflow storage.
   */
  stepQueue: StepQueue;
  registry: StepRegistry;
  /**
   * Capabilities this worker offers. Tasks whose `needs` are a subset of
   * this list are claimable. Empty / omitted = generalist — can only claim
   * tasks with no `needs` declared.
   *
   * @example
   * ```ts
   * createWorker({
   *   // ...
   *   capabilities: ["gpu", "h265-hw-encode"],
   * });
   * ```
   */
  capabilities?: readonly string[];
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
  /**
   * Workflow versions this worker is willing to process. When set, only
   * tasks whose `version` is in this list are claimed (the filter runs
   * inside the queue's claim, so other versions are never taken).
   * Unversioned tasks (`version === undefined`, from workflows without a
   * `version`) are always accepted.
   *
   * Typical use during a rolling deploy: a worker running both v1 and v2
   * handlers sets `supportedVersions: ["1", "2"]`; after v1 drains, the
   * next deploy drops this to `["2"]`.
   */
  supportedVersions?: readonly string[];
  /**
   * Extra post-claim predicate, on top of the registry's step names and
   * `supportedVersions` (which the queue applies inside the claim). A task
   * it rejects is given back with `release()` for another worker. Because
   * the rejection happens after the claim, a rejected task at the head of
   * the queue is claimed again on the next poll — prefer `capabilities`,
   * the registry and `supportedVersions` for routing.
   */
  taskFilter?: (task: StepTask) => boolean;
  /**
   * Time source. Drives the poll-loop cadence, heartbeat interval, step
   * duration tracking and the per-attempt `timeoutMs`.
   * Default: `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Called for failures that don't belong to a step body: a failed claim,
   * a failed `complete` / `fail` (the task is then redelivered), a throwing
   * hook. Nothing reported here stops the worker. Default: `console.error`.
   */
  onError?: (event: WorkerErrorEvent) => void;
  /**
   * Upper bound for the claim loop's wait after consecutive claim
   * failures. Default: 30 000 (or `pollIntervalMs`, if larger).
   */
  maxErrorBackoffMs?: number;
}

/** Where a worker-side failure happened. */
export type WorkerErrorPhase =
  /** Claiming from the queue failed; the loop backs off and retries. */
  | "claim"
  /**
   * Settling the task (`complete` / `fail`) failed. The task stays claimed
   * and is redelivered once its lease goes stale.
   */
  | "commit"
  /** A `WorkerHooks` callback threw; the step outcome is unaffected. */
  | "hook"
  /**
   * A task heartbeat failed (storage / network). The task keeps running;
   * if heartbeats keep failing its lease goes stale and it is redelivered.
   */
  | "heartbeat"
  /**
   * Giving a `taskFilter`-rejected task back failed; it is redelivered
   * once its lease goes stale.
   */
  | "release"
  /** Anything else that escaped a task — reported, never rethrown. */
  | "task";

/** A failure reported through `WorkerConfig.onError`. */
export interface WorkerErrorEvent {
  readonly phase: WorkerErrorPhase;
  readonly error: unknown;
  /** The task involved, for every phase except `"claim"`. */
  readonly task?: StepTask;
}

// ---------------------------------------------------------------------------
// Interface
// ---------------------------------------------------------------------------

export interface WorkflowWorker {
  /**
   * Register (when a registry is configured) and run the claim loop. A
   * failed claim is reported through `onError` and retried with backoff;
   * it never ends the loop. Resolves once the worker has stopped.
   */
  start(): Promise<void>;
  /**
   * Stop claiming, wait for the in-flight claim and the running tasks to
   * finish, then deregister. With `timeoutMs`, tasks still running after
   * that long are given back to the queue (`release`, so another worker
   * claims them at once instead of after the stale timeout) and their
   * handlers' `ctx.signal` is aborted; their outcomes are dropped.
   * Without it, stop waits for every task.
   */
  stop(params?: { readonly timeoutMs?: number }): Promise<void>;
  readonly workerId: string;
}

/** What one attempt produced, before the task is settled. */
type StepOutcome =
  | { readonly kind: "completed"; readonly value: unknown; readonly durationMs: number }
  | {
      readonly kind: "failed";
      readonly error: string;
      readonly cause: unknown;
      readonly durationMs: number;
    };

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export class DefaultWorker implements WorkflowWorker {
  readonly workerId: string;
  private readonly stepQueue: StepQueue;
  private readonly registry: StepRegistry;
  private readonly capabilities: readonly string[];
  private readonly concurrency: number;
  private readonly hooks: WorkerHooks;
  private readonly middleware: WorkerMiddleware[];
  private readonly workerRegistry?: WorkerRegistry;
  private readonly heartbeatIntervalMs: number;
  private readonly workerMetadata?: Record<string, unknown>;
  private readonly supportedVersions?: readonly string[];
  private readonly taskFilter?: (task: StepTask) => boolean;
  private readonly clock: WallClock;
  private readonly onError: (event: WorkerErrorEvent) => void;
  private readonly pollLoop: PollLoop;
  /** Running tasks, by task id, with the controller behind their `ctx.signal`. */
  private readonly active = new Map<
    string,
    { readonly task: StepTask; readonly controller: AbortController }
  >();
  /** The last claim filled every free slot, so more work is likely queued. */
  private backlogLikely = false;
  private idleWaiters: (() => void)[] = [];
  private heartbeatTimer?: TimerHandle;

  constructor(config: WorkerConfig) {
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.stepQueue = config.stepQueue;
    this.registry = config.registry;
    this.capabilities = config.capabilities ?? [];
    this.concurrency = config.concurrency ?? 1;
    this.hooks = config.hooks ?? {};
    this.middleware = config.middleware ?? [];
    this.workerRegistry = config.workerRegistry;
    this.heartbeatIntervalMs = config.heartbeatIntervalMs ?? 5000;
    this.workerMetadata = config.metadata;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError ?? defaultOnError;

    this.supportedVersions = config.supportedVersions;
    this.taskFilter = config.taskFilter;

    this.pollLoop = new PollLoop({
      name: "worker",
      intervalMs: config.pollIntervalMs ?? 1000,
      clock: this.clock,
      maxBackoffMs: config.maxErrorBackoffMs,
      tick: () => this.claimOnce(),
      onError: (error) => this.report({ phase: "claim", error }),
    });
  }

  async start(): Promise<void> {
    if (this.pollLoop.running) return this.pollLoop.start();

    // Register with worker registry
    if (this.workerRegistry) {
      await this.workerRegistry.register({
        workerId: this.workerId,
        capabilities: this.capabilities,
        concurrency: this.concurrency,
        metadata: this.workerMetadata,
      });
      this.heartbeatTimer = this.clock.setInterval(() => {
        this.workerRegistry!.heartbeat(this.workerId).catch(() => {});
      }, this.heartbeatIntervalMs);
    }

    await this.pollLoop.start();
  }

  async stop(params?: { readonly timeoutMs?: number }): Promise<void> {
    // Stop claiming first. Awaiting the loop means a claim already in
    // flight has handed its tasks to `launch` before we wait for them.
    await this.pollLoop.stop();

    // Mark as draining, then wait for active tasks
    if (this.workerRegistry) {
      await this.workerRegistry.drain(this.workerId);
    }

    if (this.active.size > 0) {
      const idle = new Promise<void>((resolve) => this.idleWaiters.push(resolve));
      const timeoutMs = params?.timeoutMs;
      if (timeoutMs === undefined) {
        await idle;
      } else {
        let timer: TimerHandle | undefined;
        const timedOut = new Promise<"timeout">((resolve) => {
          timer = this.clock.setTimeout(() => resolve("timeout"), timeoutMs);
        });
        const result = await Promise.race([idle, timedOut]);
        timer?.clear();
        if (result === "timeout") await this.releaseUnfinished();
      }
    }

    // Deregister and stop heartbeat
    if (this.heartbeatTimer) {
      this.heartbeatTimer.clear();
      this.heartbeatTimer = undefined;
    }
    if (this.workerRegistry) {
      await this.workerRegistry.deregister(this.workerId);
    }
  }

  /**
   * One claim-loop iteration. Claims up to the free slots. A full batch
   * means the queue likely holds more, so each slot that frees up re-claims
   * at once instead of waiting out the poll interval; a short batch means
   * the queue is drained and the loop goes back to polling.
   */
  private async claimOnce(): Promise<PollTickResult> {
    const free = this.concurrency - this.active.size;
    if (free <= 0) return "idle";

    // Routing is pushed into the claim: the queue only hands out tasks for
    // steps this worker hosts (and versions it supports), so tasks for
    // other workers never block the ones behind them.
    const claimed = await this.stepQueue.claim({
      workerId: this.workerId,
      limit: free,
      capabilities: this.capabilities,
      stepNames: this.registry.list(),
      ...(this.supportedVersions !== undefined && { versions: this.supportedVersions }),
    });
    const tasks = this.taskFilter ? await this.applyTaskFilter(claimed) : claimed;
    for (const task of tasks) this.launch(task);

    // Every free slot is now busy, so the next claim waits for one to
    // free up: `launch` wakes the loop as each task settles.
    this.backlogLikely = claimed.length >= free;
    return "idle";
  }

  /** Keep the tasks `taskFilter` accepts; give the rest back to the queue. */
  private async applyTaskFilter(tasks: StepTask[]): Promise<StepTask[]> {
    const accepted: StepTask[] = [];
    for (const task of tasks) {
      if (this.taskFilter!(task)) {
        accepted.push(task);
        continue;
      }
      try {
        await this.stepQueue.release({ taskId: task.id, claimToken: task.claimToken ?? "" });
      } catch (error) {
        this.report({ phase: "release", error, task });
      }
    }
    return accepted;
  }

  private launch(task: StepTask): void {
    const controller = new AbortController();
    this.active.set(task.id, { task, controller });
    void this.executeTask(task, controller)
      .catch((error: unknown) => this.report({ phase: "task", error, task }))
      .finally(() => {
        this.active.delete(task.id);
        if (this.active.size === 0) {
          const waiters = this.idleWaiters;
          this.idleWaiters = [];
          for (const resolve of waiters) resolve();
        }
        // A slot just freed: claim straight away while there's a backlog.
        if (this.backlogLikely) this.pollLoop.wake();
      });
  }

  /**
   * Stop-timeout path: give every task still running back to the queue so
   * another worker picks it up now, and abort its handler. The task's own
   * commit then finds its claim gone and writes nothing.
   */
  private async releaseUnfinished(): Promise<void> {
    const unfinished = [...this.active.values()];
    await Promise.all(
      unfinished.map(async ({ task, controller }) => {
        controller.abort(
          new WorkerStoppingError(
            `worker ${this.workerId} stopped before task ${task.id} finished`,
          ),
        );
        try {
          await this.stepQueue.release({ taskId: task.id, claimToken: task.claimToken ?? "" });
        } catch (error) {
          this.report({ phase: "release", error, task });
        }
      }),
    );
  }

  private report(event: WorkerErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not escape into an unhandled rejection.
    }
  }

  /**
   * Run one task: one attempt (middleware + handler, under the step's
   * `timeoutMs`), then settle the task with its outcome. Retry and
   * `onFailure` are the coordinator's: it reads the settled task.
   */
  private async executeTask(task: StepTask, controller: AbortController): Promise<void> {
    const startTime = this.clock.currentTimeMs();
    const taskHeartbeatTimer: TimerHandle = this.clock.setInterval(() => {
      void this.heartbeatTask({ task, controller, timer: taskHeartbeatTimer });
    }, this.heartbeatIntervalMs);

    try {
      const outcome = await this.computeOutcome({ task, startTime, signal: controller.signal });
      // Lease lost or given back on stop: the task belongs to someone else.
      if (controller.signal.aborted) return;
      await this.commit(task, outcome);
    } finally {
      taskHeartbeatTimer.clear();
    }
  }

  /**
   * Extend the task's lease. `false` means the claim is gone (the task was
   * reclaimed after this worker stalled, or settled elsewhere): abort the
   * handler, stop heartbeating and tell `onLeaseLost`. A failed heartbeat
   * is only reported; the next one may get through.
   */
  private async heartbeatTask(params: {
    readonly task: StepTask;
    readonly controller: AbortController;
    readonly timer: TimerHandle;
  }): Promise<void> {
    const { task, controller, timer } = params;
    if (controller.signal.aborted) return;
    let held: boolean;
    try {
      held = await this.stepQueue.heartbeat({ taskId: task.id, claimToken: task.claimToken });
    } catch (error) {
      this.report({ phase: "heartbeat", error, task });
      return;
    }
    if (held || controller.signal.aborted) return;
    timer.clear();
    controller.abort(new TaskLeaseLostError(`lost the claim on task ${task.id}`));
    try {
      await this.hooks.onLeaseLost?.(task);
    } catch (error) {
      this.report({ phase: "hook", error, task });
    }
  }

  private async computeOutcome(params: {
    readonly task: StepTask;
    readonly startTime: number;
    readonly signal: AbortSignal;
  }): Promise<StepOutcome> {
    const { task, startTime } = params;
    const elapsed = () => this.clock.currentTimeMs() - startTime;
    const registration = this.registry.resolve(task.stepName);

    if (!registration) {
      const error = `Step "${task.stepName}" not found in registry. Available: ${this.registry.list().join(", ")}`;
      return { kind: "failed", error, cause: new Error(error), durationMs: elapsed() };
    }

    // The attempt's own signal: aborted with the task's (lease lost, stop)
    // or by the step's `timeoutMs`.
    const attempt = new AbortController();
    const onTaskAbort = (): void => attempt.abort(params.signal.reason);
    if (params.signal.aborted) onTaskAbort();
    else params.signal.addEventListener("abort", onTaskAbort, { once: true });

    // The inline step context: `deps` are the declared dependencies only,
    // `prev` the first of them (the workflow input for a root step).
    const first = task.dependsOn[0];
    const ctx: WorkerStepContext = {
      input: task.input,
      prev: first !== undefined ? task.deps[first] : task.input,
      deps: task.deps,
      workflowId: task.workflowId,
      stepName: task.stepName,
      attempt: task.attempt,
      signal: attempt.signal,
    };

    let timer: TimerHandle | undefined;
    try {
      await this.hooks.beforeStep?.(task);
      const run = this.buildChain(task, registration)(ctx);
      const timeoutMs = task.timeoutMs;
      const value =
        timeoutMs === undefined
          ? await run
          : await Promise.race([
              run,
              new Promise<never>((_, reject) => {
                timer = this.clock.setTimeout(() => {
                  const error = new StepTimeoutError({
                    workflowId: task.workflowId,
                    stepName: task.stepName,
                    timeoutMs,
                    message: `Step "${task.stepName}" timed out after ${timeoutMs}ms`,
                  });
                  attempt.abort(error);
                  reject(error);
                }, timeoutMs);
              }),
            ]);
      return { kind: "completed", value, durationMs: elapsed() };
    } catch (err) {
      return { kind: "failed", error: errorMessage(err), cause: err, durationMs: elapsed() };
    } finally {
      timer?.clear();
      params.signal.removeEventListener("abort", onTaskAbort);
    }
  }

  /**
   * Settle the task with its outcome. `complete` / `fail` check the claim
   * token and write in one atomic queue operation, so a worker whose claim
   * was lost (reclaimed after a stall, failed by the coordinator's wait
   * deadline, given back on stop) is rejected and its outcome dropped. The
   * worker writes nothing else: the coordinator reads the settled task and
   * writes the step row and its attempt rows under its run lock. A failed
   * queue write leaves the task claimed; its lease goes stale and it is
   * redelivered (at-least-once), so handlers must be idempotent.
   */
  private async commit(task: StepTask, outcome: StepOutcome): Promise<void> {
    const claimToken = task.claimToken;
    const { durationMs } = outcome;
    if (claimToken === undefined) {
      const error = new Error(`task ${task.id} was claimed without a claim token`);
      this.report({ phase: "commit", error, task });
      return;
    }
    const claim = { taskId: task.id, claimToken };

    let settled: boolean;
    try {
      if (outcome.kind === "completed") {
        settled = await this.stepQueue.complete({ ...claim, result: outcome.value, durationMs });
      } else {
        const errorTag = tagOf(outcome.cause);
        settled = await this.stepQueue.fail({
          ...claim,
          error: outcome.error,
          ...(errorTag !== undefined && { errorTag }),
          durationMs,
        });
      }
    } catch (error) {
      this.report({ phase: "commit", error, task });
      return;
    }
    if (!settled) return;

    try {
      if (outcome.kind === "completed") {
        await this.hooks.afterStep?.(task, outcome.value, durationMs);
      } else {
        await this.hooks.onError?.(task, outcome.cause, durationMs);
      }
    } catch (error) {
      this.report({ phase: "hook", error, task });
    }
  }

  private buildChain(
    task: StepTask,
    registration: StepRegistration,
  ): (ctx: WorkerStepContext) => Promise<unknown> {
    const { handler } = registration;
    const base = async (ctx: WorkerStepContext): Promise<unknown> => runHookValue(handler(ctx));
    // Global middleware, outermost first.
    return this.middleware.reduceRight<(ctx: WorkerStepContext) => Promise<unknown>>(
      (next, mw) => (ctx) => mw({ task, ctx, next }),
      base,
    );
  }
}

/** Abort reason when a heartbeat finds the task's claim gone. */
export class TaskLeaseLostError extends Error {
  override readonly name = "TaskLeaseLostError";
}

/** Abort reason for tasks still running when `stop({ timeoutMs })` gives up on them. */
export class WorkerStoppingError extends Error {
  override readonly name = "WorkerStoppingError";
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** The error's `_tag`, for the coordinator's retry `when` and the step row. */
function tagOf(err: unknown): string | undefined {
  const tag = (err as { _tag?: unknown } | null | undefined)?._tag;
  return typeof tag === "string" ? tag : undefined;
}

function defaultOnError(event: WorkerErrorEvent): void {
  const where = event.task ? ` (${event.task.workflowId}/${event.task.stepName})` : "";
  console.error(`[worker] ${event.phase} failed${where}:`, event.error);
}

export function createWorker(config: WorkerConfig): WorkflowWorker {
  return new DefaultWorker(config);
}
