// ---------------------------------------------------------------------------
// WorkflowWorker — polls step queue, executes steps, checkpoints results
//
// Supports both:
// - Per-step options (retry, onFailure, compensate) via StepRegistry
// - Global middleware + hooks on the worker itself
// ---------------------------------------------------------------------------

import { runHookValue } from "../shared/eff.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import { SystemWallClock, type WallClock, type TimerHandle } from "../shared/wall-clock.ts";
import { PollLoop, type PollTickResult } from "../shared/poll-loop.ts";
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
   * Unversioned tasks (`version === undefined`) are always accepted for
   * backward compat with pre-versioning workflows.
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
   * duration tracking, retry backoff, and per-attempt timestamps.
   * Default: `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Called for failures that don't belong to a step body: a failed claim,
   * a failed outcome write (the task is then redelivered), a throwing
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
   * Writing a step outcome failed (storage or queue). The queue task stays
   * claimed and is redelivered once its lease goes stale.
   */
  | "commit"
  /** A `WorkerHooks` callback threw; the step outcome is unaffected. */
  | "hook"
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
   * Stop claiming, wait for the in-flight claim and every running task to
   * finish, then deregister.
   */
  stop(): Promise<void>;
  readonly workerId: string;
}

/**
 * What a step run produced, before anything is written. `completed` covers
 * the handler's value as well as the `skip` / `fallback` strategies.
 */
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
  private readonly storage: WorkflowStorage;
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
  private activeCount = 0;
  /** The last claim filled every free slot, so more work is likely queued. */
  private backlogLikely = false;
  private idleWaiters: (() => void)[] = [];
  private heartbeatTimer?: TimerHandle;

  constructor(config: WorkerConfig) {
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.storage = config.storage;
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

  async stop(): Promise<void> {
    // Stop claiming first. Awaiting the loop means a claim already in
    // flight has handed its tasks to `launch` before we wait for them.
    await this.pollLoop.stop();

    // Mark as draining, then wait for active tasks
    if (this.workerRegistry) {
      await this.workerRegistry.drain(this.workerId);
    }

    if (this.activeCount > 0) {
      await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
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
    const free = this.concurrency - this.activeCount;
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
    this.activeCount++;
    void this.executeTask(task)
      .catch((error: unknown) => this.report({ phase: "task", error, task }))
      .finally(() => {
        this.activeCount--;
        if (this.activeCount === 0) {
          const waiters = this.idleWaiters;
          this.idleWaiters = [];
          for (const resolve of waiters) resolve();
        }
        // A slot just freed: claim straight away while there's a backlog.
        if (this.backlogLikely) this.pollLoop.wake();
      });
  }

  private report(event: WorkerErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not escape into an unhandled rejection.
    }
  }

  /**
   * Run one task: compute its outcome (handler + retry + middleware +
   * `onFailure` strategy), then commit it. Nothing is written until the
   * outcome is known, so a throwing strategy can't half-commit.
   */
  private async executeTask(task: StepTask): Promise<void> {
    const startTime = this.clock.currentTimeMs();
    const taskHeartbeatTimer = this.clock.setInterval(() => {
      this.stepQueue.heartbeat({ taskId: task.id, claimToken: task.claimToken }).catch(() => {});
    }, this.heartbeatIntervalMs);

    try {
      const outcome = await this.computeOutcome(task, startTime);
      await this.commit(task, outcome, startTime);
    } finally {
      taskHeartbeatTimer.clear();
    }
  }

  private async computeOutcome(task: StepTask, startTime: number): Promise<StepOutcome> {
    const elapsed = () => this.clock.currentTimeMs() - startTime;
    const registration = this.registry.resolve(task.stepName);

    if (!registration) {
      const error = `Step "${task.stepName}" not found in registry. Available: ${this.registry.list().join(", ")}`;
      return { kind: "failed", error, cause: new Error(error), durationMs: elapsed() };
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
      return { kind: "completed", value, durationMs: elapsed() };
    } catch (err) {
      const durationMs = elapsed();
      const strategy = registration.options?.onFailure ?? "fail";
      if (strategy === "skip") {
        return { kind: "completed", value: undefined, durationMs };
      }
      if (typeof strategy === "object" && "fallback" in strategy) {
        try {
          return { kind: "completed", value: strategy.fallback(err), durationMs };
        } catch (fallbackErr) {
          return {
            kind: "failed",
            error: `fallback threw: ${errorMessage(fallbackErr)} (step error: ${errorMessage(err)})`,
            cause: fallbackErr,
            durationMs,
          };
        }
      }
      return { kind: "failed", error: errorMessage(err), cause: err, durationMs };
    }
  }

  /**
   * Commit an outcome: (1) check the claim is still ours, (2) write
   * storage, (3) settle the queue task. Storage goes first so the queue
   * never says `completed` / `failed` while storage has nothing — the
   * executor waits on storage, so that state would hang the workflow. If
   * the storage write fails, the queue task is left claimed; its lease goes
   * stale and it is redelivered (at-least-once). If the queue write fails
   * after storage succeeded, the step is already visible and the task is
   * redelivered and re-run, so handlers must be idempotent.
   */
  private async commit(task: StepTask, outcome: StepOutcome, startTime: number): Promise<void> {
    const claim = { taskId: task.id, claimToken: task.claimToken };
    const startedAt = new Date(startTime);
    const { durationMs } = outcome;

    try {
      // Fence: a reclaimed task belongs to another worker now; writing our
      // outcome over theirs would race them.
      if (!(await this.stepQueue.heartbeat(claim))) return;

      if (outcome.kind === "completed") {
        await this.storage.saveStepResult({
          workflowId: task.workflowId,
          stepName: task.stepName,
          result: outcome.value,
          durationMs,
          startedAt,
        });
      } else {
        await this.storage.saveStepFailure({
          workflowId: task.workflowId,
          stepName: task.stepName,
          error: outcome.error,
          durationMs,
          startedAt,
        });
      }
    } catch (error) {
      this.report({ phase: "commit", error, task });
      return;
    }

    if (isStepAttemptStorage(this.storage)) {
      try {
        await this.storage.saveStepAttempt({
          workflowId: task.workflowId,
          stepName: task.stepName,
          attempt: task.attempt,
          type: "execution",
          status: outcome.kind,
          ...(outcome.kind === "completed" ? { result: outcome.value } : { error: outcome.error }),
          durationMs,
          startedAt,
          completedAt: this.clock.now(),
          executorId: this.workerId,
        });
      } catch (error) {
        // The audit row is secondary: the step row is written, so carry on.
        this.report({ phase: "commit", error, task });
      }
    }

    let settled: boolean;
    try {
      settled =
        outcome.kind === "completed"
          ? await this.stepQueue.complete({ ...claim, result: outcome.value, durationMs })
          : await this.stepQueue.fail({ ...claim, error: outcome.error, durationMs });
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
  ): (ctx: StepContext) => Promise<unknown> {
    const { handler, options } = registration;

    // Base: resolve handler result + apply step-level retry
    let base = async (ctx: StepContext): Promise<unknown> => {
      return runHookValue(handler(ctx));
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
              await new Promise<void>((r) =>
                this.clock.setTimeout(() => r(), baseDelayMs * Math.pow(2, attempt - 1)),
              );
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

  private computePrev(task: StepTask): unknown {
    const results = task.prevResults;
    const keys = Object.keys(results);
    if (keys.length === 1) return results[keys[0]!];
    if (keys.length === 0) return task.input;
    return results;
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function defaultOnError(event: WorkerErrorEvent): void {
  const where = event.task ? ` (${event.task.workflowId}/${event.task.stepName})` : "";
  console.error(`[worker] ${event.phase} failed${where}:`, event.error);
}

export function createWorker(config: WorkerConfig): WorkflowWorker {
  return new DefaultWorker(config);
}
