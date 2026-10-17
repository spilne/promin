// ---------------------------------------------------------------------------
// DistributedWorkflowRunner — WorkflowRunner backed by a StepQueue.
//
// Implements the same WorkflowRunner interface as DefaultWorkflowRunner but
// delegates step execution to remote workers via a StepQueue instead of
// running step bodies in-process. The orchestration loop (DAG ready-set,
// lock, retry, compensation) still runs here; ordinary step bodies are
// remote, while sleep and signal-wait steps run here (they only record their
// suspension).
//
// Also runs a background leader-elected sweep loop (startLoop / stopLoop)
// that adopts orphaned runs and re-enqueues steps held by dead or stalled
// workers.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, OrphanedRun } from "../durable/workflow-storage.ts";
import type { WorkflowState, WorkflowRunEvent } from "../durable/workflow-state.ts";
import type {
  Workflow,
  WorkflowDAG,
  WorkflowStatusInfo,
  WorkflowHandle,
} from "../durable/durable-pipeline.ts";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "../durable/workflow-version-registry.ts";
import {
  createWorkflowRunner,
  type WorkflowRunner,
  type WorkflowRunnerRunParams,
  type WorkflowRunSafeError,
  type RecoveryResult,
  type RecoveryStrategy,
} from "../durable/workflow-runner.ts";
import { WorkflowLockError } from "../durable/durable-pipeline-error.ts";
import type { StepQueue } from "./step-queue.ts";
import type { WorkerRegistry } from "./worker-registry.ts";
import type { LeaderElection } from "./leader-election.ts";
import { SingleLeader } from "./leader-election.ts";
import { StepQueueExecutor } from "./step-queue-executor.ts";
import {
  CoordinatorStepExecutor,
  sharedReadStorage,
  withoutRequeue,
} from "./coordinator-step-executor.ts";
import { buildStubWorkflow } from "./stub-workflow.ts";
import { isStaleLeaseError } from "../scheduler/leader-lease.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { PollLoop } from "../shared/poll-loop.ts";

export { buildStubWorkflow } from "./stub-workflow.ts";

export interface DistributedRunnerConfig {
  /** Workflow storage for state persistence. */
  storage: WorkflowStorage;
  /** Step queue for dispatching tasks to workers. */
  stepQueue: StepQueue;
  /**
   * How often to run the dead-worker sweep (ms). Default: 1000.
   */
  pollIntervalMs?: number;
  /** Worker registry for health monitoring. Optional — without it, no dead detection. */
  workerRegistry?: WorkerRegistry;
  /** How long before a worker is considered dead (ms). Default: 30000. */
  workerTimeoutMs?: number;
  /**
   * How long a retired / dead worker row is kept before the sweep loop's
   * `gc()` reaps it (ms). Keeps gracefully-stopped workers visible to the
   * dashboard + run forensics for a window. Default: 7 days.
   */
  workerRetentionMs?: number;
  /**
   * Leader election — ensures only one instance runs the sweep. Default:
   * `SingleLeader` (always wins; for a single coordinator). With more than
   * one instance, pass a `LeaseLeaderElection` over any `LeaderLeaseStore`
   * with key `coordinatorLeaderKey({ namespace })`: its lease fences the
   * sweep's queue writes, so an instance that lost leadership while paused
   * can't commit them. Use a TTL of several `pollIntervalMs`; leadership is
   * refreshed every sweep and released by `stopLoop()`.
   */
  leaderElection?: LeaderElection;
  /**
   * Optional workflow registry. When provided, `run({ name, ... })`
   * resolves the definition by name via the registry, and recovery adopts
   * orphaned runs with their real definition instead of a stub rebuilt from
   * the stored DAG.
   */
  registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  /**
   * How often the step executor polls storage while waiting for a step to
   * complete. Waits on one run share each read. Default: 500ms.
   */
  stepPollIntervalMs?: number;
  /**
   * How often the leader looks for orphaned runs (pending / running runs
   * nobody holds the lock of) after the scan it does on becoming leader.
   * Default: 60 000.
   */
  recoveryIntervalMs?: number;
  /**
   * A run is only adopted once it hasn't been updated for this long, so a
   * run another coordinator has just created (and not locked yet) is left
   * to it. Default: `workerTimeoutMs`.
   */
  orphanGraceMs?: number;
  /**
   * How often `run()` / `waitForResult()` check storage for a run that is
   * driven elsewhere (another instance holds its lock, or it is suspended
   * and a scanner will resume it). Default: 1000.
   */
  resultPollIntervalMs?: number;
  /**
   * Time source for the sweep-loop cadence, the step executor's polls and
   * the inner runner's timestamps. Default: `SystemWallClock`. Tests pass a
   * `FakeWallClock`.
   */
  clock?: WallClock;
  /**
   * Called when background work fails: a sweep (`"sweep"`), adopting an
   * orphaned run (`"recovery"`), a step executor's storage check
   * (`"step-wait"`) or a result wait's storage check (`"result-wait"`).
   * Every loop keeps running and backs off; nothing here is fatal.
   * Default: `console.error`.
   */
  onError?: (event: DistributedRunnerErrorEvent) => void;
  /**
   * Upper bound for the sweep loop's wait after consecutive failures.
   * Default: 30 000 (or `pollIntervalMs`, if larger).
   */
  maxErrorBackoffMs?: number;
}

/** A background-loop failure reported through `DistributedRunnerConfig.onError`. */
export interface DistributedRunnerErrorEvent {
  readonly source: "sweep" | "recovery" | "step-wait" | "result-wait";
  readonly error: unknown;
  /** Failures in a row for this loop, including this one. */
  readonly consecutiveFailures: number;
  /** The run involved, when there is one. */
  readonly workflowId?: string;
}

/** @deprecated Use DistributedRunnerConfig */
export type CoordinatorConfig = DistributedRunnerConfig;

/**
 * Run worker `gc()` every Nth dead-worker sweep rather than every tick —
 * the retention window is days, so a reap scan every poll buys nothing.
 * At the default 1s poll this is roughly once a minute.
 */
const WORKER_GC_EVERY_N_TICKS = 60;

/** Page size for the orphaned-run scan. */
const RECOVERY_PAGE_SIZE = 100;

/** How a run driven by this instance ended up. */
type LocalOutcome =
  | { readonly kind: "completed"; readonly result: unknown }
  | { readonly kind: "failed"; readonly error: unknown }
  /** The run suspended (sleep / signal); a scanner resumes it later. */
  | { readonly kind: "suspended"; readonly error: unknown }
  /** Another instance holds the run's lock and is driving it. */
  | { readonly kind: "elsewhere" };

interface ResultWaiter {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
}

export class DistributedWorkflowRunner implements WorkflowRunner {
  readonly storage: WorkflowStorage;
  private readonly innerRunner: WorkflowRunner;
  private readonly stepQueue: StepQueue;
  private readonly pollIntervalMs: number;
  private readonly workerRegistry?: WorkerRegistry;
  private readonly workerTimeoutMs: number;
  private readonly workerRetentionMs: number;
  /** Dead-worker sweep counter — drives the throttled `gc()` cadence. */
  private workerSweepCount = 0;
  private readonly leaderElection: LeaderElection;
  private readonly clock: WallClock;
  private readonly registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  private readonly recoveryIntervalMs: number;
  private readonly orphanGraceMs: number;
  private readonly sweepLoop: PollLoop;
  private readonly resultLoop: PollLoop;
  private resultLoopRunning = false;
  private readonly onError: (event: DistributedRunnerErrorEvent) => void;
  private sweepLoopDone?: Promise<void>;
  private isLeader = false;
  /** When the leader next scans for orphaned runs; unset until it has led. */
  private nextRecoveryAtMs?: number;
  /** Runs this instance is driving, by workflow id. */
  private runningWorkflows = new Map<string, Promise<LocalOutcome>>();
  /** Result waits on runs driven elsewhere, checked by `resultLoop`. */
  private resultWaiters = new Map<string, ResultWaiter[]>();

  constructor(config: DistributedRunnerConfig) {
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.workerRegistry = config.workerRegistry;
    this.workerTimeoutMs = config.workerTimeoutMs ?? 30_000;
    this.workerRetentionMs = config.workerRetentionMs ?? 7 * 24 * 60 * 60 * 1000;
    this.leaderElection = config.leaderElection ?? new SingleLeader();
    this.clock = config.clock ?? SystemWallClock;
    this.registry = config.registry;
    this.recoveryIntervalMs = config.recoveryIntervalMs ?? 60_000;
    this.orphanGraceMs = config.orphanGraceMs ?? this.workerTimeoutMs;
    const onError = config.onError ?? defaultOnError;
    this.onError = onError;

    this.sweepLoop = new PollLoop({
      name: "distributed-runner-sweep",
      intervalMs: this.pollIntervalMs,
      clock: this.clock,
      maxBackoffMs: config.maxErrorBackoffMs,
      tick: () => this._sweepOnce(),
      onError: (error, info) =>
        onError({ source: "sweep", error, consecutiveFailures: info.consecutiveFailures }),
    });

    this.resultLoop = new PollLoop({
      name: "distributed-runner-results",
      intervalMs: config.resultPollIntervalMs ?? 1000,
      clock: this.clock,
      tick: () => this._checkResults(),
      onError: (error, info) =>
        onError({ source: "result-wait", error, consecutiveFailures: info.consecutiveFailures }),
    });

    const stepPollIntervalMs = config.stepPollIntervalMs ?? config.pollIntervalMs ?? 500;
    const queueExecutor = new StepQueueExecutor({
      // The leader's fenced sweep is the only requeue (see coordinator-step-executor.ts).
      stepQueue: withoutRequeue(config.stepQueue),
      storage: sharedReadStorage({
        storage: config.storage,
        clock: this.clock,
        maxAgeMs: Math.max(1, Math.floor(stepPollIntervalMs / 2)),
      }),
      pollIntervalMs: stepPollIntervalMs,
      staleTimeoutMs: this.workerTimeoutMs,
      clock: this.clock,
      onError: (error, info) =>
        onError({ source: "step-wait", error, consecutiveFailures: info.consecutiveFailures }),
    });

    this.innerRunner = createWorkflowRunner({
      storage: config.storage,
      registry: config.registry,
      stepExecutor: new CoordinatorStepExecutor({
        queueExecutor,
        storage: config.storage,
        clock: this.clock,
      }),
      clock: this.clock,
    });
  }

  // ---------------------------------------------------------------------------
  // WorkflowRunner interface
  // ---------------------------------------------------------------------------

  /**
   * Submit the run and wait for it. Resolves with the result, or rejects
   * with the run's error. Like the in-process runner, a run that suspends
   * (sleep / signal) rejects with `WorkflowSuspendedError`; use
   * `waitForResult()` to wait through suspensions. A run another instance
   * is driving (it holds the lock) is waited for through storage.
   */
  async run(params: WorkflowRunnerRunParams): Promise<unknown> {
    const { outcome: running } = await this._submit(params);
    const outcome = await running;
    switch (outcome.kind) {
      case "completed":
        return outcome.result;
      case "failed":
      case "suspended":
        throw outcome.error;
      case "elsewhere":
        return this._watchResult(params.workflowId);
    }
  }

  async runSafe(
    params: WorkflowRunnerRunParams,
  ): Promise<{ data: unknown; error: null } | { data: null; error: WorkflowRunSafeError }> {
    try {
      const data = await this.run(params);
      return { data, error: null };
    } catch (error) {
      return { data: null, error: error as WorkflowRunSafeError };
    }
  }

  async start<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly input: Input;
  }): Promise<WorkflowHandle<Output>> {
    await this._submit(params);
    return this.handle<Output>(params.workflowId);
  }

  handle<Output = unknown>(workflowId: string): WorkflowHandle<Output> {
    return this.innerRunner.handle<Output>(workflowId);
  }

  resume<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly fromStep: string;
  }): Promise<Output> {
    return this.innerRunner.resume<Input, Output>(params);
  }

  subscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent> {
    return this.innerRunner.subscribe(workflowId, options);
  }

  getStatus(
    workflowId: string,
    params?: { readonly includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<unknown> | null> {
    return this.innerRunner.getStatus(workflowId, params);
  }

  /**
   * Load the workflow's current persistent state. Pairs with the
   * deprecated `WorkflowCoordinator.status` contract — new callers should
   * prefer `getStatus()` (richer info) or `storage.loadWorkflow()`.
   */
  status(workflowId: string): Promise<WorkflowState | null> {
    return this.storage.loadWorkflow(workflowId);
  }

  recover(strategy: RecoveryStrategy): Promise<RecoveryResult> {
    return this.innerRunner.recover(strategy);
  }

  // ---------------------------------------------------------------------------
  // Distributed-specific: submit fire-and-forget (used by trigger services)
  // ---------------------------------------------------------------------------

  async submit<Input>(
    params:
      | { workflow: Workflow<Input, unknown>; workflowId: string; input: Input }
      | { name: string; version?: string; workflowId: string; input: Input },
  ): Promise<void> {
    await this._submit(params as WorkflowRunnerRunParams);
  }

  /**
   * Wait until the run is terminal, wherever it runs: resolves with the
   * result of a completed run and rejects with an `Error` carrying the
   * stored error of a failed (or tripwired) run. Suspensions are waited
   * through. Rejects when the run doesn't exist (or was deleted).
   */
  async waitForResult<Output>(workflowId: string): Promise<Output> {
    const local = this.runningWorkflows.get(workflowId);
    if (local) {
      const outcome = await local;
      if (outcome.kind === "completed") return outcome.result as Output;
      if (outcome.kind === "failed") throw outcome.error;
    }
    return this._watchResult(workflowId) as Promise<Output>;
  }

  // ---------------------------------------------------------------------------
  // Lifecycle — start/stop the background sweep loop
  // ---------------------------------------------------------------------------

  /**
   * Run the leader-elected sweep loop: adopt orphaned runs (on becoming
   * leader, then every `recoveryIntervalMs`) and re-enqueue steps held by
   * dead or stalled workers. A failed sweep is reported through `onError`
   * and retried with backoff; it never ends the loop. Resolves after
   * `stopLoop()`, once leadership has been released.
   */
  startLoop(): Promise<void> {
    if (this.sweepLoopDone) return this.sweepLoopDone;
    const done = (async () => {
      try {
        await this.sweepLoop.start();
      } finally {
        if (this.isLeader) {
          this.isLeader = false;
          this.nextRecoveryAtMs = undefined;
          try {
            await this.leaderElection.release();
          } catch (error) {
            this.onError({ source: "sweep", error, consecutiveFailures: 1 });
          }
        }
      }
    })().finally(() => {
      if (this.sweepLoopDone === done) this.sweepLoopDone = undefined;
    });
    this.sweepLoopDone = done;
    return done;
  }

  /**
   * Stop the sweep loop. Cancels the pending wait, then resolves once the
   * in-flight sweep has finished and leadership has been released.
   */
  async stopLoop(): Promise<void> {
    const done = this.sweepLoopDone;
    await this.sweepLoop.stop();
    await done;
  }

  private async _sweepOnce(): Promise<void> {
    const wasLeader = this.isLeader;
    this.isLeader = await this.leaderElection.tryAcquire();
    if (!this.isLeader) {
      this.nextRecoveryAtMs = undefined;
      return;
    }
    try {
      // Recovery is leadership-triggered: once on taking over (a previous
      // leader may have died with runs in flight), then at a coarse cadence.
      const now = this.clock.currentTimeMs();
      if (!wasLeader || this.nextRecoveryAtMs === undefined || now >= this.nextRecoveryAtMs) {
        await this._recoverOrphanedRuns();
        this.nextRecoveryAtMs = now + this.recoveryIntervalMs;
      }
      await this._tickDeadWorkers();
    } catch (error) {
      // A fenced write was rejected: another instance leads now.
      if (isStaleLeaseError(error)) {
        this.isLeader = false;
        this.nextRecoveryAtMs = undefined;
      }
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Create the run and start driving it here (unless it already is). Resolves once started. */
  private async _submit(
    params: WorkflowRunnerRunParams,
  ): Promise<{ readonly outcome: Promise<LocalOutcome> }> {
    const { workflowId, input } = params;
    const existing = this.runningWorkflows.get(workflowId);
    if (existing) return { outcome: existing };

    let workflow: Workflow<unknown, unknown>;
    if ("workflow" in params) {
      workflow = params.workflow as Workflow<unknown, unknown>;
    } else {
      workflow = await this._resolveByName(params.name, params.version);
    }

    // Pre-create in storage with DAG embedded in metadata so crash-recovery
    // can rebuild the stub without the original definition object.
    await this.storage.createWorkflow({
      workflowId,
      workflowName: workflow.name,
      input,
      version: workflow.version,
      metadata: { ...workflow._definition.metadata, _dag: workflow.dag },
    });

    return {
      outcome:
        this.runningWorkflows.get(workflowId) ?? this._launch({ workflow, workflowId, input }),
    };
  }

  /** Drive the run in this process; the outcome promise never rejects. */
  private _launch(params: {
    readonly workflow: Workflow<unknown, unknown>;
    readonly workflowId: string;
    readonly input: unknown;
  }): Promise<LocalOutcome> {
    const { workflowId } = params;
    const outcome = this.innerRunner
      .run(params)
      .then(
        (result): LocalOutcome => ({ kind: "completed", result }),
        (error: unknown): LocalOutcome => {
          if (error instanceof WorkflowLockError || tagOf(error) === "WorkflowLockError") {
            return { kind: "elsewhere" };
          }
          if (tagOf(error) === "WorkflowSuspendedError") return { kind: "suspended", error };
          return { kind: "failed", error };
        },
      )
      .finally(() => {
        if (this.runningWorkflows.get(workflowId) === outcome) {
          this.runningWorkflows.delete(workflowId);
        }
      });
    this.runningWorkflows.set(workflowId, outcome);
    return outcome;
  }

  /** Wait for a run this process isn't driving to become terminal. */
  private _watchResult(workflowId: string): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      const list = this.resultWaiters.get(workflowId) ?? [];
      list.push({ resolve, reject });
      this.resultWaiters.set(workflowId, list);
      this._ensureResultLoop();
    });
  }

  private _ensureResultLoop(): void {
    if (this.resultLoopRunning) {
      this.resultLoop.wake();
      return;
    }
    this.resultLoopRunning = true;
    void this.resultLoop.start().finally(() => {
      this.resultLoopRunning = false;
      // A waiter registered while the loop was exiting.
      if (this.resultWaiters.size > 0) this._ensureResultLoop();
    });
  }

  /** One pass of the result loop: settle every waiter whose run is terminal. */
  private async _checkResults(): Promise<"stop" | "idle"> {
    const ids = [...this.resultWaiters.keys()].filter((id) => !this.runningWorkflows.has(id));
    await Promise.all(
      ids.map(async (workflowId) => {
        const state = await this.storage.loadWorkflow(workflowId);
        if (state === null) {
          this._settleWaiters({
            workflowId,
            error: new Error(`Workflow "${workflowId}" not found`),
          });
        } else if (state.status === "completed") {
          this._settleWaiters({ workflowId, result: state.result });
        } else if (state.status === "failed") {
          this._settleWaiters({ workflowId, error: new Error(state.error ?? "Workflow failed") });
        } else if (state.status === "tripwire") {
          this._settleWaiters({
            workflowId,
            error: new Error(state.error ?? `Workflow "${workflowId}" ended via tripwire`),
          });
        }
      }),
    );
    return this.resultWaiters.size === 0 ? "stop" : "idle";
  }

  private _settleWaiters(
    params: { readonly workflowId: string } & (
      | { readonly result: unknown; readonly error?: undefined }
      | { readonly error: Error; readonly result?: undefined }
    ),
  ): void {
    const waiters = this.resultWaiters.get(params.workflowId) ?? [];
    this.resultWaiters.delete(params.workflowId);
    for (const w of waiters) {
      if (params.error) w.reject(params.error);
      else w.resolve(params.result);
    }
  }

  private async _resolveByName(
    name: string,
    version?: string,
  ): Promise<Workflow<unknown, unknown>> {
    const registry = this.registry;
    if (!registry) {
      throw new Error(
        `DistributedWorkflowRunner.run({ name }) requires \`registry\` on the config. ` +
          `Pass a WorkflowVersionRegistry or use the { workflow } shape.`,
      );
    }
    const def = await registry.resolve(name, version);
    if (!def) {
      const allNames = await registry.names();
      throw new Error(
        `No workflow "${name}"${version ? ` version "${version}"` : ""} in registry. ` +
          `Registered: ${(allNames as string[]).join(", ") || "(none)"}.`,
      );
    }
    return def;
  }

  private async _tickDeadWorkers(): Promise<void> {
    const lease = this.leaderElection.lease ?? undefined;
    const fence = lease ? { lease } : {};
    if (this.workerRegistry) {
      const dead = await this.workerRegistry.detectDead(this.workerTimeoutMs);
      for (const worker of dead) {
        await this.stepQueue.requeueStuck({ mode: "worker", workerId: worker.workerId, ...fence });
      }
      // Reap worker rows past the retention window. Throttled — see
      // WORKER_GC_EVERY_N_TICKS — so retired / dead rows stay visible
      // for the window, then go.
      this.workerSweepCount += 1;
      if (this.workerSweepCount % WORKER_GC_EVERY_N_TICKS === 0) {
        await this.workerRegistry.gc({ retainMs: this.workerRetentionMs });
      }
    }
    await this.stepQueue.requeueStuck({
      mode: "stale",
      olderThanMs: this.workerTimeoutMs,
      ...fence,
    });
  }

  /**
   * Adopt runs nobody is driving: `pending` / `running` runs whose lock is
   * free or expired and that haven't been touched for `orphanGraceMs`.
   * Suspended runs are never adopted: the sleep / signal scanners resume
   * them when they're due. Uses `storage.listOrphanedRuns` (keyset-paged)
   * when the backend has it; otherwise lists pending and running runs and
   * lets the run lock turn away the ones still owned.
   */
  private async _recoverOrphanedRuns(): Promise<void> {
    const nowMs = this.clock.currentTimeMs();
    const now = new Date(nowMs);
    const updatedBefore = new Date(nowMs - this.orphanGraceMs);

    if (this.storage.listOrphanedRuns) {
      let afterWorkflowId: string | undefined;
      while (true) {
        const page = await this.storage.listOrphanedRuns({
          now,
          updatedBefore,
          limit: RECOVERY_PAGE_SIZE,
          ...(afterWorkflowId !== undefined && { afterWorkflowId }),
        });
        for (const run of page) await this._adopt(run);
        if (page.length < RECOVERY_PAGE_SIZE) return;
        afterWorkflowId = page[page.length - 1]!.workflowId;
      }
    }

    // Collect first, adopt after: adopting moves runs out of `pending`, which
    // would make offset paging over that status skip rows.
    const candidates: OrphanedRun[] = [];
    for (const status of ["pending", "running"] as const) {
      for (let offset = 0; ; offset += RECOVERY_PAGE_SIZE) {
        const page = await this.storage.listWorkflows({
          status,
          limit: RECOVERY_PAGE_SIZE,
          offset,
          orderBy: "createdAt",
          orderDir: "asc",
        });
        for (const state of page) {
          if (state.updatedAt >= updatedBefore) continue;
          candidates.push({
            workflowId: state.workflowId,
            workflowName: state.workflowName,
            status,
            input: state.input,
            ...(state.version !== undefined && { version: state.version }),
            ...(state.metadata !== undefined && { metadata: state.metadata }),
          });
        }
        if (page.length < RECOVERY_PAGE_SIZE) break;
      }
    }
    for (const run of candidates) await this._adopt(run);
  }

  /** Start driving an orphaned run here. Failures are reported, never thrown. */
  private async _adopt(run: OrphanedRun): Promise<void> {
    if (this.runningWorkflows.has(run.workflowId)) return;
    let workflow: Workflow<unknown, unknown> | undefined;
    try {
      workflow = await this._definitionFor(run);
    } catch (error) {
      this.onError({
        source: "recovery",
        error,
        consecutiveFailures: 1,
        workflowId: run.workflowId,
      });
      return;
    }
    if (!workflow) {
      this.onError({
        source: "recovery",
        error: new Error(
          `Cannot adopt orphaned run "${run.workflowId}" (${run.workflowName}): no registered ` +
            `definition and no stored DAG`,
        ),
        consecutiveFailures: 1,
        workflowId: run.workflowId,
      });
      return;
    }
    if (this.runningWorkflows.has(run.workflowId)) return;
    void this._launch({ workflow, workflowId: run.workflowId, input: run.input }).then(
      (outcome) => {
        if (outcome.kind === "failed") {
          this.onError({
            source: "recovery",
            error: outcome.error,
            consecutiveFailures: 1,
            workflowId: run.workflowId,
          });
        }
      },
    );
  }

  /** The registered definition for a run, else a stub from its stored DAG. */
  private async _definitionFor(run: OrphanedRun): Promise<Workflow<unknown, unknown> | undefined> {
    if (this.registry) {
      const def = await this.registry.resolve(run.workflowName, run.version);
      if (def) return def;
    }
    const dag = run.metadata?._dag as WorkflowDAG | undefined;
    if (!dag) return undefined;
    return buildStubWorkflow(dag, run.workflowName ?? dag.name, run.version);
  }
}

function tagOf(error: unknown): string | undefined {
  return (error as { _tag?: string } | null)?._tag;
}

function defaultOnError(event: DistributedRunnerErrorEvent): void {
  const run = event.workflowId ? ` (${event.workflowId})` : "";
  console.error(
    `[distributed-runner] ${event.source} failed${run} (${event.consecutiveFailures} in a row):`,
    event.error,
  );
}

export function createDistributedWorkflowRunner(
  config: DistributedRunnerConfig,
): DistributedWorkflowRunner {
  return new DistributedWorkflowRunner(config);
}

// ---------------------------------------------------------------------------
// Backward-compat aliases
// ---------------------------------------------------------------------------

/** @deprecated Use DistributedWorkflowRunner */
export interface WorkflowCoordinator {
  submit<Input>(params: {
    workflow: Workflow<Input, unknown>;
    workflowId: string;
    input: Input;
  }): Promise<void>;
  submit<Input>(params: {
    name: string;
    workflowId: string;
    input: Input;
    version?: string;
  }): Promise<void>;
  status(workflowId: string): Promise<WorkflowState | null>;
  waitForResult<Output>(workflowId: string): Promise<Output>;
  /** @deprecated Use startLoop() */
  startLoop(): Promise<void>;
  /** @deprecated Use stopLoop() */
  stopLoop(): Promise<void>;
}

/** @deprecated Use DistributedWorkflowRunner */
export const DefaultCoordinator = DistributedWorkflowRunner;

/** @deprecated Use createDistributedWorkflowRunner */
export function createCoordinator(config: CoordinatorConfig): DistributedWorkflowRunner {
  return new DistributedWorkflowRunner(config);
}
