// ---------------------------------------------------------------------------
// WorkflowRunner — public facade of the orchestration engine.
//
// `DefaultWorkflowRunner` resolves which definition drives a run, then hands
// it to the orchestration loop in `runner/orchestrate.ts`. The engine itself
// lives under `runner/`:
//
//   workflow-observer      handles, status snapshots, polling event stream
//   recovery               RecoveryStrategy + startup sweep
//   definition-resolver    registry / version-drain resolution
//   orchestrate            continue-as-new, lock, retry, terminal transitions
//   dag-executor           wave loop (inline-wave / executor-wave /
//                          remote-dispatch, step-checkpoint)
//   compensation, dlq, idempotency, hooks
//   step-executor          StepExecutor contract + InProcessStepExecutor
//   step-policies          per-step timeout / retry / onFailure
//
// Step bodies run inline or through a pluggable `StepExecutor`, so
// in-process handlers, queue-dispatched workers and remote workers share one
// orchestration codepath. This module re-exports the engine's public pieces
// so existing imports keep working.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import type {
  Workflow,
  WorkflowHandle,
  WorkflowHooks,
  WorkflowStatusInfo,
} from "./durable-pipeline.ts";
import { isSubscribableStorage, type WorkflowStorage } from "./workflow-storage.ts";
import type { WorkflowRunEvent } from "./workflow-state.ts";
import {
  StepError,
  WorkflowError,
  WorkflowDeadlineError,
  StepTimeoutError,
  WorkflowLockError,
  WorkflowTripwireError,
  TripwireStorageMissingError,
} from "./durable-pipeline-error.ts";
import type { WorkflowSuspendedError, WorkflowTimeoutError } from "./durable-pipeline-error.ts";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "./workflow-version-registry.ts";
import type { StepExecutor } from "./runner/step-executor.ts";
import { recoverWorkflows, type RecoveryResult, type RecoveryStrategy } from "./runner/recovery.ts";
import { resolveRunDefinition } from "./runner/definition-resolver.ts";
import { orchestrationContextFor } from "./runner/orchestration-context.ts";
import { runWorkflowOrchestration } from "./runner/orchestrate.ts";
import {
  createWorkflowHandle,
  pollWorkflowEvents,
  toStatusInfo,
} from "./runner/workflow-observer.ts";

export {
  InProcessStepExecutor,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepExecutor,
} from "./runner/step-executor.ts";
export type { StepAttemptFailure } from "./runner/step-body.ts";
export {
  RecoveryStrategy,
  RecoveryStrategyBuilder,
  type RecoveryResult,
  type StaleTerminationAction,
} from "./runner/recovery.ts";
export {
  orchestrationContextFor,
  type OrchestrationRuntime,
  type WorkflowOrchestrationContext,
} from "./runner/orchestration-context.ts";
export { runWorkflowOrchestration } from "./runner/orchestrate.ts";
export type { DagExecutionContext } from "./runner/dag-context.ts";
export { executeWorkflowDag } from "./runner/dag-executor.ts";
export { compensateWorkflow, type CompensatableStep } from "./runner/compensation.ts";
export { publishDlqRecord } from "./runner/dlq.ts";
export { getIdempotencyTtl } from "./runner/idempotency.ts";

export type WorkflowRunSafeError =
  | Error
  | WorkflowError
  | StepError
  | WorkflowLockError
  | WorkflowSuspendedError
  | WorkflowTimeoutError
  | StepTimeoutError
  | WorkflowDeadlineError
  | WorkflowTripwireError
  | TripwireStorageMissingError
  | TaggedError;

/**
 * Params for `WorkflowRunner.run` / `runSafe`. Two shapes:
 *
 * - `{ workflow, ... }` — run a specific definition directly. The runner
 *   uses the workflow's internal orchestration config + its own storage.
 * - `{ name, version?, ... }` — resolve via the runner's registry, then run.
 *   Implements version-drain-resume: if a prior run exists in storage under
 *   a different version, the matching definition from the registry drives
 *   the resume.
 */
export type WorkflowRunnerRunParams =
  | {
      readonly workflow: Workflow<unknown, unknown>;
      readonly workflowId: string;
      readonly input: unknown;
      /**
       * Skip the idempotency cache, and run a workflow whose stored run already
       * ended (completed, failed, cancelled, tripwire) as a fresh run instead of
       * answering with that outcome.
       */
      readonly force?: boolean;
      /** Optional namespace used for workflow creation and idempotency-key scoping. */
      readonly namespace?: string;
      /**
       * Per-call dedup key. The runner first looks up an existing workflow
       * by `(namespace, workflow.name, idempotencyKey)`; if a non-expired match
       * exists, the run redirects to that workflow's id and the supplied
       * `workflowId` is ignored. On miss, the new workflow is created with
       * the key attached. Solves the auto-mint case where the caller can't
       * encode dedup intent into a fresh UUID workflowId.
       */
      readonly idempotencyKey?: string;
      /**
       * How long the key resolves to this run (ms). Required when
       * `idempotencyKey` is set; ignored otherwise. After expiry the key
       * is reclaimable by a future call (the workflow row is untouched —
       * only the `(name, key)` mapping lapses).
       */
      readonly idempotencyKeyTTL?: number;
    }
  | {
      readonly name: string;
      readonly version?: string;
      readonly workflowId: string;
      readonly input: unknown;
      /**
       * Skip the idempotency cache, and run a workflow whose stored run already
       * ended (completed, failed, cancelled, tripwire) as a fresh run instead of
       * answering with that outcome.
       */
      readonly force?: boolean;
      /** Optional namespace used for workflow creation and idempotency-key scoping. */
      readonly namespace?: string;
      readonly idempotencyKey?: string;
      readonly idempotencyKeyTTL?: number;
    };

/** Config for `createWorkflowRunner` / `DefaultWorkflowRunner`. */
export interface WorkflowRunnerConfig {
  /**
   * Storage backend that backs every workflow the runner executes. Required
   * — the runner is useless without it.
   */
  readonly storage: WorkflowStorage;
  /**
   * Optional workflow registry. Enables `run({ name, ... })` to resolve
   * definitions by name + version, and drives version-drain-resume on
   * resumes.
   */
  readonly registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  /**
   * Workflow-level lifecycle hooks. Fired on workflow / step boundaries.
   * Overrides any hooks carried by the workflow's own `_definition`, for
   * the run itself and for the child workflows it starts.
   */
  readonly hooks?: WorkflowHooks;
  /**
   * Pluggable step executor. When provided, step bodies run through this
   * executor instead of the default inline Eff execution. Use
   * `InProcessStepExecutor` for in-process execution with explicit
   * storage/clock wiring, or `StepQueueExecutor` for queue-backed dispatch.
   * Version-drained runs and child workflows run through it too (rebound
   * via `StepExecutor.forWorkflow` when it is definition-bound). Defaults
   * to the inline pipeline when omitted.
   */
  readonly stepExecutor?: StepExecutor;
  /**
   * Time source + scheduler. Drives all orchestration-level time math —
   * workflow deadline, step duration tracking, retry backoff, poll waits,
   * heartbeat cadence via `withLock`. Default: real system clock. Tests
   * pass a `FakeWallClock` to advance time deterministically.
   */
  readonly clock?: WallClock;
  /**
   * Identifier of whatever entity is running this runner — a Zorya
   * worker, an in-process app, a script, the scheduler-loop, etc.
   * Stamped onto every `StepAttemptRecord` so the dashboard can answer
   * "which executor handled this step?". Optional; leave undefined and
   * the field stays empty in the audit trail.
   */
  readonly executorId?: string;
}

/**
 * Runs a workflow end-to-end. Holds orchestration (DAG ready-set, lock,
 * heartbeat, retry, compensation, idempotency) and delegates step execution
 * to a `StepExecutor`.
 *
 * Two shapes:
 * - `run({ workflow, ... })` / `runSafe({ workflow, ... })` — drive a pure
 *   `Workflow` through the runner's configured storage.
 * - `run({ name, version?, ... })` — resolve via the configured registry.
 *
 * `start` returns a `WorkflowHandle` for fire-and-forget + polling.
 * `getStatus` reads the current state snapshot.
 */
export interface WorkflowRunner {
  /** Storage the runner writes workflow state to. */
  readonly storage: WorkflowStorage;
  /** Run a workflow and throw on failure. */
  run(params: WorkflowRunnerRunParams): Promise<unknown>;
  /** Run a workflow and return `{ data, error }` instead of throwing. */
  runSafe(
    params: WorkflowRunnerRunParams,
  ): Promise<{ data: unknown; error: null } | { data: null; error: WorkflowRunSafeError }>;
  /**
   * Fire-and-forget start that returns a `WorkflowHandle` for async inspection.
   * Honors the workflow's `idempotency.onInFlight` policy: `"reject"` throws
   * `WorkflowLockError` if a run is already active; `"join"` returns a handle
   * to the running workflow without starting a second execution.
   */
  start<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly input: Input;
  }): Promise<WorkflowHandle<Output>>;
  /**
   * Build a `WorkflowHandle` for a workflow that is *already running* — does
   * not enqueue or start anything. Useful when execution lives elsewhere
   * (a remote worker fleet, a separate coordinator process) and the caller
   * just wants to observe / signal / cancel a known `workflowId`.
   *
   * The returned handle is functionally identical to the one returned by
   * `start()`: same `status` / `signal` / `result` / `cancel` / `events`
   * surface, same polling/subscribe semantics under the hood.
   */
  handle<Output = unknown>(workflowId: string): WorkflowHandle<Output>;
  /**
   * Rewind a workflow to `fromStep` and continue executing. Resets that
   * step + everything downstream of it (transitively in the DAG) back to
   * pending; preserves all upstream completed step results so they are
   * not re-executed. Used as a debugging primitive for incident response:
   *
   *   "Step 47 failed because of a bad payload. Patch the payload, reset
   *    to step 47, and let the workflow continue from there."
   *
   * Requires the configured storage to implement `resetSteps`. Throws a
   * clear error if not. Throws `StepNotFoundError` if `fromStep` isn't
   * a step on the workflow's DAG.
   *
   * NOT a control-flow primitive — meant for one-off debugging /
   * recovery, not for normal application logic. For programmatic restart,
   * use `ctx.continueAsNew` (clean restart with fresh history) or
   * `runner.run` with `force: true` (full re-execute).
   */
  resume<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly fromStep: string;
  }): Promise<Output>;
  /**
   * Subscribe to live step/workflow-lifecycle events for a single run.
   * Returns an async iterable that yields every `WorkflowRunEvent` as it
   * happens and closes on the first terminal event
   * (`workflow-completed`, `workflow-failed`, `workflow-tripwire`) or when
   * the supplied `AbortSignal` fires.
   *
   * Works against every storage. When the configured storage implements
   * `subscribeToWorkflow` (e.g. InMemoryWorkflowStorage) the runner uses
   * the native push path; otherwise it
   * falls back to polling `loadWorkflow` on `pollIntervalMs` (default
   * 500ms) and synthesizing events from the step-state diff. User code
   * doesn't need to branch on the backend.
   */
  subscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent>;
  /**
   * Snapshot of a workflow's current status: active step, suspended reason,
   * per-step summary, timestamps. Returns `null` when the workflow doesn't
   * exist in storage. Intended for status endpoints / dashboards.
   */
  getStatus(
    workflowId: string,
    params?: { readonly includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<unknown> | null>;

  /**
   * Apply a `RecoveryStrategy` to the runner's storage — typically called once
   * at process startup to clean up stale runs and resume orphaned ones.
   *
   * Two phases, both optional (driven by the strategy):
   *
   * 1. **Terminate stale runs** (`cancelStale` / `failStale`): marks pending/
   *    running/suspended workflows whose `createdAt` is older than the
   *    configured threshold as `failed`. Uses a single bulk `UPDATE` when the
   *    storage supports `cancelStaleWorkflows` (e.g. `SqliteWorkflowStorage`);
   *    falls back to paginated per-row calls otherwise.
   *
   * 2. **Resume recent runs** (`resumeRecent`): paginates all remaining
   *    pending/running workflows (those not stale), looks up their definition
   *    in the runner's registry, and fires each one off as a non-blocking
   *    `runSafe` call. Requires the runner to have been configured with a
   *    `registry`; throws otherwise.
   *
   * @returns counts of terminated / resumed / skipped workflows.
   */
  recover(strategy: RecoveryStrategy): Promise<RecoveryResult>;
}

/**
 * Default implementation. Uses the configured storage + registry to drive
 * `runWorkflowOrchestration` from a pure `Workflow` definition.
 */
export class DefaultWorkflowRunner implements WorkflowRunner {
  readonly storage: WorkflowStorage;
  private readonly registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  private readonly hooks?: WorkflowHooks;
  private readonly stepExecutor?: StepExecutor;
  private readonly clock: WallClock;
  private readonly executorId?: string;

  constructor(config: WorkflowRunnerConfig) {
    this.storage = config.storage;
    this.registry = config.registry;
    this.hooks = config.hooks;
    this.stepExecutor = config.stepExecutor;
    this.clock = config.clock ?? SystemWallClock;
    if (config.executorId !== undefined) this.executorId = config.executorId;
  }

  async run(params: WorkflowRunnerRunParams): Promise<unknown> {
    const storage = this.storage;
    const { input, force, namespace, idempotencyKey, idempotencyKeyTTL } = params;

    // Per-call idempotency key: resolve to an existing workflowId before
    // dispatching. The supplied workflowId is the create-fallback when the
    // key is fresh; if it resolves, the caller's id is ignored. Key + TTL
    // flow into createWorkflow so a fresh create attaches the key
    // atomically — the partial-unique index resolves any concurrent-create
    // race by returning the canonical row in the conflict path.
    const workflowName = "workflow" in params ? params.workflow.name : params.name;
    const idempotencyExpiresAt =
      idempotencyKey && idempotencyKeyTTL !== undefined
        ? new Date(this.clock.currentTimeMs() + idempotencyKeyTTL)
        : undefined;
    let workflowId = params.workflowId;
    if (idempotencyKey) {
      if (idempotencyKeyTTL === undefined) {
        throw new Error(
          `WorkflowRunner.run: \`idempotencyKey\` requires \`idempotencyKeyTTL\`. ` +
            `Pass a TTL in milliseconds — there is no default.`,
        );
      }
      const hit = await storage.findWorkflowByIdempotencyKey({
        workflowName,
        ...(namespace !== undefined && { namespace }),
        idempotencyKey,
        now: this.clock.now(),
      });
      if (hit) workflowId = hit.workflowId;
    }

    // Name-based runs resolve through the registry, including
    // version-drain-resume onto the definition the run was created with.
    const workflow =
      "workflow" in params
        ? params.workflow
        : await resolveRunDefinition({
            registry: this.registry,
            storage,
            name: params.name,
            version: params.version,
            workflowId,
          });

    return this._runWorkflow({
      workflow,
      storage,
      workflowId,
      input,
      force,
      ...(namespace !== undefined && { namespace }),
      ...(idempotencyKey && idempotencyExpiresAt ? { idempotencyKey, idempotencyExpiresAt } : {}),
    });
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
    const storage = this.storage;
    const { workflow, workflowId, input } = params;

    const existing = await storage.loadWorkflow(workflowId);
    const isRunning =
      existing?.status === "pending" ||
      existing?.status === "running" ||
      existing?.status === "suspended";
    const onInFlight = workflow.idempotency?.onInFlight ?? "reject";

    if (isRunning) {
      if (onInFlight === "reject") {
        throw new WorkflowLockError({
          workflowId,
          message: `Workflow "${workflowId}" is already running`,
        });
      }
      // "join" — caller receives a handle that polls the existing run.
    } else {
      // Fire-and-forget. Failures are recorded in storage (and the DLQ /
      // hooks if configured) so we intentionally swallow the rejection
      // here to avoid unhandled-rejection warnings on the start path.
      void this.runSafe({ workflow, workflowId, input });
      await yieldToEventLoop();
    }

    return this.handle<Output>(workflowId);
  }

  async resume<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly fromStep: string;
  }): Promise<Output> {
    const { workflow, workflowId, fromStep } = params;
    const storage = this.storage;

    if (typeof storage.resetSteps !== "function") {
      throw new Error(
        `WorkflowRunner.resume requires storage that implements resetSteps. ` +
          `Got ${storage.constructor.name}.`,
      );
    }

    const state = await storage.loadWorkflow(workflowId);
    if (!state) {
      throw new Error(`Cannot resume workflow "${workflowId}" — not found in storage.`);
    }

    // Validate fromStep exists on the DAG. Step-name lookup is on the
    // workflow's _definition (storage doesn't know topology).
    const def = workflow._definition;
    const fromStepDef = def.steps.find((s) => s.name === fromStep);
    if (!fromStepDef) {
      const known = def.steps.map((s) => s.name).join(", ");
      throw new Error(
        `Cannot resume "${workflowId}" — step "${fromStep}" not found on workflow ` +
          `"${workflow.name}". Known steps: ${known}.`,
      );
    }

    // Walk the DAG to compute the downstream set: every step whose
    // `dependsOn` reaches fromStep transitively. Reset the union of
    // {fromStep, downstream} so the runner re-executes from there.
    const downstream = new Set<string>([fromStep]);
    let grew = true;
    while (grew) {
      grew = false;
      for (const step of def.steps) {
        if (downstream.has(step.name)) continue;
        if (step.dependsOn.some((dep) => downstream.has(dep))) {
          downstream.add(step.name);
          grew = true;
        }
      }
    }

    await storage.resetSteps(workflowId, [...downstream]);

    // Re-run with `force: true` so idempotency caching doesn't
    // short-circuit "already completed" — we just reverted the terminal
    // status so it shouldn't trip, but `force` makes that explicit.
    return (await this.run({
      workflow,
      workflowId,
      input: state.input as Input,
      force: true,
    })) as Output;
  }

  handle<Output = unknown>(workflowId: string): WorkflowHandle<Output> {
    return createWorkflowHandle<Output>({
      workflowId,
      storage: this.storage,
      clock: this.clock,
      getStatus: (p) => this.getStatus(workflowId, p),
      subscribe: (opts) => this.subscribe(workflowId, opts),
    });
  }

  subscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent> {
    // Fast path: storage has native push support.
    if (isSubscribableStorage(this.storage)) {
      return this.storage.subscribeToWorkflow(workflowId, options);
    }
    // Fallback: poll loadWorkflow, diff step-state map, synthesize events.
    // Works against any storage so user code doesn't have to branch on the
    // backend. Default cadence 500ms is a reasonable tradeoff between
    // perceived latency and read load — callers can dial it via
    // `pollIntervalMs`.
    return pollWorkflowEvents({ storage: this.storage, clock: this.clock, workflowId, options });
  }

  async getStatus(
    workflowId: string,
    params?: { readonly includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<unknown> | null> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) return null;
    return toStatusInfo({ state, includeStepResults: params?.includeStepResults ?? false });
  }

  recover(strategy: RecoveryStrategy): Promise<RecoveryResult> {
    return recoverWorkflows({
      strategy,
      storage: this.storage,
      registry: this.registry,
      clock: this.clock,
      resume: (run) => void this.runSafe(run),
    });
  }

  private _runWorkflow(params: {
    workflow: Workflow<unknown, unknown>;
    storage: WorkflowStorage;
    workflowId: string;
    input: unknown;
    force?: boolean;
    namespace?: string;
    idempotencyKey?: string;
    idempotencyExpiresAt?: Date;
  }): Promise<unknown> {
    const ctx = orchestrationContextFor({
      workflow: params.workflow,
      runtime: {
        storage: params.storage,
        clock: this.clock,
        ...(this.stepExecutor !== undefined && { stepExecutor: this.stepExecutor }),
        ...(this.executorId !== undefined && { executorId: this.executorId }),
        ...(this.hooks !== undefined && { hooks: this.hooks }),
      },
    });
    return runWorkflowOrchestration(ctx, {
      workflowId: params.workflowId,
      input: params.input,
      force: params.force,
      namespace: params.namespace,
      ...(params.idempotencyKey && params.idempotencyExpiresAt
        ? {
            idempotencyKey: params.idempotencyKey,
            idempotencyExpiresAt: params.idempotencyExpiresAt,
          }
        : {}),
    });
  }
}

/**
 * Convenience factory — `createWorkflowRunner(config)` mirrors how other
 * promin components construct their default implementation.
 */
export function createWorkflowRunner(config: WorkflowRunnerConfig): WorkflowRunner {
  return new DefaultWorkflowRunner(config);
}

/**
 * Yield one macrotask so a just-launched fire-and-forget run gets going
 * before `start()` returns its handle. An event-loop yield, not time math —
 * no delay is measured, so it does not go through the WallClock (a
 * `FakeWallClock` would never fire it).
 */
function yieldToEventLoop(): Promise<void> {
  return new Promise((r) => setTimeout(r, 0));
}
