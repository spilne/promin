// ---------------------------------------------------------------------------
// WorkflowRunner — orchestration engine.
//
// Owns run-loop orchestration: lock / heartbeat / state CRUD, ready-set
// computation, workflow-level retry, compensation, version drain,
// idempotency TTL, DLQ publish, journaled-step replay, suspend / resume,
// deadlines. Step bodies themselves run through a pluggable `StepExecutor`
// so in-process TS handlers, remote gRPC workers, and queue-dispatched
// workers can share one orchestration codepath (promin-e0hd phase 2
// fills that seam in).
// ---------------------------------------------------------------------------

import { Effect } from "effect";
import { Pipeline, type Sinkable, type TaggedError } from "@promin/core";
import { SystemClock, type Clock } from "@promin/core";
import type {
  Workflow,
  CompensateConfig,
  DispatchConfig,
  StepDefinition,
  WorkflowHandle,
  WorkflowStatusInfo,
} from "./durable-pipeline.ts";
import type { WorkflowHooks, IdempotencyConfig } from "./durable-pipeline.ts";
import {
  isStepAttemptStorage,
  isSubscribableStorage,
  isTripwireCapableStorage,
  type WorkflowStorage,
  type FenceGuard,
} from "./workflow-storage.ts";
import { createWorkflowEventStream } from "./workflow-event-stream.ts";
import type { StepState } from "./workflow-state.ts";
import { computeReadySet, type DagNode } from "./workflow-dag.ts";
import type { FailedWorkflowRecord, WorkflowState, WorkflowRunEvent } from "./workflow-state.ts";
import {
  StepError,
  WorkflowError,
  WorkflowDeadlineError,
  WorkflowVersionMismatchError,
  StepTimeoutError,
  WorkflowLockError,
  WorkflowTripwireError,
  TripwireStorageMissingError,
} from "./durable-pipeline-error.ts";
import { withLock } from "./with-lock.ts";
import { topologicalSort } from "./workflow-dag.ts";
import type { RetryPolicy } from "@promin/core";
import {
  WorkflowContinueAsNewError,
  type WorkflowSuspendedError,
  type WorkflowTimeoutError,
} from "./durable-pipeline-error.ts";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "./workflow-version-registry.ts";

// ---------------------------------------------------------------------------
// StepExecutor — pluggable "how to run a single step body" boundary.
//
// Not yet threaded through the runner in phase 1 — defined here so downstream
// modules (distributed worker, future gRPC executor) can code against the
// shape, and phase 2 has a fixed surface to extract onto. The shape
// intentionally stays request/response; streaming (logs, backpressure) lands
// in phase 4 once promin-m7c's gRPC semantics are concrete.
// ---------------------------------------------------------------------------

export interface StepExecutionRequest {
  readonly workflowId: string;
  readonly stepName: string;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  /** Capabilities the step requires — forwarded to capability-aware workers. */
  readonly needs?: readonly string[];
  readonly priority?: number;
  readonly version?: string;
}

export type StepExecutionResult =
  | {
      readonly ok: true;
      readonly result: unknown;
      readonly metadata?: Record<string, unknown>;
      /** When true, the executor already persisted this step — runner skips saveStepResult. */
      readonly storageAlreadyCheckpointed?: boolean;
    }
  | { readonly ok: false; readonly error: string };

export interface StepExecutor {
  /**
   * Run a single step body and return its terminal result. Implementations
   * are responsible for applying step-level retry (since the policy is owned
   * by the step definition), per-step timeout, and onFailure strategy —
   * the runner only decides *when* to invoke `executeStep` based on the DAG
   * ready-set, workflow-level retry, compensation, etc.
   *
   * Errors should be surfaced as `{ ok: false, error }` rather than thrown,
   * so the runner's state machine can branch uniformly across in-process
   * and remote executors.
   */
  executeStep(req: StepExecutionRequest): Promise<StepExecutionResult>;
}

// ---------------------------------------------------------------------------
// WorkflowRunner — orchestration engine.
// ---------------------------------------------------------------------------

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
      readonly force?: boolean;
    }
  | {
      readonly name: string;
      readonly version?: string;
      readonly workflowId: string;
      readonly input: unknown;
      readonly force?: boolean;
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
   * Overrides any hooks carried by the workflow's own `_definition`.
   */
  readonly hooks?: WorkflowHooks;
  /**
   * Pluggable step executor. When provided, step bodies run through this
   * executor instead of the default inline Effect pipeline. Use
   * `InProcessStepExecutor` for in-process execution with explicit
   * storage/clock wiring, or `StepQueueExecutor` for queue-backed dispatch.
   * Defaults to the inline pipeline when omitted.
   */
  readonly stepExecutor?: StepExecutor;
  /**
   * Time source + scheduler. Drives all orchestration-level time math —
   * workflow deadline, step duration tracking, retry backoff, poll waits,
   * heartbeat cadence via `withLock`. Default: real system clock. Tests
   * pass a `FakeClock` to advance time deterministically.
   */
  readonly clock?: Clock;
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
   * Subscribe to live step/workflow-lifecycle events for a single run.
   * Returns an async iterable that yields every `WorkflowRunEvent` as it
   * happens and closes on the first terminal event
   * (`workflow-completed`, `workflow-failed`, `workflow-tripwire`) or when
   * the supplied `AbortSignal` fires.
   *
   * Works against every storage. When the configured storage implements
   * `subscribeToWorkflow` (InMemoryWorkflowStorage today; Postgres via
   * `pg_notify` later) the runner uses the native push path; otherwise it
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
}

/**
 * Default implementation. Uses the configured storage + registry to drive
 * `runWorkflowOrchestration` from a pure `Workflow` definition.
 *
 * Intentionally a class (not a bare function) so the runner can add
 * step-executor wiring, observability hooks, and tracing context without
 * breaking callers.
 */
export class DefaultWorkflowRunner implements WorkflowRunner {
  readonly storage: WorkflowStorage;
  private readonly registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  private readonly hooks?: WorkflowHooks;
  private readonly stepExecutor?: StepExecutor;
  private readonly clock: Clock;

  constructor(config: WorkflowRunnerConfig) {
    this.storage = config.storage;
    this.registry = config.registry;
    this.hooks = config.hooks;
    this.stepExecutor = config.stepExecutor;
    this.clock = config.clock ?? SystemClock;
  }

  async run(params: WorkflowRunnerRunParams): Promise<unknown> {
    const storage = this.storage;
    const { workflowId, input, force } = params;

    if ("workflow" in params) {
      return this._runWorkflow({ workflow: params.workflow, storage, workflowId, input, force });
    }

    // Name-based — resolve via registry, implement version-drain-resume:
    // if an existing row is stored under a different version, use the
    // matching older definition to continue the run.
    const registry = this.registry;
    if (!registry) {
      throw new Error(
        `WorkflowRunner.run({ name }) requires a registry on the runner config. ` +
          `Pass \`createWorkflowRunner({ storage, registry })\`.`,
      );
    }
    const latestDef = await registry.resolve(params.name, params.version);
    if (!latestDef) {
      const allNames = await registry.names();
      throw new Error(
        `No workflow "${params.name}"${params.version ? ` version "${params.version}"` : ""} in registry. ` +
          `Registered: ${(allNames as string[]).join(", ") || "(none)"}.`,
      );
    }

    const existing = await storage.loadWorkflow(workflowId);
    if (existing && existing.version && existing.version !== latestDef.version) {
      const storedDef = await registry.resolve(params.name, existing.version);
      if (!storedDef) {
        const allVersions = await registry.versions(params.name);
        throw new Error(
          `Workflow "${params.name}" version "${existing.version}" not found in registry. ` +
            `Available versions: ${(allVersions as string[]).join(", ")}. ` +
            `Keep old definitions registered until in-flight workflows drain.`,
        );
      }
      return this._runWorkflow({ workflow: storedDef, storage, workflowId, input, force });
    }

    return this._runWorkflow({ workflow: latestDef, storage, workflowId, input, force });
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
      await new Promise((r) => setTimeout(r, 0));
    }

    return this.handle<Output>(workflowId);
  }

  handle<Output = unknown>(workflowId: string): WorkflowHandle<Output> {
    const storage = this.storage;
    const clock = this.clock;
    const self = this;
    return {
      workflowId,
      status: (p) => self.getStatus(workflowId, p) as Promise<WorkflowStatusInfo<Output> | null>,
      signal: (signalName, payload) => storage.deliverSignal(workflowId, signalName, payload),
      cancel: (_reason) => {
        // `reason` is reserved for future use — when storage.cancelWorkflow
        // grows a reason field it'll thread through here without breaking
        // existing callers. Today we just invoke cancellation; the caller's
        // reason is observable via their own logs / audit trail.
        return storage.cancelWorkflow(workflowId);
      },
      events: (opts) => self.subscribe(workflowId, opts),
      result: async (p) => {
        const intervalMs = p?.intervalMs ?? 1_000;
        const timeoutMs = p?.timeoutMs ?? 60_000;
        const deadline = clock.currentTimeMs() + timeoutMs;

        while (clock.currentTimeMs() < deadline) {
          const state = await storage.loadWorkflow(workflowId);
          if (state?.status === "completed") return state.result as Output;
          if (state?.status === "failed") {
            throw new Error(state.error ?? `Workflow ${workflowId} failed`);
          }
          if (state?.status === "tripwire") {
            // Look up which step fired. Tripwire steps record
            // `metadata.tripwireFired = true` on their step row at save
            // time; find that row to report the step name in the error.
            const firedStep = Object.values(state.steps).find(
              (s) =>
                s.metadata !== undefined &&
                (s.metadata as { tripwireFired?: boolean }).tripwireFired === true,
            );
            throw new WorkflowTripwireError({
              workflowId,
              stepName: firedStep?.stepName ?? "unknown",
              reason: state.tripwire,
              message: `Workflow "${workflowId}" ended via tripwire`,
            });
          }
          await new Promise((r) => clock.setTimeout(() => r(undefined), intervalMs));
        }
        throw new Error(`Workflow ${workflowId} did not complete within ${timeoutMs}ms`);
      },
    };
  }

  subscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent> {
    // Fast path: storage has native push support (EventBus / pg_notify).
    if (isSubscribableStorage(this.storage)) {
      return this.storage.subscribeToWorkflow(workflowId, options);
    }
    // Fallback: poll loadWorkflow, diff step-state map, synthesize events.
    // Works against any storage so user code doesn't have to branch on the
    // backend. Default cadence 500ms is a reasonable tradeoff between
    // perceived latency and read load — callers can dial it via
    // `pollIntervalMs`.
    return this._pollSubscribe(workflowId, options);
  }

  private _pollSubscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent> {
    const storage = this.storage;
    const clock = this.clock;
    const pollMs = options?.pollIntervalMs ?? 500;

    return createWorkflowEventStream((producer) => {
      let stopped = false;
      let prevSteps: Record<string, StepState> = {};
      let prevStatus: string | null = null;

      const stop = (): void => {
        stopped = true;
        producer.end();
      };
      const onAbort = (): void => stop();
      options?.signal?.addEventListener("abort", onAbort, { once: true });

      const tick = async (): Promise<void> => {
        while (!stopped && !producer.done) {
          let state;
          try {
            state = await storage.loadWorkflow(workflowId);
          } catch {
            // Transient storage error — keep polling; the workflow may still
            // materialize. Swallowing here keeps the stream alive in face
            // of network blips on remote storages.
            await new Promise<void>((r) => clock.setTimeout(() => r(), pollMs));
            continue;
          }
          if (state) {
            // Emit step transitions vs the last observed snapshot. Using
            // completedAt as the event timestamp so the ordering is stable
            // across polls; falls back to now when a storage omits it.
            for (const [stepName, step] of Object.entries(state.steps)) {
              const before = prevSteps[stepName];
              if (step.status === "completed" && (!before || before.status !== "completed")) {
                producer.push({
                  type: "step-completed",
                  stepName,
                  result: step.result,
                  durationMs: step.durationMs ?? 0,
                  at: step.completedAt ?? clock.now(),
                });
              } else if (step.status === "failed" && (!before || before.status !== "failed")) {
                producer.push({
                  type: "step-failed",
                  stepName,
                  error: step.error ?? "",
                  at: step.completedAt ?? clock.now(),
                });
              }
            }
            // Workflow-terminal transitions close the stream.
            if (state.status !== prevStatus) {
              if (state.status === "completed") {
                producer.push({
                  type: "workflow-completed",
                  result: state.result,
                  at: state.completedAt ?? clock.now(),
                });
                stop();
                return;
              } else if (state.status === "failed") {
                producer.push({
                  type: "workflow-failed",
                  error: state.error ?? "",
                  at: state.completedAt ?? clock.now(),
                });
                stop();
                return;
              } else if (state.status === "tripwire") {
                const fired = Object.values(state.steps).find(
                  (s) =>
                    (s.metadata as { tripwireFired?: boolean } | undefined)?.tripwireFired === true,
                );
                producer.push({
                  type: "workflow-tripwire",
                  stepName: fired?.stepName ?? "unknown",
                  reason: state.tripwire,
                  at: state.completedAt ?? clock.now(),
                });
                stop();
                return;
              }
            }
            prevSteps = state.steps;
            prevStatus = state.status;
          }
          await new Promise<void>((r) => clock.setTimeout(() => r(), pollMs));
        }
      };
      void tick();

      return () => {
        stopped = true;
        options?.signal?.removeEventListener("abort", onAbort);
      };
    });
  }

  async getStatus(
    workflowId: string,
    params?: { readonly includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<unknown> | null> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) return null;

    const includeResults = params?.includeStepResults ?? false;

    let currentStep: string | undefined;
    let suspendedReason: "sleeping" | "waiting_for_signal" | undefined;

    for (const [name, step] of Object.entries(state.steps)) {
      if (step.status === "running" || step.status === "pending") {
        currentStep = currentStep ?? name;
      }
      if (step.status === "sleeping") {
        currentStep = name;
        suspendedReason = "sleeping";
      }
      if (step.status === "waiting_for_signal") {
        currentStep = name;
        suspendedReason = "waiting_for_signal";
      }
    }

    const steps: Record<string, { status: string; result?: unknown }> = {};
    for (const [name, step] of Object.entries(state.steps)) {
      steps[name] = includeResults
        ? { status: step.status, result: step.result }
        : { status: step.status };
    }

    return {
      state: state.status === "compensating" ? "failed" : state.status,
      result: state.status === "completed" ? state.result : undefined,
      error: state.error,
      tripwire: state.status === "tripwire" ? state.tripwire : undefined,
      currentStep,
      suspendedReason,
      steps,
      createdAt: state.createdAt,
      startedAt: state.startedAt,
      updatedAt: state.updatedAt,
    };
  }

  private _runWorkflow(params: {
    workflow: Workflow<unknown, unknown>;
    storage: WorkflowStorage;
    workflowId: string;
    input: unknown;
    force?: boolean;
  }): Promise<unknown> {
    const def = params.workflow._definition;
    const ctx: WorkflowOrchestrationContext = {
      storage: params.storage,
      name: params.workflow.name,
      version: params.workflow.version,
      idempotency: params.workflow.idempotency,
      type: def.type,
      metadata: def.metadata,
      steps: def.steps,
      retry: def.retry,
      compensateConfig: def.compensateConfig,
      dlq: def.dlq,
      dispatch: def.dispatch,
      timeoutMs: def.timeoutMs,
      onVersionMismatch: def.onVersionMismatch,
      previousVersions: def.previousVersions,
      hooks: this.hooks ?? def.hooks,
      stepExecutor: this.stepExecutor,
      clock: this.clock,
    };
    return runWorkflowOrchestration(ctx, {
      workflowId: params.workflowId,
      input: params.input,
      force: params.force,
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

// ---------------------------------------------------------------------------
// Orchestration helpers — pure, context-taking versions of the utilities
// that used to be private methods on WorkflowBuilder. They live here so
// phase 1b/1c can incrementally move orchestration out of the builder
// without the runner needing access to private class state.
// ---------------------------------------------------------------------------

/**
 * Full workflow-runtime state the orchestration loop needs. A superset
 * of DagExecutionContext — adds the lock / retry / compensation / DLQ
 * / idempotency / version knobs that live outside the DAG executor.
 * Built once per `run()` from the bound workflow's builder state.
 */
export interface WorkflowOrchestrationContext {
  readonly storage: WorkflowStorage;
  readonly name: string;
  readonly version?: string;
  readonly type?: string;
  readonly metadata?: Record<string, unknown>;
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly retry?: RetryPolicy<TaggedError>;
  readonly compensateConfig?: CompensateConfig;
  readonly dlq?: Sinkable<FailedWorkflowRecord>;
  readonly dispatch?: DispatchConfig;
  readonly idempotency?: IdempotencyConfig;
  readonly timeoutMs?: number;
  readonly onVersionMismatch: "strict" | "drain";
  readonly previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  readonly hooks?: WorkflowHooks;
  /**
   * Pluggable step executor. Threaded through to `DagExecutionContext` so
   * the DAG loop delegates step bodies to the configured executor.
   */
  readonly stepExecutor?: StepExecutor;
  /**
   * Time source. Drives workflow start/deadline math, idempotency TTL
   * comparisons, step duration tracking, retry/compensation backoff sleeps,
   * and the `withLock` heartbeat interval. Defaults to `SystemClock` when
   * omitted — callers building contexts by hand should only override it
   * for tests.
   */
  readonly clock?: Clock;
}

/** Default lock TTL. Re-declared here for the runner's own withLock call. */
/**
 * Default lock extension for orchestration runs. Matches `withLock`'s
 * `DEFAULT_LOCK_EXTENSION_MS` so a 30s heartbeat keeps the lock healthy
 * with a ~90s grace window for transient network hiccups.
 */
const DEFAULT_LOCK_DURATION_MS = 120_000;

/**
 * Run a workflow end-to-end. Orchestrates version-drain pre-check,
 * idempotency TTL, lock acquisition + heartbeat, workflow-level retry
 * around executeWorkflowDag, compensation cascade on exhausted retries,
 * DLQ publish on failure. Hooks fire at every natural boundary
 * (onWorkflowComplete / onWorkflowFailure / onStep*).
 *
 * Formerly `WorkflowBuilder.run`. Pure function now — takes the full
 * orchestration context directly so the same loop can be driven by
 * remote storage, a non-TS submitter process, or future executor
 * flavors (phase 2 step-queue, phase 3 gRPC).
 */
export async function runWorkflowOrchestration(
  ctx: WorkflowOrchestrationContext,
  params: { workflowId: string; input: unknown; force?: boolean },
): Promise<unknown> {
  // Continue-as-new wrapper: catch WorkflowContinueAsNewError thrown out
  // of withLock, archive the current run via startFreshRun, then re-run
  // under the same workflowId with the carried input. Hard cap at 1024
  // chained continue-as-new calls to catch infinite loops in user code
  // (matches Temporal's safety belt).
  let currentInput = params.input;
  for (let chain = 0; chain < 1024; chain++) {
    try {
      return await runOneOrchestrationCycle(ctx, {
        workflowId: params.workflowId,
        input: currentInput,
        force: chain > 0 ? true : params.force,
      });
    } catch (err) {
      if (err instanceof WorkflowContinueAsNewError) {
        await ctx.storage.startFreshRun(params.workflowId);
        currentInput = err.nextInput;
        continue;
      }
      throw err;
    }
  }
  throw new Error(
    `Workflow "${params.workflowId}" exceeded continue-as-new chain limit (1024). ` +
      `Likely an infinite continue-as-new loop in the workflow body.`,
  );
}

async function runOneOrchestrationCycle(
  ctx: WorkflowOrchestrationContext,
  params: { workflowId: string; input: unknown; force?: boolean },
): Promise<unknown> {
  const { workflowId, input, force } = params;
  const clock = ctx.clock ?? SystemClock;
  const workflowStartTime = clock.currentTimeMs();
  const compensateTrigger = ctx.compensateConfig?.trigger ?? "after-retries";
  const maxWorkflowRetries = compensateTrigger === "immediate" ? 0 : (ctx.retry?.maxRetries ?? 0);
  const workflowRetryDelayMs = ctx.retry?.baseDelayMs ?? 1000;
  const idempotency = force ? undefined : ctx.idempotency;

  // Drain pre-check — if the stored workflow was created under a different
  // version and this definition has `onVersionMismatch: "drain"`, delegate
  // the whole run to the matching previousVersion definition. Stored
  // version is immutable per workflow, so this is race-safe.
  if (ctx.onVersionMismatch === "drain" && ctx.version) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    if (existing && existing.version !== ctx.version) {
      const previousDef = ctx.previousVersions?.find((d) => d.version === existing.version);
      if (!previousDef) {
        throw new WorkflowVersionMismatchError({
          workflowId,
          expected: ctx.version,
          actual: existing.version ?? "(none)",
          message:
            `Workflow "${workflowId}" was created with version "${existing.version ?? "(none)"}" ` +
            `but current code is version "${ctx.version}". ` +
            `onVersionMismatch is "drain" but no matching previousVersion was registered.`,
        });
      }
      // Delegate drain to the previous version by building its own
      // orchestration context — both versions share this workflow's
      // storage so the stored state keeps one source of truth.
      const prevDef = previousDef._definition;
      const prevCtx: WorkflowOrchestrationContext = {
        storage: ctx.storage,
        name: previousDef.name,
        version: previousDef.version,
        idempotency: previousDef.idempotency,
        type: prevDef.type,
        metadata: prevDef.metadata,
        steps: prevDef.steps,
        retry: prevDef.retry,
        compensateConfig: prevDef.compensateConfig,
        dlq: prevDef.dlq,
        dispatch: prevDef.dispatch,
        timeoutMs: prevDef.timeoutMs,
        onVersionMismatch: prevDef.onVersionMismatch,
        previousVersions: prevDef.previousVersions,
        hooks: ctx.hooks ?? prevDef.hooks,
        clock,
      };
      return runWorkflowOrchestration(prevCtx, { workflowId, input, force });
    }
  }

  // 0. Idempotency check — return cached result if within TTL
  if (idempotency) {
    const existing = await ctx.storage.loadWorkflow(workflowId);
    if (existing?.completedAt) {
      const elapsed = clock.currentTimeMs() - existing.completedAt.getTime();
      const ttl = getIdempotencyTtl(idempotency, existing.status);
      if (ttl !== undefined && elapsed < ttl) {
        if (existing.status === "completed") return existing.result;
        if (existing.status === "failed")
          throw new WorkflowError({
            workflowId,
            message: existing.error ?? `Workflow "${workflowId}" failed (cached, TTL ${ttl}ms)`,
          });
      }
    }
  }

  // 1. Acquire lock with heartbeat — keeps lock alive during long steps
  return withLock({
    storage: ctx.storage,
    workflowId,
    options: { lockDurationMs: DEFAULT_LOCK_DURATION_MS, clock },
    fn: async ({ fenceToken }) => {
      const guard = fenceToken ? { fenceToken } : undefined;
      // 2. Load or create workflow state
      let state = await ctx.storage.loadWorkflow(workflowId);

      // Double-check idempotency after lock — prevents race
      if (idempotency && state?.completedAt) {
        const elapsed = clock.currentTimeMs() - state.completedAt.getTime();
        const ttl = getIdempotencyTtl(idempotency, state.status);
        if (ttl !== undefined && elapsed < ttl) {
          if (state.status === "completed") return state.result;
          if (state.status === "failed")
            throw new WorkflowError({
              workflowId,
              message: state.error ?? `Workflow "${workflowId}" failed (cached)`,
            });
        }
        // TTL expired — check if we should start a fresh run
        const onExpiry = idempotency.onExpiry ?? "fresh-run";
        if (
          onExpiry === "fresh-run" &&
          (state.status === "completed" || state.status === "failed")
        ) {
          await ctx.storage.startFreshRun(workflowId);
          state = await ctx.storage.loadWorkflow(workflowId);
        }
      }

      if (!state) {
        const createResult = await ctx.storage.createWorkflow({
          workflowId,
          workflowName: ctx.name,
          input,
          workflowType: ctx.type,
          metadata: ctx.metadata,
          version: ctx.version,
        });
        if (!createResult.created) {
          // Race: another caller created the workflow between our load and create
          state = createResult.existing;
        } else {
          state = await ctx.storage.loadWorkflow(workflowId);
        }
      } else if (ctx.version) {
        // Version mismatch check — only when builder explicitly sets a version
        const storedVersion = state.version;
        if (storedVersion !== ctx.version) {
          throw new WorkflowVersionMismatchError({
            workflowId,
            expected: ctx.version,
            actual: storedVersion ?? "(none)",
            message:
              `Workflow "${workflowId}" was created with version "${storedVersion ?? "(none)"}" ` +
              `but current code is version "${ctx.version}". ` +
              `To resume this workflow, either use \`onVersionMismatch: "drain"\` + ` +
              `\`previousVersions: [v${storedVersion ?? "N"}]\` on the workflow config, or ` +
              `register both versions in a WorkflowVersionRegistry.`,
          });
        }
      }

      // 3. Validate DAG
      const dagNodes: DagNode[] = ctx.steps.map((s) => ({
        name: s.name,
        dependsOn: s.dependsOn,
      }));
      topologicalSort({ nodes: dagNodes, workflowId });

      // 4. Execute DAG with workflow-level retry
      let lastStepError: unknown = null;
      // Shared across workflow retries so attempt counters keep incrementing
      const stepAttempts = new Map<string, number>();

      const dagCtx: DagExecutionContext = {
        storage: ctx.storage,
        steps: ctx.steps,
        hooks: ctx.hooks,
        timeoutMs: ctx.timeoutMs,
        dispatch: ctx.dispatch,
        stepExecutor: ctx.stepExecutor,
        guard,
        clock,
      };

      for (let workflowAttempt = 0; workflowAttempt <= maxWorkflowRetries; workflowAttempt++) {
        // On retry, wait before re-attempting
        if (workflowAttempt > 0) {
          const delay = workflowRetryDelayMs * Math.pow(2, workflowAttempt - 1);
          await new Promise((r) => clock.setTimeout(() => r(undefined), delay));
        }

        const dagResult = await executeWorkflowDag(dagCtx, {
          workflowId,
          input,
          dagNodes,
          state,
          workflowStartTime,
          stepAttempts,
          deadlineMs: ctx.timeoutMs != null ? workflowStartTime + ctx.timeoutMs : undefined,
        });

        if (dagResult.success) {
          // 5. Complete workflow
          const finalResult = dagResult.result;
          await ctx.storage.completeWorkflow(workflowId, finalResult, guard);
          await ctx.hooks?.onWorkflowComplete?.({
            workflowId,
            result: finalResult,
            durationMs: clock.currentTimeMs() - workflowStartTime,
          });
          return finalResult;
        }

        // Tripwire — intentional early exit. Skip compensation + DLQ since
        // this is not a failure. Mark the workflow with `status: "tripwire"`
        // and throw a typed error carrying the reason so callers using
        // `run()` can `instanceof`-check it; `runSafe()` surfaces it as
        // `{ data: null, error }`.
        if ("tripwire" in dagResult) {
          const stepName = dagResult.stepName;
          const reason = dagResult.reason;
          if (!isTripwireCapableStorage(ctx.storage)) {
            throw new TripwireStorageMissingError({
              workflowId,
              stepName,
              message:
                `Tripwire step "${stepName}" fired but the configured ` +
                `WorkflowStorage does not implement tripwireWorkflow. Use a ` +
                `storage backend that supports tripwire (InMemory, Postgres) ` +
                `or remove the .tripwire() step.`,
            });
          }
          await ctx.storage.tripwireWorkflow(workflowId, reason, guard);
          await ctx.hooks?.onWorkflowTripwire?.({
            workflowId,
            stepName,
            reason,
            durationMs: clock.currentTimeMs() - workflowStartTime,
          });
          throw new WorkflowTripwireError({
            workflowId,
            stepName,
            reason,
            message: `Workflow "${workflowId}" ended via tripwire at step "${stepName}"`,
          });
        }

        // DAG failed — suspension errors always propagate immediately.
        // Continue-as-new is also a clean unwind (no compensation, no
        // failure recording) — throw it out of withLock so the lock is
        // released by withLock's finally, then the outer wrapper catches
        // it and recurses with the carried input under the same workflowId.
        if (dagResult.suspension) {
          throw dagResult.error;
        }
        if ("continueAsNew" in dagResult && dagResult.continueAsNew) {
          throw dagResult.error;
        }

        lastStepError = dagResult.error;

        // Check if this error is retryable (workflow-level `when` predicate)
        const shouldRetry =
          workflowAttempt < maxWorkflowRetries &&
          (!ctx.retry?.when || ctx.retry.when(dagResult.error as TaggedError));

        if (shouldRetry) {
          // Reload state to pick up checkpointed steps
          state = await ctx.storage.loadWorkflow(workflowId);
        } else {
          // Not retryable or retries exhausted — break to compensation
          break;
        }
      }

      // All workflow retries exhausted — run compensation cascade
      const compensationReport = await compensateWorkflow({
        storage: ctx.storage,
        steps: ctx.steps,
        compensateConfig: ctx.compensateConfig,
        workflowId,
        input,
        dagNodes,
        guard,
        clock,
      });

      // Fire workflow-level onComplete callback
      if (ctx.compensateConfig?.onComplete) {
        try {
          const result = ctx.compensateConfig.onComplete({
            input,
            error: lastStepError,
            compensatedSteps: compensationReport.compensated,
            failedCompensations: compensationReport.failed,
          });
          if (result instanceof Pipeline) {
            await result.runPromise();
          } else if (result && typeof (result as Promise<void>).then === "function") {
            await result;
          }
        } catch {
          // onComplete failure is swallowed — the original error is more important
        }
      }

      // Fail the workflow
      const errorMsg =
        lastStepError instanceof globalThis.Error ? lastStepError.message : String(lastStepError);
      await ctx.storage.failWorkflow(workflowId, errorMsg, guard);
      await ctx.hooks?.onWorkflowFailure?.({
        workflowId,
        error: errorMsg,
        durationMs: clock.currentTimeMs() - workflowStartTime,
      });

      // Publish to DLQ
      if (ctx.dlq) {
        await publishDlqRecord({
          dlq: ctx.dlq,
          storage: ctx.storage,
          workflowId,
          workflowName: ctx.name,
          input,
          errorMsg,
          clock,
          compensationReport,
          metadata: ctx.metadata,
        });
      }

      throw lastStepError;
    },
  });
}

/**
 * Slice of the workflow-runtime state the DAG executor needs. A strict
 * subset of the broader orchestration context so callers that only want
 * to drive the DAG (without the surrounding lock / retry / compensation
 * loop) don't need to construct unused fields.
 */
export interface DagExecutionContext {
  readonly storage: WorkflowStorage;
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly hooks?: WorkflowHooks;
  readonly timeoutMs?: number;
  readonly dispatch?: DispatchConfig;
  /**
   * Fence guard for mutating writes. Captured by the orchestration loop
   * after `tryLock` and threaded into every `saveStepResult` /
   * `saveStepFailure` / `saveStepAttempt` call so a stale holder that
   * wakes up past lock expiry is rejected by the backend. `undefined`
   * on backends without fencing or when the caller is running a
   * sub-DAG outside a lock.
   */
  readonly guard?: FenceGuard;
  /**
   * Pluggable step executor. When set, step bodies run through this executor
   * instead of the inline Effect pipeline. `undefined` preserves the existing
   * Pipeline.all in-process path.
   */
  readonly stepExecutor?: StepExecutor;
  /** Time source. Drives deadline checks, step durations, dispatch poll waits. Default: `SystemClock`. */
  readonly clock?: Clock;
}

/**
 * Execute the workflow DAG against its current state. Computes the ready
 * set per iteration, dispatches remote steps via the step queue, runs local
 * ready steps in parallel with per-step retry + timeout + onFailure, and
 * checkpoints every completed step via `saveStepResult`. Suspension
 * errors propagate through as `{ suspension: true }` so the caller can
 * distinguish "workflow is sleeping / waiting for signal" from real
 * failures.
 *
 * Formerly `WorkflowBuilder._executeDag`. Pure function now — takes
 * the DAG context directly so the runner can drive DAG execution with
 * different executors in later phases (step-queue-backed, gRPC).
 */
export async function executeWorkflowDag(
  ctx: DagExecutionContext,
  params: {
    workflowId: string;
    input: unknown;
    dagNodes: DagNode[];
    state: WorkflowState | null;
    workflowStartTime: number;
    /** Tracks attempt numbers per step — shared across workflow retries so counters keep incrementing. */
    stepAttempts: Map<string, number>;
    /** Workflow-level deadline (absolute timestamp). Steps completing after this fail the workflow. */
    deadlineMs?: number;
  },
): Promise<
  | { success: true; result: unknown }
  | { success: false; error: unknown; suspension: boolean; continueAsNew?: boolean }
  | { success: false; tripwire: true; stepName: string; reason: unknown }
> {
  const { workflowId, input, dagNodes, state } = params;
  const clock = ctx.clock ?? SystemClock;
  const results: Record<string, unknown> = {};

  // Load previously completed step results. The stored shape is always the
  // codec's encoded form (written by saveStepResult above), so we decode
  // through the step's codec here so downstream steps see the same shape
  // they would on a fresh run. Skip step rows that aren't declared in the
  // DAG — e.g., synthetic `<loop>.iter.<n>` rows written by `.dowhile()`,
  // or orphans from a prior version's topology. Those rows stay in
  // storage for observability but must not count as DAG progress, or the
  // `completed.size < ctx.steps.length` gate would skip the real step.
  if (state) {
    for (const [stepName, stepState] of Object.entries(state.steps)) {
      if (stepState.status !== "completed") continue;
      const stepDef = ctx.steps.find((s) => s.name === stepName);
      if (!stepDef) continue;
      results[stepName] = stepDef.codec.decode(stepState.result);
    }
  }

  const completed = new Set(Object.keys(results));
  const running = new Set<string>();

  while (completed.size < ctx.steps.length) {
    // Check workflow-level deadline before each batch
    if (params.deadlineMs != null && clock.currentTimeMs() > params.deadlineMs) {
      return {
        success: false,
        error: new WorkflowDeadlineError({
          workflowId,
          timeoutMs: ctx.timeoutMs!,
          message: `Workflow "${workflowId}" exceeded global deadline of ${ctx.timeoutMs}ms`,
        }),
        suspension: false,
      };
    }

    const ready = computeReadySet({ nodes: dagNodes, completed, running });

    if (ready.length === 0 && running.size === 0) {
      return {
        success: false,
        error: new WorkflowError({
          workflowId,
          message: "Deadlock: no steps are ready and none are running",
        }),
        suspension: false,
      };
    }

    if (ready.length === 0) {
      break;
    }

    for (const name of ready) {
      running.add(name);
    }

    // Split into local and dispatched steps
    const remoteSet = new Set(ctx.dispatch?.remoteSteps ?? []);
    const localReady: string[] = [];
    const dispatchReady: string[] = [];

    for (const name of ready) {
      if (remoteSet.has(name) && ctx.dispatch) {
        dispatchReady.push(name);
      } else {
        localReady.push(name);
      }
    }

    // Dispatch remote steps — enqueue (with the step's declared needs) and
    // poll until completed.
    for (const name of dispatchReady) {
      const stepDef = ctx.steps.find((s) => s.name === name);
      await ctx.dispatch!.stepQueue.enqueue({
        workflowId,
        stepName: name,
        needs: stepDef?.needs,
        priority: stepDef?.priority,
        input,
        prevResults: { ...results },
      });
      // Poll until the worker completes this step
      const pollMs = ctx.dispatch!.pollIntervalMs ?? 500;
      while (true) {
        await new Promise((r) => clock.setTimeout(() => r(undefined), pollMs));
        const currentState = await ctx.storage.loadWorkflow(workflowId);
        const stepState = currentState?.steps[name];
        if (stepState?.status === "completed") {
          results[name] = stepState.result;
          completed.add(name);
          running.delete(name);
          await ctx.hooks?.onStepComplete?.({
            workflowId,
            stepName: name,
            result: stepState.result,
            durationMs: stepState.durationMs ?? 0,
          });
          break;
        }
        if (stepState?.status === "failed") {
          const errorMsg = stepState.error ?? "Remote step failed";
          await ctx.hooks?.onStepFailure?.({
            workflowId,
            stepName: name,
            error: errorMsg,
            durationMs: 0,
          });
          return {
            success: false,
            error: new StepError({ workflowId, stepName: name, message: errorMsg }),
            suspension: false,
          };
        }
      }
    }

    // If all ready steps were dispatched, skip local execution
    if (localReady.length === 0) continue;

    // Execute local ready steps in parallel, with per-step retry and failure handling
    const readySteps = localReady.map((name) => ctx.steps.find((s) => s.name === name)!);

    // Emit `step-started` events to any subscribers before kicking the
    // batch off. Storages without the optional hook are silently skipped —
    // polling-only callers don't see step-started (no reliable signal
    // from snapshot diffs).
    if (typeof ctx.storage.notifyStepStarted === "function") {
      for (const stepDef of readySteps) {
        // Swallow errors from the notify path — subscription is advisory,
        // not load-bearing. A broken event bus must not fail a workflow.
        try {
          await ctx.storage.notifyStepStarted(workflowId, stepDef.name);
        } catch {
          // ignore
        }
      }
    }

    // Per-parallel-batch audit metadata map. `.match()` writes its chosen
    // case here via metadataRef; the failure path reads it back by step
    // name to persist metadata even when a match branch throws.
    const stepMetadata = new Map<string, Record<string, unknown>>();

    type LocalStepResult = {
      name: string;
      result: unknown;
      metadata?: Record<string, unknown>;
      storageAlreadyCheckpointed?: boolean;
      durationMs: number;
      startedAt: Date;
      skipped?: true;
    };
    let batchResults: LocalStepResult[] | null = null;
    let batchError: unknown = null;

    if (ctx.stepExecutor) {
      // Executor path — delegates step body to the pluggable executor.
      // skipWhen and attempt tracking remain the runner's responsibility.
      //
      // Eager save: each step's `saveStepResult` (+ optional
      // `saveStepAttempt`) fires inside its own async closure, the moment
      // the body resolves, instead of being deferred to a post-wave
      // serial loop. Three benefits over the old "wait for the slowest in
      // the wave, then sequentially save" pattern:
      //   - `completedAt` reflects real wall-clock completion (the
      //     timeline used to draw a wide gap between work-end and save
      //     because the slowest sibling stalled the loop).
      //   - Concurrent writes against storage instead of N sequential
      //     awaits (every backend handles per-step row isolation).
      //   - Partial-wave failures persist more progress: if A throws
      //     while B already finished, B's save lands; on resume B is
      //     `completed` and gets skipped instead of re-run.
      // Skipped steps still don't persist (today's behavior preserved).
      try {
        batchResults = await Promise.all(
          readySteps.map(async (stepDef): Promise<LocalStepResult> => {
            if (stepDef.skipWhen) {
              const prevStepName = stepDef.dependsOn[0];
              const prev = prevStepName != null ? results[prevStepName] : input;
              if (stepDef.skipWhen(prev)) {
                const skipResult = stepDef.skipValue ? stepDef.skipValue(prev) : prev;
                return {
                  name: stepDef.name,
                  result: stepDef.codec.encode(skipResult),
                  durationMs: 0,
                  startedAt: clock.now(),
                  skipped: true,
                };
              }
            }

            const startedAt = clock.now();
            const startTime = startedAt.getTime();
            const currentAttempt = (params.stepAttempts.get(stepDef.name) ?? 0) + 1;
            params.stepAttempts.set(stepDef.name, currentAttempt);

            const req: StepExecutionRequest = {
              workflowId,
              stepName: stepDef.name,
              input,
              prevResults: { ...results },
              attempt: currentAttempt,
              needs: stepDef.needs,
              priority: stepDef.priority,
            };
            const res = await ctx.stepExecutor!.executeStep(req);
            if (!res.ok) {
              throw new StepError({ workflowId, stepName: stepDef.name, message: res.error });
            }
            const durationMs = clock.currentTimeMs() - startTime;

            // Eager save fires here unless the executor already wrote the
            // step row (Postgres step-queue / coordinator path sets
            // `storageAlreadyCheckpointed`). Returns the same shape as
            // before with that flag set so the post-wave loop knows to
            // skip its (now redundant) save.
            if (!res.storageAlreadyCheckpointed) {
              await ctx.storage.saveStepResult(
                {
                  workflowId,
                  stepName: stepDef.name,
                  result: res.result,
                  metadata: res.metadata,
                  durationMs,
                  startedAt,
                },
                ctx.guard,
              );
              if (isStepAttemptStorage(ctx.storage)) {
                await ctx.storage.saveStepAttempt(
                  {
                    workflowId,
                    stepName: stepDef.name,
                    attempt: currentAttempt,
                    type: "execution",
                    status: "completed",
                    result: res.result,
                    durationMs,
                    startedAt,
                    completedAt: clock.now(),
                  },
                  ctx.guard,
                );
              }
            }
            return {
              name: stepDef.name,
              result: res.result,
              metadata: res.metadata,
              storageAlreadyCheckpointed: true,
              durationMs,
              startedAt,
            };
          }),
        );
      } catch (err) {
        batchError = err;
      }
    } else {
      // Pipeline.all path — existing inline Effect execution.
      const pipeline = Pipeline.all(
        ...readySteps.map((stepDef) => {
          // Evaluate skipWhen before entering the step execution pipeline
          if (stepDef.skipWhen) {
            const prevStepName = stepDef.dependsOn[0];
            const prev = prevStepName != null ? results[prevStepName] : input;
            if (stepDef.skipWhen(prev)) {
              const skipResult = stepDef.skipValue ? stepDef.skipValue(prev) : prev;
              const encoded = stepDef.codec.encode(skipResult);
              return Pipeline.succeed({
                name: stepDef.name,
                result: encoded,
                durationMs: 0,
                startedAt: clock.now(),
                skipped: true as const,
              });
            }
          }

          const startedAt = clock.now();
          const startTime = startedAt.getTime();

          // Get or initialize attempt counter for this step (persists across workflow retries)
          const currentAttemptForStep = params.stepAttempts.get(stepDef.name) ?? 0;
          const attemptRef = { current: currentAttemptForStep + 1 };
          // Shared audit-metadata slot — `.match()` fills it at selector time;
          // the map() below forwards it onto the stepResult shape. Fresh per
          // step (not per attempt) so a retry overwrites rather than appends.
          const metadataRef: { current?: Record<string, unknown> } = { current: undefined };

          // Raw step execution — wrapped in suspend so retry re-invokes the step fn.
          // attemptRef tracks the attempt number; incremented each invocation so
          // step retries and workflow retries both see monotonically increasing attempts.
          let raw: Pipeline<unknown, TaggedError> = Pipeline.from(
            Effect.suspend(() => {
              const currentAttempt = attemptRef.current;
              attemptRef.current = currentAttempt + 1;
              // Write back to shared map so workflow retries pick up the right count
              params.stepAttempts.set(stepDef.name, currentAttempt);
              const executed = stepDef.execute({
                input,
                results,
                workflowId,
                storage: ctx.storage,
                attemptRef: { current: currentAttempt },
                metadataRef,
              });
              // Kinds that set metadata synchronously in their execute (e.g.
              // `.match()` after selector resolution) surface it here BEFORE
              // the branch pipeline runs. The failure path can then read
              // the map by step name even when the branch throws.
              if (metadataRef.current) {
                stepMetadata.set(stepDef.name, metadataRef.current);
              }
              return executed.effect;
            }),
          ) as Pipeline<unknown, TaggedError>;

          // Per-step activity timeout — wraps the user's function with a deadline
          if (stepDef.timeoutMs != null) {
            const stepTimeoutMs = stepDef.timeoutMs;
            const stepName = stepDef.name;
            const timeoutEffect = Effect.sleep(stepTimeoutMs).pipe(
              Effect.andThen(
                Effect.fail(
                  new StepTimeoutError({
                    workflowId,
                    stepName,
                    timeoutMs: stepTimeoutMs,
                    message: `Step "${stepName}" timed out after ${stepTimeoutMs}ms`,
                  }),
                ),
              ),
            );
            raw = Pipeline.from(Effect.raceFirst(raw.effect, timeoutEffect)) as Pipeline<
              unknown,
              TaggedError
            >;
          }

          // Step-level retry (before mapping to result shape)
          if (stepDef.retry) {
            raw = raw.retry(stepDef.retry);
          }

          // Step-level failure strategy
          const strategy = stepDef.onFailure ?? "fail";
          if (strategy === "skip") {
            raw = raw.handleError(() => undefined);
          } else if (strategy !== "fail" && "fallback" in strategy) {
            const fallbackFn = strategy.fallback;
            raw = raw.handleError((err) => fallbackFn(err));
          }

          // Map to step result + eager save. Without flatMap-ing the
          // save into the pipeline, the legacy path has the same wave-
          // tail lag the executor path used to: `Pipeline.all` waits
          // for every parallel step, then a post-wave for-loop saves
          // them serially. Pulling the save into the per-step
          // pipeline collapses the lag the same way the executor path
          // does — completedAt becomes truthful per step.
          return raw
            .map((result) => {
              const encoded = stepDef.codec.encode(result);
              return {
                name: stepDef.name,
                result: encoded,
                metadata: metadataRef.current,
                durationMs: clock.currentTimeMs() - startTime,
                startedAt,
              };
            })
            .flatMap((stepResult) =>
              Pipeline.fromPromise(async () => {
                await ctx.storage.saveStepResult(
                  {
                    workflowId,
                    stepName: stepResult.name,
                    result: stepResult.result,
                    metadata: stepResult.metadata,
                    durationMs: stepResult.durationMs,
                    startedAt: stepResult.startedAt,
                  },
                  ctx.guard,
                );
                if (isStepAttemptStorage(ctx.storage)) {
                  await ctx.storage.saveStepAttempt(
                    {
                      workflowId,
                      stepName: stepResult.name,
                      attempt: params.stepAttempts.get(stepResult.name) ?? 1,
                      type: "execution",
                      status: "completed",
                      result: stepResult.result,
                      durationMs: stepResult.durationMs,
                      startedAt: stepResult.startedAt,
                      completedAt: clock.now(),
                    },
                    ctx.guard,
                  );
                }
                return { ...stepResult, storageAlreadyCheckpointed: true };
              }),
            );
        }),
      );

      // `catchAll: true` converts Effect defects (including rejections
      // from `Pipeline.fromPromise` → `Effect.promise`) into typed errors
      // that runSafe returns through the `error` channel. Without it, any
      // user step that throws synchronously — common with `.stepAsync()`
      // or user code that builds its own `Pipeline.fromPromise(...)` —
      // escapes `runSafe` unobserved, skips the `saveStepFailure` path
      // below, and leaves the workflow stuck in `pending`. See
      // promin-4ace. The extra `catchAll` changes what classes of error
      // reach `batchError` but not what the downstream code does with it:
      // the fallback branch already handles arbitrary Error instances.
      const { data, error } = await pipeline.runSafe({ catchAll: true });
      batchResults = data as LocalStepResult[] | null;
      batchError = error ?? null;
    }

    if (batchError) {
      const tag = (batchError as TaggedError)._tag;

      // Suspension errors propagate without failing the workflow
      if (tag === "WorkflowSuspendedError") {
        return { success: false, error: batchError, suspension: true };
      }

      // Continue-as-new requests also unwind cleanly — no compensation,
      // no failure recording. The orchestration wrapper catches the
      // thrown error and chains a fresh run.
      if (tag === "WorkflowContinueAsNewError") {
        return { success: false, error: batchError, suspension: false, continueAsNew: true };
      }

      // Record step failure
      const stepName =
        tag === "StepError"
          ? (batchError as StepError).stepName
          : tag === "WorkflowTimeoutError"
            ? (batchError as WorkflowTimeoutError).stepName
            : tag === "StepTimeoutError"
              ? (batchError as StepTimeoutError).stepName
              : (ready[0] ?? "unknown");
      const errorMsg =
        batchError instanceof globalThis.Error ? batchError.message : String(batchError);
      const failStartedAt = clock.now();
      await ctx.storage.saveStepFailure(
        {
          workflowId,
          stepName,
          error: errorMsg,
          durationMs: 0,
          startedAt: failStartedAt,
          metadata: stepMetadata.get(stepName),
        },
        ctx.guard,
      );
      if (isStepAttemptStorage(ctx.storage)) {
        await ctx.storage.saveStepAttempt(
          {
            workflowId,
            stepName,
            attempt: params.stepAttempts.get(stepName) ?? 1,
            type: "execution",
            status: "failed",
            error: errorMsg,
            durationMs: 0,
            startedAt: failStartedAt,
            completedAt: clock.now(),
          },
          ctx.guard,
        );
      }
      await ctx.hooks?.onStepFailure?.({
        workflowId,
        stepName,
        error: errorMsg,
        durationMs: 0,
      });

      return { success: false, error: batchError, suspension: false };
    }

    // Checkpoint each completed step. `result` here is the codec-encoded
    // form; storage keeps that shape. Downstream steps and the
    // onStepComplete hook see the round-tripped decoded form so fresh-run
    // and replay paths are identical.
    let tripwireFire: { stepName: string; reason: unknown } | null = null;
    for (const stepResult of batchResults!) {
      const { name, result, durationMs, startedAt } = stepResult;
      const metadata = "metadata" in stepResult ? stepResult.metadata : undefined;
      const storageAlreadyCheckpointed =
        "storageAlreadyCheckpointed" in stepResult &&
        stepResult.storageAlreadyCheckpointed === true;
      const wasSkipped = "skipped" in stepResult && stepResult.skipped === true;
      const stepDef = ctx.steps.find((s) => s.name === name);
      const decoded = stepDef ? stepDef.codec.decode(result) : result;
      if (!storageAlreadyCheckpointed) {
        await ctx.storage.saveStepResult(
          {
            workflowId,
            stepName: name,
            result,
            metadata,
            durationMs,
            startedAt,
          },
          ctx.guard,
        );
        if (isStepAttemptStorage(ctx.storage)) {
          await ctx.storage.saveStepAttempt(
            {
              workflowId,
              stepName: name,
              attempt: params.stepAttempts.get(name) ?? 1,
              type: "execution",
              status: "completed",
              result,
              durationMs,
              startedAt,
              completedAt: clock.now(),
            },
            ctx.guard,
          );
        }
      }
      if (!wasSkipped) {
        await ctx.hooks?.onStepComplete?.({
          workflowId,
          stepName: name,
          result: decoded,
          durationMs,
        });
      }
      results[name] = decoded;
      completed.add(name);
      running.delete(name);

      // Tripwire detection: a `.tripwire()` step signals termination by
      // writing `{ tripwireFired: true, reason }` to its metadata. Captured
      // here after save so the step row shows `status: completed` with the
      // reason as its result — ops can still query the step history.
      // Breaks out of DAG execution after the batch settles.
      if (
        stepDef?.kind === "tripwire" &&
        metadata &&
        (metadata as { tripwireFired?: boolean }).tripwireFired === true
      ) {
        tripwireFire = {
          stepName: name,
          reason: (metadata as { reason: unknown }).reason,
        };
      }
    }

    if (tripwireFire) {
      return { success: false, tripwire: true, ...tripwireFire };
    }

    // Check workflow-level deadline after steps complete
    if (
      params.deadlineMs != null &&
      clock.currentTimeMs() > params.deadlineMs &&
      completed.size < ctx.steps.length
    ) {
      return {
        success: false,
        error: new WorkflowDeadlineError({
          workflowId,
          timeoutMs: ctx.timeoutMs!,
          message: `Workflow "${workflowId}" exceeded global deadline of ${ctx.timeoutMs}ms`,
        }),
        suspension: false,
      };
    }
  }

  const lastStepName = ctx.steps[ctx.steps.length - 1]!.name;
  return { success: true, result: results[lastStepName] };
}

/**
 * A step definition's view as needed by compensation — just the name and
 * the rollback function. A full `StepDefinition` is a superset and passes
 * this check via structural typing; keeps compensation decoupled from the
 * rest of the step shape (execute / codec / retry / etc.).
 */
export interface CompensatableStep {
  readonly name: string;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, TaggedError> | Promise<void>;
}

/**
 * Run the saga rollback for a workflow — reverses completed steps that
 * carry a `compensate` function, in last-completed-first order, honoring
 * the compensate config's per-step retry policy. Records an attempt row
 * per try when the storage supports `StepAttemptStorage`.
 *
 * Formerly `WorkflowBuilder._compensate`. Pure function now — takes the
 * storage + step list + retry config directly so the runner can drive
 * compensation without holding a builder reference.
 */
export async function compensateWorkflow(params: {
  storage: WorkflowStorage;
  steps: ReadonlyArray<CompensatableStep>;
  compensateConfig?: CompensateConfig;
  workflowId: string;
  input: unknown;
  dagNodes: DagNode[];
  /** Optional fence guard — threaded to `saveStepAttempt` writes so a stale holder's compensation rows are rejected. */
  guard?: FenceGuard;
  /** Time source. Drives compensation retry backoff + attempt timestamps. Default: SystemClock. */
  clock?: Clock;
}): Promise<{
  compensated: string[];
  failed: { stepName: string; error: unknown }[];
}> {
  const { storage, steps, compensateConfig, workflowId, input, guard } = params;
  const clock = params.clock ?? SystemClock;
  const compensated: string[] = [];
  const failed: { stepName: string; error: unknown }[] = [];

  const state = await storage.loadWorkflow(workflowId);
  if (!state) return { compensated, failed };

  // Reverse order so last-completed is compensated first — saga semantics.
  const stepsToCompensate: { stepDef: CompensatableStep; result: unknown }[] = [];
  for (let i = steps.length - 1; i >= 0; i--) {
    const stepDef = steps[i]!;
    const stepState = state.steps[stepDef.name];
    if (stepState?.status === "completed" && stepDef.compensate) {
      stepsToCompensate.push({ stepDef, result: stepState.result });
    }
  }

  const retryConfig = compensateConfig?.retry;
  const maxCompRetries = retryConfig?.maxRetries ?? 0;
  const compRetryDelayMs = retryConfig?.baseDelayMs ?? 500;

  const recordsAttempts = isStepAttemptStorage(storage);

  for (const { stepDef, result } of stepsToCompensate) {
    for (let attempt = 0; attempt <= maxCompRetries; attempt++) {
      const compStartedAt = clock.now();
      try {
        if (attempt > 0) {
          await new Promise((r) =>
            clock.setTimeout(() => r(undefined), compRetryDelayMs * Math.pow(2, attempt - 1)),
          );
        }
        const compensateResult = stepDef.compensate!({ result, input, workflowId });
        if (compensateResult instanceof Pipeline) {
          await compensateResult.runPromise();
        } else if (
          compensateResult &&
          typeof (compensateResult as Promise<void>).then === "function"
        ) {
          await compensateResult;
        }
        compensated.push(stepDef.name);
        if (recordsAttempts) {
          await (storage as any).saveStepAttempt(
            {
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "completed",
              durationMs: clock.currentTimeMs() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: clock.now(),
            },
            guard,
          );
        }
        break;
      } catch (err) {
        if (recordsAttempts) {
          await (storage as any).saveStepAttempt(
            {
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "failed",
              error: err instanceof Error ? err.message : String(err),
              durationMs: clock.currentTimeMs() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: clock.now(),
            },
            guard,
          );
        }
        if (attempt === maxCompRetries) {
          failed.push({ stepName: stepDef.name, error: err });
        }
      }
    }
  }

  return { compensated, failed };
}

/**
 * Publish a dead-letter record after a workflow exhausts retries +
 * compensation. Load the final state (so the DLQ record reflects the
 * last-checkpointed steps), build the record, and hand it to the sink.
 *
 * Failures are swallowed — a broken DLQ sink shouldn't mask the
 * original error. Formerly inline in `WorkflowBuilder.run`'s failure
 * path; extracted so the runner can drive it without the builder's
 * private fields.
 */
export async function publishDlqRecord(params: {
  dlq: Sinkable<FailedWorkflowRecord>;
  storage: WorkflowStorage;
  workflowId: string;
  workflowName: string;
  input: unknown;
  errorMsg: string;
  compensationReport: {
    compensated: string[];
    failed: { stepName: string; error: unknown }[];
  };
  metadata?: Record<string, unknown>;
  /** Time source for the `failedAt` timestamp. Default: SystemClock. */
  clock?: Clock;
}): Promise<void> {
  const clock = params.clock ?? SystemClock;
  try {
    const failedState = await params.storage.loadWorkflow(params.workflowId);
    await params.dlq.publish({
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      input: params.input,
      error: params.errorMsg,
      failedAt: clock.now(),
      steps: failedState?.steps ?? {},
      compensatedSteps: params.compensationReport.compensated,
      failedCompensations: params.compensationReport.failed.map((f) => ({
        stepName: f.stepName,
        error: f.error instanceof Error ? f.error.message : String(f.error),
      })),
      metadata: params.metadata,
    });
  } catch {
    // DLQ failure is swallowed — the original error is more important.
  }
}

/**
 * Resolve the idempotency TTL for a given terminal status.
 * Returns `undefined` when no idempotency config is active or no TTL is
 * configured for the passed status. Formerly a private method on
 * WorkflowBuilder (`_getIdempotencyTtl`); extracted here so the runner
 * can make the caching decision without holding a reference to the
 * builder instance.
 */
export function getIdempotencyTtl(
  idempotency: IdempotencyConfig | undefined,
  status: string,
): number | undefined {
  if (!idempotency) return undefined;
  const ttl = idempotency.ttl;
  if (typeof ttl === "number") return ttl;
  if (status === "completed") return ttl.success;
  if (status === "failed") return ttl.failure;
  return undefined;
}

// ---------------------------------------------------------------------------
// InProcessStepExecutor — runs a single step body in-process.
//
// Replicates the per-step retry / timeout / onFailure logic from the DAG
// executor, but scoped to one step at a time. Used by the distributed worker
// to execute a step it claimed from the queue without the full orchestration
// loop.
// ---------------------------------------------------------------------------

/**
 * Runs a single step body in-process: applies per-step timeout, retry, and
 * onFailure strategy, then returns the encoded result. Used by the
 * distributed worker so each worker node executes only the steps it claims
 * from the queue, without running the full orchestration loop.
 *
 * Re-throws `WorkflowSuspendedError` directly rather than wrapping it in
 * `{ ok: false }` so the caller can distinguish suspension from failure.
 */
export class InProcessStepExecutor implements StepExecutor {
  private readonly workflow: Workflow<unknown, unknown>;
  private readonly storage: WorkflowStorage;

  constructor(workflow: Workflow<unknown, unknown>, config: { storage: WorkflowStorage }) {
    this.workflow = workflow;
    this.storage = config.storage;
  }

  async executeStep(req: StepExecutionRequest): Promise<StepExecutionResult> {
    const stepDef = this.workflow._definition.steps.find((s) => s.name === req.stepName);
    if (!stepDef) {
      return {
        ok: false,
        error: `Step "${req.stepName}" not found in workflow "${this.workflow.name}"`,
      };
    }

    const attemptRef = { current: req.attempt };
    const metadataRef: { current?: Record<string, unknown> } = { current: undefined };

    let raw: Pipeline<unknown, TaggedError> = Pipeline.from(
      Effect.suspend(() => {
        const currentAttempt = attemptRef.current;
        attemptRef.current = currentAttempt + 1;
        const executed = stepDef.execute({
          input: req.input,
          results: req.prevResults,
          workflowId: req.workflowId,
          storage: this.storage,
          attemptRef: { current: currentAttempt },
          metadataRef,
        });
        return executed.effect;
      }),
    ) as Pipeline<unknown, TaggedError>;

    if (stepDef.timeoutMs != null) {
      const stepTimeoutMs = stepDef.timeoutMs;
      const stepName = stepDef.name;
      const workflowId = req.workflowId;
      const timeoutEffect = Effect.sleep(stepTimeoutMs).pipe(
        Effect.andThen(
          Effect.fail(
            new StepTimeoutError({
              workflowId,
              stepName,
              timeoutMs: stepTimeoutMs,
              message: `Step "${stepName}" timed out after ${stepTimeoutMs}ms`,
            }),
          ),
        ),
      );
      raw = Pipeline.from(Effect.raceFirst(raw.effect, timeoutEffect)) as Pipeline<
        unknown,
        TaggedError
      >;
    }

    if (stepDef.retry) {
      raw = raw.retry(stepDef.retry);
    }

    const strategy = stepDef.onFailure ?? "fail";
    if (strategy === "skip") {
      raw = raw.handleError(() => undefined);
    } else if (strategy !== "fail" && "fallback" in strategy) {
      const fallbackFn = strategy.fallback;
      raw = raw.handleError((err) => fallbackFn(err));
    }

    const { data, error } = await raw.map((result) => stepDef.codec.encode(result)).runSafe();

    if (error) {
      if ((error as TaggedError)._tag === "WorkflowSuspendedError") {
        throw error;
      }
      const errorMsg = error instanceof globalThis.Error ? error.message : String(error);
      return { ok: false, error: errorMsg };
    }

    return { ok: true, result: data!, metadata: metadataRef.current };
  }
}
