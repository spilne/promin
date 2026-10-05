// ---------------------------------------------------------------------------
// Workflow types — the frozen `Workflow` definition the builder produces and
// the runner consumes, its runtime internals, and the workflow-level config
// shapes (hooks, idempotency, compensation, dispatch, queue). Type-only: the
// runner imports this module and `step-definition.ts`, never the builder.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { WorkflowRetryPolicy } from "../shared/retry-policy.ts";
import type { Sinkable } from "../shared/streamable.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import type { StepDefinition } from "./step-definition.ts";
import type { WorkflowDAG } from "./workflow-dag-viz.ts";
import type { FailedWorkflowRecord, WorkflowRunEvent } from "./workflow-state.ts";

// ---------------------------------------------------------------------------
// Queue concurrency config
// ---------------------------------------------------------------------------

/**
 * Per-workflow concurrency cap. Each step task this workflow enqueues
 * carries the resolved key + limit; the step queue's `claim()` enforces
 * the cap by counting currently-running tasks with the same
 * `(workflowName, key)` tuple.
 *
 * Set `concurrencyKey` to a function of input — typical "cap per tenant"
 * usage:
 *
 * ```ts
 * workflow<{ tenantId: string }>({
 *   name: "send-email",
 *   queue: {
 *     concurrencyLimit: 5,
 *     concurrencyKey: (input) => input.tenantId,
 *   },
 * });
 * ```
 *
 * Without `concurrencyKey`, the limit applies globally to every running
 * step of this workflow regardless of input.
 */
export interface WorkflowQueueConfig<Input> {
  readonly concurrencyLimit: number;
  readonly concurrencyKey?: (input: Input) => string;
}

// ---------------------------------------------------------------------------
// Idempotency config
// ---------------------------------------------------------------------------

export interface IdempotencyConfig {
  /** Time-to-live before re-execution is allowed. */
  readonly ttl: number | { readonly success: number; readonly failure?: number };
  /**
   * What to do when the TTL expires.
   * - `"fresh-run"` — increment run counter, re-execute all steps (default)
   * - `"replay"` — re-enter engine, replay completed steps from storage
   */
  readonly onExpiry?: "fresh-run" | "replay";
  /**
   * What to do when another execution is already in-flight.
   * - `"join"` — return a handle to the existing run (singleflight, default)
   * - `"reject"` — throw WorkflowLockError
   */
  readonly onInFlight?: "join" | "reject";
}

// ---------------------------------------------------------------------------
// Workflow — pure definition (no storage, portable)
// ---------------------------------------------------------------------------

/**
 * A frozen, portable workflow definition. Pure data — name, version, DAG,
 * idempotency config. Executed via a `WorkflowRunner`:
 *
 * ```ts
 * const runner = createWorkflowRunner({ storage });
 * await runner.run({ workflow, workflowId, input });
 * ```
 *
 * Keeping definitions storage-free lets a single process import and dispatch
 * workflow DAGs without also wiring up storage for every submitter — the
 * runner or coordinator owns that.
 *
 * The `_definition` field carries the runtime internals (steps, retry,
 * compensation, etc.) so the runner can build an orchestration context from
 * the pure shape. Part of the runtime contract between builder and runner,
 * not a public surface — callers shouldn't read it directly.
 *
 * `E` is the union of typed step failures the workflow can end with: what
 * the builder collected from its steps' `Eff` error channels and from the
 * step kinds (`GuardError`, `MatchError`, `LoopLimitExceededError`, ...).
 * Engine control flow (suspension, continue-as-new) is not part of it. The
 * runner surfaces `E` on `run` / `runSafe` / `start` (see `WorkflowRunError`).
 *
 * `E` is informational and defaults to `never`, so an annotation that leaves
 * it out (`Workflow<I, O>`) still accepts a workflow with typed errors, and
 * one written with fewer or more errors than the builder found still
 * compiles. Read it with `WorkflowErrorOf<typeof wf>`.
 */
export interface Workflow<Input, Output, E extends TaggedError = never> {
  readonly name: string;
  readonly version?: string;
  readonly dag: WorkflowDAG;
  readonly idempotency?: IdempotencyConfig;
  /**
   * @internal — runtime internals consumed by WorkflowRunner when executing
   * this workflow. Not part of the public API. Shape may change without
   * notice; treat as opaque.
   */
  readonly _definition: WorkflowDefinitionInternals;
  /**
   * Generic parameter carriers so `Workflow<Input, Output>` stays
   * distinguishable structurally. Never populated at runtime.
   * @internal
   */
  readonly __input?: Input;
  readonly __output?: Output;
  /** @internal Carrier of `E` (see `ErrorCarrier`). Never populated at runtime. */
  readonly __error?: ErrorCarrier<E>;
}

/**
 * Phantom carrier of a typed error union. A method parameter is checked
 * bivariantly, so `Workflow<I, O, A>` and `Workflow<I, O, B>` are mutually
 * assignable whenever `A` and `B` are related (`never` is related to every
 * union). That keeps `E` additive: existing `Workflow<I, O>` annotations
 * and `Workflow<unknown, unknown>` parameters accept any workflow, while
 * inference from a `Workflow<I, O, E>` parameter still recovers `E`.
 */
interface ErrorCarrier<E> {
  carry(error: E): void;
}

/** The typed error union `E` of a `Workflow<I, O, E>` (`never` when it has none). */
export type WorkflowErrorOf<W> = W extends Workflow<any, any, infer E> ? E : never;

/**
 * Runtime internals of a Workflow. Captured from the builder at `.build()` time
 * and consumed by `WorkflowRunner` to construct an orchestration context at
 * run time. Not exported outside the package.
 *
 * @internal
 */
export interface WorkflowDefinitionInternals {
  readonly steps: ReadonlyArray<StepDefinition>;
  readonly type?: string;
  readonly metadata?: Record<string, unknown>;
  readonly retry?: WorkflowRetryPolicy;
  readonly compensateConfig?: CompensateConfig;
  readonly dlq?: Sinkable<FailedWorkflowRecord>;
  readonly timeoutMs?: number;
  readonly onVersionMismatch: "strict" | "drain";
  readonly previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  readonly hooks?: WorkflowHooks;
  /**
   * Workflow-level concurrency cap — applied as a default to every step's
   * dispatched task. Step-level `StepOptions.queue` wins for steps that
   * declare their own. See `WorkflowQueueConfig`.
   */
  readonly queue?: WorkflowQueueConfig<unknown>;
  /**
   * Patch names active in this definition. The runner hands them to every
   * step as `ExecuteParams.patches` (journaled `ctx.patched(name)`).
   */
  readonly patches?: readonly string[];
}

/**
 * Handle to a running workflow. Returned by `WorkflowRunner.start()`.
 *
 * `E` is the workflow's typed error union (see `Workflow`); `result()`
 * reports a typed failure by its `_tag` (see there).
 */
export interface WorkflowHandle<Output, E extends TaggedError = never> {
  readonly workflowId: string;
  /** @internal Carrier of `E`. Never populated at runtime. */
  readonly __error?: ErrorCarrier<E>;

  /** Get the current workflow status. */
  status(params?: { includeStepResults?: boolean }): Promise<WorkflowStatusInfo<Output> | null>;

  /** Send a signal to the workflow (e.g. from a webhook). */
  signal(params: { signalName: string; payload: unknown }): Promise<void>;

  /**
   * Wait for the workflow to complete by polling storage (it does not
   * resume a suspended run; the scanners or another `run` do). The run may
   * execute elsewhere, so a failure is read back from
   * storage: `result()` rejects with `WorkflowFailedError`, whose `errorTag`
   * is the `_tag` of the error that failed the run (for a typed failure,
   * one of `E["_tag"]`), `WorkflowCancelledError` or `WorkflowTripwireError`.
   */
  result(params?: { intervalMs?: number; timeoutMs?: number }): Promise<Output>;

  /**
   * Cancel the workflow. Marks it `failed` with the given reason and skips
   * any in-flight steps. Idempotent — cancelling an already-terminal
   * workflow is a no-op (matches storage.cancelWorkflow semantics).
   */
  cancel(reason?: string): Promise<void>;

  /**
   * Subscribe to live step / lifecycle events for this workflow. Closes on
   * the first terminal event (`workflow-completed`, `workflow-failed`,
   * `workflow-tripwire`) or when `options.signal` fires. Delegates to
   * `runner.subscribe({ workflowId, ...options })` — uses the storage's native
   * push path when available, falls back to polling otherwise.
   */
  events(options?: {
    signal?: AbortSignal;
    pollIntervalMs?: number;
  }): AsyncIterable<WorkflowRunEvent>;
}

export interface WorkflowStatusInfo<Output> {
  readonly state: "pending" | "running" | "completed" | "failed" | "suspended" | "tripwire";
  readonly result?: Output;
  readonly error?: string;
  /**
   * `_tag` of the error that failed the run, as stored with it
   * (`"WorkflowCancelledError"` for a cancelled run). Present only when
   * `state === "failed"` and the error carried a tag.
   */
  readonly errorTag?: string;
  /**
   * Structured tripwire reason — present only when `state === "tripwire"`.
   * Opaque payload returned by the firing `.tripwire()` step's `reason(prev)`.
   */
  readonly tripwire?: unknown;
  /** Which step is currently active or blocked. */
  readonly currentStep?: string;
  /** Why the workflow is suspended (if applicable). */
  readonly suspendedReason?: "sleeping" | "waiting_for_signal";
  /** Summary of all step statuses. */
  readonly steps: Record<string, { status: string; result?: unknown }>;
  readonly createdAt: Date;
  readonly startedAt?: Date;
  readonly updatedAt: Date;
}

// ---------------------------------------------------------------------------
// Workflow hooks — lifecycle callbacks
// ---------------------------------------------------------------------------

export interface WorkflowHooks {
  onStepComplete?: (params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
  }) => void | Promise<void>;
  onStepFailure?: (params: {
    workflowId: string;
    stepName: string;
    error: string;
    durationMs: number;
  }) => void | Promise<void>;
  onWorkflowComplete?: (params: {
    workflowId: string;
    result: unknown;
    durationMs: number;
  }) => void | Promise<void>;
  onWorkflowFailure?: (params: {
    workflowId: string;
    error: string;
    durationMs: number;
  }) => void | Promise<void>;
  /**
   * Fired when a `.tripwire()` step terminates the workflow. Not a failure —
   * intentional short-circuit with a structured reason. `stepName` names the
   * tripwire step; `reason` is the opaque payload it returned.
   */
  onWorkflowTripwire?: (params: {
    workflowId: string;
    stepName: string;
    reason: unknown;
    durationMs: number;
  }) => void | Promise<void>;
  /**
   * Called when one of the hooks above throws or rejects. Hooks are
   * observers: their errors never change the run's outcome, and the run
   * goes on after the report. Default: `console.error`. An error thrown
   * here is dropped.
   */
  onHookError?: (params: {
    workflowId: string;
    hook: Exclude<keyof WorkflowHooks, "onHookError">;
    error: unknown;
  }) => void;
}

// ---------------------------------------------------------------------------
// Workflow-level compensation config
// ---------------------------------------------------------------------------

export interface CompensateConfig {
  /**
   * When to trigger compensation.
   * - `"after-retries"` (default) — compensate after all workflow retries exhausted.
   * - `"immediate"` — compensate on first workflow failure (skip workflow retries).
   */
  trigger?: "after-retries" | "immediate";
  /**
   * Retry policy for each compensation function. Default: no retry; with
   * `maxRetries` set, `baseDelayMs` defaults to 250ms (`RETRY_POLICY_DEFAULTS`).
   */
  retry?: { maxRetries?: number; baseDelayMs?: number };
  /**
   * Callback after all step compensations complete.
   * Receives the original error, list of compensated steps, and any compensation failures.
   */
  onComplete?: (params: {
    input: unknown;
    error: unknown;
    compensatedSteps: string[];
    failedCompensations: { stepName: string; error: unknown }[];
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
}

// ---------------------------------------------------------------------------
// workflow() parameters and the builder's frozen copy of them
// ---------------------------------------------------------------------------

/** Parameters of `workflow()`. */
export interface WorkflowParams<Input> {
  readonly name: string;
  readonly hooks?: WorkflowHooks;
  readonly type?: string;
  readonly metadata?: Record<string, unknown>;
  /** Workflow-level retry policy. Re-runs from the failed step (completed steps are checkpointed). */
  readonly retry?: WorkflowRetryPolicy;
  /** Compensation configuration — controls when and how saga rollback runs. */
  readonly compensate?: CompensateConfig;
  /** Dead letter queue — failed workflows are published here after all retries + compensation. */
  readonly dlq?: Sinkable<FailedWorkflowRecord>;
  /** Workflow version tag — used to detect code/state mismatch on resume. Unset by default. */
  readonly version?: string;
  /** Global deadline for the entire workflow execution (ms). Fails with WorkflowDeadlineError if exceeded. */
  readonly timeoutMs?: number;
  /**
   * How to handle resumes of workflows created with a different `version`:
   * - `"strict"` (default) — throw `WorkflowVersionMismatchError`.
   * - `"drain"` — delegate the resume to the matching definition in
   *   `previousVersions`. Use this to let in-flight workflows finish on
   *   their original code while new workflows use the updated code.
   */
  readonly onVersionMismatch?: "strict" | "drain";
  /**
   * Definitions of prior versions of this workflow. Consulted only when
   * `onVersionMismatch: "drain"` is set and a resume encounters a stored
   * version different from the current one. Each entry must have its own
   * `version` field set, or it can't be looked up.
   */
  readonly previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  /**
   * Patch names active in this workflow version. Inside a journaled step
   * body, `ctx.patched(name)` returns `patches.includes(name)`. Each
   * workflow version declares its own active set — no comparison logic.
   */
  readonly patches?: readonly string[];
  /**
   * Default codec for every step and activity in this workflow. Individual
   * steps and activities can still override via their own `options.codec`.
   * Omit to use `LosslessJsonCodec` (Date / BigInt / Map / Set / Error /
   * RegExp / URL / undefined / NaN / ±Infinity / -0 round-trip). Set to
   * `JsonCodec` to opt out everywhere and accept that non-JSON values will
   * be lost across storage boundaries.
   */
  readonly codec?: Codec<unknown>;
  /**
   * Enable activity-input fingerprinting across this whole workflow. When
   * `true`, every 3-arg `ctx.activity(name, input, fn)` canonicalizes its
   * input and stores a SHA-256 hex hash on the journal row; on replay, a
   * mismatch throws `JournalNonDeterminismError` to catch silent payload
   * drift (same activity name, different input between runs).
   *
   * Per-activity `ActivityOptions.payloadHash` still wins — pass `false`
   * there to opt out of a specific activity even when the workflow default
   * is on. The 2-arg `ctx.activity(name, fn)` form is unaffected because
   * it has no reified input to hash.
   *
   * Off by default. Hashing adds one SHA-256 per activity invocation,
   * which is cheap but not free.
   */
  readonly payloadHash?: boolean;
  /**
   * Workflow-level concurrency cap. Stamped onto every dispatched step
   * task; the step queue's `claim()` enforces. Caller's
   * `concurrencyKey(input)` runs once at coordinator-enqueue time and the
   * resolved string is stored on each task so workers don't re-evaluate.
   * A step's own `StepOptions.queue` overrides this for that step.
   *
   * ```ts
   * workflow<{ tenantId: string }>({
   *   name: "send-email",
   *   queue: { concurrencyLimit: 5, concurrencyKey: (input) => input.tenantId },
   * });
   * ```
   */
  readonly queue?: WorkflowQueueConfig<Input>;
}

/**
 * The workflow-level config a builder carries: the `workflow()` params with
 * defaults applied, plus what `.build({ idempotency })` adds.
 */
export interface WorkflowConfig<Input> extends Omit<WorkflowParams<Input>, "onVersionMismatch"> {
  readonly onVersionMismatch: "strict" | "drain";
  readonly idempotency?: IdempotencyConfig;
}
