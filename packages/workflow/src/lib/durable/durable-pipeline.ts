// ---------------------------------------------------------------------------
// WorkflowBuilder — fluent, type-safe DAG-based durable pipeline
// ---------------------------------------------------------------------------
//
// Type safety strategy:
//
// The public API (.step() overloads) is fully typed — Input, Steps record,
// Current, and Error are tracked through the chain via type-level computation.
//
// Internally, StepDefinition uses `unknown` because the builder holds a
// heterogeneous array where step 1 returns User, step 2 returns Account, etc.
// TypeScript can't express Array<∃T. StepDef<T>> (existential types).
// The type safety boundary is the public overloads — this is the same pattern
// used by Effect, Zod, and RxJS for heterogeneous collections.
// ---------------------------------------------------------------------------

import { Effect, Data } from "effect";
import { isActivityJournalStorage, type ActivityJournalStorage } from "./activity-journal.ts";
import {
  runJournaledStep,
  JournalStorageMissingError,
  type JournaledStepBody,
} from "./journaled-step.ts";
import { Pipeline, type TaggedError } from "@promin/core";
import type { RetryPolicy } from "@promin/core";
import type { Codec } from "@promin/core";
import { JsonCodec } from "@promin/core";
import type { Show } from "@promin/core";
import type { Sinkable } from "@promin/core";
import type { FailedWorkflowRecord } from "./workflow-state.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import { isStepAttemptStorage } from "./workflow-storage.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import type { DagNode } from "./workflow-dag.ts";
import { topologicalSort, computeReadySet } from "./workflow-dag.ts";
import {
  WorkflowError,
  StepError,
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  StepTimeoutError,
  WorkflowDeadlineError,
  WorkflowVersionMismatchError,
} from "./durable-pipeline-error.ts";
import { withLock } from "./with-lock.ts";

// ---------------------------------------------------------------------------
// Step contexts
// ---------------------------------------------------------------------------

/** Context for linear steps (no dependsOn). */
export interface StepContext<Input, Prev> {
  readonly input: Input;
  readonly prev: Prev;
  readonly workflowId: string;
  readonly attempt: number;
}

/** Context for DAG steps (with dependsOn). */
export interface DagStepContext<Input, Deps extends Record<string, unknown>> {
  readonly input: Input;
  readonly deps: Deps;
  readonly workflowId: string;
  readonly attempt: number;
}

/** Context for map steps (one per array element). */
export interface MapStepContext<Input> {
  readonly input: Input;
  readonly workflowId: string;
  readonly taskIndex: number;
  readonly attempt: number;
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
// WorkflowDefinition — reusable workflow template
// ---------------------------------------------------------------------------

/** A frozen workflow definition. Produced by `.build()` on WorkflowBuilder. */
export interface WorkflowDefinition<Input, Output> {
  readonly name: string;
  readonly version?: string;
  readonly storage: WorkflowStorage;
  readonly dag: WorkflowDAG;
  readonly idempotency?: IdempotencyConfig;
  /** Execute workflow synchronously — blocks until completion. */
  run(params: { workflowId: string; input: Input; force?: boolean }): Promise<Output>;
  /** Execute workflow synchronously — returns `{ data, error }` instead of throwing. */
  runSafe(params: {
    workflowId: string;
    input: Input;
    force?: boolean;
  }): Promise<{ data: Output; error: null } | { data: null; error: unknown }>;
  /**
   * Invoke as a child workflow — returns Pipeline for composition.
   * Automatically sets parentWorkflowId for tracking.
   *
   * @example
   * ```ts
   * .step("enrich", ({ prev }) =>
   *   enrichUser.invoke({
   *     workflowId: `enrich-${prev.id}`,
   *     input: { userId: prev.id },
   *   })
   * )
   * ```
   */
  invoke(params: {
    workflowId: string;
    input: Input;
    parentWorkflowId?: string;
  }): Pipeline<Output, StepError>;

  /**
   * Wait for a workflow to complete, polling the storage at intervals.
   * Resumes suspended workflows automatically on each poll.
   *
   * @example
   * ```ts
   * // Start workflow (may suspend at waitForSignal)
   * await kycVerification.runSafe({ workflowId, input });
   *
   * // Wait for completion (resumes on each poll if signal arrived)
   * const result = await kycVerification.waitForResult(workflowId, {
   *   input,
   *   intervalMs: 5_000,
   *   timeoutMs: 60_000,
   * });
   * ```
   */
  waitForResult(
    workflowId: string,
    params: {
      input: Input;
      intervalMs?: number;
      timeoutMs?: number;
    },
  ): Promise<Output>;

  /**
   * Get the current status of a workflow. Useful for status endpoints.
   *
   * @example
   * ```ts
   * // GET /kyc/status handler
   * const status = await kycVerification.getStatus(workflowId);
   * if (!status) return { status: 404 };
   * return { status: 200, body: status };
   * ```
   */
  getStatus(
    workflowId: string,
    params?: { includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<Output> | null>;

  /**
   * Start a workflow and return a handle for interacting with it.
   * With idempotency config: joins in-flight runs, respects TTL.
   * Without idempotency: blocks until completion, throws if locked.
   *
   * @example
   * ```ts
   * const handle = await myWorkflow.start("order-123", orderInput);
   * const status = await handle.status();
   * const result = await handle.result({ timeoutMs: 60_000 });
   * ```
   */
  start(workflowId: string, input: Input): Promise<WorkflowHandle<Output>>;
  /** Start with ID derived from input (requires idempotency.deriveId config). */
  start(input: Input): Promise<WorkflowHandle<Output>>;
}

/**
 * Handle to a running workflow. Returned by `workflow.start()`.
 */
export interface WorkflowHandle<Output> {
  readonly workflowId: string;

  /** Get the current workflow status. */
  status(params?: { includeStepResults?: boolean }): Promise<WorkflowStatusInfo<Output> | null>;

  /** Send a signal to the workflow (e.g. from a webhook). */
  signal(signalName: string, payload: unknown): Promise<void>;

  /** Wait for the workflow to complete. Resumes suspended workflows on each poll. */
  result(params?: { intervalMs?: number; timeoutMs?: number }): Promise<Output>;
}

export interface WorkflowStatusInfo<Output> {
  readonly state: "pending" | "running" | "completed" | "failed" | "suspended";
  readonly result?: Output;
  readonly error?: string;
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
}

// ---------------------------------------------------------------------------
// Step options
// ---------------------------------------------------------------------------

export type StepFailureStrategy<T> =
  | "fail"
  | "skip"
  | { fallback: (error: unknown) => T }
  | { handler: (error: unknown) => "retry" | "skip" | "fail" };

export interface StepOptions<T> {
  readonly codec?: Codec<T>;
  readonly show?: Show<T>;
  readonly timeoutMs?: number;
  /** Retry the entire step at the workflow layer. Same RetryPolicy as Pipeline.retry(). */
  readonly retry?: RetryPolicy<TaggedError>;
  /** What to do when the step fails (after retries exhausted). Default: "fail". */
  readonly onFailure?: StepFailureStrategy<T>;
  /**
   * Compensation function — undoes this step's side effects during saga rollback.
   * Only runs when a *later* step fails and the workflow triggers compensation.
   * Receives the step's successful result and the workflow input.
   */
  readonly compensate?: (params: {
    result: T;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, any> | Promise<void>;
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
  /** Retry policy for each compensation function. Default: no retry. */
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
  }) => Pipeline<void, any> | Promise<void>;
}

// ---------------------------------------------------------------------------
// Dispatch config — send specific steps to remote workers
// ---------------------------------------------------------------------------

export interface DispatchConfig {
  /** Step queue for dispatching tasks to remote workers. */
  stepQueue: import("../distributed/step-queue.ts").StepQueue;
  /**
   * Map step names to queue names. Unmatched steps execute locally.
   *
   * @example
   * ```ts
   * routing: { "transcribe": "gpu", "train-model": "gpu" }
   * ```
   */
  routing: Record<string, string>;
  /** How often to poll for dispatched step completion (ms). Default: 500. */
  pollIntervalMs?: number;
}

// ---------------------------------------------------------------------------
// match() — params and error type
// ---------------------------------------------------------------------------

/**
 * Thrown by `.match()` when no case applies and no `default` was provided.
 * Carries the step name, mode, and (for selector mode) the resolved key so
 * debugging prod failures doesn't require re-running the workflow.
 */
export class MatchError extends Data.TaggedError("MatchError")<{
  readonly stepName: string;
  readonly mode: "selector" | "predicate";
  readonly selectorKey?: string;
  readonly message: string;
}> {}

type MatchCaseFn<Input, Current, Output, E extends TaggedError> = (
  ctx: StepContext<Input, Current>,
) => Pipeline<Output, E>;

/**
 * Two-mode params for `.match()`:
 * - **Selector**: `on` returns a key; `cases` is a record keyed by that string.
 * - **Predicate**: `cases` is an array of `{when, then}`; first match wins.
 *
 * `default` is optional in both modes; missing match throws `MatchError`.
 */
export type MatchParams<Input, Current, Output, E extends TaggedError> =
  | {
      readonly on: (value: Current) => string;
      readonly cases: Record<string, MatchCaseFn<Input, Current, Output, E>>;
      readonly default?: MatchCaseFn<Input, Current, Output, E>;
    }
  | {
      readonly cases: ReadonlyArray<{
        /**
         * Optional human-readable label for this case. Surfaced in DAG
         * visualization (Mermaid/DOT edge labels). Without a label, the case
         * shows up as `case[N]` in diagrams — useful for debugging but easy
         * to lose track of after refactors. Strongly recommended.
         */
        readonly label?: string;
        readonly when: (value: Current) => boolean;
        readonly then: MatchCaseFn<Input, Current, Output, E>;
      }>;
      readonly default?: MatchCaseFn<Input, Current, Output, E>;
    };

/**
 * Build viz metadata (case labels) from MatchParams for the DAG. The input
 * type is loose because this only reads the shape (`cases` keys/labels) — the
 * actual generic parameters of MatchParams are irrelevant to visualization.
 */
function matchVizMeta(params: {
  on?: unknown;
  cases: Record<string, unknown> | ReadonlyArray<{ label?: string }>;
  default?: unknown;
}): { cases: readonly string[]; hasDefault: boolean } {
  if (typeof params.on === "function") {
    return {
      cases: Object.keys(params.cases as Record<string, unknown>),
      hasDefault: params.default !== undefined,
    };
  }
  const arrayCases = params.cases as ReadonlyArray<{ label?: string }>;
  return {
    cases: arrayCases.map((c, i) => c.label ?? `case[${i}]`),
    hasDefault: params.default !== undefined,
  };
}

/** Resolve which case fires for `prev`. Throws `MatchError` if none + no default. */
function pickMatchBranch<Input, Current, Output, E extends TaggedError>(
  params: MatchParams<Input, Current, Output, E>,
  prev: Current,
  stepName: string,
): MatchCaseFn<Input, Current, Output, E> {
  // Selector mode (`on` is a function, `cases` is a record).
  if ("on" in params && typeof params.on === "function") {
    const key = params.on(prev);
    const hit = (params.cases as Record<string, MatchCaseFn<Input, Current, Output, E>>)[key];
    if (hit) return hit;
    if (params.default) return params.default;
    throw new MatchError({
      stepName,
      mode: "selector",
      selectorKey: key,
      message: `match step "${stepName}" — no case for selector key "${key}" and no default`,
    });
  }

  // Predicate mode (`cases` is an array).
  const cases = params.cases as ReadonlyArray<{
    when: (v: Current) => boolean;
    then: MatchCaseFn<Input, Current, Output, E>;
  }>;
  for (const c of cases) {
    if (c.when(prev)) return c.then;
  }
  if (params.default) return params.default;
  throw new MatchError({
    stepName,
    mode: "predicate",
    message: `match step "${stepName}" — no predicate matched and no default`,
  });
}

// ---------------------------------------------------------------------------
// Internal step definition
// ---------------------------------------------------------------------------

type StepKind = "normal" | "map" | "branch" | "match" | "sleep" | "signal" | "journaled";

interface StepDefinition {
  readonly name: string;
  readonly dependsOn: string[];
  readonly kind: StepKind;
  readonly execute: (params: ExecuteParams) => Pipeline<unknown, TaggedError>;
  readonly codec: Codec<unknown>;
  readonly timeoutMs?: number;
  readonly retry?: RetryPolicy<TaggedError>;
  readonly onFailure?: StepFailureStrategy<unknown>;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, any> | Promise<void>;
  /**
   * Static metadata for visualization/documentation. Currently set by `.match()`
   * to expose its case labels so the DAG can render decision branches; future
   * step kinds (e.g. branch with named arms) can populate it too. Runtime
   * execution does not read this — it's purely a visualization hint.
   */
  readonly viz?: {
    /** For `.match()` steps: the case labels (selector keys or predicate labels). */
    readonly cases?: readonly string[];
    /** For `.match()` steps: true if a `default` case exists. */
    readonly hasDefault?: boolean;
  };
}

interface ExecuteParams {
  readonly input: unknown;
  readonly results: Record<string, unknown>;
  readonly workflowId: string;
  readonly storage: WorkflowStorage;
  /** Mutable ref — incremented by the retry wrapper before each re-invocation. */
  readonly attemptRef: { current: number };
}

// ---------------------------------------------------------------------------
// Lock duration
// ---------------------------------------------------------------------------

const DEFAULT_LOCK_DURATION_MS = 30_000;

// ---------------------------------------------------------------------------
// WorkflowBuilder
// ---------------------------------------------------------------------------

export class WorkflowBuilder<
  Input,
  Steps extends Record<string, unknown> = {},
  Current = Input,
  Error extends TaggedError = never,
> {
  /** @internal */
  constructor(
    private readonly _name: string,
    private readonly _storage: WorkflowStorage,
    private readonly _steps: StepDefinition[],
    private readonly _lastStepName: string | null,
    private readonly _hooks?: WorkflowHooks,
    private readonly _type?: string,
    private readonly _metadata?: Record<string, unknown>,
    private readonly _retry?: RetryPolicy<TaggedError>,
    private readonly _compensateConfig?: CompensateConfig,
    private readonly _dlq?: Sinkable<FailedWorkflowRecord>,
    private readonly _dispatch?: DispatchConfig,
    private readonly _idempotency?: IdempotencyConfig,
    private readonly _version?: string,
    private readonly _timeoutMs?: number,
    /**
     * How to handle a `.run()` against a workflow row created with a
     * different `version`. Default: `"strict"` (throw WorkflowVersionMismatchError).
     * `"drain"` delegates the resume to the matching definition in
     * `_previousVersions` — requires that list to contain the stored version.
     */
    private readonly _onVersionMismatch: "strict" | "drain" = "strict",
    /**
     * Definitions of prior versions of this workflow, indexed by their own
     * `version` field. Used by `onVersionMismatch: "drain"` to resume
     * in-flight workflows with the exact code they were started on while
     * new workflows use the current definition.
     */
    private readonly _previousVersions?: ReadonlyArray<WorkflowDefinition<unknown, unknown>>,
    /**
     * Patch names active in this workflow version. `ctx.patched(name)` in a
     * journaled step returns `patches.includes(name)`. Using the drain
     * policy ensures each stored version's own patch list is authoritative —
     * no cross-version comparison needed.
     */
    private readonly _patches?: readonly string[],
  ) {}

  /** Resolve TTL for a given workflow status. Returns undefined if no TTL applies. */
  private _getIdempotencyTtl(status: string): number | undefined {
    if (!this._idempotency) return undefined;
    const ttl = this._idempotency.ttl;
    if (typeof ttl === "number") return ttl;
    if (status === "completed") return ttl.success;
    if (status === "failed") return ttl.failure;
    return undefined;
  }

  /** Set the workflow version. Used to detect code/state mismatch on resume. */
  version(v: string): WorkflowBuilder<Input, Steps, Current, Error> {
    return new WorkflowBuilder(
      this._name,
      this._storage,
      this._steps,
      this._lastStepName,
      this._hooks,
      this._type,
      this._metadata,
      this._retry,
      this._compensateConfig,
      this._dlq,
      this._dispatch,
      this._idempotency,
      v,
      this._timeoutMs,
      this._onVersionMismatch,
      this._previousVersions,
      this._patches,
    );
  }

  // ---------------------------------------------------------------------------
  // Linear step — Pipeline-returning
  // ---------------------------------------------------------------------------

  step<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    fn: (ctx: StepContext<Input, Current>) => Pipeline<Output, E2>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error | E2>;

  // ---------------------------------------------------------------------------
  // DAG step — Pipeline-returning
  // ---------------------------------------------------------------------------

  step<
    Name extends string,
    DependsOn extends (keyof Steps & string)[],
    Output,
    E2 extends TaggedError = never,
  >(
    name: Name,
    config: { dependsOn: [...DependsOn] },
    fn: (ctx: DagStepContext<Input, Pick<Steps, DependsOn[number]>>) => Pipeline<Output, E2>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error | E2>;

  // ---------------------------------------------------------------------------
  // Overload implementation
  // ---------------------------------------------------------------------------

  step(
    name: string,
    fnOrConfig: ((ctx: any) => Pipeline<unknown, any>) | { dependsOn: string[] },
    fnOrOptions?: ((ctx: any) => Pipeline<unknown, any>) | StepOptions<unknown>,
    maybeOptions?: StepOptions<unknown>,
  ): WorkflowBuilder<Input, any, any, any> {
    let dependsOn: string[];
    let fn: (ctx: any) => Pipeline<unknown, any>;
    let options: StepOptions<unknown> | undefined;

    if (typeof fnOrConfig === "function") {
      dependsOn = this._lastStepName ? [this._lastStepName] : [];
      fn = fnOrConfig;
      options = fnOrOptions as StepOptions<unknown> | undefined;
    } else {
      dependsOn = fnOrConfig.dependsOn;
      fn = fnOrOptions as (ctx: any) => Pipeline<unknown, any>;
      options = maybeOptions;
    }

    return this._addStep({
      name,
      dependsOn,
      fn,
      isLinear: typeof fnOrConfig === "function",
      kind: "normal",
      options,
    });
  }

  // ---------------------------------------------------------------------------
  // Linear stepAsync — Promise-returning convenience
  // ---------------------------------------------------------------------------

  stepAsync<Name extends string, Output>(
    name: Name,
    fn: (ctx: StepContext<Input, Current>) => Promise<Output>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error>;

  // ---------------------------------------------------------------------------
  // DAG stepAsync — Promise-returning convenience
  // ---------------------------------------------------------------------------

  stepAsync<Name extends string, DependsOn extends (keyof Steps & string)[], Output>(
    name: Name,
    config: { dependsOn: [...DependsOn] },
    fn: (ctx: DagStepContext<Input, Pick<Steps, DependsOn[number]>>) => Promise<Output>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error>;

  // ---------------------------------------------------------------------------
  // stepAsync overload implementation
  // ---------------------------------------------------------------------------

  stepAsync(
    name: string,
    fnOrConfig: ((ctx: any) => Promise<unknown>) | { dependsOn: string[] },
    fnOrOptions?: ((ctx: any) => Promise<unknown>) | StepOptions<unknown>,
    maybeOptions?: StepOptions<unknown>,
  ): WorkflowBuilder<Input, any, any, any> {
    let dependsOn: string[];
    let asyncFn: (ctx: any) => Promise<unknown>;
    let options: StepOptions<unknown> | undefined;

    if (typeof fnOrConfig === "function") {
      dependsOn = this._lastStepName ? [this._lastStepName] : [];
      asyncFn = fnOrConfig;
      options = fnOrOptions as StepOptions<unknown> | undefined;
    } else {
      dependsOn = fnOrConfig.dependsOn;
      asyncFn = fnOrOptions as (ctx: any) => Promise<unknown>;
      options = maybeOptions;
    }

    const wrappedFn = (ctx: any) => Pipeline.fromPromise(() => asyncFn(ctx));
    return this._addStep({
      name,
      dependsOn,
      fn: wrappedFn,
      isLinear: typeof fnOrConfig === "function",
      kind: "normal",
      options,
    });
  }

  // ---------------------------------------------------------------------------
  // mapOver — fan-out over array with per-element retry
  // ---------------------------------------------------------------------------

  mapOver<
    Name extends string,
    ArrayStep extends keyof Steps & string,
    Output,
    E2 extends TaggedError = never,
  >(
    name: Name,
    config: { array: ArrayStep; concurrency?: number },
    fn: (
      element: Steps[ArrayStep] extends readonly (infer U)[] ? U : never,
      ctx: MapStepContext<Input>,
    ) => Pipeline<Output, E2>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output[]>, Output[], Error | E2> {
    this._validateName(name);

    const codec = (options?.codec ?? JsonCodec) as Codec<unknown>;
    const concurrency = config.concurrency ?? Infinity;

    const stepDef: StepDefinition = {
      name,
      dependsOn: [config.array],
      kind: "map",
      codec,
      execute: (params) => {
        const sourceArray = params.results[config.array] as unknown[];
        if (!Array.isArray(sourceArray)) {
          return Pipeline.fail(
            new StepError({
              workflowId: params.workflowId,
              stepName: name,
              message: `mapOver source "${config.array}" is not an array`,
            }),
          ) as Pipeline<unknown, TaggedError>;
        }

        return Pipeline.forEach(
          sourceArray.map((element, taskIndex) => ({ element, taskIndex })),
          (item) => {
            const ctx: MapStepContext<unknown> = {
              input: params.input,
              workflowId: params.workflowId,
              taskIndex: item.taskIndex,
              attempt: 1,
            };
            return (fn as any)(item.element, ctx).tap(async (result: unknown) => {
              await params.storage.saveTaskResult({
                workflowId: params.workflowId,
                stepName: name,
                taskIndex: item.taskIndex,
                result: codec.encode(result),
              });
            });
          },
          { concurrency },
        ) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // mapOverAsync — Promise-returning convenience for mapOver
  // ---------------------------------------------------------------------------

  mapOverAsync<Name extends string, ArrayStep extends keyof Steps & string, Output>(
    name: Name,
    config: { array: ArrayStep; concurrency?: number },
    fn: (
      element: Steps[ArrayStep] extends readonly (infer U)[] ? U : never,
      ctx: MapStepContext<Input>,
    ) => Promise<Output>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output[]>, Output[], Error> {
    const wrappedFn = (element: any, ctx: MapStepContext<Input>) =>
      Pipeline.fromPromise(() => fn(element, ctx));
    return this.mapOver(name, config, wrappedFn as any, options) as any;
  }

  // ---------------------------------------------------------------------------
  // branch — conditional paths
  // ---------------------------------------------------------------------------

  branch<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    params: {
      condition: (value: Current) => boolean;
      ifTrue: (ctx: StepContext<Input, Current>) => Pipeline<Output, E2>;
      ifFalse: (ctx: StepContext<Input, Current>) => Pipeline<Output, E2>;
    },
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error | E2> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? JsonCodec) as Codec<unknown>;

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "branch",
      codec,
      execute: (execParams) => {
        const prevStepName = dependsOn[0];
        const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
        const ctx: StepContext<unknown, unknown> = {
          input: execParams.input,
          prev,
          workflowId: execParams.workflowId,
          attempt: 1,
        };
        const branch = params.condition(prev as Current) ? params.ifTrue : params.ifFalse;
        return branch(ctx as any) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // journaled — generator body with per-activity replay (promin-0kt / Phase 1)
  // ---------------------------------------------------------------------------

  /**
   * Add a journaled step — a step whose body is a generator, with each
   * `yield* ctx.activity(name, fn)` journaled as a checkpoint. On retry or
   * replay within the step, already-journaled activities return their
   * recorded value without re-executing; only un-journaled activities run.
   *
   * Requires the configured `WorkflowStorage` to also implement
   * `ActivityJournalStorage` (InMemoryWorkflowStorage and
   * PostgresWorkflowStorage do). `.build()` throws otherwise.
   *
   * ```typescript
   * workflow({ name: "signup", storage })
   *   .step("load", ({ input }) => Pipeline.succeed(input))
   *   .journaled("create-and-notify", function*(ctx, prev) {
   *     const user = yield* ctx.activity("create", () => createUser(prev))
   *     const email = yield* ctx.activity("send-email", () => sendEmail(user))
   *     return { user, email }
   *   })
   * ```
   *
   * Why a generator (not async): the body signature rejects bare `await` at
   * compile time, so every side effect flows through `ctx.activity()` — the
   * only thing that writes to the journal. Without this constraint, a bare
   * `await fetch(...)` in the body silently re-fires on replay with a
   * different result than the journaled one. That's the footgun we prevent.
   */
  journaled<Name extends string, Output>(
    name: Name,
    body: JournaledStepBody<Input, Current, Output>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<
    Input,
    Steps & Record<Name, Output>,
    Output,
    Error | JournalStorageMissingError
  > {
    this._validateName(name);
    if (!isActivityJournalStorage(this._storage)) {
      throw new JournalStorageMissingError(name);
    }

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? JsonCodec) as Codec<unknown>;
    const journalStorage = this._storage as WorkflowStorage & ActivityJournalStorage;

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "journaled",
      codec,
      execute: (execParams) => {
        const prevStepName = dependsOn[0];
        const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
        const builderVersion = this._version;
        const builderPatches = this._patches;
        return Pipeline.fromPromise(() =>
          runJournaledStep<Input, Current, Output>({
            input: execParams.input as Input,
            prev: prev as Current,
            workflowId: execParams.workflowId,
            stepName: name,
            storage: journalStorage,
            workflowStorage: execParams.storage,
            workflowVersion: builderVersion,
            patches: builderPatches,
            body,
          }),
        ) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // match — multi-way branching (selector or first-matching-predicate)
  // ---------------------------------------------------------------------------

  /**
   * Multi-way conditional routing. Two modes — pick whichever matches your data:
   *
   * **Selector mode** — like `switch (key)`. `on` returns a string key that
   * selects from `cases`. `default` is optional; missing key throws `MatchError`.
   *
   * ```typescript
   * .match("route", {
   *   on: (order) => order.type,
   *   cases: {
   *     express: ({ prev }) => Pipeline.fromPromise(() => expressShip(prev)),
   *     standard: ({ prev }) => Pipeline.fromPromise(() => standardShip(prev)),
   *     freight: ({ prev }) => Pipeline.fromPromise(() => freightShip(prev)),
   *   },
   *   default: ({ prev }) => Pipeline.fromPromise(() => standardShip(prev)),
   * })
   * ```
   *
   * **Predicate mode** — like `if/else if`. `cases` is an array; first
   * matching `when` wins. Order matters.
   *
   * ```typescript
   * .match("route", {
   *   cases: [
   *     { when: (o) => o.total > 10_000, then: ({ prev }) => Pipeline.fromPromise(() => vipProcess(prev)) },
   *     { when: (o) => o.type === "express", then: ({ prev }) => Pipeline.fromPromise(() => expressShip(prev)) },
   *   ],
   *   default: ({ prev }) => Pipeline.fromPromise(() => standardShip(prev)),
   * })
   * ```
   *
   * Output type is inferred as the union of all case Pipeline outputs (or the
   * common type when they all match). Match contributes one node to the DAG;
   * deterministic from `prev` so replay re-runs the same case.
   */
  match<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    params: MatchParams<Input, Current, Output, E2>,
    options?: StepOptions<Output>,
  ): WorkflowBuilder<Input, Steps & Record<Name, Output>, Output, Error | E2 | MatchError> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? JsonCodec) as Codec<unknown>;

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "match",
      codec,
      execute: (execParams) => {
        const prevStepName = dependsOn[0];
        const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
        const ctx: StepContext<unknown, unknown> = {
          input: execParams.input,
          prev,
          workflowId: execParams.workflowId,
          attempt: 1,
        };

        const branch = pickMatchBranch(params, prev as Current, name);
        return branch(ctx as any) as Pipeline<unknown, TaggedError>;
      },
      viz: matchVizMeta(params),
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // subworkflow — invoke a child workflow as a step
  // ---------------------------------------------------------------------------

  /**
   * Invoke a child workflow as a step. The child is independently durable.
   * Automatically sets parentWorkflowId for tracking.
   *
   * @example
   * ```ts
   * .subworkflow("enrich", enrichUser, {
   *   input: (prev) => ({ userId: prev.id }),
   *   workflowId: (prev) => `enrich-${prev.id}`,
   * })
   * ```
   */
  subworkflow<Name extends string, ChildInput, ChildOutput>(
    name: Name,
    definition: WorkflowDefinition<ChildInput, ChildOutput>,
    config: {
      input: (prev: Current) => ChildInput;
      workflowId: (prev: Current) => string;
    },
    options?: StepOptions<ChildOutput>,
  ): WorkflowBuilder<Input, Steps & Record<Name, ChildOutput>, ChildOutput, Error | StepError> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? JsonCodec) as Codec<unknown>;

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "normal",
      codec,
      execute: (execParams) => {
        const prevStepName = dependsOn[0];
        const prev = (
          prevStepName != null ? execParams.results[prevStepName] : execParams.input
        ) as Current;
        return definition.invoke({
          workflowId: config.workflowId(prev),
          input: config.input(prev),
          parentWorkflowId: execParams.workflowId,
        }) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // sleep — durable timer
  // ---------------------------------------------------------------------------

  sleep(
    name: string,
    ms: number,
  ): WorkflowBuilder<Input, Steps, Current, Error | WorkflowSuspendedError> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "sleep",
      codec: JsonCodec,
      execute: (params) => {
        const eff = Effect.gen(function* () {
          const state = yield* Effect.promise(() => params.storage.loadWorkflow(params.workflowId));
          const stepState = state?.steps[name];

          if (stepState?.status === "sleeping" && stepState.wakeAt) {
            if (new Date() >= stepState.wakeAt) {
              return undefined;
            }
            return yield* Effect.fail(
              new WorkflowSuspendedError({
                workflowId: params.workflowId,
                stepName: name,
                reason: "sleep",
                message: `Sleeping until ${stepState.wakeAt.toISOString()}`,
              }),
            );
          }

          const wakeAt = new Date(Date.now() + ms);
          yield* Effect.promise(() =>
            params.storage.suspendWorkflow(params.workflowId, name, {
              status: "sleeping",
              stepType: "sleep",
              wakeAt,
            }),
          );
          return yield* Effect.fail(
            new WorkflowSuspendedError({
              workflowId: params.workflowId,
              stepName: name,
              reason: "sleep",
              message: `Sleeping until ${wakeAt.toISOString()}`,
            }),
          );
        });
        return Pipeline.from(eff) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // waitForSignal — wait for external event
  // ---------------------------------------------------------------------------

  waitForSignal<T>(
    name: string,
    params: {
      signalName: string;
      timeoutMs?: number;
      codec?: Codec<T>;
    },
  ): WorkflowBuilder<
    Input,
    Steps & Record<string, T>,
    T,
    Error | WorkflowSuspendedError | WorkflowTimeoutError
  > {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (params.codec ?? JsonCodec) as Codec<unknown>;
    const signalName = params.signalName;
    const timeoutMs = params.timeoutMs;

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "signal",
      codec,
      execute: (execParams) => {
        const eff = Effect.gen(function* () {
          // Check if signal has been delivered
          const signals = yield* Effect.promise(() =>
            execParams.storage.loadSignals(execParams.workflowId),
          );
          const signal = signals.find((s) => s.signalName === signalName);

          if (signal) {
            return codec.decode(signal.payload);
          }

          // Check if this is a re-entry with timeout
          const state = yield* Effect.promise(() =>
            execParams.storage.loadWorkflow(execParams.workflowId),
          );
          const stepState = state?.steps[name];

          if (stepState?.status === "waiting_for_signal" && stepState.signalTimeoutAt) {
            if (new Date() >= stepState.signalTimeoutAt) {
              return yield* Effect.fail(
                new WorkflowTimeoutError({
                  workflowId: execParams.workflowId,
                  stepName: name,
                  message: `Signal "${signalName}" timed out after ${timeoutMs}ms`,
                }),
              );
            }
          }

          // First execution or still waiting — suspend
          const signalTimeoutAt = timeoutMs != null ? new Date(Date.now() + timeoutMs) : undefined;
          yield* Effect.promise(() =>
            execParams.storage.suspendWorkflow(execParams.workflowId, name, {
              status: "waiting_for_signal",
              stepType: "signal",
              signalName,
              signalTimeoutAt,
            }),
          );
          return yield* Effect.fail(
            new WorkflowSuspendedError({
              workflowId: execParams.workflowId,
              stepName: name,
              reason: "signal",
              message: `Waiting for signal "${signalName}"`,
            }),
          );
        });
        return Pipeline.from(eff) as Pipeline<unknown, TaggedError>;
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
  }

  // ---------------------------------------------------------------------------
  // Pure transform (not checkpointed)
  // ---------------------------------------------------------------------------

  map<Output>(fn: (value: Current) => Output): WorkflowBuilder<Input, Steps, Output, Error> {
    if (this._steps.length === 0) {
      throw new WorkflowError({
        workflowId: "",
        message: "Cannot call .map() on a workflow with no steps",
      });
    }

    const lastStep = this._steps[this._steps.length - 1]!;
    const originalExecute = lastStep.execute;

    const transformedStep: StepDefinition = {
      ...lastStep,
      execute: (params) => {
        return originalExecute(params).map(fn as (v: unknown) => unknown);
      },
    };

    const newSteps = [...this._steps.slice(0, -1), transformedStep];
    return this._derive(newSteps, this._lastStepName) as any;
  }

  // ---------------------------------------------------------------------------
  // Terminal: run
  // ---------------------------------------------------------------------------

  async run(params: { workflowId: string; input: Input; force?: boolean }): Promise<Current> {
    const { workflowId, input, force } = params;
    const workflowStartTime = Date.now();
    const compensateTrigger = this._compensateConfig?.trigger ?? "after-retries";
    const maxWorkflowRetries =
      compensateTrigger === "immediate" ? 0 : (this._retry?.maxRetries ?? 0);
    const workflowRetryDelayMs = this._retry?.baseDelayMs ?? 1000;
    const idempotency = force ? undefined : this._idempotency;

    // Drain pre-check — if the stored workflow was created under a different
    // version and this definition has `onVersionMismatch: "drain"`, delegate
    // the whole run to the matching previousVersion definition. Stored
    // version is immutable per workflow, so this is race-safe.
    if (this._onVersionMismatch === "drain" && this._version) {
      const existing = await this._storage.loadWorkflow(workflowId);
      if (existing && existing.version !== this._version) {
        const previousDef = this._previousVersions?.find((d) => d.version === existing.version);
        if (!previousDef) {
          throw new WorkflowVersionMismatchError({
            workflowId,
            expected: this._version,
            actual: existing.version ?? "(none)",
            message:
              `Workflow "${workflowId}" was created with version "${existing.version ?? "(none)"}" ` +
              `but current code is version "${this._version}". ` +
              `onVersionMismatch is "drain" but no matching previousVersion was registered.`,
          });
        }
        return previousDef.run({ workflowId, input, force }) as Promise<Current>;
      }
    }

    // 0. Idempotency check — return cached result if within TTL
    if (idempotency) {
      const existing = await this._storage.loadWorkflow(workflowId);
      if (existing?.completedAt) {
        const elapsed = Date.now() - existing.completedAt.getTime();
        const ttl = this._getIdempotencyTtl(existing.status);
        if (ttl !== undefined && elapsed < ttl) {
          if (existing.status === "completed") return existing.result as Current;
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
      storage: this._storage,
      workflowId,
      options: { lockDurationMs: DEFAULT_LOCK_DURATION_MS },
      fn: async () => {
        // 2. Load or create workflow state
        let state = await this._storage.loadWorkflow(workflowId);

        // Double-check idempotency after lock — prevents race
        if (idempotency && state?.completedAt) {
          const elapsed = Date.now() - state.completedAt.getTime();
          const ttl = this._getIdempotencyTtl(state.status);
          if (ttl !== undefined && elapsed < ttl) {
            if (state.status === "completed") return state.result as Current;
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
            await this._storage.startFreshRun(workflowId);
            state = await this._storage.loadWorkflow(workflowId);
          }
        }

        if (!state) {
          const createResult = await this._storage.createWorkflow({
            workflowId,
            workflowName: this._name,
            input,
            workflowType: this._type,
            metadata: this._metadata,
            version: this._version,
          });
          if (!createResult.created) {
            // Race: another caller created the workflow between our load and create
            state = createResult.existing;
          } else {
            state = await this._storage.loadWorkflow(workflowId);
          }
        } else if (this._version) {
          // Version mismatch check — only when builder explicitly sets a version
          const storedVersion = state.version;
          if (storedVersion !== this._version) {
            throw new WorkflowVersionMismatchError({
              workflowId,
              expected: this._version,
              actual: storedVersion ?? "(none)",
              message:
                `Workflow "${workflowId}" was created with version "${storedVersion ?? "(none)"}" ` +
                `but current code is version "${this._version}". ` +
                `To resume this workflow, either use \`onVersionMismatch: "drain"\` + ` +
                `\`previousVersions: [v${storedVersion ?? "N"}]\` on the workflow config, or ` +
                `register both versions in a WorkflowVersionRegistry.`,
            });
          }
        }

        // 3. Validate DAG
        const dagNodes: DagNode[] = this._steps.map((s) => ({
          name: s.name,
          dependsOn: s.dependsOn,
        }));
        topologicalSort({ nodes: dagNodes, workflowId });

        // 4. Execute DAG with workflow-level retry
        let lastStepError: unknown = null;
        // Shared across workflow retries so attempt counters keep incrementing
        const stepAttempts = new Map<string, number>();

        for (let workflowAttempt = 0; workflowAttempt <= maxWorkflowRetries; workflowAttempt++) {
          // On retry, wait before re-attempting
          if (workflowAttempt > 0) {
            const delay = workflowRetryDelayMs * Math.pow(2, workflowAttempt - 1);
            await new Promise((r) => setTimeout(r, delay));
          }

          const dagResult = await this._executeDag({
            workflowId,
            input,
            dagNodes,
            state,
            workflowStartTime,
            stepAttempts,
            deadlineMs: this._timeoutMs != null ? workflowStartTime + this._timeoutMs : undefined,
          });

          if (dagResult.success) {
            // 5. Complete workflow
            const finalResult = dagResult.result;
            await this._storage.completeWorkflow(workflowId, finalResult);
            await this._hooks?.onWorkflowComplete?.({
              workflowId,
              result: finalResult,
              durationMs: Date.now() - workflowStartTime,
            });
            return finalResult as Current;
          }

          // DAG failed — suspension errors always propagate immediately
          if (dagResult.suspension) {
            throw dagResult.error;
          }

          lastStepError = dagResult.error;

          // Check if this error is retryable (workflow-level `when` predicate)
          const shouldRetry =
            workflowAttempt < maxWorkflowRetries &&
            (!this._retry?.when || this._retry.when(dagResult.error as TaggedError));

          if (shouldRetry) {
            // Reload state to pick up checkpointed steps
            state = await this._storage.loadWorkflow(workflowId);
          } else {
            // Not retryable or retries exhausted — break to compensation
            break;
          }
        }

        // All workflow retries exhausted — run compensation cascade
        const compensationReport = await this._compensate({ workflowId, input, dagNodes });

        // Fire workflow-level onComplete callback
        if (this._compensateConfig?.onComplete) {
          try {
            const result = this._compensateConfig.onComplete({
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
        await this._storage.failWorkflow(workflowId, errorMsg);
        await this._hooks?.onWorkflowFailure?.({
          workflowId,
          error: errorMsg,
          durationMs: Date.now() - workflowStartTime,
        });

        // Publish to DLQ
        if (this._dlq) {
          try {
            const failedState = await this._storage.loadWorkflow(workflowId);
            await this._dlq.publish({
              workflowId,
              workflowName: this._name,
              input,
              error: errorMsg,
              failedAt: new Date(),
              steps: failedState?.steps ?? {},
              compensatedSteps: compensationReport.compensated,
              failedCompensations: compensationReport.failed.map((f) => ({
                stepName: f.stepName,
                error: f.error instanceof Error ? f.error.message : String(f.error),
              })),
              metadata: this._metadata,
            });
          } catch {
            // DLQ failure is swallowed — the original error is more important
          }
        }

        throw lastStepError;
      },
    });
  }

  // ---------------------------------------------------------------------------
  // DAG execution (extracted from run for retry loop)
  // ---------------------------------------------------------------------------

  private async _executeDag(params: {
    workflowId: string;
    input: Input;
    dagNodes: DagNode[];
    state: import("./workflow-state.ts").WorkflowState | null;
    workflowStartTime: number;
    /** Tracks attempt numbers per step — shared across workflow retries so counters keep incrementing. */
    stepAttempts: Map<string, number>;
    /** Workflow-level deadline (absolute timestamp). Steps completing after this fail the workflow. */
    deadlineMs?: number;
  }): Promise<
    { success: true; result: unknown } | { success: false; error: unknown; suspension: boolean }
  > {
    const { workflowId, input, dagNodes, state } = params;
    const results: Record<string, unknown> = {};

    // Load previously completed step results
    if (state) {
      for (const [stepName, stepState] of Object.entries(state.steps)) {
        if (stepState.status === "completed") {
          results[stepName] = stepState.result;
        }
      }
    }

    const completed = new Set(Object.keys(results));
    const running = new Set<string>();

    while (completed.size < this._steps.length) {
      // Check workflow-level deadline before each batch
      if (params.deadlineMs != null && Date.now() > params.deadlineMs) {
        return {
          success: false,
          error: new WorkflowDeadlineError({
            workflowId,
            timeoutMs: this._timeoutMs!,
            message: `Workflow "${workflowId}" exceeded global deadline of ${this._timeoutMs}ms`,
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
      const dispatchRouting = this._dispatch?.routing ?? {};
      const localReady: string[] = [];
      const dispatchReady: { name: string; queue: string }[] = [];

      for (const name of ready) {
        const queue = dispatchRouting[name];
        if (queue && this._dispatch) {
          dispatchReady.push({ name, queue });
        } else {
          localReady.push(name);
        }
      }

      // Dispatch remote steps — enqueue and poll until completed
      for (const { name, queue } of dispatchReady) {
        await this._dispatch!.stepQueue.enqueue({
          workflowId,
          stepName: name,
          queue,
          input,
          prevResults: { ...results },
        });
        // Poll until the worker completes this step
        const pollMs = this._dispatch!.pollIntervalMs ?? 500;
        while (true) {
          await new Promise((r) => setTimeout(r, pollMs));
          const currentState = await this._storage.loadWorkflow(workflowId);
          const stepState = currentState?.steps[name];
          if (stepState?.status === "completed") {
            results[name] = stepState.result;
            completed.add(name);
            running.delete(name);
            await this._hooks?.onStepComplete?.({
              workflowId,
              stepName: name,
              result: stepState.result,
              durationMs: stepState.durationMs ?? 0,
            });
            break;
          }
          if (stepState?.status === "failed") {
            const errorMsg = stepState.error ?? "Remote step failed";
            await this._hooks?.onStepFailure?.({
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
      const readySteps = localReady.map((name) => this._steps.find((s) => s.name === name)!);

      const pipeline = Pipeline.all(
        ...readySteps.map((stepDef) => {
          const startedAt = new Date();
          const startTime = startedAt.getTime();

          // Get or initialize attempt counter for this step (persists across workflow retries)
          const currentAttemptForStep = params.stepAttempts.get(stepDef.name) ?? 0;
          const attemptRef = { current: currentAttemptForStep + 1 };

          // Raw step execution — wrapped in suspend so retry re-invokes the step fn.
          // attemptRef tracks the attempt number; incremented each invocation so
          // step retries and workflow retries both see monotonically increasing attempts.
          let raw: Pipeline<unknown, TaggedError> = Pipeline.from(
            Effect.suspend(() => {
              const currentAttempt = attemptRef.current;
              attemptRef.current = currentAttempt + 1;
              // Write back to shared map so workflow retries pick up the right count
              params.stepAttempts.set(stepDef.name, currentAttempt);
              return stepDef.execute({
                input,
                results,
                workflowId,
                storage: this._storage,
                attemptRef: { current: currentAttempt },
              }).effect;
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

          // Map to step result
          return raw.map((result) => {
            const encoded = stepDef.codec.encode(result);
            return {
              name: stepDef.name,
              result: encoded,
              durationMs: Date.now() - startTime,
              startedAt,
            };
          });
        }),
      );

      const { data: stepResults, error: stepError } = await pipeline.runSafe();

      if (stepError) {
        const tag = (stepError as TaggedError)._tag;

        // Suspension errors propagate without failing the workflow
        if (tag === "WorkflowSuspendedError") {
          return { success: false, error: stepError, suspension: true };
        }

        // Record step failure
        const stepName =
          tag === "StepError"
            ? (stepError as StepError).stepName
            : tag === "WorkflowTimeoutError"
              ? (stepError as WorkflowTimeoutError).stepName
              : tag === "StepTimeoutError"
                ? (stepError as StepTimeoutError).stepName
                : (ready[0] ?? "unknown");
        const errorMsg =
          stepError instanceof globalThis.Error ? stepError.message : String(stepError);
        const failStartedAt = new Date();
        await this._storage.saveStepFailure({
          workflowId,
          stepName,
          error: errorMsg,
          durationMs: 0,
          startedAt: failStartedAt,
        });
        if (isStepAttemptStorage(this._storage)) {
          await this._storage.saveStepAttempt({
            workflowId,
            stepName,
            attempt: params.stepAttempts.get(stepName) ?? 1,
            type: "execution",
            status: "failed",
            error: errorMsg,
            durationMs: 0,
            startedAt: failStartedAt,
            completedAt: new Date(),
          });
        }
        await this._hooks?.onStepFailure?.({
          workflowId,
          stepName,
          error: errorMsg,
          durationMs: 0,
        });

        return { success: false, error: stepError, suspension: false };
      }

      // Checkpoint each completed step
      for (const { name, result, durationMs, startedAt } of stepResults!) {
        await this._storage.saveStepResult({
          workflowId,
          stepName: name,
          result,
          durationMs,
          startedAt,
        });
        if (isStepAttemptStorage(this._storage)) {
          await this._storage.saveStepAttempt({
            workflowId,
            stepName: name,
            attempt: params.stepAttempts.get(name) ?? 1,
            type: "execution",
            status: "completed",
            result,
            durationMs,
            startedAt,
            completedAt: new Date(),
          });
        }
        await this._hooks?.onStepComplete?.({ workflowId, stepName: name, result, durationMs });
        results[name] = result;
        completed.add(name);
        running.delete(name);
      }

      // Check workflow-level deadline after steps complete
      if (
        params.deadlineMs != null &&
        Date.now() > params.deadlineMs &&
        completed.size < this._steps.length
      ) {
        return {
          success: false,
          error: new WorkflowDeadlineError({
            workflowId,
            timeoutMs: this._timeoutMs!,
            message: `Workflow "${workflowId}" exceeded global deadline of ${this._timeoutMs}ms`,
          }),
          suspension: false,
        };
      }
    }

    const lastStepName = this._steps[this._steps.length - 1]!.name;
    return { success: true, result: results[lastStepName] };
  }

  // ---------------------------------------------------------------------------
  // Compensation cascade — undo completed steps in reverse order
  // ---------------------------------------------------------------------------

  private async _compensate(params: {
    workflowId: string;
    input: Input;
    dagNodes: DagNode[];
  }): Promise<{
    compensated: string[];
    failed: { stepName: string; error: unknown }[];
  }> {
    const { workflowId, input } = params;
    const compensated: string[] = [];
    const failed: { stepName: string; error: unknown }[] = [];

    // Load current state to find completed steps
    const state = await this._storage.loadWorkflow(workflowId);
    if (!state) return { compensated, failed };

    // Find completed steps that have compensation functions, in reverse order
    const stepsToCompensate: { stepDef: StepDefinition; result: unknown }[] = [];
    // Reverse the step list order — last completed first
    for (let i = this._steps.length - 1; i >= 0; i--) {
      const stepDef = this._steps[i]!;
      const stepState = state.steps[stepDef.name];
      if (stepState?.status === "completed" && stepDef.compensate) {
        stepsToCompensate.push({ stepDef, result: stepState.result });
      }
    }

    // Run compensations sequentially in reverse order
    const retryConfig = this._compensateConfig?.retry;
    const maxCompRetries = retryConfig?.maxRetries ?? 0;
    const compRetryDelayMs = retryConfig?.baseDelayMs ?? 500;

    const recordsAttempts = isStepAttemptStorage(this._storage);

    for (const { stepDef, result } of stepsToCompensate) {
      for (let attempt = 0; attempt <= maxCompRetries; attempt++) {
        const compStartedAt = new Date();
        try {
          if (attempt > 0) {
            await new Promise((r) => setTimeout(r, compRetryDelayMs * Math.pow(2, attempt - 1)));
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
            await (this._storage as any).saveStepAttempt({
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "completed",
              durationMs: Date.now() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: new Date(),
            });
          }
          break;
        } catch (err) {
          if (recordsAttempts) {
            await (this._storage as any).saveStepAttempt({
              workflowId,
              stepName: stepDef.name,
              attempt: attempt + 1,
              type: "compensation",
              status: "failed",
              error: err instanceof Error ? err.message : String(err),
              durationMs: Date.now() - compStartedAt.getTime(),
              startedAt: compStartedAt,
              completedAt: new Date(),
            });
          }
          if (attempt === maxCompRetries) {
            failed.push({ stepName: stepDef.name, error: err });
          }
        }
      }
    }

    return { compensated, failed };
  }

  // ---------------------------------------------------------------------------
  // Terminal: runSafe
  // ---------------------------------------------------------------------------

  async runSafe(params: { workflowId: string; input: Input; force?: boolean }): Promise<
    | { data: Current; error: null }
    | {
        data: null;
        error:
          | Error
          | WorkflowError
          | StepError
          | WorkflowLockError
          | WorkflowSuspendedError
          | WorkflowTimeoutError
          | StepTimeoutError
          | WorkflowDeadlineError;
      }
  > {
    try {
      const data = await this.run(params);
      return { data, error: null };
    } catch (error) {
      return { data: null, error: error as any };
    }
  }

  // ---------------------------------------------------------------------------
  // Terminal: execute — run without requiring a workflowId (auto-generated)
  // ---------------------------------------------------------------------------

  /** Execute the workflow with an auto-generated workflowId. For non-durable flows. */
  async execute(input: Input): Promise<Current> {
    return this.run({ workflowId: crypto.randomUUID(), input });
  }

  /** Execute with auto-generated workflowId, returns { data, error }. */
  async executeSafe(input: Input): Promise<
    | { data: Current; error: null }
    | {
        data: null;
        error:
          | Error
          | WorkflowError
          | StepError
          | WorkflowLockError
          | WorkflowSuspendedError
          | WorkflowTimeoutError
          | StepTimeoutError
          | WorkflowDeadlineError;
      }
  > {
    return this.runSafe({ workflowId: crypto.randomUUID(), input });
  }

  // ---------------------------------------------------------------------------
  // build — freeze into a reusable WorkflowDefinition
  // ---------------------------------------------------------------------------

  build(options?: {
    idempotency?: IdempotencyConfig;
    /** Derive workflowId from input. Makes the ID deterministic — same input → same workflow. */
    deriveId?: (input: Input) => string;
  }): WorkflowDefinition<Input, Current> {
    const builder = options?.idempotency ? this._deriveWithIdempotency(options.idempotency) : this;
    const self = builder;
    const deriveId = options?.deriveId;
    return {
      name: self._name,
      version: self._version,
      storage: self._storage,
      dag: self.toJSON(),
      idempotency: self._idempotency,
      run: (params) => self.run(params),
      runSafe: (params) => self.runSafe(params) as any,
      invoke: (params) =>
        Pipeline.fromPromise(async () => {
          // If parentWorkflowId provided, store it in metadata
          if (params.parentWorkflowId) {
            const state = await self._storage.loadWorkflow(params.workflowId);
            if (!state) {
              // Best-effort create — if conflict, another caller already created it
              await self._storage.createWorkflow({
                workflowId: params.workflowId,
                workflowName: self._name,
                input: params.input,
                workflowType: self._type,
                parentWorkflowId: params.parentWorkflowId,
                metadata: self._metadata,
              });
            }
          }
          return self.run(params);
        }) as Pipeline<Current, StepError>,

      getStatus: async (workflowId, params) => {
        const state = await self._storage.loadWorkflow(workflowId);
        if (!state) return null;

        const includeResults = params?.includeStepResults ?? false;

        // Find the current/blocked step
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
          state: state.status === "compensating" ? ("failed" as const) : state.status,
          result: state.status === "completed" ? (state.result as Current) : undefined,
          error: state.error,
          currentStep,
          suspendedReason,
          steps,
          createdAt: state.createdAt,
          startedAt: state.startedAt,
          updatedAt: state.updatedAt,
        };
      },

      waitForResult: async (workflowId, params) => {
        const intervalMs = params.intervalMs ?? 5_000;
        const timeoutMs = params.timeoutMs ?? 60_000;
        const deadline = Date.now() + timeoutMs;

        while (Date.now() < deadline) {
          // Try to resume (picks up signals, completes sleep timers)
          const { data, error } = await self.runSafe({ workflowId, input: params.input });

          // Completed — return result
          if (data !== null) return data;

          // Failed (non-suspended) — throw
          if (error && (error as any)._tag !== "WorkflowSuspendedError") {
            throw error;
          }

          // Suspended — wait and retry
          await new Promise((r) => setTimeout(r, intervalMs));
        }

        throw new Error(`Workflow ${workflowId} did not complete within ${timeoutMs}ms`);
      },

      start: async (workflowIdOrInput: string | Input, maybeInput?: Input) => {
        // Resolve workflowId: explicit → deriveId(input) → error
        let workflowId: string;
        let input: Input;
        if (maybeInput !== undefined) {
          workflowId = workflowIdOrInput as string;
          input = maybeInput;
        } else if (deriveId) {
          input = workflowIdOrInput as Input;
          workflowId = deriveId(input);
        } else {
          // TODO: enforce at compile time via conditional return types on build()
          // so start(input) is only callable when deriveId is configured
          throw new Error("workflowId is required when deriveId is not configured");
        }

        const definition = self.build(
          self._idempotency ? { idempotency: self._idempotency } : undefined,
        );
        const existing = await self._storage.loadWorkflow(workflowId);
        const isRunning =
          existing?.status === "pending" ||
          existing?.status === "running" ||
          existing?.status === "suspended";
        const onInFlight = self._idempotency?.onInFlight ?? "reject";

        if (isRunning) {
          if (onInFlight === "reject") {
            throw new WorkflowLockError({
              workflowId,
              message: `Workflow "${workflowId}" is already running`,
            });
          }
          // onInFlight === "join" — return handle to existing run
        } else {
          // Not running — fire-and-forget execution
          definition.runSafe({ workflowId, input });
          await new Promise((r) => setTimeout(r, 0));
        }

        return {
          workflowId,
          status: (params) => definition.getStatus(workflowId, params),
          signal: (signalName, payload) =>
            self._storage.deliverSignal(workflowId, signalName, payload),
          result: async (params) => {
            const intervalMs = params?.intervalMs ?? 1_000;
            const timeoutMs = params?.timeoutMs ?? 60_000;
            const deadline = Date.now() + timeoutMs;

            while (Date.now() < deadline) {
              const state = await self._storage.loadWorkflow(workflowId);
              if (state?.status === "completed") return state.result as Current;
              if (state?.status === "failed") {
                throw new Error(state.error ?? `Workflow ${workflowId} failed`);
              }
              await new Promise((r) => setTimeout(r, intervalMs));
            }
            throw new Error(`Workflow ${workflowId} did not complete within ${timeoutMs}ms`);
          },
        };
      },
    };
  }

  // ---------------------------------------------------------------------------
  // DAG export
  // ---------------------------------------------------------------------------

  /** Export the step DAG as a serializable JSON structure. */
  toJSON(): WorkflowDAG {
    return {
      name: this._name,
      steps: this._steps.map((s) => ({
        name: s.name,
        dependsOn: s.dependsOn,
        kind: s.kind,
        ...(s.viz?.cases ? { cases: s.viz.cases } : {}),
        ...(s.viz?.hasDefault ? { hasDefault: true } : {}),
      })),
    };
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  /** Create a new builder inheriting all config from this one. */
  private _derive(
    steps: StepDefinition[],
    lastStepName: string | null,
  ): WorkflowBuilder<Input, any, any, any> {
    return new WorkflowBuilder(
      this._name,
      this._storage,
      steps,
      lastStepName,
      this._hooks,
      this._type,
      this._metadata,
      this._retry,
      this._compensateConfig,
      this._dlq,
      this._dispatch,
      this._idempotency,
      this._version,
      this._timeoutMs,
      this._onVersionMismatch,
      this._previousVersions,
      this._patches,
    );
  }

  private _deriveWithIdempotency(
    idempotency: IdempotencyConfig,
  ): WorkflowBuilder<Input, Steps, Current, Error> {
    return new WorkflowBuilder(
      this._name,
      this._storage,
      this._steps,
      this._lastStepName,
      this._hooks,
      this._type,
      this._metadata,
      this._retry,
      this._compensateConfig,
      this._dlq,
      this._dispatch,
      idempotency,
      this._version,
      this._timeoutMs,
      this._onVersionMismatch,
      this._previousVersions,
      this._patches,
    );
  }

  private _validateName(name: string): void {
    if (this._steps.some((s) => s.name === name)) {
      throw new WorkflowError({
        workflowId: "",
        message: `Duplicate step name: "${name}"`,
      });
    }
  }

  private _addStep(params: {
    name: string;
    dependsOn: string[];
    fn: (ctx: any) => Pipeline<unknown, any>;
    isLinear: boolean;
    kind: StepKind;
    options?: StepOptions<unknown>;
  }): WorkflowBuilder<Input, any, any, any> {
    this._validateName(params.name);

    const codec = (params.options?.codec ?? JsonCodec) as Codec<unknown>;

    const stepDef: StepDefinition = {
      name: params.name,
      dependsOn: params.dependsOn,
      kind: params.kind,
      codec,
      timeoutMs: params.options?.timeoutMs,
      retry: params.options?.retry as RetryPolicy<TaggedError> | undefined,
      onFailure: params.options?.onFailure as StepFailureStrategy<unknown> | undefined,
      compensate: params.options?.compensate as StepDefinition["compensate"],
      execute: (execParams) => {
        if (params.isLinear) {
          const prevStepName = params.dependsOn[0];
          const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
          return params.fn({
            input: execParams.input,
            prev,
            workflowId: execParams.workflowId,
            attempt: execParams.attemptRef.current,
          });
        } else {
          const deps: Record<string, unknown> = {};
          for (const dep of params.dependsOn) {
            deps[dep] = execParams.results[dep];
          }
          return params.fn({
            input: execParams.input,
            deps,
            workflowId: execParams.workflowId,
            attempt: execParams.attemptRef.current,
          });
        }
      },
    };

    return this._derive([...this._steps, stepDef], params.name);
  }
}

// ---------------------------------------------------------------------------
// Constructor function
// ---------------------------------------------------------------------------

export function workflow<Input>(params: {
  name: string;
  storage: WorkflowStorage;
  hooks?: WorkflowHooks;
  type?: string;
  metadata?: Record<string, unknown>;
  /** Workflow-level retry policy. Re-runs from the failed step (completed steps are checkpointed). */
  retry?: RetryPolicy<TaggedError>;
  /** Compensation configuration — controls when and how saga rollback runs. */
  compensate?: CompensateConfig;
  /** Dead letter queue — failed workflows are published here after all retries + compensation. */
  dlq?: Sinkable<FailedWorkflowRecord>;
  /** Dispatch specific steps to remote workers instead of executing locally. */
  dispatch?: DispatchConfig;
  /** Workflow version tag — used to detect code/state mismatch on resume. Defaults to "1". */
  version?: string;
  /** Global deadline for the entire workflow execution (ms). Fails with WorkflowDeadlineError if exceeded. */
  timeoutMs?: number;
  /**
   * How to handle resumes of workflows created with a different `version`:
   * - `"strict"` (default) — throw `WorkflowVersionMismatchError`.
   * - `"drain"` — delegate the resume to the matching definition in
   *   `previousVersions`. Use this to let in-flight workflows finish on
   *   their original code while new workflows use the updated code.
   */
  onVersionMismatch?: "strict" | "drain";
  /**
   * Definitions of prior versions of this workflow. Consulted only when
   * `onVersionMismatch: "drain"` is set and a resume encounters a stored
   * version different from the current one. Each entry must have its own
   * `version` field set, or it can't be looked up.
   */
  previousVersions?: ReadonlyArray<WorkflowDefinition<unknown, unknown>>;
  /**
   * Patch names active in this workflow version. Inside a journaled step
   * body, `ctx.patched(name)` returns `patches.includes(name)`. Each
   * workflow version declares its own active set — no comparison logic.
   */
  patches?: readonly string[];
}): WorkflowBuilder<Input> {
  if (params.onVersionMismatch === "drain" && !params.previousVersions?.length) {
    throw new WorkflowError({
      workflowId: "",
      message: `workflow "${params.name}": onVersionMismatch: "drain" requires previousVersions to be non-empty`,
    });
  }
  if (params.previousVersions) {
    for (const prev of params.previousVersions) {
      if (!prev.version) {
        throw new WorkflowError({
          workflowId: "",
          message: `workflow "${params.name}": previousVersions entries must have a \`version\` field set`,
        });
      }
    }
  }

  return new WorkflowBuilder(
    params.name,
    params.storage,
    [],
    null,
    params.hooks,
    params.type,
    params.metadata,
    params.retry as RetryPolicy<TaggedError> | undefined,
    params.compensate,
    params.dlq,
    params.dispatch,
    undefined,
    params.version,
    params.timeoutMs,
    params.onVersionMismatch ?? "strict",
    params.previousVersions,
    params.patches,
  );
}

/**
 * Create a non-durable flow — same composition as workflow but without persistence.
 * Uses in-memory storage, no workflowId needed. For request handlers, scripts, and
 * compositions that don't need crash recovery.
 *
 * To make it durable later, change `flow()` to `workflow({ storage })`.
 *
 * @example
 * ```ts
 * const result = await flow<{ userId: string }>("process-user")
 *   .step("fetch", ({ input }) => api.get(`/users/${input.userId}`))
 *   .stepAsync("enrich", async ({ prev }) => enrichUser(prev))
 *   .execute({ userId: "123" });
 * ```
 */
export function flow<Input>(name: string, hooks?: WorkflowHooks): WorkflowBuilder<Input> {
  return new WorkflowBuilder(name, new InMemoryWorkflowStorage(), [], null, hooks);
}

// ---------------------------------------------------------------------------
// WorkflowDAG — serializable DAG structure
// ---------------------------------------------------------------------------

export interface WorkflowDAG {
  readonly name: string;
  readonly steps: readonly {
    readonly name: string;
    readonly dependsOn: readonly string[];
    readonly kind: string;
    /**
     * For decision nodes (currently `match`): the named case alternatives.
     * Visualizers render these as labeled outgoing edges so the static
     * diagram shows every possible path the workflow can take, not just
     * the one that fired in some run.
     */
    readonly cases?: readonly string[];
    /** True if a `default` fallback exists for the cases. */
    readonly hasDefault?: boolean;
  }[];
}

const sanitize = (s: string): string => s.replace(/[^a-zA-Z0-9]/g, "_");

/** Convert a WorkflowDAG to Mermaid graph syntax. */
export function dagToMermaid(dag: WorkflowDAG): string {
  const lines: string[] = ["graph LR"];
  for (const step of dag.steps) {
    const id = sanitize(step.name);
    // Decision nodes get the diamond shape `{...}`; others stay rectangular.
    const isDecision = step.kind === "match" && step.cases && step.cases.length > 0;
    lines.push(isDecision ? `    ${id}{"${step.name}"}` : `    ${id}["${step.name}"]`);

    for (const dep of step.dependsOn) {
      lines.push(`    ${sanitize(dep)} --> ${id}`);
    }

    // For match nodes, render each case as a labeled phantom node so the
    // alternatives are visible even though only one fires per run.
    if (isDecision) {
      const allCases = [...step.cases!, ...(step.hasDefault ? ["default"] : [])];
      for (const label of allCases) {
        const caseId = `${id}_${sanitize(label)}`;
        lines.push(`    ${caseId}(["${label}"])`);
        lines.push(`    ${id} -->|"${label}"| ${caseId}`);
      }
    }
  }
  return lines.join("\n");
}

/** Convert a WorkflowDAG to DOT (Graphviz) syntax. */
export function dagToDot(dag: WorkflowDAG): string {
  const lines: string[] = [`digraph "${dag.name}" {`];
  for (const step of dag.steps) {
    const isDecision = step.kind === "match" && step.cases && step.cases.length > 0;
    if (isDecision) {
      lines.push(`    "${step.name}" [shape=diamond];`);
    } else {
      lines.push(`    "${step.name}";`);
    }
    for (const dep of step.dependsOn) {
      lines.push(`    "${dep}" -> "${step.name}";`);
    }
    if (isDecision) {
      const allCases = [...step.cases!, ...(step.hasDefault ? ["default"] : [])];
      for (const label of allCases) {
        const caseNode = `${step.name}.${label}`;
        lines.push(`    "${caseNode}" [shape=ellipse];`);
        lines.push(`    "${step.name}" -> "${caseNode}" [label="${label}"];`);
      }
    }
  }
  lines.push("}");
  return lines.join("\n");
}
