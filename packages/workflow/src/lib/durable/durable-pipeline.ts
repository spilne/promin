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
import type { CacheStore, Codec } from "@promin/core";
import { LosslessJsonCodec } from "@promin/core";
import type { Show } from "@promin/core";
import type { Sinkable } from "@promin/core";
import type { FailedWorkflowRecord } from "./workflow-state.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { runWorkflowOrchestration } from "./workflow-runner.ts";
import {
  WorkflowError,
  StepError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  GuardError,
} from "./durable-pipeline-error.ts";

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
 */
export interface Workflow<Input, Output> {
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
}

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
  readonly retry?: RetryPolicy<TaggedError>;
  readonly compensateConfig?: CompensateConfig;
  readonly dlq?: Sinkable<FailedWorkflowRecord>;
  readonly dispatch?: DispatchConfig;
  readonly timeoutMs?: number;
  readonly onVersionMismatch: "strict" | "drain";
  readonly previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  readonly hooks?: WorkflowHooks;
}

/**
 * Handle to a running workflow. Returned by `WorkflowRunner.start()`.
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
  /** Skip this step when the predicate returns true. Skipped steps are recorded as 'skipped' and do not trigger compensation. */
  readonly skipWhen?: (prev: unknown) => boolean;
  /** Value to pass to the next step when this step is skipped. Defaults to prev. */
  readonly skipValue?: (prev: unknown) => T;
  /**
   * Capabilities this step requires from a worker. Only takes effect under
   * distributed execution (coordinator + workers); in-process `.run()`
   * ignores it. A worker claims this step only when its declared
   * `capabilities ⊇ needs`. Empty / omitted = any worker can run it.
   *
   * Example: a video-processing step that needs GPU hardware encodes this
   * declaratively:
   *   .step("transcode", handler, { needs: ["gpu"] })
   */
  readonly needs?: readonly string[];
  /**
   * Dispatch priority for distributed execution. Higher numbers claim
   * ahead of lower. Honoured by both `coordinator.submit`'s enqueueReady
   * and `wf.run`'s DispatchConfig-backed path. Ignored by pure in-process
   * `.run()` (no queue involved there). Range 0–10, default 5.
   */
  readonly priority?: number;
  /**
   * Skip-if-recent semantics: wrap the step body in a cache lookup keyed by
   * the user-supplied `key(ctx)`. On hit, return the cached value without
   * running the body. On miss, run the body and cache the result for
   * `ttlMs`. Cache failures never fail the workflow — they fall through to
   * a cache miss.
   */
  readonly cache?: StepCacheOption;
}

export interface StepCacheOption {
  /** Compute the cache key from step context (input + prev + workflowId). */
  readonly key: (ctx: StepContext<unknown, unknown>) => string;
  /** TTL in milliseconds. Entries expire after this window. */
  readonly ttlMs: number;
  /**
   * Cache backend. Any `CacheStore<string, unknown>` — the in-memory
   * `MemoryCache`, a `RedisCacheStore`, a `LayeredCache`, etc. No default:
   * pass the store explicitly so the retention domain is obvious.
   */
  readonly store: CacheStore<string, unknown>;
  /**
   * Key prefix. Defaults to the workflow name so caches for different
   * workflows don't collide when they share a backing store.
   */
  readonly namespace?: string;
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
   * Step names to dispatch remotely. Unlisted steps execute locally. Each
   * dispatched task carries the step's declared `needs` (from its
   * `StepOptions.needs`), so workers match by capability rather than
   * queue name.
   *
   * @example
   * ```ts
   * remoteSteps: ["transcribe", "train-model"]
   * ```
   */
  remoteSteps: readonly string[];
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

/** Tagged result of a `.match()` selection — carries both the branch fn and the
 *  audit label the runner persists on the step row. `label` is:
 *    - selector mode: the selector key or `"default"` when the default ran.
 *    - predicate mode: the matched case's `.label`, falling back to
 *      `case[N]` (matching the DAG viz fallback), or `"default"`. */
interface PickedMatchBranch<Input, Current, Output, E extends TaggedError> {
  readonly fn: MatchCaseFn<Input, Current, Output, E>;
  readonly mode: "selector" | "predicate";
  readonly label: string;
}

/** Resolve which case fires for `prev`. Throws `MatchError` if none + no default. */
function pickMatchBranch<Input, Current, Output, E extends TaggedError>(
  params: MatchParams<Input, Current, Output, E>,
  prev: Current,
  stepName: string,
): PickedMatchBranch<Input, Current, Output, E> {
  // Selector mode (`on` is a function, `cases` is a record).
  if ("on" in params && typeof params.on === "function") {
    const key = params.on(prev);
    const hit = (params.cases as Record<string, MatchCaseFn<Input, Current, Output, E>>)[key];
    if (hit) return { fn: hit, mode: "selector", label: key };
    if (params.default) return { fn: params.default, mode: "selector", label: "default" };
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
    label?: string;
  }>;
  for (let i = 0; i < cases.length; i++) {
    const c = cases[i]!;
    if (c.when(prev)) {
      return { fn: c.then, mode: "predicate", label: c.label ?? `case[${i}]` };
    }
  }
  if (params.default) return { fn: params.default, mode: "predicate", label: "default" };
  throw new MatchError({
    stepName,
    mode: "predicate",
    message: `match step "${stepName}" — no predicate matched and no default`,
  });
}

// ---------------------------------------------------------------------------
// Internal step definition
// ---------------------------------------------------------------------------

export type StepKind =
  | "normal"
  | "map"
  | "branch"
  | "match"
  | "sleep"
  | "signal"
  | "journaled"
  | "guard";

export interface StepDefinition {
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
  readonly skipWhen?: (prev: unknown) => boolean;
  readonly skipValue?: (prev: unknown) => unknown;
  /** Capability requirements copied onto the dispatched task by the coordinator. */
  readonly needs?: readonly string[];
  /** Dispatch priority copied onto the dispatched task. */
  readonly priority?: number;
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

export interface ExecuteParams {
  readonly input: unknown;
  readonly results: Record<string, unknown>;
  readonly workflowId: string;
  readonly storage: WorkflowStorage;
  /** Mutable ref — incremented by the retry wrapper before each re-invocation. */
  readonly attemptRef: { current: number };
  /**
   * Mutable slot for step-kind-specific audit metadata. `.match()` writes the
   * chosen case here; the runner forwards it to `saveStepResult` so the step
   * row carries a `{ matchCase, matchMode, ... }` record queryable from SQL.
   * Stays `undefined` for step kinds that don't produce audit data.
   */
  readonly metadataRef: { current?: Record<string, unknown> };
}

// ---------------------------------------------------------------------------
// Lock duration
// ---------------------------------------------------------------------------

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
    private readonly _previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>,
    /**
     * Patch names active in this workflow version. `ctx.patched(name)` in a
     * journaled step returns `patches.includes(name)`. Using the drain
     * policy ensures each stored version's own patch list is authoritative —
     * no cross-version comparison needed.
     */
    private readonly _patches?: readonly string[],
    /**
     * Pipeline-level default codec. Used when a step or activity doesn't
     * supply its own `options.codec`. Falls back to `LosslessJsonCodec` when
     * unset, so every boundary is lossless by default — but callers who want
     * a custom serializer (superjson, Zod schema, etc.) can set it once here
     * rather than threading the option into every step.
     */
    private readonly _defaultCodec?: Codec<unknown>,
    /**
     * Pipeline-level default for `ActivityOptions.payloadHash`. When `true`,
     * every 3-arg `ctx.activity(name, input, fn)` in every journaled step
     * hashes its input by default. Per-activity `payloadHash: false` still
     * wins locally. Off by default — payload hashing is optional and costs
     * one SHA-256 per activity invocation.
     */
    private readonly _defaultPayloadHash?: boolean,
  ) {}

  /** Resolve the codec a step or activity should use when no explicit override is set. */
  private _codec(): Codec<unknown> {
    return this._defaultCodec ?? LosslessJsonCodec;
  }

  /** Set the workflow version. Used to detect code/state mismatch on resume. */
  version(v: string): WorkflowBuilder<Input, Steps, Current, Error> {
    return new WorkflowBuilder(
      this._name,
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
      this._defaultCodec,
      this._defaultPayloadHash,
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

    const codec = (options?.codec ?? this._codec()) as Codec<unknown>;
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
  // guard — precondition assertion that fails fast (no retry)
  // ---------------------------------------------------------------------------

  guard<Name extends string>(
    name: Name,
    predicate: (prev: Current) => boolean,
    options?: { failureMessage?: string },
  ): WorkflowBuilder<Input, Steps & Record<Name, Current>, Current, Error | GuardError> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];

    const stepDef: StepDefinition = {
      name,
      dependsOn,
      kind: "guard",
      codec: this._codec() as Codec<unknown>,
      execute: (execParams) => {
        const prevStepName = dependsOn[0];
        const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
        if (predicate(prev as Current)) {
          return Pipeline.succeed(prev);
        }
        return Pipeline.fail(
          new GuardError({
            workflowId: execParams.workflowId,
            stepName: name,
            message: options?.failureMessage ?? `Guard "${name}" failed`,
          }),
        );
      },
    };

    return this._derive([...this._steps, stepDef], name) as any;
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
    const codec = (options?.codec ?? this._codec()) as Codec<unknown>;

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
  // journaled — generator body with per-activity replay
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

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? this._codec()) as Codec<unknown>;
    // Storage capability check is deferred to execute time — the builder has
    // no storage of its own; validation runs against the runner's storage
    // via `execParams.storage`.
    const getJournalStorage = (
      runtimeStorage: WorkflowStorage,
    ): WorkflowStorage & ActivityJournalStorage => {
      if (!isActivityJournalStorage(runtimeStorage)) {
        throw new JournalStorageMissingError(name);
      }
      return runtimeStorage as WorkflowStorage & ActivityJournalStorage;
    };

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
        const runtimeStorage = execParams.storage;
        return Pipeline.fromPromise(() =>
          runJournaledStep<Input, Current, Output>({
            input: execParams.input as Input,
            prev: prev as Current,
            workflowId: execParams.workflowId,
            stepName: name,
            storage: getJournalStorage(runtimeStorage),
            workflowStorage: runtimeStorage,
            workflowVersion: builderVersion,
            patches: builderPatches,
            codec,
            payloadHash: this._defaultPayloadHash,
            runChild: async ({
              workflow: childWorkflow,
              workflowId: childId,
              input: childInput,
            }) => {
              const childDef = (childWorkflow as any)._definition as any;
              await runtimeStorage
                .createWorkflow({
                  workflowId: childId,
                  workflowName: childWorkflow.name,
                  input: childInput,
                  parentWorkflowId: execParams.workflowId,
                  version: childWorkflow.version,
                  workflowType: childDef.type,
                  metadata: childDef.metadata,
                })
                .catch(() => undefined); // no-op on conflict (idempotent re-run)
              return runWorkflowOrchestration(
                {
                  storage: runtimeStorage,
                  name: childWorkflow.name,
                  version: childWorkflow.version,
                  idempotency: childWorkflow.idempotency,
                  type: childDef.type,
                  metadata: childDef.metadata,
                  steps: childDef.steps,
                  retry: childDef.retry,
                  compensateConfig: childDef.compensateConfig,
                  dlq: childDef.dlq,
                  dispatch: childDef.dispatch,
                  timeoutMs: childDef.timeoutMs,
                  onVersionMismatch: childDef.onVersionMismatch,
                  previousVersions: childDef.previousVersions,
                  hooks: childDef.hooks,
                },
                { workflowId: childId, input: childInput },
              );
            },
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
    const codec = (options?.codec ?? this._codec()) as Codec<unknown>;

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

        const picked = pickMatchBranch(params, prev as Current, name);
        // Record the chosen case BEFORE running it — even if the branch
        // throws, the metadata is still there to debug "which case fired".
        execParams.metadataRef.current = {
          matchCase: picked.label,
          matchMode: picked.mode,
        };
        return picked.fn(ctx as any) as Pipeline<unknown, TaggedError>;
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
    child: Workflow<ChildInput, ChildOutput>,
    config: {
      input: (prev: Current) => ChildInput;
      workflowId: (prev: Current) => string;
    },
    options?: StepOptions<ChildOutput>,
  ): WorkflowBuilder<Input, Steps & Record<Name, ChildOutput>, ChildOutput, Error | StepError> {
    this._validateName(name);

    const dependsOn = this._lastStepName ? [this._lastStepName] : [];
    const codec = (options?.codec ?? this._codec()) as Codec<unknown>;
    const childInternals = child._definition;

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
        const childWorkflowId = config.workflowId(prev);
        const childInput = config.input(prev);
        const parentWorkflowId = execParams.workflowId;
        const storage = execParams.storage;

        return Pipeline.fromPromise(async () => {
          // Seed the child row with the parent pointer before handing it to
          // the runner so downstream `listWorkflows({ parentId })` queries
          // and the coordinator's recovery see the relationship.
          const existing = await storage.loadWorkflow(childWorkflowId);
          if (!existing) {
            await storage.createWorkflow({
              workflowId: childWorkflowId,
              workflowName: child.name,
              input: childInput,
              workflowType: childInternals.type,
              parentWorkflowId,
              metadata: childInternals.metadata,
            });
          }
          return runWorkflowOrchestration(
            {
              storage,
              name: child.name,
              version: child.version,
              idempotency: child.idempotency,
              type: childInternals.type,
              metadata: childInternals.metadata,
              steps: childInternals.steps,
              retry: childInternals.retry,
              compensateConfig: childInternals.compensateConfig,
              dlq: childInternals.dlq,
              dispatch: childInternals.dispatch,
              timeoutMs: childInternals.timeoutMs,
              onVersionMismatch: childInternals.onVersionMismatch,
              previousVersions: childInternals.previousVersions,
              hooks: childInternals.hooks,
            },
            { workflowId: childWorkflowId, input: childInput },
          );
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
      codec: this._codec(),
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
    const codec = (params.codec ?? this._codec()) as Codec<unknown>;
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
  // Terminal: execute — build an ephemeral runner + workflow, run to completion
  // ---------------------------------------------------------------------------

  /**
   * Execute this workflow with an auto-generated workflowId against an
   * ephemeral in-memory storage. Convenient for non-durable flows, request
   * handlers, and scripts. Construct a `createWorkflowRunner({ storage })`
   * against a real backend when you need durability.
   */
  async execute(input: Input): Promise<Current> {
    const storage = new InMemoryWorkflowStorage();
    return runWorkflowOrchestration(
      {
        storage,
        name: this._name,
        version: this._version,
        type: this._type,
        metadata: this._metadata,
        steps: this._steps,
        retry: this._retry,
        compensateConfig: this._compensateConfig,
        dlq: this._dlq,
        dispatch: this._dispatch,
        idempotency: this._idempotency,
        timeoutMs: this._timeoutMs,
        onVersionMismatch: this._onVersionMismatch,
        previousVersions: this._previousVersions,
        hooks: this._hooks,
      },
      { workflowId: crypto.randomUUID(), input },
    ) as Promise<Current>;
  }

  // ---------------------------------------------------------------------------
  // build — freeze into a reusable Workflow definition
  // ---------------------------------------------------------------------------

  /**
   * Freeze this builder into a portable `Workflow` (pure data — no storage,
   * no run methods). Hand it to a `WorkflowRunner` to execute.
   *
   * This split keeps workflow definitions importable without dragging the
   * state backend along: one process (the coordinator, a registry) wires
   * storage, and other processes (submitters, HTTP handlers) work off the
   * bare definition.
   */
  build(options?: { idempotency?: IdempotencyConfig }): Workflow<Input, Current> {
    const source = options?.idempotency ? this._deriveWithIdempotency(options.idempotency) : this;
    return {
      name: source._name,
      version: source._version,
      dag: source.toJSON(),
      idempotency: source._idempotency,
      _definition: source._toDefinitionInternals(),
    };
  }

  /** @internal — project builder state into the Workflow._definition shape. */
  private _toDefinitionInternals(): WorkflowDefinitionInternals {
    return {
      steps: this._steps,
      type: this._type,
      metadata: this._metadata,
      retry: this._retry,
      compensateConfig: this._compensateConfig,
      dlq: this._dlq,
      dispatch: this._dispatch,
      timeoutMs: this._timeoutMs,
      onVersionMismatch: this._onVersionMismatch,
      previousVersions: this._previousVersions,
      hooks: this._hooks,
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
        ...(s.needs && s.needs.length > 0 ? { needs: s.needs } : {}),
        ...(s.priority !== undefined ? { priority: s.priority } : {}),
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
      this._defaultCodec,
      this._defaultPayloadHash,
    );
  }

  private _deriveWithIdempotency(
    idempotency: IdempotencyConfig,
  ): WorkflowBuilder<Input, Steps, Current, Error> {
    return new WorkflowBuilder(
      this._name,
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
      this._defaultCodec,
      this._defaultPayloadHash,
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

    const codec = (params.options?.codec ?? this._codec()) as Codec<unknown>;
    const workflowName = this._name;

    const stepDef: StepDefinition = {
      name: params.name,
      dependsOn: params.dependsOn,
      kind: params.kind,
      codec,
      timeoutMs: params.options?.timeoutMs,
      retry: params.options?.retry as RetryPolicy<TaggedError> | undefined,
      onFailure: params.options?.onFailure as StepFailureStrategy<unknown> | undefined,
      compensate: params.options?.compensate as StepDefinition["compensate"],
      skipWhen: params.options?.skipWhen as StepDefinition["skipWhen"],
      skipValue: params.options?.skipValue as StepDefinition["skipValue"],
      needs: params.options?.needs,
      priority: params.options?.priority,
      execute: (execParams) => {
        let ctx: StepContext<unknown, unknown> | DagStepContext<unknown, Record<string, unknown>>;
        if (params.isLinear) {
          const prevStepName = params.dependsOn[0];
          const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
          ctx = {
            input: execParams.input,
            prev,
            workflowId: execParams.workflowId,
            attempt: execParams.attemptRef.current,
          };
        } else {
          const deps: Record<string, unknown> = {};
          for (const dep of params.dependsOn) deps[dep] = execParams.results[dep];
          ctx = {
            input: execParams.input,
            deps,
            workflowId: execParams.workflowId,
            attempt: execParams.attemptRef.current,
          };
        }

        const cacheOption = params.options?.cache;
        if (!cacheOption) return params.fn(ctx);
        return wrapWithStepCache(
          cacheOption,
          ctx as StepContext<unknown, unknown>,
          () => params.fn(ctx),
          cacheOption.namespace ?? workflowName,
        );
      },
    };

    return this._derive([...this._steps, stepDef], params.name);
  }
}

// ---------------------------------------------------------------------------
// Step-level cache wrapper — runs before the step body, falls through to
// a cache miss on any cache error so storage hiccups never fail the workflow.
// ---------------------------------------------------------------------------

function wrapWithStepCache(
  cache: StepCacheOption,
  ctx: StepContext<unknown, unknown>,
  runBody: () => Pipeline<unknown, TaggedError>,
  namespace: string,
): Pipeline<unknown, TaggedError> {
  const cacheKey = `${namespace}:${cache.key(ctx)}`;

  // Lookup is expressed as a Pipeline so we can keep everything inside the
  // caller's error channel. A sentinel object marks "miss" so `undefined`
  // cached values are distinguishable from misses.
  const miss = Symbol("cache-miss");
  const lookup = Pipeline.fromPromise(async () => {
    try {
      const hit = await cache.store.get(cacheKey);
      return hit === undefined ? miss : hit;
    } catch {
      return miss;
    }
  });

  return lookup.flatMap((value) => {
    if (value !== miss) return Pipeline.succeed(value);
    // Miss — run the body, then write to cache on success. tapAsync blocks
    // until the write completes (so tests see cache state deterministically),
    // and we swallow write errors so cache backends can never fail a step.
    return runBody().tapAsync(async (result) => {
      try {
        await cache.store.set(cacheKey, result, cache.ttlMs);
      } catch {
        /* ignore cache write failures — ticket contract */
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Constructor function
// ---------------------------------------------------------------------------

export function workflow<Input>(params: {
  name: string;
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
  previousVersions?: ReadonlyArray<Workflow<unknown, unknown>>;
  /**
   * Patch names active in this workflow version. Inside a journaled step
   * body, `ctx.patched(name)` returns `patches.includes(name)`. Each
   * workflow version declares its own active set — no comparison logic.
   */
  patches?: readonly string[];
  /**
   * Default codec for every step and activity in this workflow. Individual
   * steps and activities can still override via their own `options.codec`.
   * Omit to use `LosslessJsonCodec` (Date / BigInt / Map / Set / Error /
   * RegExp / URL / undefined / NaN / ±Infinity / -0 round-trip). Set to
   * `JsonCodec` to opt out everywhere and accept that non-JSON values will
   * be lost across storage boundaries.
   */
  codec?: Codec<unknown>;
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
  payloadHash?: boolean;
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
    params.codec,
    params.payloadHash,
  );
}

/**
 * Create a non-durable flow — same composition as workflow but tuned for
 * one-shot, in-memory use. Calling `.execute(input)` constructs an
 * ephemeral `InMemoryWorkflowStorage` and runs to completion.
 *
 * To make it durable later, swap `flow(name)` for
 * `workflow({ name })` and drive it via `createWorkflowRunner({ storage })`.
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
  return new WorkflowBuilder(name, [], null, hooks);
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
     * Capabilities this step requires from a worker. Empty / omitted = any
     * worker can run it. The coordinator copies this onto the dispatched
     * task at enqueue time; workers claim only tasks whose needs are a
     * subset of their own declared `capabilities`.
     */
    readonly needs?: readonly string[];
    /** Dispatch priority — higher runs first (default 5 at the queue level). */
    readonly priority?: number;
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
