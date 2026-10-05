// ---------------------------------------------------------------------------
// Step definition — the contract between the builder's step factories and
// the runner: `StepDefinition`, what `execute` receives (`ExecuteParams` /
// `StepRuntime`), the contexts user step functions see, and the step option
// shapes. Plus the small runtime helpers every step kind shares.
//
// Type safety strategy: `StepDefinition` uses `unknown` because the builder
// holds a heterogeneous list where step 1 returns User, step 2 Account, and
// so on. TypeScript cannot express Array<∃T. StepDef<T>> (existential
// types), so the type safety boundary is the builder's public overloads —
// the same pattern Effect, Zod and RxJS use for heterogeneous collections.
// ---------------------------------------------------------------------------

import { die, succeed, type Eff, type Throws } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { CacheStore } from "../shared/cache-store.ts";
import { isEff, isThenable, promiseOrDie, promiseOrEff } from "../shared/eff.ts";
import type { RetryPolicy } from "../shared/retry-policy.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { Workflow } from "./workflow-types.ts";
import type { StepState } from "./workflow-state.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";

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
  /** The step attempt plus this element's `element.retry` retries so far. */
  readonly attempt: number;
}

/**
 * What a step body returns: an `Eff` producing `A` whose typed failures are
 * `E`. Steps must not leave service requirements (`Needs`) unprovided.
 */
export type StepEff<A, E extends TaggedError = never> = Eff<A, Throws<E>>;

/**
 * What a step-level `queue.concurrencyKey` receives. It is evaluated when
 * the runner hands the step to a step executor (dispatch), before the body
 * runs.
 */
export interface StepQueueContext {
  /** The workflow input the run was started with. */
  readonly input: unknown;
  /**
   * The value the step receives as `prev`: its first dependency's result,
   * or the workflow input for a step with no dependencies.
   */
  readonly prev: unknown;
  /** Results of every step completed so far in this run, by step name. */
  readonly deps: Readonly<Record<string, unknown>>;
  readonly workflowId: string;
  readonly attempt: number;
}

/**
 * Per-step concurrency cap — same shape as workflow-level but scoped to
 * just one step's tasks. Step-level wins over workflow-level for that
 * specific step. Useful when one step is rate-limited by an external API
 * (e.g. `concurrencyLimit: 3` on a "send-email" step that hits a vendor
 * with a 3-rps cap, while the rest of the workflow has no cap).
 */
export interface StepQueueOption {
  readonly concurrencyLimit: number;
  readonly concurrencyKey?: (ctx: StepQueueContext) => string;
}

// ---------------------------------------------------------------------------
// Step options
// ---------------------------------------------------------------------------

/**
 * What a step does once it has failed with a typed error and its retries
 * are spent: `"fail"` (default) fails the step, `"skip"` completes it with
 * `undefined`, `fallback` completes it with the returned value. Defects and
 * engine control flow (suspension, continue-as-new) are never handled.
 */
export type StepFailureStrategy<T> = "fail" | "skip" | { readonly fallback: (error: unknown) => T };

/**
 * Options of `.step()` / `.stepAsync()`, `.branch()` and `.match()`, which
 * honour every field. Other step kinds take a narrowed variant that leaves
 * out what they cannot honour (`MapOverOptions`, `ParallelStepsOptions`,
 * `JournaledStepOptions`, `SubworkflowOptions`, `LoopOptions`,
 * `TripwireOptions`), so an unsupported option is a compile error rather
 * than silently ignored.
 *
 * Retry and `onFailure` act on typed failures (an `Eff` failure). A throw
 * from a synchronous callback (`.branch()` `condition`, `.match()` `on` /
 * `when`, a step function that throws before returning its `Eff`) is a
 * defect: it fails the step without retry or `onFailure`.
 *
 * `Input` is the workflow input and `Prev` the value the step receives as
 * `prev` (its first dependency's result, or the workflow input for a step
 * with no dependencies); the builder fills both in, so `compensate`,
 * `skipWhen`, `skipValue` and `cache.key` see typed values.
 */
export interface StepOptions<T, Input = unknown, Prev = unknown> {
  readonly codec?: Codec<T>;
  /**
   * Per-attempt timeout. An attempt that does not settle in time is
   * interrupted and fails with `StepTimeoutError` (a typed failure, so
   * `retry` and `onFailure` apply to it).
   */
  readonly timeoutMs?: number;
  /** Retry the step on typed failures. Default delays: 250ms base, doubling, 3 retries. */
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
    input: Input;
    workflowId: string;
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
  /** Skip this step when the predicate returns true. Skipped steps are recorded as 'skipped' and do not trigger compensation. */
  readonly skipWhen?: (prev: Prev) => boolean;
  /** Value to pass to the next step when this step is skipped. Defaults to prev. */
  readonly skipValue?: (prev: Prev) => T;
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
   * ahead of lower. Honoured by `coordinator.submit`'s enqueueReady and
   * by `StepQueueExecutor`. Ignored by pure in-process
   * `.run()` (no queue involved there). Range 0–10, default 5.
   */
  readonly priority?: number;
  /**
   * Skip-if-recent semantics: wrap the step body in a cache lookup keyed by
   * the user-supplied `key(ctx)`. On hit, return the cached value without
   * running the body. On miss, run the body and cache the result for
   * `ttlMs`. Cache failures never fail the workflow — they fall through to
   * a cache miss.
   *
   * The store key is `${namespace}:${stepName}:${key(ctx)}`: the step name
   * is always included, so one cache config can be shared by several steps
   * (or used as the `parallelSteps` option, where it applies to every
   * branch) without their entries colliding. Values are stored encoded with
   * the step's codec.
   */
  readonly cache?: StepCacheOption<Input, Prev>;
  /**
   * Per-step concurrency cap. Step-level wins over workflow-level — set
   * this when one step is rate-limited by an external API while the rest
   * of the workflow has no cap. See `WorkflowQueueConfig` for the shape.
   */
  readonly queue?: StepQueueOption;
}

/**
 * Per-element options of `.mapOver()`. Each element runs under its own
 * timeout and retry, so one flaky element is retried alone instead of
 * failing (and, with a step-level `retry`, re-running) the whole map.
 */
export interface MapElementOptions<T> {
  /** Codec for each element's task row. Default: the workflow codec. */
  readonly codec?: Codec<T>;
  /** Per-element attempt timeout (a `StepTimeoutError` whose message names `step[index]`). */
  readonly timeoutMs?: number;
  /** Retry one element on typed failures. */
  readonly retry?: RetryPolicy<TaggedError>;
}

/**
 * Options of `.mapOver()` / `.mapOverAsync()`. The step-level fields apply
 * to the whole map step, whose result is the `T[]` array: `codec` encodes
 * that array (default: `element.codec` lifted to arrays, else the workflow
 * codec), `onFailure.fallback` and `skipValue` return an array, `compensate`
 * receives it, and `skipWhen` / `skipValue` / `cache.key` see the source
 * array as `prev`. Elements with a saved result are not run again: a
 * step-level `retry`, a workflow retry or a resume after a crash runs only
 * the elements that have not completed. Use `element` to retry or time out
 * one element in place.
 */
export interface MapOverOptions<T, Input = unknown, Prev = unknown> extends StepOptions<
  T[],
  Input,
  Prev
> {
  readonly element?: MapElementOptions<T>;
}

/**
 * Options of `.parallelSteps()`. `codec` encodes the joined record (default:
 * the branch codecs combined per key). `timeoutMs`, `retry`, `needs`,
 * `priority`, `queue` and `cache` are defaults for every branch; `branches`
 * sets any `StepOptions` field per branch, overriding those defaults.
 * Failure handling, compensation and skipping are per branch only, because
 * each branch is its own step with its own result type.
 */
export interface ParallelStepsOptions<
  Outputs extends Record<string, unknown>,
  Input = unknown,
  Prev = unknown,
> extends Pick<
  StepOptions<Outputs, Input, Prev>,
  "codec" | "timeoutMs" | "retry" | "needs" | "priority" | "queue" | "cache"
> {
  readonly branches?: { readonly [K in keyof Outputs]?: StepOptions<Outputs[K], Input, Prev> };
}

/**
 * Options of `.journaled()`. A `retry` re-runs the body; activities already
 * in the journal return their recorded result without re-executing (also
 * after the step's own compensations ran), so prefer activity-level retry
 * for work that must be redone. No `cache` (the journal is the body's
 * memo) and no `timeoutMs` (the body cannot be interrupted, so a timeout
 * would leave it running against the journal; time out activities or the
 * workflow instead).
 */
export type JournaledStepOptions<T, Input = unknown, Prev = unknown> = Omit<
  StepOptions<T, Input, Prev>,
  "cache" | "timeoutMs"
>;

/**
 * Options of `.subworkflow()`. A child that fails surfaces as a typed
 * `StepError`, so `retry` re-drives the same child run (it resumes from its
 * failed step) and `onFailure` / `compensate` apply. No `cache` (the child
 * row is the memo) and no `timeoutMs` (the child would keep running and
 * holding its lock; set the child workflow's own `timeoutMs` instead).
 */
export type SubworkflowOptions<T, Input = unknown, Prev = unknown> = Omit<
  StepOptions<T, Input, Prev>,
  "cache" | "timeoutMs"
>;

/**
 * Options of `.tripwire()`. Only the codec: the step is a synchronous
 * predicate over `prev`, so there is nothing to retry, time out, dispatch,
 * cache or compensate.
 */
export type TripwireOptions<T> = Pick<StepOptions<T>, "codec">;

export interface StepCacheOption<Input = unknown, Prev = unknown> {
  /**
   * Compute the cache key from the step context: the workflow input, `prev`
   * (the first dependency's result, also for a step with `dependsOn`) and
   * the workflow id.
   */
  readonly key: (ctx: StepContext<Input, Prev>) => string;
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
   * workflows don't collide when they share a backing store. The step name
   * follows the namespace in the key; two workflows that set the same
   * namespace share entries only for steps with the same name.
   */
  readonly namespace?: string;
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
  | "guard"
  | "tripwire"
  | "parallel"
  | "loop"
  | "child"
  /** The pure step `.map()` adds after the head. */
  | "transform";

export interface StepDefinition {
  readonly name: string;
  readonly dependsOn: string[];
  readonly kind: StepKind;
  readonly execute: (params: ExecuteParams) => StepEff<unknown, TaggedError>;
  readonly codec: Codec<unknown>;
  readonly timeoutMs?: number;
  readonly retry?: RetryPolicy<TaggedError>;
  readonly onFailure?: StepFailureStrategy<unknown>;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Eff<unknown, Throws<unknown>> | Promise<void>;
  readonly skipWhen?: (prev: unknown) => boolean;
  readonly skipValue?: (prev: unknown) => unknown;
  /** Capability requirements copied onto the dispatched task by the coordinator. */
  readonly needs?: readonly string[];
  /** Dispatch priority copied onto the dispatched task. */
  readonly priority?: number;
  /**
   * Per-step concurrency cap. The coordinator evaluates `queue.concurrencyKey`
   * against the step ctx at enqueue time and stamps the result on the
   * dispatched task. The step queue's `claim()` counts currently-running
   * tasks with the same `(scope, key)` and refuses to claim past the limit.
   * When set, takes precedence over the workflow's queue config for this
   * specific step.
   */
  readonly queue?: StepQueueOption;
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

/**
 * Launch a child workflow on behalf of the running step and resolve with its
 * result. Supplied by the runner, bound to the parent run: the child row is
 * created (create-if-absent) with `parentWorkflowId` pointing at the parent,
 * and the child runs on the parent's runtime (storage, clock, step executor,
 * executor id, runner-level hooks). An existing child row is resumed as-is.
 */
export type RunChildWorkflow = (params: {
  readonly workflow: Workflow<unknown, unknown>;
  readonly workflowId: string;
  readonly input: unknown;
}) => Promise<unknown>;

/**
 * Runtime the runner hands every step body next to the step's data. Every
 * field is optional so a step can be driven by hand (tests, custom
 * executors); each step kind documents its fallback.
 */
export interface StepRuntime {
  /**
   * The runner's time source. Drives sleep / signal-timeout deadlines and
   * loop-iteration timing. Default: `SystemWallClock`.
   */
  readonly clock?: WallClock;
  /**
   * Fence guard of the lock the runner holds on this run. Passed on every
   * storage write a step kind makes itself (sleep / signal suspension,
   * `mapOver` task rows, loop iteration rows, journaled suspension), so a
   * holder that lost the lock is rejected by fencing backends.
   */
  readonly guard?: FenceGuard;
  /**
   * This step's stored row as of the runner's last load of the run (`null`
   * when the step has no row yet). `undefined` means the caller did not
   * supply it; step kinds that need it (sleep, waitForSignal, mapOver) then load it.
   */
  readonly stepState?: StepState | null;
  /**
   * Run a child workflow (`.subworkflow()`, journaled `ctx.child`). Without
   * it those step kinds fail with a clear error.
   */
  readonly runChild?: RunChildWorkflow;
  /** Version of the definition driving this run (`ctx.workflowVersion` in `.journaled()`). */
  readonly workflowVersion?: string;
  /**
   * The run number (`WorkflowState.run`) being driven. Queue-backed
   * executors stamp it on dispatched tasks, so a settled task of an earlier
   * run is never taken as this run's outcome.
   */
  readonly run?: number;
  /** Patch names active in the definition driving this run (`ctx.patched(name)`). */
  readonly patches?: readonly string[];
  /**
   * The run's metadata as the runner loaded it, kept current with the
   * run's own `ctx.metadata` writes. `.journaled()` seeds `ctx.metadata`
   * from it instead of re-reading the run; without it, it loads the run.
   */
  readonly workflowMetadata?: WorkflowMetadataRef;
}

/**
 * A run's metadata shared by the steps of one execution: `current` is
 * replaced (never mutated) on every write.
 */
export interface WorkflowMetadataRef {
  current: Record<string, unknown> | undefined;
}

export interface ExecuteParams extends StepRuntime {
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
// Shared runtime helpers
// ---------------------------------------------------------------------------

/**
 * Normalise what a user step function returned into the `Eff` the runner
 * executes. An `Eff` passes through. A Promise (e.g. an `async` function
 * handed to `.step()`) is awaited with `.stepAsync()` semantics — its
 * rejection is a defect. A Promise that resolves to an `Eff` runs that
 * `Eff` as the step body, so its typed failures stay typed and the step's
 * retry policy applies to them. Anything else is a defect whose message
 * names `asyncVariant` (default `.stepAsync()`).
 */
export function asStepEff(params: {
  readonly result: unknown;
  readonly stepName: string;
  /** The Promise-body method to suggest in the defect message. */
  readonly asyncVariant?: string;
}): StepEff<unknown, TaggedError> {
  const { result, stepName } = params;
  if (isEff(result)) return result as StepEff<unknown, TaggedError>;
  if (isThenable(result)) return promiseOrEff(() => result) as StepEff<unknown, TaggedError>;
  return die(
    new TypeError(
      `Step "${stepName}" must return an Eff (got ${result === null ? "null" : typeof result}); ` +
        `use ${params.asyncVariant ?? ".stepAsync()"} for Promise-returning functions`,
    ),
  );
}

/**
 * This step's stored row: the runner-supplied `stepState` when present
 * (`null` = no row), else a fresh load (steps driven without a runner).
 * `reload` forces the fresh load, for a step that needs writes made after
 * the runner's last load of the run (an earlier attempt's task rows).
 */
export function currentStepState(params: {
  readonly exec: ExecuteParams;
  readonly stepName: string;
  readonly reload?: boolean;
}): Eff<StepState | undefined> {
  const { exec, stepName } = params;
  if (exec.stepState !== undefined && params.reload !== true) {
    return succeed(exec.stepState ?? undefined);
  }
  return promiseOrDie(() => exec.storage.loadWorkflow(exec.workflowId)).map(
    (state) => state?.steps[stepName],
  );
}

/**
 * The value a step receives as `prev`: its first dependency's result, or
 * the workflow input for a step with no dependencies.
 */
export function readPrev(params: {
  readonly dependsOn: readonly string[];
  readonly results: Readonly<Record<string, unknown>>;
  readonly input: unknown;
}): unknown {
  const first = params.dependsOn[0];
  return first != null ? params.results[first] : params.input;
}

/** The `StepContext` a linear step body receives (`prev` from `dependsOn[0]`). */
export function linearStepContext(params: {
  readonly dependsOn: readonly string[];
  readonly exec: ExecuteParams;
}): StepContext<unknown, unknown> {
  const { exec } = params;
  return {
    input: exec.input,
    prev: readPrev({ dependsOn: params.dependsOn, results: exec.results, input: exec.input }),
    workflowId: exec.workflowId,
    attempt: exec.attemptRef.current,
  };
}
