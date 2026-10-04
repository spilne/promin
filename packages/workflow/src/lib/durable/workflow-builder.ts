// ---------------------------------------------------------------------------
// WorkflowBuilder — fluent, type-safe DAG-based durable pipeline
// ---------------------------------------------------------------------------
//
// The public API (`.step()` overloads and friends) is fully typed — Input,
// the Steps record, Current and Err (the typed error channel, carried into
// `Workflow<I, O, E>` by `build()`) are tracked through the chain via
// type-level computation. Each method is a thin generic signature over a
// pure step factory in `steps/`: `this._append(createXStep({...}))`. The
// builder wraps one immutable `BuilderState` (see `builder-state.ts`), so a
// builder value can be branched and every append returns a new builder.
// ---------------------------------------------------------------------------

import { LosslessJsonCodec, type Codec } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../shared/eff.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import {
  appendSteps,
  emptySteps,
  hasStep,
  lastStep,
  stepsToArray,
  type BuilderState,
} from "./builder-state.ts";
import {
  GuardError,
  LoopLimitExceededError,
  StepError,
  WorkflowError,
  WorkflowTimeoutError,
} from "./durable-pipeline-error.ts";
import type { JournalStorageMissingError, JournaledStepBody } from "./journaled-step.ts";
import type {
  DagStepContext,
  JournaledStepOptions,
  MapOverOptions,
  MapStepContext,
  ParallelStepsOptions,
  StepContext,
  StepDefinition,
  StepEff,
  StepOptions,
  SubworkflowOptions,
  TripwireOptions,
} from "./step-definition.ts";
import { createBasicStep, createTransformStep } from "./steps/basic-step.ts";
import { createBranchStep } from "./steps/branch-step.ts";
import { createGuardStep, createTripwireStep } from "./steps/guard-steps.ts";
import { createJournaledStep } from "./steps/journaled-step-def.ts";
import { createLoopStep, type LoopOptions } from "./steps/loop-step.ts";
import { createMapOverStep } from "./steps/map-over-step.ts";
import { createMatchStep, type MatchError, type MatchParams } from "./steps/match-step.ts";
import {
  createParallelSteps,
  type BranchError,
  type BranchOutput,
} from "./steps/parallel-steps.ts";
import { createSleepStep, createWaitForSignalStep } from "./steps/suspend-steps.ts";
import { createSubworkflowStep } from "./steps/subworkflow-step.ts";
import { toWorkflowDag, type WorkflowDAG } from "./workflow-dag-viz.ts";
import type {
  IdempotencyConfig,
  Workflow,
  WorkflowDefinitionInternals,
  WorkflowHooks,
  WorkflowParams,
  WorkflowQueueConfig,
} from "./workflow-types.ts";

/** A step function as the overload implementations see it. */
type AnyStepFn = (ctx: any) => unknown;

/** Step options as the overload implementations see them. */
type AnyStepOptions = StepOptions<any, any, any>;

/** What `.step()` / `.stepAsync()` resolve their overloaded arguments to. */
interface StepArgs<F> {
  readonly dependsOn: string[];
  /** No explicit `dependsOn`: the step follows the current head. */
  readonly isLinear: boolean;
  readonly fn: F;
  readonly options: StepOptions<unknown> | undefined;
}

/**
 * Resolve `(fn, options?)` (linear) or `({ dependsOn }, fn, options?)`
 * (DAG) into one shape.
 */
function parseStepArgs<F extends AnyStepFn>(params: {
  readonly fnOrConfig: F | { dependsOn: string[] };
  readonly fnOrOptions: F | AnyStepOptions | undefined;
  readonly maybeOptions: AnyStepOptions | undefined;
  /** Dependencies of a linear step (the current head, if any). */
  readonly linearDeps: string[];
}): StepArgs<F> {
  const { fnOrConfig, fnOrOptions } = params;
  if (typeof fnOrConfig === "function") {
    return {
      dependsOn: params.linearDeps,
      isLinear: true,
      fn: fnOrConfig,
      options: fnOrOptions as StepOptions<unknown> | undefined,
    };
  }
  return {
    dependsOn: fnOrConfig.dependsOn,
    isLinear: false,
    fn: fnOrOptions as F,
    options: params.maybeOptions,
  };
}

/**
 * `Steps` with step `Name` added. A non-literal `Name` (a `string`) adds
 * nothing: it would otherwise widen `Steps` to `Record<string, T>`, after
 * which every `dependsOn` name type-checks.
 */
type AddStep<Steps, Name extends string, T> = string extends Name ? Steps : Steps & Record<Name, T>;

/**
 * The value a DAG step with `dependsOn: DependsOn` receives as `prev`: its
 * first dependency's result, or the workflow input when it has none.
 */
type FirstDepValue<
  Steps,
  DependsOn extends readonly unknown[],
  Input,
> = DependsOn extends readonly [infer First, ...unknown[]]
  ? First extends keyof Steps
    ? Steps[First]
    : Input
  : Input;

const duplicateStepError = (name: string): WorkflowError =>
  new WorkflowError({ workflowId: "", message: `Duplicate step name: "${name}"` });

export class WorkflowBuilder<
  Input,
  Steps extends Record<string, unknown> = {},
  Current = Input,
  Err extends TaggedError = never,
> {
  /** @internal Use `workflow()` or `flow()`. */
  constructor(private readonly s: BuilderState<Input>) {}

  /** Set the workflow version. Used to detect code/state mismatch on resume. */
  version(v: string): WorkflowBuilder<Input, Steps, Current, Err> {
    return new WorkflowBuilder({ ...this.s, config: { ...this.s.config, version: v } });
  }

  // ---------------------------------------------------------------------------
  // step / stepAsync
  // ---------------------------------------------------------------------------

  /** Linear step — Eff-returning. */
  step<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    fn: (ctx: StepContext<Input, Current>) => StepEff<Output, E2>,
    options?: StepOptions<Output, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err | E2>;

  /** DAG step — Eff-returning. */
  step<
    Name extends string,
    DependsOn extends (keyof Steps & string)[],
    Output,
    E2 extends TaggedError = never,
  >(
    name: Name,
    config: { dependsOn: [...DependsOn] },
    fn: (ctx: DagStepContext<Input, Pick<Steps, DependsOn[number]>>) => StepEff<Output, E2>,
    options?: StepOptions<Output, Input, FirstDepValue<Steps, DependsOn, Input>>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err | E2>;

  step(
    name: string,
    fnOrConfig: AnyStepFn | { dependsOn: string[] },
    fnOrOptions?: AnyStepFn | AnyStepOptions,
    maybeOptions?: AnyStepOptions,
  ): WorkflowBuilder<Input, any, any, any> {
    return this._basicStep(
      name,
      parseStepArgs({ fnOrConfig, fnOrOptions, maybeOptions, linearDeps: this._linearDeps() }),
    );
  }

  /** Linear stepAsync — Promise-returning convenience; a rejection is a defect. */
  stepAsync<Name extends string, Output>(
    name: Name,
    fn: (ctx: StepContext<Input, Current>) => Promise<Output>,
    options?: StepOptions<Output, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err>;

  /** DAG stepAsync — Promise-returning convenience; a rejection is a defect. */
  stepAsync<Name extends string, DependsOn extends (keyof Steps & string)[], Output>(
    name: Name,
    config: { dependsOn: [...DependsOn] },
    fn: (ctx: DagStepContext<Input, Pick<Steps, DependsOn[number]>>) => Promise<Output>,
    options?: StepOptions<Output, Input, FirstDepValue<Steps, DependsOn, Input>>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err>;

  stepAsync(
    name: string,
    fnOrConfig: AnyStepFn | { dependsOn: string[] },
    fnOrOptions?: AnyStepFn | AnyStepOptions,
    maybeOptions?: AnyStepOptions,
  ): WorkflowBuilder<Input, any, any, any> {
    const args = parseStepArgs({
      fnOrConfig,
      fnOrOptions,
      maybeOptions,
      linearDeps: this._linearDeps(),
    });
    const asyncFn = args.fn as (ctx: unknown) => Promise<unknown>;
    return this._basicStep(name, {
      ...args,
      fn: (ctx: unknown) => promiseOrDie(() => asyncFn(ctx)),
    });
  }

  // ---------------------------------------------------------------------------
  // mapOver — fan-out over array with per-element retry
  // ---------------------------------------------------------------------------

  /**
   * Run `fn` for every element of the array produced by step `config.array`
   * (up to `config.concurrency` at a time) and complete with the results in
   * order. Each element's result is written as a task row (encoded with the
   * element codec), and an element that fails past its retries gets a failed
   * task row. The step-level options apply to the map step as a whole;
   * `options.element` sets an element's own codec, timeout and retry (see
   * `MapOverOptions`).
   *
   * Resume: when the map step runs again after a failure or a crash (a
   * step-level retry, a workflow retry, a resumed run), elements that
   * already have a completed task row return the saved result and only the
   * rest run. Element bodies are therefore at-least-once per element, not
   * per map step.
   *
   * `ctx.attempt` is the step attempt plus the element-level retries so far,
   * so it starts at the step attempt and grows with each element retry.
   */
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
    ) => StepEff<Output, E2>,
    options?: MapOverOptions<Output, Input, Steps[ArrayStep]>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output[]>, Output[], Err | E2> {
    this._validateName(name);
    return this._append(
      createMapOverStep({
        name,
        array: config.array,
        concurrency: config.concurrency,
        fn,
        options: options as MapOverOptions<unknown> | undefined,
        defaultCodec: this._codec(),
        cacheNamespace: this.s.config.name,
      }),
    );
  }

  /** `mapOver` with a Promise-returning `fn`; a rejection is a defect. */
  mapOverAsync<Name extends string, ArrayStep extends keyof Steps & string, Output>(
    name: Name,
    config: { array: ArrayStep; concurrency?: number },
    fn: (
      element: Steps[ArrayStep] extends readonly (infer U)[] ? U : never,
      ctx: MapStepContext<Input>,
    ) => Promise<Output>,
    options?: MapOverOptions<Output, Input, Steps[ArrayStep]>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output[]>, Output[], Err> {
    return this.mapOver(
      name,
      config,
      (element, ctx) => promiseOrDie(() => fn(element, ctx)),
      options,
    );
  }

  // ---------------------------------------------------------------------------
  // guard — precondition assertion that fails fast (no retry)
  // ---------------------------------------------------------------------------

  guard<Name extends string>(
    name: Name,
    predicate: (prev: Current) => boolean,
    options?: { failureMessage?: string },
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Current>, Current, Err | GuardError> {
    this._validateName(name);
    return this._append(
      createGuardStep({
        name,
        dependsOn: this._linearDeps(),
        predicate,
        failureMessage: options?.failureMessage,
        codec: this._codec(),
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // tripwire — short-circuit early exit with a structured reason
  // ---------------------------------------------------------------------------

  /**
   * Add a tripwire step — a predicate that ends the workflow early with a
   * structured outcome when it fires. Not a failure; an intentional short
   * circuit. When `when(prev)` returns `false`, the step passes `prev`
   * through unchanged and execution continues. When it returns `true`, the
   * runner stops the DAG, persists the reason, and marks the workflow with
   * `status: "tripwire"`.
   *
   * Use for business short-circuits that are not errors: a fraud check that
   * decides the transaction is fraudulent, a validation step that finds
   * nothing to do, a rate-limit decision to drop the request. Callers
   * inspect the outcome via `handle.status()` (reads `tripwire` from state)
   * or via `WorkflowTripwireError` thrown from `run()`.
   *
   * ```typescript
   * workflow({ name: "charge", storage })
   *   .step("load", ({ input }) => succeed(input))
   *   .tripwire("fraud-check", {
   *     when: (order) => order.riskScore > 0.9,
   *     reason: (order) => ({ code: "fraud", score: order.riskScore }),
   *   })
   *   .step("charge", (ctx) => chargeCard(ctx.prev))
   * ```
   *
   * Requires the configured `WorkflowStorage` to implement
   * `tripwireWorkflow`. The runner raises `TripwireStorageMissingError`
   * at the fire site for storages that don't support it, rather than
   * silently falling back to `failed`.
   */
  tripwire<Name extends string>(
    name: Name,
    params: {
      when: (prev: Current) => boolean;
      reason: (prev: Current) => unknown;
    },
    options?: TripwireOptions<Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Current>, Current, Err> {
    this._validateName(name);
    return this._append(
      createTripwireStep({
        name,
        dependsOn: this._linearDeps(),
        when: params.when,
        reason: params.reason,
        codec: this._codec(options?.codec),
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // dowhile / dountil — durable iteration loops
  // ---------------------------------------------------------------------------

  /**
   * Add a step-based loop — iterates `body` while `condition(result, iter)`
   * stays `true`. The body returns an `Eff`, like a `.step()` function: its
   * typed failures (`E2`) fail the loop step as typed failures, so `retry`
   * and `onFailure` apply; a throw is a defect. For a Promise-returning
   * body use `.dowhileAsync()`.
   *
   * Each iteration is persisted as its own step row (named
   * `"<name>.iter.<n>"`) via `saveStepResult`, with its own duration /
   * timestamps / attempt record — useful when each pass represents
   * meaningful work you want visible in dashboards, logs, and attempt
   * history (`loadStepAttempts`). The outer loop step also completes as a
   * normal DAG node; `onStepComplete` fires for it.
   *
   * Body always runs at least once. Exits when the condition flips or
   * when `maxIterations` (default 100) is exceeded — overflow raises
   * `LoopLimitExceededError` as a typed step failure.
   *
   * ```typescript
   * workflow({ name: "drain" })
   *   .dowhile(
   *     "process-batch",
   *     (ctx, iter) => processNextBatch(ctx.prev), // returns an Eff
   *     (batch) => batch.length > 0,
   *   )
   * ```
   *
   * **Choosing vs `ctx.dowhile`:** this form is for workflow-level loops
   * where each iteration is a discrete unit of work worth making visible.
   * For tight in-process polling (activity-journal durability, single
   * queue entry, no per-iter step rows), use `ctx.dowhile` inside a
   * `.journaled()` body instead.
   *
   * **Crash recovery:** iteration step rows double as a durability
   * checkpoint. On restart, the loop replays completed `<name>.iter.<n>`
   * rows in order, re-evaluating the condition against each persisted
   * result, and resumes from the first missing iteration. Already-run
   * iterations are never re-executed.
   *
   * **Timeouts:** the loop is one interruptible effect. When the step's
   * `timeoutMs` fires, the loop stops before its next iteration and writes
   * no further iteration rows.
   */
  dowhile<Name extends string, T, E2 extends TaggedError = never>(
    name: Name,
    body: (ctx: StepContext<Input, Current>, iter: number) => StepEff<T, E2>,
    condition: (result: T, iter: number) => boolean,
    options?: LoopOptions<T, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, T>, T, Err | E2 | LoopLimitExceededError> {
    return this._loop({
      name,
      body,
      keepGoing: condition,
      options,
      asyncVariant: ".dowhileAsync()",
    });
  }

  /**
   * `.dowhile()` with a body that returns a value or a Promise. A throw or
   * rejection is a defect (no `retry` / `onFailure`), as with `.stepAsync()`;
   * return a failed `Eff` from `.dowhile()` for a typed failure.
   *
   * ```typescript
   * workflow({ name: "drain" })
   *   .dowhileAsync(
   *     "process-batch",
   *     async (ctx) => processNextBatch(ctx.prev),
   *     (batch) => batch.length > 0,
   *   )
   * ```
   */
  dowhileAsync<Name extends string, T>(
    name: Name,
    body: (ctx: StepContext<Input, Current>, iter: number) => T | PromiseLike<T>,
    condition: (result: T, iter: number) => boolean,
    options?: LoopOptions<T, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, T>, T, Err | LoopLimitExceededError> {
    return this._loop({
      name,
      body: asyncLoopBody(body),
      keepGoing: condition,
      options,
      asyncVariant: ".dowhileAsync()",
    });
  }

  /**
   * Inverse polarity of `.dowhile()` — iterate `body` until the condition
   * becomes `true`. Same step-row-per-iteration semantics and the same
   * `Eff` body contract. Body always runs at least once. See `.dowhile()`
   * for the step-vs-journaled tradeoff.
   *
   * ```typescript
   * workflow({ name: "poll" })
   *   .dountil(
   *     "wait-ready",
   *     ({ prev }) => checkStatus(prev.jobId), // returns an Eff
   *     (status) => status === "ready",
   *     { maxIterations: 60 },
   *   )
   * ```
   */
  dountil<Name extends string, T, E2 extends TaggedError = never>(
    name: Name,
    body: (ctx: StepContext<Input, Current>, iter: number) => StepEff<T, E2>,
    condition: (result: T, iter: number) => boolean,
    options?: LoopOptions<T, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, T>, T, Err | E2 | LoopLimitExceededError> {
    // `dountil(cond) ≡ dowhile(!cond)`.
    return this._loop({
      name,
      body,
      keepGoing: (r: T, i: number) => !condition(r, i),
      options,
      asyncVariant: ".dountilAsync()",
    });
  }

  /** `.dountil()` with a body that returns a value or a Promise (see `.dowhileAsync()`). */
  dountilAsync<Name extends string, T>(
    name: Name,
    body: (ctx: StepContext<Input, Current>, iter: number) => T | PromiseLike<T>,
    condition: (result: T, iter: number) => boolean,
    options?: LoopOptions<T, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, T>, T, Err | LoopLimitExceededError> {
    return this._loop({
      name,
      body: asyncLoopBody(body),
      keepGoing: (r: T, i: number) => !condition(r, i),
      options,
      asyncVariant: ".dountilAsync()",
    });
  }

  // ---------------------------------------------------------------------------
  // parallelSteps — fluent fork/join over multiple named DAG steps
  // ---------------------------------------------------------------------------

  /**
   * Add a parallel-steps block — forks from the current head into N
   * branches that run concurrently as **distinct DAG steps** (each
   * distributable to workers and retriable independently), then joins them
   * into a keyed record that downstream steps consume via `prev`.
   *
   * Named `parallelSteps` rather than `parallel` to disambiguate from
   * `ctx.parallel()` on `JournaledContext`, which fans out **activities
   * in-process** inside a single journaled step. This one operates at the
   * DAG level — each branch is a separate queue entry that can land on a
   * different worker.
   *
   * Follows the house style from `.branch()` and `.match()`: the outer
   * block takes a `name`; branch labels are keys in a record rather than
   * top-level step names. Each branch becomes a physical step named
   * `"<block-name>.<label>"` in storage and the step queue, so branch
   * names scoped under the block don't collide with unrelated siblings.
   *
   * ```typescript
   * workflow({ name: "signup", storage })
   *   .step("load", ({ input }) => succeed(input))
   *   .parallelSteps("enrich", {
   *     user: ({ prev }) => tryPromise(() => fetchUser(prev), toShipError),
   *     perms: ({ prev }) => tryPromise(() => fetchPerms(prev), toShipError),
   *   })
   *   .step("join", ({ prev }) => succeed({ ...prev.user, ...prev.perms }))
   * ```
   *
   * Semantics match the existing DAG executor: branches run concurrently
   * within a ready-set batch and the block fails on the first branch
   * failure (remaining branches' results are not preserved; compensation
   * cascades via the usual saga path).
   *
   * Options: block-level `timeoutMs` / `retry` / `needs` / `priority` /
   * `queue` / `cache` apply to every branch; `branches` gives one branch
   * its own `StepOptions` (codec, retry, `onFailure`, `compensate`, ...),
   * typed by that branch's output. `codec` encodes the joined record.
   *
   * ```typescript
   * .parallelSteps("enrich", { user, perms }, {
   *   retry: { maxRetries: 2 },
   *   branches: { perms: { onFailure: { fallback: () => [] } } },
   * })
   * ```
   */
  parallelSteps<
    Name extends string,
    Branches extends Record<
      string,
      (ctx: StepContext<Input, Current>) => StepEff<unknown, TaggedError>
    >,
  >(
    name: Name,
    branches: Branches,
    options?: ParallelStepsOptions<
      { [K in keyof Branches]: BranchOutput<Branches[K]> },
      Input,
      Current
    >,
  ): WorkflowBuilder<
    Input,
    AddStep<Steps, Name, { [K in keyof Branches]: BranchOutput<Branches[K]> }>,
    { [K in keyof Branches]: BranchOutput<Branches[K]> },
    Err | BranchError<Branches>
  > {
    this._validateName(name);
    return this._append(
      createParallelSteps({
        name,
        dependsOn: this._linearDeps(),
        branches,
        options: options as ParallelStepsOptions<Record<string, unknown>> | undefined,
        defaultCodec: this._codec(),
        cacheNamespace: this.s.config.name,
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // branch — conditional paths
  // ---------------------------------------------------------------------------

  /**
   * Two-way conditional: run `ifTrue` or `ifFalse` depending on
   * `condition(prev)`. One DAG node; every `StepOptions` field applies to it
   * (a `retry` re-evaluates `condition`). A throw from `condition` is a
   * defect and is not retried; return a failed `Eff` from the branch for a
   * typed failure.
   */
  branch<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    params: {
      condition: (value: Current) => boolean;
      ifTrue: (ctx: StepContext<Input, Current>) => StepEff<Output, E2>;
      ifFalse: (ctx: StepContext<Input, Current>) => StepEff<Output, E2>;
    },
    options?: StepOptions<Output, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err | E2> {
    this._validateName(name);
    return this._append(
      createBranchStep({
        name,
        dependsOn: this._linearDeps(),
        condition: params.condition,
        ifTrue: params.ifTrue,
        ifFalse: params.ifFalse,
        options: options as StepOptions<unknown> | undefined,
        codec: this._codec(options?.codec),
        cacheNamespace: this.s.config.name,
      }),
    );
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
   * `JournalStore` (InMemoryWorkflowStorage and
   * PostgresWorkflowStorage do); otherwise the step fails with
   * `JournalStorageMissingError` when it runs. Options: see
   * `JournaledStepOptions` (no `cache`, no `timeoutMs`).
   *
   * ```typescript
   * workflow({ name: "signup", storage })
   *   .step("load", ({ input }) => succeed(input))
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
    options?: JournaledStepOptions<Output, Input, Current>,
  ): WorkflowBuilder<
    Input,
    AddStep<Steps, Name, Output>,
    Output,
    Err | JournalStorageMissingError
  > {
    this._validateName(name);
    return this._append(
      createJournaledStep({
        name,
        dependsOn: this._linearDeps(),
        body: body as unknown as JournaledStepBody<unknown, unknown, unknown>,
        options: options as JournaledStepOptions<unknown> | undefined,
        codec: this._codec(options?.codec),
        payloadHash: this.s.config.payloadHash,
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // match — multi-way branching (selector or first-matching-predicate)
  // ---------------------------------------------------------------------------

  /**
   * Multi-way conditional routing. Two modes — pick whichever matches your data:
   *
   * **Selector mode** — like `switch (key)`. `on` returns a string key that
   * selects from `cases` (own keys only). `default` is optional; a missing
   * key fails the step with `MatchError`.
   *
   * ```typescript
   * .match("route", {
   *   on: (order) => order.type,
   *   cases: {
   *     express: ({ prev }) => tryPromise(() => expressShip(prev), toShipError),
   *     standard: ({ prev }) => tryPromise(() => standardShip(prev), toShipError),
   *     freight: ({ prev }) => tryPromise(() => freightShip(prev), toShipError),
   *   },
   *   default: ({ prev }) => tryPromise(() => standardShip(prev), toShipError),
   * })
   * ```
   *
   * **Predicate mode** — like `if/else if`. `cases` is an array; first
   * matching `when` wins. Order matters.
   *
   * ```typescript
   * .match("route", {
   *   cases: [
   *     { when: (o) => o.total > 10_000, then: ({ prev }) => tryPromise(() => vipProcess(prev), toShipError) },
   *     { when: (o) => o.type === "express", then: ({ prev }) => tryPromise(() => expressShip(prev), toShipError) },
   *   ],
   *   default: ({ prev }) => tryPromise(() => standardShip(prev), toShipError),
   * })
   * ```
   *
   * Output type is inferred as the union of all case outputs (or the
   * common type when they all match). Match contributes one node to the DAG;
   * deterministic from `prev` so replay re-runs the same case.
   *
   * Every `StepOptions` field applies. `MatchError` is a typed failure, so
   * `retry` (which re-runs the selection) and `onFailure` handle it. A throw
   * from `on` or a `when` predicate is a defect and is not retried.
   */
  match<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    params: MatchParams<Input, Current, Output, E2>,
    options?: StepOptions<Output, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Output>, Output, Err | E2 | MatchError> {
    this._validateName(name);
    return this._append(
      createMatchStep({
        name,
        dependsOn: this._linearDeps(),
        match: params as unknown as MatchParams<unknown, unknown, unknown, TaggedError>,
        options: options as StepOptions<unknown> | undefined,
        codec: this._codec(options?.codec),
        cacheNamespace: this.s.config.name,
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // subworkflow — invoke a child workflow as a step
  // ---------------------------------------------------------------------------

  /**
   * Invoke a child workflow as a step. The child is independently durable.
   * Automatically sets parentWorkflowId for tracking. The child row is
   * created with the child's `version` if it does not exist yet; an existing
   * row with that `workflowId` is resumed as-is (its original parent and
   * version are kept).
   *
   * A failed child fails this step with a typed `StepError`, so `retry`
   * re-drives the same child run and `onFailure` / `compensate` apply (see
   * `SubworkflowOptions`). A throw from `config.input` / `config.workflowId`
   * is a defect and is not retried.
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
    options?: SubworkflowOptions<ChildOutput, Input, Current>,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, ChildOutput>, ChildOutput, Err | StepError> {
    this._validateName(name);
    return this._append(
      createSubworkflowStep({
        name,
        dependsOn: this._linearDeps(),
        child: child as Workflow<unknown, unknown>,
        input: config.input,
        workflowId: config.workflowId,
        options: options as SubworkflowOptions<unknown> | undefined,
        codec: this._codec(options?.codec),
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // sleep — durable timer
  // ---------------------------------------------------------------------------

  /**
   * Durable timer. The first execution stores a wake time (`now + ms`) and
   * suspends; every later resume compares against that stored wake time, so
   * resuming early does not move it. On wake the step passes its predecessor's
   * value through unchanged: the next step's `prev` is the value from before
   * the sleep, and the sleep step's own checkpointed result is that same value
   * (encoded with the predecessor's codec), so a later `dependsOn: [name]`
   * sees that value too.
   *
   * Suspension is engine control flow, not a step failure, so it does not
   * enter the typed error channel.
   */
  sleep<Name extends string>(
    name: Name,
    ms: number,
  ): WorkflowBuilder<Input, AddStep<Steps, Name, Current>, Current, Err> {
    this._validateName(name);
    return this._append(
      createSleepStep({
        name,
        dependsOn: this._linearDeps(),
        ms,
        // The checkpointed result is the predecessor's value, so round-trip
        // it with the predecessor's codec.
        codec: lastStep(this.s.steps)?.codec ?? this._codec(),
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // waitForSignal — wait for external event
  // ---------------------------------------------------------------------------

  /**
   * Suspend until a signal named `signalName` has been delivered to this run,
   * then complete with its payload (decoded through `codec`).
   *
   * Signal semantics:
   * - A signal is a named value on the run, not a queued event. Delivering
   *   the same name again replaces the earlier payload (last delivery wins),
   *   and signals are cleared when a fresh run starts.
   * - A signal delivered before the step runs is picked up immediately.
   * - Signals are not consumed. Every `waitForSignal` on the same
   *   `signalName` in one run resolves with the payload delivered at the
   *   time it runs, so two waits on one name are both satisfied by a single
   *   delivery. To wait for distinct events, use distinct signal names (e.g.
   *   `approve-1`, `approve-2`, or a name that includes a loop counter).
   *
   * `timeoutMs` is measured from the step's first execution. The deadline is
   * stored with the step and reused on every resume, so resuming the run
   * before the deadline does not extend it; the first resume at or after the
   * deadline fails the step with `WorkflowTimeoutError`.
   *
   * Typing: the step is added to the named steps (for a later `dependsOn`)
   * only when its name is inferred as a literal. The usual
   * `.waitForSignal<Payload>("approval", …)` passes `T` explicitly, which
   * leaves `Name` at `string`, so the step is not addressable by name; pass
   * both (`.waitForSignal<Payload, "approval">(…)`) or let `codec` infer
   * `T` to make it addressable. A non-literal name never widens the named
   * steps to `Record<string, T>`.
   */
  waitForSignal<T, Name extends string = string>(
    name: Name,
    params: {
      signalName: string;
      timeoutMs?: number;
      codec?: Codec<T>;
    },
  ): WorkflowBuilder<Input, AddStep<Steps, Name, T>, T, Err | WorkflowTimeoutError> {
    this._validateName(name);
    return this._append(
      createWaitForSignalStep({
        name,
        dependsOn: this._linearDeps(),
        signalName: params.signalName,
        timeoutMs: params.timeoutMs,
        codec: this._codec(params.codec),
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // map — transform the last step's result
  // ---------------------------------------------------------------------------

  /**
   * Transform the current head's value with a pure function. `.map(fn)`
   * adds a step named `"<head>.map"` (`"<head>.map.2"`, ... when taken)
   * that applies `fn` to the head's result and checkpoints the mapped value
   * with the workflow codec; the next step's `prev` is the mapped value.
   *
   * The head step itself is unchanged: its checkpointed result, codec and
   * options stay those of the unmapped value, so `fn` also maps the head's
   * `skipValue`, `onFailure` fallback and cache hits, the head's
   * `compensate` receives the unmapped result, and a later
   * `dependsOn: [head]` sees the unmapped value. A throw from `fn` is a
   * defect. The map step is not addressable by name in `dependsOn`.
   */
  map<Output>(fn: (value: Current) => Output): WorkflowBuilder<Input, Steps, Output, Err> {
    const head = this.s.lastStepName;
    if (head === null) {
      throw new WorkflowError({
        workflowId: "",
        message: "Cannot call .map() on a workflow with no steps",
      });
    }
    let name = `${head}.map`;
    for (let n = 2; hasStep({ seq: this.s.steps, name }); n++) name = `${head}.map.${n}`;
    return this._append(
      createTransformStep({
        name,
        head,
        fn: fn as (value: unknown) => unknown,
        codec: this._codec(),
      }),
    );
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
    // Loaded on demand: a `Workflow` is pure data, so the builder module
    // keeps no static dependency on the runner or a storage backend.
    const [{ createWorkflowRunner }, { InMemoryWorkflowStorage }] = await Promise.all([
      import("./workflow-runner.ts"),
      import("./in-memory-storage.ts"),
    ]);
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    return runner.run({
      workflow: this.build() as Workflow<unknown, unknown>,
      workflowId: crypto.randomUUID(),
      input,
    }) as Promise<Current>;
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
   *
   * The result carries the typed errors the steps declared (`Err`), which
   * the runner surfaces on `run` / `runSafe` / `start`.
   */
  build(options?: { idempotency?: IdempotencyConfig }): Workflow<Input, Current, Err> {
    const config = options?.idempotency
      ? { ...this.s.config, idempotency: options.idempotency }
      : this.s.config;
    const steps = stepsToArray(this.s.steps);
    const definition: WorkflowDefinitionInternals = {
      steps,
      type: config.type,
      metadata: config.metadata,
      retry: config.retry,
      compensateConfig: config.compensate,
      dlq: config.dlq,
      timeoutMs: config.timeoutMs,
      onVersionMismatch: config.onVersionMismatch,
      previousVersions: config.previousVersions,
      hooks: config.hooks,
      queue: config.queue as WorkflowQueueConfig<unknown> | undefined,
      patches: config.patches,
    };
    return {
      name: config.name,
      version: config.version,
      dag: toWorkflowDag({ name: config.name, steps }),
      idempotency: config.idempotency,
      _definition: definition,
    };
  }

  /** Export the step DAG as a serializable JSON structure. */
  toJSON(): WorkflowDAG {
    return toWorkflowDag({ name: this.s.config.name, steps: stepsToArray(this.s.steps) });
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  /** `this` with `defs` appended; the last of them becomes the head. */
  private _append(
    defs: StepDefinition | readonly StepDefinition[],
  ): WorkflowBuilder<Input, any, any, any> {
    const list: readonly StepDefinition[] = Array.isArray(defs) ? defs : [defs as StepDefinition];
    return new WorkflowBuilder({
      ...this.s,
      steps: appendSteps({ seq: this.s.steps, defs: list, onDuplicate: duplicateStepError }),
      lastStepName: list[list.length - 1]!.name,
    });
  }

  /** Dependencies of a linear step: the current head, if any. */
  private _linearDeps(): string[] {
    return this.s.lastStepName ? [this.s.lastStepName] : [];
  }

  /** The codec a step uses: its own `override`, else the workflow default. */
  private _codec(override?: Codec<any>): Codec<unknown> {
    return (override ?? this.s.config.codec ?? LosslessJsonCodec) as Codec<unknown>;
  }

  private _validateName(name: string): void {
    if (hasStep({ seq: this.s.steps, name })) throw duplicateStepError(name);
  }

  private _basicStep(
    name: string,
    args: StepArgs<AnyStepFn>,
  ): WorkflowBuilder<Input, any, any, any> {
    this._validateName(name);
    return this._append(
      createBasicStep({
        name,
        dependsOn: args.dependsOn,
        isLinear: args.isLinear,
        fn: args.fn,
        options: args.options,
        codec: this._codec(args.options?.codec),
        cacheNamespace: this.s.config.name,
      }),
    );
  }

  private _loop<T>(params: {
    readonly name: string;
    readonly body: (ctx: StepContext<Input, Current>, iter: number) => unknown;
    readonly keepGoing: (result: T, iter: number) => boolean;
    readonly options: LoopOptions<T, Input, Current> | undefined;
    readonly asyncVariant: string;
  }): WorkflowBuilder<Input, any, any, any> {
    this._validateName(params.name);
    return this._append(
      createLoopStep({
        name: params.name,
        dependsOn: this._linearDeps(),
        body: params.body,
        keepGoing: params.keepGoing,
        options: params.options as LoopOptions<unknown> | undefined,
        codec: this._codec(params.options?.codec),
        asyncVariant: params.asyncVariant,
      }),
    );
  }
}

/** Lift a value-or-Promise loop body into an `Eff` body; a rejection is a defect. */
function asyncLoopBody<C, T>(
  body: (ctx: C, iter: number) => T | PromiseLike<T>,
): (ctx: C, iter: number) => StepEff<T> {
  return (ctx, iter) => promiseOrDie(async () => body(ctx, iter));
}

// ---------------------------------------------------------------------------
// Constructor functions
// ---------------------------------------------------------------------------

export function workflow<Input>(params: WorkflowParams<Input>): WorkflowBuilder<Input> {
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
  return new WorkflowBuilder({
    config: { ...params, onVersionMismatch: params.onVersionMismatch ?? "strict" },
    steps: emptySteps(),
    lastStepName: null,
  });
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
  return new WorkflowBuilder({
    config: { name, hooks, onVersionMismatch: "strict" },
    steps: emptySteps(),
    lastStepName: null,
  });
}
