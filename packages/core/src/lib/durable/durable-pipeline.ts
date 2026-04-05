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

import { Effect } from "effect";
import { Pipeline, type TaggedError } from "../pipeline.ts";
import type { RetryPolicy } from "../retry.ts";
import type { Codec } from "../typeclasses/codec.ts";
import { JsonCodec } from "../typeclasses/codec.ts";
import type { Show } from "../typeclasses/show.ts";
import type { Sinkable } from "../typeclasses/streamable.ts";
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
// WorkflowDefinition — reusable workflow template
// ---------------------------------------------------------------------------

/** A frozen workflow definition. Produced by `.build()` on WorkflowBuilder. */
export interface WorkflowDefinition<Input, Output> {
  readonly name: string;
  readonly storage: WorkflowStorage;
  readonly dag: WorkflowDAG;
  run(params: { workflowId: string; input: Input }): Promise<Output>;
  runSafe(params: {
    workflowId: string;
    input: Input;
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
// Internal step definition
// ---------------------------------------------------------------------------

type StepKind = "normal" | "map" | "branch" | "sleep" | "signal";

interface StepDefinition {
  readonly name: string;
  readonly dependsOn: string[];
  readonly kind: StepKind;
  readonly execute: (params: ExecuteParams) => Pipeline<unknown, TaggedError>;
  readonly codec: Codec<unknown>;
  readonly retry?: RetryPolicy<TaggedError>;
  readonly onFailure?: StepFailureStrategy<unknown>;
  readonly compensate?: (params: {
    result: unknown;
    input: unknown;
    workflowId: string;
  }) => Pipeline<void, any> | Promise<void>;
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
  ) {}

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

  async run(params: { workflowId: string; input: Input }): Promise<Current> {
    const { workflowId, input } = params;
    const workflowStartTime = Date.now();
    const compensateTrigger = this._compensateConfig?.trigger ?? "after-retries";
    const maxWorkflowRetries =
      compensateTrigger === "immediate" ? 0 : (this._retry?.maxRetries ?? 0);
    const workflowRetryDelayMs = this._retry?.baseDelayMs ?? 1000;

    // 1. Acquire lock
    const locked = await this._storage.tryLock(workflowId, DEFAULT_LOCK_DURATION_MS);
    if (!locked) {
      throw new WorkflowLockError({
        workflowId,
        message: `Could not acquire lock on workflow "${workflowId}" — already running`,
      });
    }

    try {
      // 2. Load or create workflow state
      let state = await this._storage.loadWorkflow(workflowId);
      if (!state) {
        await this._storage.createWorkflow({
          workflowId,
          workflowName: this._name,
          input,
          workflowType: this._type,
          metadata: this._metadata,
        });
        state = await this._storage.loadWorkflow(workflowId);
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
    } finally {
      // 6. Release lock
      await this._storage.releaseLock(workflowId);
    }
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

  async runSafe(params: { workflowId: string; input: Input }): Promise<
    | { data: Current; error: null }
    | {
        data: null;
        error:
          | Error
          | WorkflowError
          | StepError
          | WorkflowLockError
          | WorkflowSuspendedError
          | WorkflowTimeoutError;
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
          | WorkflowTimeoutError;
      }
  > {
    return this.runSafe({ workflowId: crypto.randomUUID(), input });
  }

  // ---------------------------------------------------------------------------
  // build — freeze into a reusable WorkflowDefinition
  // ---------------------------------------------------------------------------

  build(): WorkflowDefinition<Input, Current> {
    const self = this;
    return {
      name: this._name,
      storage: this._storage,
      dag: this.toJSON(),
      run: (params) => self.run(params),
      runSafe: (params) => self.runSafe(params) as any,
      invoke: (params) =>
        Pipeline.fromPromise(async () => {
          // If parentWorkflowId provided, store it in metadata
          if (params.parentWorkflowId) {
            const state = await self._storage.loadWorkflow(params.workflowId);
            if (!state) {
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
}): WorkflowBuilder<Input> {
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
  }[];
}

/** Convert a WorkflowDAG to Mermaid graph syntax. */
export function dagToMermaid(dag: WorkflowDAG): string {
  const lines: string[] = ["graph LR"];
  for (const step of dag.steps) {
    const id = step.name.replace(/[^a-zA-Z0-9]/g, "_");
    lines.push(`    ${id}["${step.name}"]`);
    for (const dep of step.dependsOn) {
      lines.push(`    ${dep.replace(/[^a-zA-Z0-9]/g, "_")} --> ${id}`);
    }
  }
  return lines.join("\n");
}

/** Convert a WorkflowDAG to DOT (Graphviz) syntax. */
export function dagToDot(dag: WorkflowDAG): string {
  const lines: string[] = [`digraph "${dag.name}" {`];
  for (const step of dag.steps) {
    lines.push(`    "${step.name}";`);
    for (const dep of step.dependsOn) {
      lines.push(`    "${dep}" -> "${step.name}";`);
    }
  }
  lines.push("}");
  return lines.join("\n");
}
