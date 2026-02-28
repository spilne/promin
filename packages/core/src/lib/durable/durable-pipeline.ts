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
import type { Codec } from "../typeclasses/codec.ts";
import { JsonCodec } from "../typeclasses/codec.ts";
import type { Show } from "../typeclasses/show.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
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
  run(params: { workflowId: string; input: Input }): Promise<Output>;
  runSafe(params: {
    workflowId: string;
    input: Input;
  }): Promise<{ data: Output; error: null } | { data: null; error: unknown }>;
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

export interface StepOptions<T> {
  readonly codec?: Codec<T>;
  readonly show?: Show<T>;
  readonly timeoutMs?: number;
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
}

interface ExecuteParams {
  readonly input: unknown;
  readonly results: Record<string, unknown>;
  readonly workflowId: string;
  readonly storage: WorkflowStorage;
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

    return new WorkflowBuilder(
      this._name,
      this._storage,
      [...this._steps, stepDef],
      name,
      this._hooks,
    ) as any;
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

    return new WorkflowBuilder(
      this._name,
      this._storage,
      [...this._steps, stepDef],
      name,
      this._hooks,
    ) as any;
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

    return new WorkflowBuilder(
      this._name,
      this._storage,
      [...this._steps, stepDef],
      name,
      this._hooks,
    ) as any;
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

    return new WorkflowBuilder(
      this._name,
      this._storage,
      [...this._steps, stepDef],
      name,
      this._hooks,
    ) as any;
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
    return new WorkflowBuilder(
      this._name,
      this._storage,
      newSteps,
      this._lastStepName,
      this._hooks,
    ) as any;
  }

  // ---------------------------------------------------------------------------
  // Terminal: run
  // ---------------------------------------------------------------------------

  async run(params: { workflowId: string; input: Input }): Promise<Current> {
    const { workflowId, input } = params;
    const workflowStartTime = Date.now();

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
        });
        state = await this._storage.loadWorkflow(workflowId);
      }

      // 3. Validate DAG
      const dagNodes: DagNode[] = this._steps.map((s) => ({
        name: s.name,
        dependsOn: s.dependsOn,
      }));
      topologicalSort({ nodes: dagNodes, workflowId });

      // 4. Execute DAG
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
          throw new WorkflowError({
            workflowId,
            message: "Deadlock: no steps are ready and none are running",
          });
        }

        if (ready.length === 0) {
          break;
        }

        for (const name of ready) {
          running.add(name);
        }

        // Execute all ready steps in parallel
        const readySteps = ready.map((name) => this._steps.find((s) => s.name === name)!);

        const pipeline = Pipeline.all(
          ...readySteps.map((stepDef) => {
            const startedAt = new Date();
            const startTime = startedAt.getTime();
            return stepDef
              .execute({ input, results, workflowId, storage: this._storage })
              .map((result) => {
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
            throw stepError;
          }

          // Timeout errors fail the workflow
          if (tag === "WorkflowTimeoutError") {
            const te = stepError as WorkflowTimeoutError;
            await this._storage.saveStepFailure({
              workflowId,
              stepName: te.stepName,
              error: te.message,
              durationMs: 0,
              startedAt: new Date(),
            });
            await this._hooks?.onStepFailure?.({
              workflowId,
              stepName: te.stepName,
              error: te.message,
              durationMs: 0,
            });
            await this._storage.failWorkflow(workflowId, te.message);
            await this._hooks?.onWorkflowFailure?.({
              workflowId,
              error: te.message,
              durationMs: Date.now() - workflowStartTime,
            });
            throw stepError;
          }

          // All other errors
          const stepName =
            tag === "StepError" ? (stepError as StepError).stepName : (ready[0] ?? "unknown");
          const errorMsg =
            stepError instanceof globalThis.Error ? stepError.message : String(stepError);
          await this._storage.saveStepFailure({
            workflowId,
            stepName,
            error: errorMsg,
            durationMs: 0,
            startedAt: new Date(),
          });
          await this._hooks?.onStepFailure?.({
            workflowId,
            stepName,
            error: errorMsg,
            durationMs: 0,
          });
          await this._storage.failWorkflow(workflowId, errorMsg);
          await this._hooks?.onWorkflowFailure?.({
            workflowId,
            error: errorMsg,
            durationMs: Date.now() - workflowStartTime,
          });
          throw stepError;
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
          await this._hooks?.onStepComplete?.({ workflowId, stepName: name, result, durationMs });
          results[name] = result;
          completed.add(name);
          running.delete(name);
        }
      }

      // 5. Complete workflow
      const lastStepName = this._steps[this._steps.length - 1]!.name;
      const finalResult = results[lastStepName];
      await this._storage.completeWorkflow(workflowId, finalResult);
      await this._hooks?.onWorkflowComplete?.({
        workflowId,
        result: finalResult,
        durationMs: Date.now() - workflowStartTime,
      });

      return finalResult as Current;
    } finally {
      // 6. Release lock
      await this._storage.releaseLock(workflowId);
    }
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
  // build — freeze into a reusable WorkflowDefinition
  // ---------------------------------------------------------------------------

  build(): WorkflowDefinition<Input, Current> {
    return {
      name: this._name,
      storage: this._storage,
      run: (params) => this.run(params),
      runSafe: (params) => this.runSafe(params) as any,
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
      execute: (execParams) => {
        if (params.isLinear) {
          const prevStepName = params.dependsOn[0];
          const prev = prevStepName != null ? execParams.results[prevStepName] : execParams.input;
          return params.fn({
            input: execParams.input,
            prev,
            workflowId: execParams.workflowId,
            attempt: 1,
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
            attempt: 1,
          });
        }
      },
    };

    return new WorkflowBuilder(
      this._name,
      this._storage,
      [...this._steps, stepDef],
      params.name,
      this._hooks,
    );
  }
}

// ---------------------------------------------------------------------------
// Constructor function
// ---------------------------------------------------------------------------

export function workflow<Input>(params: {
  name: string;
  storage: WorkflowStorage;
  hooks?: WorkflowHooks;
}): WorkflowBuilder<Input> {
  return new WorkflowBuilder(params.name, params.storage, [], null, params.hooks);
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
