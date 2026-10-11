// ---------------------------------------------------------------------------
// WorkflowFragment — a reusable run of steps, applied to a workflow with
// `WorkflowBuilder.use()`. Built with the top-level `step()`, extended with
// `.andThen()` / `.parallel()`. A fragment only records the calls; `use()`
// replays them on the builder, so the steps it adds are ordinary steps of
// that workflow.
// ---------------------------------------------------------------------------

import type { TaggedError } from "../shared/tagged-error.ts";
import type { ParallelOptions, StepBody, StepContext, StepOptions } from "./step-definition.ts";
import type { BranchError, BranchOutput } from "./steps/parallel-steps.ts";
import type { AddStep } from "./workflow-builder.ts";

type FragmentOperation =
  | {
      readonly kind: "step";
      readonly name: string;
      readonly fn: (ctx: never) => unknown;
      readonly options: StepOptions<any, any, any> | undefined;
    }
  | {
      readonly kind: "parallel";
      readonly name: string;
      readonly branches: Readonly<Record<string, (ctx: never) => unknown>>;
      readonly options: ParallelOptions<Record<string, unknown>, any, any> | undefined;
    };

/**
 * `unknown` when a fragment written for input `FInput` and head value
 * `FPrev` can be applied to a workflow with input `Input` whose head value
 * is `Current`; otherwise an object type naming what the fragment expects,
 * so the `use()` call fails to compile with a readable message.
 */
export type FragmentFits<Input, Current, FInput, FPrev> = [Input] extends [FInput]
  ? [Current] extends [FPrev]
    ? unknown
    : { readonly "fragment expects a different prev": FPrev }
  : { readonly "fragment expects a different workflow input": FInput };

/** The record a `.parallel()` block joins its branches into. */
type ParallelOutputs<Branches> = { [K in keyof Branches]: BranchOutput<Branches[K]> };

/**
 * A reusable run of steps. `Input` is the workflow input the steps read,
 * `Prev` the value the first step expects as `prev`, `Steps` the named
 * steps it adds, `Current` its last step's value and `Err` the typed
 * failures of its steps. Immutable: every call returns a new fragment.
 */
export class WorkflowFragment<
  Input,
  Prev,
  Steps extends Record<string, unknown>,
  Current,
  Err extends TaggedError = never,
> {
  /** @internal Use the top-level `step()`. */
  constructor(private readonly operations: readonly FragmentOperation[]) {}

  /** Add a step after the fragment's last one (the same contract as `WorkflowBuilder.step`). */
  andThen<Name extends string, Output, E2 extends TaggedError = never>(
    name: Name,
    fn: StepBody<StepContext<Input, Current>, Output, E2>,
    options?: StepOptions<Output, Input, Current>,
  ): WorkflowFragment<Input, Prev, AddStep<Steps, Name, Output>, Output, Err | E2> {
    return new WorkflowFragment([
      ...this.operations,
      { kind: "step", name, fn, options: options as StepOptions<any, any, any> | undefined },
    ]);
  }

  /** Fork after the fragment's last step (the same contract as `WorkflowBuilder.parallel`). */
  parallel<
    Name extends string,
    Branches extends Record<string, StepBody<StepContext<Input, Current>, unknown, TaggedError>>,
  >(
    name: Name,
    branches: Branches,
    options?: ParallelOptions<ParallelOutputs<Branches>, Input, Current>,
  ): WorkflowFragment<
    Input,
    Prev,
    AddStep<Steps, Name, ParallelOutputs<Branches>>,
    ParallelOutputs<Branches>,
    Err | BranchError<Branches>
  > {
    return new WorkflowFragment([
      ...this.operations,
      {
        kind: "parallel",
        name,
        branches,
        options: options as ParallelOptions<Record<string, unknown>, any, any> | undefined,
      },
    ]);
  }

  /**
   * @internal Replay the fragment's steps on `builder` (a `WorkflowBuilder`)
   * and return the result. Called by `WorkflowBuilder.use()`.
   */
  applyTo(builder: unknown): any {
    let next = builder as {
      step(name: string, fn: unknown, options: unknown): unknown;
      parallel(name: string, branches: unknown, options: unknown): unknown;
    };
    for (const op of this.operations) {
      next = (
        op.kind === "step"
          ? next.step(op.name, op.fn, op.options)
          : next.parallel(op.name, op.branches, op.options)
      ) as typeof next;
    }
    return next;
  }
}

/**
 * Start a reusable fragment with one step. Type the step's context by
 * annotating `fn`'s parameter (or pass the type arguments explicitly);
 * extend it with `.andThen()` / `.parallel()` and apply it with
 * `workflow(...).use(fragment)`.
 *
 * ```typescript
 * const normalizeUser = step("trim", ({ input }: { input: { name: string } }) =>
 *   input.name.trim(),
 * ).andThen("upper", ({ prev }) => prev.toUpperCase());
 *
 * const onboard = workflow<{ name: string }>({ name: "onboard" })
 *   .use(normalizeUser)
 *   .step("greet", ({ prev }) => `hello ${prev}`)
 *   .build();
 * ```
 */
export function step<
  Input = unknown,
  Output = unknown,
  E extends TaggedError = never,
  Prev = unknown,
  Name extends string = string,
>(
  name: Name,
  fn: StepBody<StepContext<Input, Prev>, Output, E>,
  options?: StepOptions<Output, Input, Prev>,
): WorkflowFragment<Input, Prev, AddStep<{}, Name, Output>, Output, E> {
  return new WorkflowFragment([
    { kind: "step", name, fn, options: options as StepOptions<any, any, any> | undefined },
  ]);
}
