// ---------------------------------------------------------------------------
// Composite ctx methods built from other yields: ctx.parallel (branches
// driven concurrently, each in its own slot scope), ctx.dowhile /
// ctx.dountil (a loop of journaled activities) and ctx.proxy (activity
// functions bound by property name).
// ---------------------------------------------------------------------------

import { LoopLimitExceededError } from "./durable-pipeline-error.ts";
import { branchPrefix } from "./journal-format.ts";
import { activityScope, type ActivityScope } from "./journaled-body-scope.ts";
import type { ActivityFn } from "./journaled-ctx-activity.ts";
import type {
  ActivityOptions,
  ActivityYield,
  JournaledContext,
  JournaledCtxEnv,
} from "./journaled-context.ts";

type Ctx = JournaledContext<unknown, unknown>;

/** Build `ctx.parallel` for one body run. */
export function makeParallel(env: JournaledCtxEnv): Ctx["parallel"] {
  const { cursor } = env;
  return function* parallel<T>(
    branches: ReadonlyArray<Generator<ActivityYield, T, T>>,
  ): Generator<ActivityYield, T[], unknown> {
    // The parallel takes its own slot like any yield: from the enclosing
    // branch when nested, else from the step-level counter.
    const { activityIndex: parallelActivityIndex, branchPath: parallelPath } = cursor.allocateSlot({
      suspendOrChild: false,
    });
    const format = cursor.formatForParallel(parallelActivityIndex);

    // Drive each branch sub-generator in its own ActivityScope so its
    // yields consume slots from a branch-local counter with a branch-
    // specific path prefix.
    const promise = Promise.all(
      branches.map((branchGen, i) => {
        const branchScope: ActivityScope = {
          parallelActivityIndex,
          pathPrefix: branchPrefix({ format, parallelPath, branch: i }),
          localCounter: { next: 0 },
          format,
        };
        return activityScope.run(branchScope, () => driveSubGenerator(branchGen));
      }),
    );

    return (yield { _tag: "Activity", name: "parallel", promise }) as unknown as T[];
  };
}

/**
 * Drive a branch sub-generator to completion, awaiting each yielded
 * ActivityYield. Mirrors the outer runner but stays inside whatever
 * `activityScope` the caller has set, so activities inside the branch see
 * the branch-local scope.
 */
async function driveSubGenerator<T>(gen: Generator<ActivityYield, T, T>): Promise<T> {
  let step = gen.next();
  while (!step.done) {
    const yielded = step.value;
    try {
      const resolved = await yielded.promise;
      step = gen.next(resolved as never);
    } catch (err) {
      step = gen.throw(err);
    }
  }
  return step.value;
}

/** Build `ctx.dowhile` and `ctx.dountil` for one body run. */
export function makeLoops(params: {
  readonly env: JournaledCtxEnv;
  readonly activity: ActivityFn;
}): { readonly dowhile: Ctx["dowhile"]; readonly dountil: Ctx["dountil"] } {
  const { env, activity } = params;
  const { workflowId, stepName } = env;

  // Each iteration yields through `ctx.activity` so it lands in the journal
  // as its own entry; the generator delegates each yield to the outer
  // driver, preserving the single-shot per-yield contract the engine
  // expects.
  function* dowhile<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown> {
    const max = options?.maxIterations ?? 100;
    if (max < 1) {
      throw new LoopLimitExceededError({
        workflowId,
        stepName,
        maxIterations: max,
        message: `ctx.dowhile("${name}"): maxIterations must be >= 1`,
      });
    }
    let result: T = undefined as unknown as T;
    let iter = 0;
    while (true) {
      if (iter >= max) {
        throw new LoopLimitExceededError({
          workflowId,
          stepName,
          maxIterations: max,
          message: `ctx.dowhile("${name}") exceeded ${max} iterations without converging`,
        });
      }
      const currentIter = iter;
      const iterGen = activity(`${name}-iter-${currentIter}`, () => fn(currentIter)) as Generator<
        ActivityYield,
        T,
        unknown
      >;
      result = yield* iterGen;
      iter++;
      if (!condition(result, currentIter)) break;
    }
    return result;
  }

  // `dountil(cond) ≡ dowhile(!cond)` — no need for a parallel loop body.
  function dountil<T>(
    name: string,
    fn: (iter: number) => T | Promise<T>,
    condition: (result: T, iter: number) => boolean,
    options?: { readonly maxIterations?: number },
  ): Generator<ActivityYield, T, unknown> {
    return dowhile(name, fn, (r, i) => !condition(r, i), options);
  }

  return { dowhile, dountil };
}

/** Build `ctx.proxy` over `activity`. */
export function makeProxy(activity: ActivityFn): Ctx["proxy"] {
  return function proxy<Acts extends Record<string, (...args: any[]) => any>>(
    activities: Acts,
    options?: {
      readonly defaultOptions?: ActivityOptions<unknown>;
      readonly optionsByName?: { readonly [K in keyof Acts]?: ActivityOptions<unknown> };
    },
  ): {
    readonly [K in keyof Acts]: (
      ...args: Parameters<Acts[K]>
    ) => Generator<ActivityYield, Awaited<ReturnType<Acts[K]>>, unknown>;
  } {
    const out: Record<string, (...args: unknown[]) => Generator<ActivityYield, unknown, unknown>> =
      {};
    for (const key of Object.keys(activities)) {
      const fn = activities[key as keyof Acts];
      const perKey = options?.optionsByName?.[key as keyof Acts];
      const merged = perKey ?? options?.defaultOptions;
      out[key] = (...args: unknown[]) =>
        // Proxy methods are static activities — closure over `args` keeps
        // input capture local to this call, matching the 2-arg
        // `ctx.activity(name, fn)` form. Names are taken from the property
        // key (not from `fn.name`, which is mangled by bundlers).
        activity(key, () => fn(...args), merged as ActivityOptions<unknown> | undefined);
    }
    return out as {
      readonly [K in keyof Acts]: (
        ...args: Parameters<Acts[K]>
      ) => Generator<ActivityYield, Awaited<ReturnType<Acts[K]>>, unknown>;
    };
  };
}
