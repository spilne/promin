// ---------------------------------------------------------------------------
// Eff helpers — the small set of runtime glue the engine needs around
// perfect's `Eff`: recognising one, lifting a Promise with defect
// semantics, sleeping on a WallClock, and running to a `{ data, error }`.
// ---------------------------------------------------------------------------

import {
  Cause,
  async,
  runExit,
  succeed,
  tryPromise,
  type Eff,
  type ExitT as Exit,
  type Scheduler,
} from "@spilne/perfect-core";
import { createEngineScheduler } from "./engine-scheduler.ts";
import type { WallClock } from "./wall-clock.ts";

// perfect brands every effect with this registered symbol, so the check
// holds across duplicate copies of the library in one process.
const EFF_BRAND = Symbol.for("spilne/eff");

/** `true` when `value` is a perfect `Eff`. */
export function isEff(value: unknown): value is Eff<unknown, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    (value as Record<symbol, unknown>)[EFF_BRAND] === true
  );
}

/** `true` for any object with a callable `then`, e.g. a Promise. */
export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

/**
 * Lift a Promise-returning thunk into an `Eff` whose rejections — and
 * synchronous throws — are defects rather than typed failures. Typed retry
 * and `onFailure` strategies only see typed failures, so wrapping plain
 * async code this way keeps a rejection from being retried or skipped.
 */
export function promiseOrDie<A>(thunk: () => PromiseLike<A>): Eff<A, never> {
  return tryPromise(
    () => Promise.resolve(thunk()),
    (e) => e,
  ).orDie();
}

/**
 * Lift a Promise into an `Eff` like `promiseOrDie`, except that when the
 * Promise resolves to an `Eff` (an `async` function that returns one), that
 * `Eff` is then run in its place — its typed failures stay typed. Effects
 * are not thenable, so a Promise never runs an `Eff` it resolves to.
 */
export function promiseOrEff(thunk: () => PromiseLike<unknown>): Eff<unknown, unknown> {
  // Box the resolved value: perfect would otherwise run a success value
  // that is itself an `Eff` inside `orDie`, turning its failures into defects.
  return promiseOrDie(() => Promise.resolve(thunk()).then((value) => ({ value }))).flatMap(
    ({ value }) => (isEff(value) ? value : succeed(value)),
  );
}

/** Sleep `ms` on a `WallClock`, so `FakeWallClock.advance()` drives it. */
export function sleepOn(clock: WallClock, ms: number): Eff<void, never> {
  return async<void>((resume) => {
    const handle = clock.setTimeout(() => resume(succeed(undefined)), ms);
    return () => handle.clear();
  }) as Eff<void, never>;
}

let engineScheduler: Scheduler | undefined;
function getEngineScheduler(): Scheduler {
  engineScheduler ??= createEngineScheduler();
  return engineScheduler;
}

/** `runExit` on the engine scheduler (see `engine-scheduler.ts`). */
export function runEngineExit<A>(eff: Eff<A, unknown>): Promise<Exit<unknown, A>> {
  return runExit(eff, getEngineScheduler());
}

/**
 * Run an `Eff` and settle it as `{ data, error }`. Typed failures land in
 * `error`. Defects reject unless `catchDefects` is set, in which case they
 * land in `error` too — as the thrown `Error`, or wrapped in one when the
 * defect is not an `Error`.
 */
export async function runEffSafe<A>(
  eff: Eff<A, unknown>,
  options?: { catchDefects?: boolean },
): Promise<{ data: A; error: null } | { data: null; error: unknown }> {
  const exit = await runEngineExit(eff);
  if (exit._tag === "Success") return { data: exit.value, error: null };
  const failure = Cause.firstFail(exit.cause);
  if (failure) return { data: null, error: failure.value };
  const defect = Cause.squash(exit.cause);
  if (!options?.catchDefects) throw defect;
  return {
    data: null,
    error: defect instanceof Error ? defect : new Error(String(defect), { cause: defect }),
  };
}

/**
 * Resolve what a user callback returned — an `Eff`, a Promise, or a plain
 * value. An `Eff` — returned directly or as what a Promise resolves to — is
 * run; its typed failure or defect rejects as the plain error (never a
 * wrapper).
 */
export async function runHookValue(result: unknown): Promise<unknown> {
  const value = !isEff(result) && isThenable(result) ? await result : result;
  if (isEff(value)) {
    const exit = await runEngineExit(value);
    if (exit._tag === "Failure") throw Cause.squash(exit.cause);
    return exit.value;
  }
  return value;
}

/** `runHookValue` for callbacks whose value is ignored (compensation, hooks). */
export async function runHookResult(result: unknown): Promise<void> {
  await runHookValue(result);
}
