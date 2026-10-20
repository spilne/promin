// ---------------------------------------------------------------------------
// Compile-time fixtures for step options per step kind.
//
// Each step kind's options type admits exactly the options that kind
// honours (runtime behaviour: `tests/step-options-uniform.test.ts`). An
// option a kind cannot honour must be a compile error, not silently
// ignored: every `@ts-expect-error` below fails the typecheck if the option
// ever becomes accepted again.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { MemoryCache } from "../shared/cache-store.ts";
import { workflow, type StepQueueOption } from "./durable-pipeline.ts";

const store = new MemoryCache<string, unknown>({ ttlMs: 1_000 });
const cache = { key: () => "k", ttlMs: 1_000, store };
const retry = { maxRetries: 1 };

const base = workflow<number>({ name: "fixture" }).step("arr", () => succeed([1, 2]));
const child = workflow<number>({ name: "fixture-child" })
  .step("c", ({ input }) => succeed(input * 2))
  .build();

// ---------------------------------------------------------------------------
// Kinds that honour every StepOptions field.
// ---------------------------------------------------------------------------

export const _branchAll = base.branch(
  "b",
  { condition: () => true, ifTrue: () => succeed(1), ifFalse: () => succeed(2) },
  { retry, timeoutMs: 1, onFailure: { fallback: () => 0 }, cache, needs: ["x"], priority: 1 },
);

export const _matchAll = base.match(
  "m",
  { on: () => "a", cases: { a: () => succeed("A") } },
  { retry, timeoutMs: 1, onFailure: "skip", cache, queue: { concurrencyLimit: 1 } },
);

// ---------------------------------------------------------------------------
// mapOver: step-level options are typed by the array result; element
// options by the element.
// ---------------------------------------------------------------------------

export const _mapOverAll = base.mapOver("m", { array: "arr" }, (n) => succeed(String(n)), {
  retry,
  timeoutMs: 1,
  cache,
  onFailure: { fallback: () => ["a"] },
  skipValue: () => [],
  element: { retry, timeoutMs: 1 },
});

base.mapOver("m", { array: "arr" }, (n) => succeed(String(n)), {
  // @ts-expect-error — the fallback replaces the whole array, not one element
  onFailure: { fallback: () => "a" },
});

base.mapOver("m", { array: "arr" }, (n) => succeed(String(n)), {
  // @ts-expect-error — element options have no onFailure (use the step's)
  element: { onFailure: "skip" },
});

// ---------------------------------------------------------------------------
// parallelSteps: per-branch options typed by that branch's output; failure
// handling, compensation and skipping are per branch only.
// ---------------------------------------------------------------------------

export const _parallelOk = base.parallelSteps(
  "p",
  { a: () => succeed(1), b: () => succeed("s") },
  {
    retry,
    timeoutMs: 1,
    cache,
    branches: { a: { onFailure: { fallback: () => 0 } }, b: { compensate: () => succeed(1) } },
  },
);

base.parallelSteps(
  "p",
  { a: () => succeed(1), b: () => succeed("s") },
  // @ts-expect-error — block-level onFailure: each branch has its own result type
  { onFailure: { fallback: () => ({ a: 0, b: "" }) } },
);

base.parallelSteps(
  "p",
  { a: () => succeed(1) },
  // @ts-expect-error — block-level compensate: compensate a branch instead
  { compensate: () => succeed(undefined) },
);

base.parallelSteps(
  "p",
  { a: () => succeed(1) },
  // @ts-expect-error — block-level skipWhen: skip a branch instead
  { skipWhen: () => true },
);

base.parallelSteps(
  "p",
  { a: () => succeed(1) },
  // @ts-expect-error — a branch fallback must return that branch's output (number)
  { branches: { a: { onFailure: { fallback: () => "zero" } } } },
);

base.parallelSteps(
  "p",
  { a: () => succeed(1) },
  // @ts-expect-error — options for a branch that does not exist
  { branches: { nope: { retry } } },
);

// ---------------------------------------------------------------------------
// journaled: no cache, no timeoutMs.
// ---------------------------------------------------------------------------

export const _journaledOk = base.journaled(
  "j",
  function* (ctx) {
    return yield* ctx.activity("one", async () => 1);
  },
  { retry, onFailure: "skip", compensate: () => succeed(undefined), needs: ["x"] },
);

base.journaled(
  "j",
  function* (ctx) {
    return yield* ctx.activity("one", async () => 1);
  },
  // @ts-expect-error — journaled steps cannot be cached
  { cache },
);

base.journaled(
  "j",
  function* (ctx) {
    return yield* ctx.activity("one", async () => 1);
  },
  // @ts-expect-error — a journaled body cannot be interrupted by a step timeout
  { timeoutMs: 1 },
);

// ---------------------------------------------------------------------------
// subworkflow: no cache, no timeoutMs.
// ---------------------------------------------------------------------------

const childConfig = { input: () => 1, workflowId: () => "c-1" };

export const _subworkflowOk = base.subworkflow("s", child, childConfig, {
  retry,
  onFailure: { fallback: () => 0 },
  priority: 3,
});

// @ts-expect-error — the child row is the memo; no step cache
base.subworkflow("s", child, childConfig, { cache });

// @ts-expect-error — set the child workflow's own timeoutMs instead
base.subworkflow("s", child, childConfig, { timeoutMs: 1 });

// ---------------------------------------------------------------------------
// dowhile / dountil: everything but cache.
// ---------------------------------------------------------------------------

export const _loopOk = base.dowhile(
  "l",
  () => 1,
  () => false,
  { retry, timeoutMs: 1, onFailure: { fallback: () => 0 }, maxIterations: 3 },
);

base.dountil(
  "l",
  () => 1,
  () => true,
  // @ts-expect-error — iteration rows are the loop's memo; no step cache
  { cache },
);

// ---------------------------------------------------------------------------
// tripwire: codec only.
// ---------------------------------------------------------------------------

export const _tripwireOk = base.tripwire("t", { when: () => false, reason: () => "r" }, {});

// @ts-expect-error — a synchronous predicate has nothing to retry
base.tripwire("t", { when: () => false, reason: () => "r" }, { retry });

// @ts-expect-error — nothing to dispatch to a worker
base.tripwire("t", { when: () => false, reason: () => "r" }, { needs: ["x"] });

// ---------------------------------------------------------------------------
// Removed fields.
// ---------------------------------------------------------------------------

// @ts-expect-error — onFailure.handler was never applied and is removed
base.step("s", () => succeed(1), { onFailure: { handler: () => "skip" } });

// @ts-expect-error — StepOptions.show was never read and is removed
base.step("s", () => succeed(1), { show: () => "" });

// The queue key sees the workflow input, prev and deps (its runtime context).
export const _queue: StepQueueOption = {
  concurrencyLimit: 1,
  concurrencyKey: (ctx) => `${String(ctx.prev)}:${Object.keys(ctx.deps).length}`,
};
export const _queueInput: StepQueueOption = {
  concurrencyLimit: 1,
  concurrencyKey: (ctx) => (ctx.input as { tenantId: string }).tenantId,
};
export const _queueInputUnknown: StepQueueOption = {
  concurrencyLimit: 1,
  // @ts-expect-error — ctx.input is the untyped workflow input, not a string
  concurrencyKey: (ctx) => ctx.input,
};
