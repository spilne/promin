// ---------------------------------------------------------------------------
// Compile-time fixtures for the builder's public types.
//
//   1. The typed error channel reaches `Workflow<I, O, E>` and the runner's
//      `run` / `runSafe` / `start`; engine control flow stays out of it.
//   2. Step names: `waitForSignal`, `sleep` and non-literal names never
//      widen the named steps, so a `dependsOn` typo is a compile error.
//   3. Step option callbacks (`compensate`, `skipWhen`, `skipValue`,
//      `cache.key`) see the workflow input and `prev` typed.
//   4. `.map()` changes `prev` for the next step only; the head step keeps
//      its own (unmapped) type under its name.
//
// Every `@ts-expect-error` fails the typecheck if the line ever compiles,
// and every `Equal` assertion fails if the type drifts.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { MemoryCache } from "../shared/cache-store.ts";
import type {
  GuardError,
  LoopLimitExceededError,
  WorkflowTimeoutError,
} from "./durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import type { MatchError } from "./steps/match-step.ts";
import { workflow } from "./workflow-builder.ts";
import { createWorkflowRunner, type WorkflowRunError } from "./workflow-runner.ts";
import type { Workflow, WorkflowErrorOf } from "./workflow-types.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

class PaymentDeclined extends TaggedError("PaymentDeclined")<{ readonly amount: number }>() {}
class OutOfStock extends TaggedError("OutOfStock")<{ readonly sku: string }>() {}

interface Order {
  readonly id: string;
  readonly sku: string;
  readonly amount: number;
}

// ---------------------------------------------------------------------------
// 1. Typed errors reach the Workflow and the runner.
// ---------------------------------------------------------------------------

const checkout = workflow<Order>({ name: "checkout" })
  .step("reserve", ({ input }) =>
    input.sku === "" ? fail(new OutOfStock({ sku: input.sku })) : succeed(input.sku),
  )
  .guard("has-sku", (sku) => sku.length > 0)
  .step("charge", ({ input }) =>
    input.amount > 100 ? fail(new PaymentDeclined({ amount: input.amount })) : succeed(input.id),
  )
  .build();

export const _checkoutErrors: Equal<
  WorkflowErrorOf<typeof checkout>,
  OutOfStock | GuardError | PaymentDeclined
> = true;

// Step kinds add their own failures; suspension is control flow, not one.
const kinds = workflow<number>({ name: "kinds" })
  .sleep("nap", 1_000)
  .waitForSignal<string>("approval", { signalName: "approve" })
  .match("route", { on: (s) => s, cases: { a: () => succeed(1) } })
  .dowhile(
    "poll",
    () => succeed(1),
    () => false,
  )
  .build();

export const _kindErrors: Equal<
  WorkflowErrorOf<typeof kinds>,
  WorkflowTimeoutError | MatchError | LoopLimitExceededError
> = true;

// A workflow without typed failures has `E = never`.
export const _noErrors: Equal<
  WorkflowErrorOf<ReturnType<ReturnType<typeof workflow<number>>["build"]>>,
  never
> = true;

// Additive: annotations without `E` still accept a workflow with errors.
export const _annotated: Workflow<Order, string> = checkout;
export const _erased: Workflow<unknown, unknown> = checkout;

export async function _runTyping(): Promise<void> {
  const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });

  // `run` resolves with the workflow's output.
  const out: string = await runner.run({ workflow: checkout, workflowId: "w", input: {} });
  // @ts-expect-error — the output is a string
  const wrong: number = await runner.run({ workflow: checkout, workflowId: "w", input: {} });

  // `runSafe` types `data` by the output and lists the typed errors first.
  const r = await runner.runSafe({ workflow: checkout, workflowId: "w", input: {} });
  if (r.error === null) {
    const data: string = r.data;
    void data;
  } else {
    const errors: Equal<
      typeof r.error,
      WorkflowRunError<OutOfStock | GuardError | PaymentDeclined>
    > = true;
    void errors;
    if (r.error instanceof PaymentDeclined) {
      const amount: number = r.error.amount;
      void amount;
    }
  }

  // The `{ name }` form resolves through the registry: untyped, as before.
  const byName = await runner.run({ name: "checkout", workflowId: "w", input: {} });
  const unknownOut: Equal<typeof byName, unknown> = true;

  // `start` carries `E` on the handle.
  const handle = await runner.start({ workflow: checkout, workflowId: "w", input: order() });
  const result: string = await handle.result();
  void [out, wrong, unknownOut, result];
}

function order(): Order {
  return { id: "o", sku: "s", amount: 1 };
}

// ---------------------------------------------------------------------------
// 2. Step names never widen the named steps.
// ---------------------------------------------------------------------------

const signalled = workflow<number>({ name: "signals" })
  .step("start", ({ input }) => succeed(input))
  .waitForSignal<string>("approval", { signalName: "approve" });

// @ts-expect-error — a typo is not a step name (waitForSignal used to widen to Record<string, T>)
signalled.step("next", { dependsOn: ["tpyo"] }, () => succeed(1));

// @ts-expect-error — with `T` passed explicitly the name is not inferred as a literal
signalled.step("next", { dependsOn: ["approval"] }, () => succeed(1));

// The steps before it are still addressable.
export const _beforeSignal = signalled.step("next", { dependsOn: ["start"] }, ({ deps }) =>
  succeed(deps.start + 1),
);

// Passing the name type too (or letting `codec` infer `T`) makes it addressable.
const stringCodec: Codec<string> = { encode: (s) => s, decode: (raw) => String(raw) };
export const _signalByName = workflow<number>({ name: "signals-named" })
  .waitForSignal<string, "approval">("approval", { signalName: "approve" })
  .waitForSignal("comment", { signalName: "comment", codec: stringCodec })
  .step("next", { dependsOn: ["approval", "comment"] }, ({ deps }) => {
    const both: Equal<typeof deps, { approval: string; comment: string }> = true;
    return succeed(both);
  });

// `sleep` passes `prev` through and is addressable with the predecessor's type.
export const _sleep = workflow<number>({ name: "sleepy" })
  .step("load", () => succeed({ id: "x" }))
  .sleep("nap", 1_000)
  .step("after", ({ prev }) => succeed(prev.id))
  .step("dep", { dependsOn: ["nap"] }, ({ deps }) => succeed(deps.nap.id));

// A non-literal (`string`) step name adds nothing to the named steps.
declare const dynamicName: string;
workflow<number>({ name: "dynamic" })
  .step(dynamicName, () => succeed(1))
  // @ts-expect-error — `string` must not widen the steps to Record<string, number>
  .step("next", { dependsOn: ["anything"] }, () => succeed(1));

// ---------------------------------------------------------------------------
// 3. Option callbacks are typed by the workflow input and `prev`.
// ---------------------------------------------------------------------------

const cacheStore = new MemoryCache<string, unknown>({ ttlMs: 1_000 });
const loaded = workflow<Order>({ name: "callbacks" }).step("load", ({ input }) =>
  succeed({ orderId: input.id, total: input.amount }),
);

export const _callbacksOk = loaded.step("ship", ({ prev }) => succeed(prev.orderId), {
  skipWhen: (prev) => prev.total === 0,
  skipValue: (prev) => prev.orderId,
  compensate: ({ result, input }) => succeed(`${result}:${input.sku}`),
  cache: { key: ({ input, prev }) => `${input.id}:${prev.total}`, ttlMs: 1_000, store: cacheStore },
});

// @ts-expect-error — `prev` is the loaded record, which has no `nope`
loaded.step("ship", () => succeed(1), { skipWhen: (prev) => prev.nope });

// @ts-expect-error — `input` is the workflow input (Order), which has no `nope`
loaded.step("ship", () => succeed(1), { compensate: ({ input }) => succeed(input.nope) });

// @ts-expect-error — skipValue must return the step's output (number)
loaded.step("ship", () => succeed(1), { skipValue: (prev) => prev.orderId });

// A DAG step's `prev` is its first dependency's result.
export const _dagPrev = loaded
  .step("count", () => succeed(3))
  .step("dag", { dependsOn: ["count", "load"] }, () => succeed(true), {
    skipWhen: (prev) => {
      const isCount: Equal<typeof prev, number> = true;
      return isCount && prev > 2;
    },
  });

// mapOver: `prev` is the source array.
export const _mapOverPrev = loaded
  .step("items", () => succeed([1, 2, 3]))
  .mapOver("double", { array: "items" }, (n) => succeed(n * 2), {
    skipWhen: (prev) => prev.length === 0,
  });

// ---------------------------------------------------------------------------
// 4. `.map()` maps `prev`; the head keeps its unmapped type by name.
// ---------------------------------------------------------------------------

export const _mapped = workflow<number>({ name: "mapped" })
  .step("load", () => succeed({ id: "x", n: 1 }))
  .map((v) => v.n)
  .step("next", ({ prev }) => {
    const isNumber: Equal<typeof prev, number> = true;
    return succeed(isNumber);
  })
  .step("byName", { dependsOn: ["load"] }, ({ deps }) => succeed(deps.load.id));

// parallelSteps: branch outputs are inferred without `any`.
export const _parallel = workflow<number>({ name: "par" })
  .parallelSteps("both", { a: () => succeed(1), b: () => succeed("s") })
  .step("join", ({ prev }) => {
    const joined: Equal<typeof prev, { a: number; b: string }> = true;
    return succeed(joined);
  });
