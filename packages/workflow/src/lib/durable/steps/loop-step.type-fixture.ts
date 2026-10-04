// ---------------------------------------------------------------------------
// Compile-time fixtures for loop body types. `.dowhile()` / `.dountil()`
// take an `Eff` body (like `.step()`): the condition sees the effect's
// value, and its typed failures join the builder's Error channel.
// `.dowhileAsync()` / `.dountilAsync()` take a value or Promise body.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import type { LoopLimitExceededError } from "../durable-pipeline-error.ts";
import { workflow, type WorkflowBuilder } from "../workflow-builder.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

type ErrorOf<B> = B extends WorkflowBuilder<any, any, any, infer E> ? E : never;
type CurrentOf<B> = B extends WorkflowBuilder<any, any, infer C, any> ? C : never;
type Equals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const assertType = <T extends true>(_: T): void => {};

const base = workflow<number>({ name: "loop-fixture" });

// The condition receives the Eff's value, not the Eff.
const effLoop = base.dowhile(
  "l",
  ({ prev }, iter) => (iter > 3 ? fail(new Boom({ message: "x" })) : succeed(prev + iter)),
  (n, iter) => n.toFixed(0) !== "" && iter < 10,
);
assertType<Equals<CurrentOf<typeof effLoop>, number>>(true);
// Typed failures of the body join the Error channel.
assertType<Equals<ErrorOf<typeof effLoop>, Boom | LoopLimitExceededError>>(true);

const untilLoop = base.dountil(
  "u",
  () => succeed("s"),
  (s) => s.length > 0,
);
assertType<Equals<CurrentOf<typeof untilLoop>, string>>(true);

// A plain value is not an Eff body: use `.dowhileAsync()`.
base.dowhile(
  "plain",
  // @ts-expect-error — `.dowhile()` bodies return an Eff
  () => 1,
  () => false,
);

// Promise (or plain value) bodies go through the Async variants; their
// rejections are defects, so nothing joins the Error channel.
const asyncLoop = base.dowhileAsync(
  "a",
  async (_ctx, iter) => ({ iter }),
  (r) => r.iter < 3,
);
assertType<Equals<CurrentOf<typeof asyncLoop>, { iter: number }>>(true);
assertType<Equals<ErrorOf<typeof asyncLoop>, LoopLimitExceededError>>(true);

const syncUntil = base.dountilAsync(
  "s",
  (_ctx, iter) => iter,
  (n) => n >= 3,
);
assertType<Equals<CurrentOf<typeof syncUntil>, number>>(true);
