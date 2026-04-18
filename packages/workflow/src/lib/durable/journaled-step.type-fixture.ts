// ---------------------------------------------------------------------------
// Compile-time fixtures for the .journaled() step body contract.
//
// This file participates in `tsc --build` but contains no runtime logic.
// Purpose: lock down the type of `JournaledStepBody` so the two footguns
// promin-o4qf wanted a lint rule for stay caught by TypeScript itself:
//
//   1. Bare `await` in the body — re-fires on replay, not journaled.
//      Already impossible: the body is a sync `function*`, so `await` is
//      a SYNTAX error (TS1308). No type-level guard needed — the parser
//      refuses to accept it.
//
//   2. `async function*` body — would let its own `await` compile, but the
//      resulting `AsyncGenerator<...>` must NOT be assignable to
//      `Generator<ActivityYield, ..., ...>`. This IS a type-level property,
//      and the fixture below asserts it directly with a conditional-type
//      check. If someone ever widened JournaledStepBody to accept async
//      generators, this assertion flips from "PASS" to "FAIL" and tsc
//      fails the build.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import type { ActivityYield, JournaledContext, JournaledStepBody } from "./journaled-step.ts";

// Synthesized stand-ins for the generics — specifics don't matter; only the
// yield type and return type drive assignability here.
type Ctx = JournaledContext<unknown, unknown>;
type Body<Out> = JournaledStepBody<unknown, unknown, Out>;

// ---------------------------------------------------------------------------
// 1. Async-generator bodies must NOT be assignable to JournaledStepBody.
// ---------------------------------------------------------------------------

// An async generator body: the function shape a user might write to "make
// await work" inside a journaled step. It MUST NOT satisfy JournaledStepBody.
type AsyncBody<Out> = (ctx: Ctx, prev: unknown) => AsyncGenerator<ActivityYield, Out, unknown>;

// Conditional-type probe: if AsyncBody<number> were assignable to Body<number>
// the `extends` clause would take the first branch ("FAIL") and tsc would
// then reject `const _notAsync: "PASS" = probe` on the next line. That's how
// we turn "this type relationship must not hold" into a tsc-visible error.
type _ProbeAsyncBodyNotAssignable = AsyncBody<number> extends Body<number> ? "FAIL" : "PASS";
export const _asyncBodyIsRejected: _ProbeAsyncBodyNotAssignable = "PASS";

// ---------------------------------------------------------------------------
// 2. Sync-generator bodies with a matching yield type MUST be assignable.
//    If this assertion ever fails, we accidentally broke the happy path.
// ---------------------------------------------------------------------------

type SyncBody<Out> = (ctx: Ctx, prev: unknown) => Generator<ActivityYield, Out, unknown>;
type _ProbeSyncBodyAccepted = SyncBody<number> extends Body<number> ? "PASS" : "FAIL";
export const _syncBodyIsAccepted: _ProbeSyncBodyAccepted = "PASS";

// ---------------------------------------------------------------------------
// 3. Positive runtime fixture — a legal body that only yields through
//    ctx.activity. Here to catch regressions that would make even the
//    blessed pattern fail to compile.
// ---------------------------------------------------------------------------

export const _happyPath: Body<number> = function* (ctx: Ctx) {
  const n = yield* ctx.activity("unit", async () => 1);
  return n;
};
