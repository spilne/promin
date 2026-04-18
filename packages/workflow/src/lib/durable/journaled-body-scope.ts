// ---------------------------------------------------------------------------
// journaledBodyScope — AsyncLocalStorage used to detect whether code is
// currently executing inside a `.journaled()` step body.
//
// The runner wraps each synchronous tick of the generator in
// `journaledBodyScope.run(...)` so that code written between `yield*
// ctx.activity()` calls runs in this scope. Activity implementations (the
// `fn` arg) run OUTSIDE the scope so external side effects aren't treated as
// non-deterministic.
//
// Consumed today by:
//   - `@promin/workflow/dev` `instrumentNonDeterminism()` — warns when
//     Date.now / Math.random / fetch / setTimeout fire inside a body.
//
// Exported separately from `journaled-step.ts` so test utilities and dev
// tooling can import the scope without dragging the whole runner in.
// ---------------------------------------------------------------------------

import { AsyncLocalStorage } from "node:async_hooks";

interface JournaledBodyContext {
  readonly stepName: string;
}

export const journaledBodyScope = new AsyncLocalStorage<JournaledBodyContext>();

/** True while the current async context is inside a journaled step body. */
export function isInJournaledBody(): boolean {
  return journaledBodyScope.getStore() !== undefined;
}

/** Returns the enclosing journaled step name, or undefined when not inside one. */
export function currentJournaledStepName(): string | undefined {
  return journaledBodyScope.getStore()?.stepName;
}
