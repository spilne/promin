// ---------------------------------------------------------------------------
// journaledBodyScope — async context used to detect whether code is
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

import { AsyncContext } from "../shared/async-context.ts";
import { pathInBranch } from "./journal-format.ts";

interface JournaledBodyContext {
  readonly stepName: string;
}

export const journaledBodyScope = new AsyncContext<JournaledBodyContext>();

/** True while the current async context is inside a journaled step body. */
export function isInJournaledBody(): boolean {
  return journaledBodyScope.getStore() !== undefined;
}

/** Returns the enclosing journaled step name, or undefined when not inside one. */
export function currentJournaledStepName(): string | undefined {
  return journaledBodyScope.getStore()?.stepName;
}

// ---------------------------------------------------------------------------
// activityScope — "am I inside a ctx.parallel branch?"
//
// Set by ctx.parallel() around each branch driver so every yield from that
// branch (activity, nested parallel, sleep, signal, child) gets the
// parallel's activity_index + a deterministic branch path. When the store is
// `undefined`, execution is at the top of the body and yields draw from the
// top-level counter the runner owns directly.
// ---------------------------------------------------------------------------

export interface ActivityScope {
  /**
   * The activity_index every yield in this scope shares with every other
   * yield in the same parallel. Frozen at the parallel's call-time position.
   */
  readonly parallelActivityIndex: number;
  /** Path of this branch; each yield's path extends it via `pathInBranch`. */
  readonly pathPrefix: string;
  /** Mutable, scope-local yield counter. `{ next: 0 }` at branch entry. */
  readonly localCounter: { next: number };
}

export const activityScope = new AsyncContext<ActivityScope>();

/**
 * Consume the next (branchPath) slot in the current scope. Mutates the
 * scope's local counter. See `journal-format.ts` for the path grammar.
 */
export function nextPathInScope(scope: ActivityScope): string {
  const seq = scope.localCounter.next++;
  return pathInBranch({ prefix: scope.pathPrefix, seq });
}
