// ---------------------------------------------------------------------------
// Engine scheduler — the fiber scheduler for every `Eff` the engine runs.
//
// Fibers drain on microtasks, like perfect's default `AsyncScheduler`, and
// after a bounded run of consecutive drains the scheduler yields one
// macrotask so timers and I/O are not starved. That yield is requested from
// `setImmediate` and `setTimeout(0)` at once and served by whichever fires
// first.
//
// Why, under Bun: a caller resumed from an engine macrotask may block on a
// nested event loop right away (`expect(promise).resolves` spins one).
//
//   - From a `MessageChannel` message task that deadlocks: Bun never
//     dispatches a port again while one of its message tasks — including
//     the task's microtask checkpoint — is on the stack, so the engine's
//     next turn never comes.
//   - From a `setImmediate` task, once the nested loop has run immediates
//     itself, the rest of the outer task's microtasks wait until some other
//     event wakes the loop — the next pending timer or I/O, seconds away
//     (a continue-as-new chain run under `expect(...).rejects` slowed from
//     ~25 ms to 2.6 s this way). A timer task does not have the problem.
//
// So the engine never yields through a port, and every yield also arms a
// `setTimeout(0)`: if the immediate wins, the timer still fires ~1 ms later
// as a no-op wake-up that ends any such stall. `setTimeout(0)` alone would
// pay that ~1 ms on every yield.
// ---------------------------------------------------------------------------

import type { Scheduler } from "@spilne/perfect-core";

/** Consecutive microtask drains before the scheduler yields a macrotask. */
export const ENGINE_MICRO_DRAIN_BUDGET = 64;

/**
 * Schedules `fn` on some macrotask source; may return a function that
 * cancels it, called when another raced source served the yield first.
 */
export type MacrotaskHop = (fn: () => void) => (() => void) | void;

export interface EngineSchedulerConfig {
  /** Consecutive microtask drains before a macrotask yield. Default: 64. */
  microDrainBudget?: number;
  /**
   * Macrotask sources raced for each yield; the first to fire drains.
   * Default: `setImmediate` (when available) and `setTimeout(0)`.
   */
  macrotaskHops?: readonly MacrotaskHop[];
}

// Event-loop hops, not time math, so they do not go through a Clock.
const IMMEDIATE_HOP: MacrotaskHop = (fn) => {
  const handle = setImmediate(fn);
  return () => clearImmediate(handle);
};
// Never cancelled: when the immediate wins, the timer still fires ~1 ms
// later as a no-op, and that is the point — it is the wake-up that ends a
// stall of whatever the immediate task resumed (see the file header).
const TIMEOUT_HOP: MacrotaskHop = (fn) => {
  setTimeout(fn, 0);
};

function defaultMacrotaskHops(): MacrotaskHop[] {
  return typeof setImmediate === "function" ? [IMMEDIATE_HOP, TIMEOUT_HOP] : [TIMEOUT_HOP];
}

/** Create the engine's fiber scheduler (see the file header). */
export function createEngineScheduler(config: EngineSchedulerConfig = {}): Scheduler {
  const budget = config.microDrainBudget ?? ENGINE_MICRO_DRAIN_BUDGET;
  const hops = config.macrotaskHops ?? defaultMacrotaskHops();
  let queue: (() => void)[] = [];
  let scheduled = false;
  let microDrains = 0;
  // Cancels the raced sources of the yield in flight, if any.
  let pendingYield: (() => void) | undefined;

  const runBatch = (): void => {
    const batch = queue;
    queue = [];
    for (const task of batch) task();
  };

  const drain = (): void => {
    scheduled = false;
    runBatch();
    if (queue.length > 0 && !scheduled) {
      scheduled = true;
      request();
    }
  };

  const cancelYield = (): void => {
    const cancel = pendingYield;
    pendingYield = undefined;
    cancel?.();
  };

  const request = (): void => {
    if (microDrains < budget) {
      microDrains++;
      queueMicrotask(drain);
      return;
    }
    let served = false;
    const cancels: (() => void)[] = [];
    const serve = (): void => {
      if (served) return;
      served = true;
      cancelYield();
      microDrains = 0;
      drain();
    };
    for (const hop of hops) {
      const cancel = hop(serve);
      if (cancel) cancels.push(cancel);
    }
    pendingYield = () => {
      served = true;
      for (const cancel of cancels) cancel();
    };
  };

  return {
    schedule(task) {
      queue.push(task);
      if (!scheduled) {
        scheduled = true;
        request();
      }
    },
    flush() {
      cancelYield();
      while (queue.length > 0) runBatch();
      scheduled = false;
    },
    shutdown() {
      cancelYield();
      queue = [];
      scheduled = false;
    },
  };
}
