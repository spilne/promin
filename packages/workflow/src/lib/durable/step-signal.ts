// ---------------------------------------------------------------------------
// Step abort signals. A step body gets `ctx.signal`, one per attempt, that
// aborts when the attempt must stop doing work: it timed out, it failed
// (so a retry does not overlap leftovers), or the run's own signal aborted
// (lost lock; a sibling failing on the executor path). A body that ignores
// it behaves as before; one that passes it to `fetch` or an SDK call stops
// the in-flight work instead of leaking it past the timeout.
// ---------------------------------------------------------------------------

/** A signal that never aborts: what a step sees when nothing can cancel it. */
export const NEVER_ABORTED: AbortSignal = new AbortController().signal;

/** One attempt's signal, linked to the run-level `parent` when there is one. */
export interface AttemptSignal {
  readonly signal: AbortSignal;
  /** Abort the attempt (a no-op once aborted). */
  readonly abort: (reason: unknown) => void;
}

export function createAttemptSignal(parent?: AbortSignal): AttemptSignal {
  const controller = new AbortController();
  const signal = parent ? AbortSignal.any([parent, controller.signal]) : controller.signal;
  return {
    signal,
    abort: (reason) => {
      if (!controller.signal.aborted) controller.abort(reason);
    },
  };
}
