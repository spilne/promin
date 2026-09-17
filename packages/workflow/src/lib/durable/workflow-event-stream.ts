// ---------------------------------------------------------------------------
// Shared event-stream machinery for WorkflowRunEvent subscribers.
//
// Two paths produce the same shape — storage push (InMemoryWorkflowStorage's
// EventBus) and runner-side polling (fallback for storages without native
// subscribe). Both need: per-subscriber FIFO queue + FIFO waiters so
// multiple in-flight `next()` calls each get their own resolver.
// ---------------------------------------------------------------------------

import type { WorkflowRunEvent } from "./workflow-state.ts";

/** Push handle exposed to event producers (storage writers, pollers). */
export interface EventStreamProducer {
  /** Deliver an event to the subscriber. No-op after the stream is done. */
  push(event: WorkflowRunEvent): void;
  /**
   * Close the stream. Subsequent `push` is a no-op; any pending `next()`
   * resolves with `{ done: true }`. Idempotent.
   */
  end(): void;
  /** `true` once the stream has been closed. */
  readonly done: boolean;
}

/**
 * Create an AsyncIterable<WorkflowRunEvent> driven by the returned
 * producer. `setup` runs synchronously at iterable construction (not per
 * iterator) and wires the producer into whatever upstream source —
 * storage event bus, polling loop, etc. The returned disposer runs on
 * explicit `return()` or after the stream ends, so producers can
 * unregister callbacks, clear intervals, detach AbortSignal listeners.
 */
export function createWorkflowEventStream(
  setup: (producer: EventStreamProducer) => (() => void) | void,
): AsyncIterable<WorkflowRunEvent> {
  const queue: WorkflowRunEvent[] = [];
  const waiters: Array<(value: WorkflowRunEvent | null) => void> = [];
  let done = false;

  const producer: EventStreamProducer = {
    get done() {
      return done;
    },
    push(event: WorkflowRunEvent): void {
      if (done) return;
      const w = waiters.shift();
      if (w) w(event);
      else queue.push(event);
    },
    end(): void {
      if (done) return;
      done = true;
      const pending = waiters.splice(0);
      for (const w of pending) w(null);
    },
  };

  const dispose = setup(producer) ?? (() => undefined);
  let disposed = false;
  const disposeOnce = (): void => {
    if (disposed) return;
    disposed = true;
    dispose();
  };

  return {
    [Symbol.asyncIterator](): AsyncIterator<WorkflowRunEvent> {
      return {
        async next(): Promise<IteratorResult<WorkflowRunEvent>> {
          if (queue.length > 0) {
            return { value: queue.shift()!, done: false };
          }
          if (done) {
            disposeOnce();
            return { value: undefined, done: true };
          }
          const val = await new Promise<WorkflowRunEvent | null>((resolve) => {
            waiters.push(resolve);
          });
          if (val === null) {
            disposeOnce();
            return { value: undefined, done: true };
          }
          return { value: val, done: false };
        },
        async return(): Promise<IteratorResult<WorkflowRunEvent>> {
          producer.end();
          disposeOnce();
          return { value: undefined, done: true };
        },
      };
    },
  };
}
