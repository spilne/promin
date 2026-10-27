// ---------------------------------------------------------------------------
// InMemoryRunEvents — the in-process event bus behind `subscribeToWorkflow`.
// ---------------------------------------------------------------------------

import { createWorkflowEventStream } from "../workflow-event-stream.ts";
import type { WorkflowRunEvent } from "../workflow-state.ts";

export class InMemoryRunEvents {
  /**
   * Per-workflow event subscribers. Each active subscription registers a
   * push function keyed by workflowId; `emit` fans out to every registered
   * one. Passing `null` signals terminal — the iterator resolves
   * `{ done: true }` and the subscriber is removed. One set per workflowId,
   * so one subscriber's terminal doesn't starve another subscriber attached
   * to a different run.
   */
  private readonly subscribers = new Map<string, Set<(event: WorkflowRunEvent | null) => void>>();

  /**
   * Fan an event out to every subscriber for this workflow. `terminal`
   * indicates the run is ending — after delivering the event, each
   * subscriber is signalled done and the subscriber set is cleared. Called
   * synchronously from the state transitions so subscribers observe events
   * in the same order as the underlying writes.
   */
  emit(params: { workflowId: string; event: WorkflowRunEvent; terminal: boolean }): void {
    const { workflowId, event, terminal } = params;
    const subs = this.subscribers.get(workflowId);
    if (!subs || subs.size === 0) return;
    for (const push of subs) {
      push(event);
      if (terminal) push(null);
    }
    if (terminal) this.subscribers.delete(workflowId);
  }

  /**
   * Stream step/workflow-lifecycle events for a single run. Returns an
   * async iterable closed by any terminal event or by the supplied
   * `AbortSignal`. Each call registers its own push function — multiple
   * concurrent subscribers to the same workflowId each see every event.
   *
   * Events arrive synchronously from storage mutators and the iterator
   * drains them asynchronously (see `createWorkflowEventStream`).
   */
  subscribe(params: { workflowId: string; signal?: AbortSignal }): AsyncIterable<WorkflowRunEvent> {
    const { workflowId, signal } = params;
    return createWorkflowEventStream((producer) => {
      // The subscriber set uses `(event|null) => void` with `null` for
      // terminal; the shared stream's producer has separate `push` / `end`.
      const adapt = (event: WorkflowRunEvent | null): void => {
        if (event === null) producer.end();
        else producer.push(event);
      };
      const subs = this.subscribers.get(workflowId) ?? new Set();
      subs.add(adapt);
      this.subscribers.set(workflowId, subs);

      const onAbort = (): void => producer.end();
      signal?.addEventListener("abort", onAbort, { once: true });

      return () => {
        const cur = this.subscribers.get(workflowId);
        if (cur) {
          cur.delete(adapt);
          if (cur.size === 0) this.subscribers.delete(workflowId);
        }
        signal?.removeEventListener("abort", onAbort);
      };
    });
  }

  /** Drop a workflow's subscribers without ending them (purge). */
  forget(workflowId: string): void {
    this.subscribers.delete(workflowId);
  }
}
