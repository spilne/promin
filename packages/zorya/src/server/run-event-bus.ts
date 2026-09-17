// ---------------------------------------------------------------------------
// RunEventBus — in-process pub/sub for run events.
//
// Producers (the engine's storage hooks) publish RunEvent; SSE consumers
// subscribe per workflow id and receive a snapshot-then-live stream. This
// backend-agnostic bus is fed by a polling loop today; a future CDC hook
// (Postgres LISTEN/NOTIFY, Redis keyspace events) can replace the polling
// without changing the SSE layer.
// ---------------------------------------------------------------------------

import type { RunEvent } from "./api-types.ts";

type Listener = (event: RunEvent) => void;

export class RunEventBus {
  private readonly listeners = new Map<string, Set<Listener>>();

  publish(workflowId: string, event: RunEvent): void {
    const set = this.listeners.get(workflowId);
    if (!set) return;
    for (const l of set) l(event);
  }

  subscribe(workflowId: string, listener: Listener): () => void {
    let set = this.listeners.get(workflowId);
    if (!set) {
      set = new Set();
      this.listeners.set(workflowId, set);
    }
    set.add(listener);
    return () => {
      const cur = this.listeners.get(workflowId);
      if (!cur) return;
      cur.delete(listener);
      if (cur.size === 0) this.listeners.delete(workflowId);
    };
  }

  hasListeners(workflowId: string): boolean {
    const set = this.listeners.get(workflowId);
    return !!set && set.size > 0;
  }
}
