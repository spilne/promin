// ---------------------------------------------------------------------------
// Wakeups — a latency hint between the places that make work (enqueue, task
// settle, signal delivery) and the loops that poll for it.
//
// Polling stays the correctness path: a wake only makes a loop run its next
// iteration now instead of after its interval. It may be lost, duplicated or
// late; nothing may depend on it. `notify` never throws into the caller, and a
// store without a wake channel just does not call it.
//
// Backends: `InMemoryWakeups` (one process), `PgWakeups` in @promin/postgres
// (LISTEN/NOTIFY across processes). A Redis or SQLite backend implements the
// same two methods.
// ---------------------------------------------------------------------------

/**
 * What happened.
 * - `"steps"`: a step task became claimable (enqueued, released, requeued).
 *   Workers wake.
 * - `"results"`: a step task settled; the key is its task id. Coordinators
 *   waiting for that task wake.
 * - `"signals"`: a signal was delivered. Signal scanners wake.
 */
export type WakeTopic = "steps" | "results" | "signals";

/** Called on a wake. `key` is the topic's subject when the sender gave one; absent after a reconnect (anything may have been missed). */
export type WakeHandler = (key?: string) => void;

export interface Wakeups {
  /**
   * Hint that `topic` has news. Best effort: failures are swallowed, and a
   * remote backend may deliver it late, twice or not at all.
   */
  notify(params: { topic: WakeTopic; key?: string }): void | Promise<void>;
  /** Run `handler` on wakes of `topic`. Returns the unsubscribe function. */
  onWake(params: { topic: WakeTopic; handler: WakeHandler }): () => void;
}

/** Wakeups that never wake anyone: what a loop without a wake channel uses. */
export const NoWakeups: Wakeups = {
  notify: () => {},
  onWake: () => () => {},
};

/** Single-process wakeups: handlers run synchronously inside `notify`. */
export class InMemoryWakeups implements Wakeups {
  private readonly handlers = new Map<WakeTopic, Set<WakeHandler>>();

  notify(params: { topic: WakeTopic; key?: string }): void {
    for (const handler of [...(this.handlers.get(params.topic) ?? [])]) {
      try {
        handler(params.key);
      } catch {
        // A throwing handler must not break the sender or the other handlers.
      }
    }
  }

  onWake(params: { topic: WakeTopic; handler: WakeHandler }): () => void {
    const set = this.handlers.get(params.topic) ?? new Set<WakeHandler>();
    this.handlers.set(params.topic, set);
    // A fresh closure per subscription: the same handler subscribed twice
    // is two subscriptions, and each unsubscribes only itself.
    const entry: WakeHandler = (key) => params.handler(key);
    set.add(entry);
    return () => {
      set.delete(entry);
    };
  }
}

/** `notify` that never throws or rejects: a hint must not fail the operation that raised it. */
export function notifyQuietly(
  wakeups: Wakeups | undefined,
  params: { topic: WakeTopic; key?: string },
): void {
  if (wakeups === undefined) return;
  try {
    const r = wakeups.notify(params);
    if (r !== undefined) r.catch(() => {});
  } catch {
    // Best effort.
  }
}
