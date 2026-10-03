// ---------------------------------------------------------------------------
// Streamable<T> / Sinkable<T> — source and sink contracts
// ---------------------------------------------------------------------------

import type { Stream } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";

/**
 * "I can produce a stream of T."
 *
 * Streams are single-use pull cursors, so every `subscribe()` call returns a
 * fresh stream.
 */
export interface Streamable<T> {
  subscribe(params?: { group?: string }): Stream<T>;
  codec: Codec<T>;
}

/** "I can consume values of T." */
export interface Sinkable<T> {
  publish(value: T): Promise<void>;
  codec: Codec<T>;
}
