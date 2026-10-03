// ---------------------------------------------------------------------------
// Streamable<T> / Sinkable<T> — source and sink contracts
// ---------------------------------------------------------------------------

import type { StreamPipeline } from "@promin/core";
import type { Codec } from "@spilne/perfect-core/connect";

/** "I can produce a stream of T." */
export interface Streamable<T> {
  subscribe(params?: { group?: string }): StreamPipeline<T, never>;
  codec: Codec<T>;
}

/** "I can consume values of T." */
export interface Sinkable<T> {
  publish(value: T): Promise<void>;
  codec: Codec<T>;
}
