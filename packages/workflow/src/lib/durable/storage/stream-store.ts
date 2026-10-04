// ---------------------------------------------------------------------------
// StreamStore — append-only typed channels per workflow.
//
// Output streams: workflow appends, external subscribers read.
// Input streams: external subscribers append, workflow peeks/waits.
// Storage doesn't distinguish — `appendedBy` records direction so the
// dashboard / consumer can render workflow-vs-external chunks differently.
// ---------------------------------------------------------------------------

import type { FencedWrite } from "./fencing.ts";

/**
 * One chunk of a workflow stream. Returned by `readStreamChunks`. The
 * `payload` is whatever the appender wrote — the type comes from the
 * caller's `defineStream<T>` declaration; storage stores it as JSON.
 */
export interface StreamChunk {
  readonly chunkIndex: number;
  readonly payload: unknown;
  readonly appendedBy: "workflow" | "external";
  readonly appendedAt: Date;
}

/** Params of `appendStreamChunk`. */
export interface AppendStreamChunkParams extends FencedWrite {
  readonly workflowId: string;
  readonly streamId: string;
  readonly payload: unknown;
  readonly appendedBy: "workflow" | "external";
}

/** Params of `readStreamChunks`. */
export interface ReadStreamChunksParams {
  readonly workflowId: string;
  readonly streamId: string;
  /** Exclusive lower bound on `chunkIndex` (SSE reconnect). */
  readonly since?: number;
  readonly limit?: number;
}

/** Workflow streams. Workflow-scoped: they survive `startFreshRun`. */
export interface StreamStore {
  /**
   * Append one chunk to a workflow's stream. Returns the assigned
   * `chunkIndex` (monotonic per `(workflowId, streamId)`). Atomic against
   * concurrent appends: every call gets a distinct index and the indices
   * of a stream stay gap-free (0, 1, 2, …).
   *
   * A step appending on behalf of its run passes the run's `guard`;
   * external appends are unfenced.
   */
  appendStreamChunk(params: AppendStreamChunkParams): Promise<{ readonly chunkIndex: number }>;

  /**
   * Read chunks from a stream, `chunkIndex` ascending. Pass `since`
   * (exclusive) to replay from the last index observed (SSE reconnect).
   * `limit` caps the page; the default is backend-specific (in-memory:
   * unbounded, Postgres: 1000).
   */
  readStreamChunks(params: ReadStreamChunksParams): Promise<ReadonlyArray<StreamChunk>>;
}
