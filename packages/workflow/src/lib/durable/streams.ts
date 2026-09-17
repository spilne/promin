// ---------------------------------------------------------------------------
// Generic typed streams — `defineStream<T>` + `defineInputStream<T>` factories
// + `ctx.streams.*` runtime API.
//
// Output streams: workflow appends, external subscribers read.
// Input streams: external subscribers append, workflow peeks/waits.
// Same storage table backs both — direction is purely a phantom-type +
// API-level convention. The `kind` field on the descriptor lets us add
// runtime guards if we want to (e.g. ctx.streams.append rejects input
// descriptors, ctx.streams.peek rejects output descriptors).
//
// Subscribers don't know the workflow's chosen `T`; they cast on read.
// The phantom type is for the workflow author's authoring ergonomics —
// the storage layer treats payloads as JSON.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, StreamChunk } from "./workflow-storage.ts";

/** Direction of a stream — convention only, see file header. */
export type StreamKind = "output" | "input";

/**
 * A stream descriptor. Pass to `ctx.streams.append` / `peek` / `wait` to
 * scope reads/writes by stream id; the phantom `__T` is preserved so
 * the runtime API can type-check payloads at call sites.
 */
export interface StreamDescriptor<T, K extends StreamKind = "output"> {
  readonly id: string;
  readonly kind: K;
  /** Phantom — only used for type inference. */
  readonly __T?: T;
}

export interface DefineStreamOptions {
  readonly id: string;
}

/**
 * Declare an output stream descriptor. Workflow body calls
 * `ctx.streams.append(stream, payload)` to write; external subscribers
 * read via `streams.read(stream, runId, ...)` (Phase 2 SDK helper) or
 * the SSE route directly.
 *
 * ```ts
 * const progressStream = defineStream<{ percent: number; note?: string }>({
 *   id: "progress",
 * });
 * yield* ctx.streams.append(progressStream, { percent: 25, note: "loading" });
 * ```
 */
export function defineStream<T>(opts: DefineStreamOptions): StreamDescriptor<T, "output"> {
  return { id: opts.id, kind: "output" };
}

/**
 * Declare an input stream descriptor. External callers append via the
 * `POST /api/runs/:id/streams/:streamId` route; the workflow body
 * peeks/waits via `ctx.streams.peek(stream)` / `ctx.streams.wait(stream)`.
 *
 * ```ts
 * const cancelSignal = defineInputStream<{ reason?: string }>({ id: "cancel" });
 * const cancelled = yield* ctx.streams.peek(cancelSignal);
 * if (cancelled) abort();
 * ```
 */
export function defineInputStream<T>(opts: DefineStreamOptions): StreamDescriptor<T, "input"> {
  return { id: opts.id, kind: "input" };
}

// ---------------------------------------------------------------------------
// Implementation helpers — used by the journaled-step `ctx.streams` shim
// and by the standalone `streamWriter` factory.
// ---------------------------------------------------------------------------

/**
 * Append a chunk on behalf of a workflow body. Source-of-truth wrapper
 * over `storage.appendStreamChunk` — keeps the `appendedBy` tag stamped
 * correctly without callers having to think about it.
 */
export async function appendStreamChunk<T>(params: {
  storage: WorkflowStorage;
  workflowId: string;
  stream: StreamDescriptor<T, "output">;
  payload: T;
}): Promise<{ chunkIndex: number }> {
  return params.storage.appendStreamChunk({
    workflowId: params.workflowId,
    streamId: params.stream.id,
    payload: params.payload,
    appendedBy: "workflow",
  });
}

/**
 * Append on behalf of an external caller (HTTP route handler). Writes
 * `appendedBy: 'external'` so the dashboard can distinguish.
 */
export async function appendExternalStreamChunk(params: {
  storage: WorkflowStorage;
  workflowId: string;
  streamId: string;
  payload: unknown;
}): Promise<{ chunkIndex: number }> {
  return params.storage.appendStreamChunk({
    workflowId: params.workflowId,
    streamId: params.streamId,
    payload: params.payload,
    appendedBy: "external",
  });
}

/**
 * Non-blocking read of the most-recent chunk on a stream — returns
 * undefined when no chunks exist. Powers `ctx.streams.peek` for input
 * streams (poll-style "did anyone send a cancel signal?").
 */
export async function peekStreamChunk<T>(params: {
  storage: WorkflowStorage;
  workflowId: string;
  stream: StreamDescriptor<T, "input"> | StreamDescriptor<T, "output">;
}): Promise<{ payload: T; chunkIndex: number; appendedAt: Date } | undefined> {
  const chunks = await params.storage.readStreamChunks({
    workflowId: params.workflowId,
    streamId: params.stream.id,
    limit: 1,
  });
  // readStreamChunks orders ASC by chunkIndex; for "most recent" we'd
  // need the last one. With limit:1 we get the FIRST chunk, which is
  // wrong for peek-latest. Re-read with no limit and take the tail.
  if (chunks.length === 0) return undefined;
  const all = await params.storage.readStreamChunks({
    workflowId: params.workflowId,
    streamId: params.stream.id,
  });
  const last = all[all.length - 1];
  if (!last) return undefined;
  return {
    payload: last.payload as T,
    chunkIndex: last.chunkIndex,
    appendedAt: last.appendedAt,
  };
}

/** Convenience — re-export StreamChunk for downstream consumers. */
export type { StreamChunk };
