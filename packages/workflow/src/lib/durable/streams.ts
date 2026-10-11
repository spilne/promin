// ---------------------------------------------------------------------------
// Generic typed streams — `defineStream<T>` + `defineInputStream<T>`
// descriptor factories and the storage helpers that read and write them.
//
// Output streams: workflow code appends, external subscribers read.
// Input streams: external callers append, workflow code peeks.
// Same storage table backs both — direction is purely a phantom-type +
// API-level convention.
//
// There is no `ctx.streams` on the step context: the helpers below take
// the `WorkflowStorage` explicitly, so workflow code that writes or reads a
// stream closes over the storage it runs against. Zorya serves the same
// table over HTTP (`GET /api/runs/:id/streams/:streamId` as SSE,
// `.../chunks` as a page, `POST` to append as an external caller).
//
// Subscribers don't know the workflow's chosen `T`; they cast on read.
// The phantom type is for the workflow author's authoring ergonomics —
// the storage layer treats payloads as JSON.
// ---------------------------------------------------------------------------

import type { FenceGuard, WorkflowStorage, StreamChunk } from "./workflow-storage.ts";

/** Direction of a stream — convention only, see file header. */
export type StreamKind = "output" | "input";

/**
 * A stream descriptor. Pass to `appendStreamChunk` / `peekStreamChunk` to
 * scope reads/writes by stream id; the phantom `__T` is preserved so
 * those helpers type-check payloads at call sites.
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
 * Declare an output stream descriptor. Workflow code writes with
 * `appendStreamChunk`; external subscribers read through storage
 * (`readStreamChunks`) or Zorya's stream routes.
 *
 * ```ts
 * const progressStream = defineStream<{ percent: number; note?: string }>({
 *   id: "progress",
 * });
 * // inside a step, with the storage the workflow runs against:
 * await appendStreamChunk({
 *   storage,
 *   workflowId: ctx.workflowId,
 *   stream: progressStream,
 *   payload: { percent: 25, note: "loading" },
 *   guard, // the step's fence guard (`StepRuntime.guard`)
 * });
 * ```
 */
export function defineStream<T>(opts: DefineStreamOptions): StreamDescriptor<T, "output"> {
  return { id: opts.id, kind: "output" };
}

/**
 * Declare an input stream descriptor. External callers append via
 * `appendExternalStreamChunk` (Zorya: `POST /api/runs/:id/streams/:streamId`);
 * workflow code polls with `peekStreamChunk`. There is no blocking wait on
 * a stream: a workflow that must park until outside input arrives uses a
 * signal (`waitForSignal`) instead.
 *
 * ```ts
 * const cancelSignal = defineInputStream<{ reason?: string }>({ id: "cancel" });
 * const cancelled = await peekStreamChunk({ storage, workflowId, stream: cancelSignal });
 * if (cancelled) abort();
 * ```
 */
export function defineInputStream<T>(opts: DefineStreamOptions): StreamDescriptor<T, "input"> {
  return { id: opts.id, kind: "input" };
}

// ---------------------------------------------------------------------------
// Storage helpers
// ---------------------------------------------------------------------------

/**
 * Append a chunk on behalf of a workflow body. Source-of-truth wrapper
 * over `storage.appendStreamChunk` — keeps the `appendedBy` tag stamped
 * correctly without callers having to think about it. Pass the run's
 * `guard` (the step's `StepRuntime.guard`) so a worker that lost the run
 * cannot append.
 */
export async function appendStreamChunk<T>(params: {
  storage: WorkflowStorage;
  workflowId: string;
  stream: StreamDescriptor<T, "output">;
  payload: T;
  guard?: FenceGuard;
}): Promise<{ chunkIndex: number }> {
  return params.storage.appendStreamChunk({
    workflowId: params.workflowId,
    streamId: params.stream.id,
    payload: params.payload,
    appendedBy: "workflow",
    guard: params.guard,
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
 * undefined when no chunks exist. Poll-style reads of input streams ("did
 * anyone send a cancel signal?").
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
