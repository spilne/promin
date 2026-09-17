// ---------------------------------------------------------------------------
// Streams routes — generic typed channels into and out of running workflows.
//
//   GET  /api/runs/:id/streams/:streamId           — SSE replay + live tail.
//   POST /api/runs/:id/streams/:streamId           — append (input streams).
//   GET  /api/runs/:id/streams/:streamId/chunks    — JSON snapshot (no SSE).
//
// The SSE handler polls storage at a configurable interval; production
// backends will eventually layer LISTEN/NOTIFY (Postgres) or Redis pub/sub
// for push-style delivery, but the polling fallback works on every backend
// today and matches the existing `/api/runs/:id/events` pattern.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, StreamChunk } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";

export interface StreamsRoutesDeps {
  readonly storage: WorkflowStorage;
  /** Poll interval for the SSE handler. Default: 500ms. */
  readonly pollIntervalMs?: number;
}

export interface StreamChunkDto {
  readonly chunkIndex: number;
  readonly payload: unknown;
  readonly appendedBy: "workflow" | "external";
  readonly appendedAt: string;
}

function toDto(chunk: StreamChunk): StreamChunkDto {
  return {
    chunkIndex: chunk.chunkIndex,
    payload: chunk.payload,
    appendedBy: chunk.appendedBy,
    appendedAt: chunk.appendedAt.toISOString(),
  };
}

/**
 * Append to an input stream from outside the workflow. Body is the
 * payload (JSON-serializable). Returns the assigned `chunkIndex`.
 */
export function sendStreamChunk(deps: StreamsRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const streamId = params.streamId;
    if (!id) return jsonError(400, "missing_run_id");
    if (!streamId) return jsonError(400, "missing_stream_id");

    const body = await readJson<{ payload: unknown }>(req);
    if (!body || !("payload" in body)) {
      return jsonError(400, "missing_payload");
    }

    const result = await deps.storage.appendStreamChunk({
      workflowId: id,
      streamId,
      payload: body.payload,
      appendedBy: "external",
    });
    return json(201, { chunkIndex: result.chunkIndex });
  };
}

/**
 * Snapshot read — returns all chunks (or chunks > `since`) as a single
 * JSON array. Used by tests + clients that don't want SSE.
 */
export function getStreamChunks(deps: StreamsRoutesDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const streamId = params.streamId;
    if (!id) return jsonError(400, "missing_run_id");
    if (!streamId) return jsonError(400, "missing_stream_id");

    const url = new URL(req.url);
    const sinceRaw = url.searchParams.get("since");
    const since = sinceRaw !== null ? Number(sinceRaw) : undefined;
    const chunks = await deps.storage.readStreamChunks({
      workflowId: id,
      streamId,
      ...(since !== undefined && Number.isFinite(since) ? { since } : {}),
    });
    return json(200, { chunks: chunks.map(toDto) });
  };
}

/**
 * SSE handler — replays chunks since `last-event-id` (or `?since=`),
 * then polls storage for new chunks. Closes when the request is aborted.
 *
 * Each chunk is sent as `id: <chunkIndex>\nevent: chunk\ndata: <json>\n\n`
 * so EventSource clients automatically replay from the last seen id on
 * reconnect via the `Last-Event-ID` header.
 */
export function streamChunks(deps: StreamsRoutesDeps) {
  const pollIntervalMs = deps.pollIntervalMs ?? 500;

  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    const streamId = params.streamId;
    if (!id) return jsonError(400, "missing_run_id");
    if (!streamId) return jsonError(400, "missing_stream_id");

    const url = new URL(req.url);
    const lastEventId = req.headers.get("last-event-id");
    const sinceRaw = lastEventId ?? url.searchParams.get("since");
    let since = sinceRaw !== null ? Number(sinceRaw) : -1;
    if (!Number.isFinite(since)) since = -1;

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      async start(controller) {
        const writeChunk = (chunk: StreamChunk): void => {
          const data = JSON.stringify(toDto(chunk));
          controller.enqueue(
            encoder.encode(`id: ${chunk.chunkIndex}\nevent: chunk\ndata: ${data}\n\n`),
          );
        };

        const aborted = req.signal;
        let closed = false;
        const close = (): void => {
          if (closed) return;
          closed = true;
          try {
            controller.close();
          } catch {
            // Already closed by the runtime.
          }
        };
        aborted.addEventListener("abort", close);

        try {
          while (!closed) {
            const chunks = await deps.storage.readStreamChunks({
              workflowId: id,
              streamId,
              since: since >= 0 ? since : undefined,
            });
            for (const chunk of chunks) {
              if (closed) break;
              writeChunk(chunk);
              since = chunk.chunkIndex;
            }
            if (closed) break;
            await new Promise((r) => setTimeout(r, pollIntervalMs));
          }
        } finally {
          close();
        }
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
        "x-accel-buffering": "no",
      },
    });
  };
}
