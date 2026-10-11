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
// today and matches the existing `/api/runs/:id/events` pattern. It follows
// the run through the shared `RunWatcherPool`: once the run reaches a
// terminal status (or disappears) the handler drains the chunks written
// before that, sends `event: end` and closes, leaving no timer behind.
// ---------------------------------------------------------------------------

import type { StreamChunk, TimerHandle, WallClock, WorkflowStorage } from "@promin/workflow";
import { SystemWallClock, isTerminalWorkflowStatus } from "@promin/workflow";
import type { RunWatcherPool } from "../run-poll-watcher.ts";
import { z } from "zod";
import { json, jsonError } from "../router.ts";
import { IdSchema, StreamAppendRequestSchema } from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { requireRun } from "./run-extras.ts";

const StreamParamsSchema = z.object({ id: IdSchema, streamId: IdSchema });

/** `?since=<chunkIndex>`: only chunks after it. */
export const StreamChunksQuerySchema = z.object({
  since: z.preprocess(
    (v) => (v === "" ? undefined : v),
    z.coerce.number().int().min(-1).max(Number.MAX_SAFE_INTEGER).optional(),
  ),
});

export interface StreamsRoutesDeps {
  readonly storage: WorkflowStorage;
  /** Poll interval for the SSE handler. Default: 500ms. */
  readonly pollIntervalMs?: number;
  /** Time source for the SSE poll wait. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export interface StreamSseDeps extends StreamsRoutesDeps {
  /** Shared per-run watchers: the SSE stream ends when its run ends. */
  readonly watchers: RunWatcherPool;
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
    const { id, streamId } = parseParams({ params, schema: StreamParamsSchema });
    const body = await parseBody({ req, schema: StreamAppendRequestSchema });

    // Only a run that can still read the stream takes input.
    const run = await deps.storage.loadWorkflowStatus(id);
    if (!run) return jsonError(404, "not_found");
    if (isTerminalWorkflowStatus(run.status)) {
      return jsonError(409, "run_ended", `Run has already ended (status: ${run.status})`);
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
    const { id, streamId } = parseParams({ params, schema: StreamParamsSchema });
    const { since } = parseQuery({ req, schema: StreamChunksQuerySchema });
    await requireRun({ storage: deps.storage, workflowId: id });
    const chunks = await deps.storage.readStreamChunks({
      workflowId: id,
      streamId,
      ...(since !== undefined && since >= 0 && { since }),
    });
    return json(200, { chunks: chunks.map(toDto) });
  };
}

/**
 * SSE handler — replays chunks since `last-event-id` (or `?since=`),
 * then polls storage for new chunks until the run ends or the request is
 * aborted.
 *
 * Each chunk is sent as `id: <chunkIndex>\nevent: chunk\ndata: <json>\n\n`
 * so EventSource clients automatically replay from the last seen id on
 * reconnect via the `Last-Event-ID` header. When the run reaches a terminal
 * status (or is deleted) the handler reads the stream once more, so every
 * chunk written before the end is delivered, then sends `event: end` —
 * clients close on it instead of reconnecting — and closes the response.
 */
export function streamChunks(deps: StreamSseDeps) {
  const pollIntervalMs = deps.pollIntervalMs ?? 500;
  const clock = deps.clock ?? SystemWallClock;

  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, streamId } = parseParams({ params, schema: StreamParamsSchema });
    const query = parseQuery({ req, schema: StreamChunksQuerySchema });
    // A reconnecting EventSource resumes from its `Last-Event-ID`.
    const lastEventId = Number.parseInt(req.headers.get("last-event-id") ?? "", 10);
    let since = Number.isSafeInteger(lastEventId) ? lastEventId : (query.since ?? -1);
    const state = await deps.storage.loadWorkflow(id);
    if (!state) return jsonError(404, "not_found");

    const encoder = new TextEncoder();
    let closed = false;
    let runEnded = false;
    let unsubscribe: (() => void) | undefined;
    let timer: TimerHandle | undefined;
    /** Ends the current between-poll wait early; set only while waiting. */
    let wake: (() => void) | undefined;

    const stopWaiting = (): void => {
      timer?.clear();
      timer = undefined;
      const resolve = wake;
      wake = undefined;
      resolve?.();
    };
    const teardown = (): void => {
      closed = true;
      unsubscribe?.();
      unsubscribe = undefined;
      stopWaiting();
    };
    const waitForNextPoll = (): Promise<void> =>
      new Promise<void>((resolve) => {
        wake = resolve;
        timer = clock.setTimeout(stopWaiting, pollIntervalMs);
      });

    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const send = (text: string): void => {
          try {
            controller.enqueue(encoder.encode(text));
          } catch {
            // Closed by the runtime (client went away).
            teardown();
          }
        };
        const close = (): void => {
          teardown();
          try {
            controller.close();
          } catch {
            // Already closed by the runtime.
          }
        };
        req.signal.addEventListener("abort", close);

        unsubscribe = deps.watchers.subscribe({
          state,
          listener: (event) => {
            if (event.type !== "end") return;
            runEnded = true;
            stopWaiting();
          },
        });

        try {
          while (!closed) {
            // Read after noting the end, so the last read sees every chunk
            // the run wrote before it ended.
            const finalRead = runEnded;
            const chunks = await deps.storage.readStreamChunks({
              workflowId: id,
              streamId,
              since: since >= 0 ? since : undefined,
            });
            for (const chunk of chunks) {
              if (closed) break;
              const data = JSON.stringify(toDto(chunk));
              send(`id: ${chunk.chunkIndex}\nevent: chunk\ndata: ${data}\n\n`);
              since = chunk.chunkIndex;
            }
            if (closed) break;
            if (finalRead) {
              send(`event: end\ndata: {}\n\n`);
              break;
            }
            // Ended while reading: go straight to the final read.
            if (runEnded) continue;
            await waitForNextPoll();
          }
        } finally {
          close();
        }
      },
      cancel() {
        teardown();
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
