// ---------------------------------------------------------------------------
// SSE route — /api/runs/:id/events
//
// Opens a Server-Sent Events stream for a single workflow: a snapshot, then
// per-step / status diffs from the run's shared watcher (`RunWatcherPool`),
// until the run reaches a terminal state, disappears, or the watcher gives
// up after repeated storage failures (an `error` event, then `end`).
// ---------------------------------------------------------------------------

import type { TimerHandle, WallClock, WorkflowStorage } from "@promin/workflow";
import { SystemWallClock } from "@promin/workflow";
import { jsonError } from "../router.ts";
import type { RunWatcherPool } from "../run-poll-watcher.ts";
import type { RunEvent } from "../api-types.ts";

export interface SseDeps {
  storage: WorkflowStorage;
  /** Shared per-run watchers. */
  watchers: RunWatcherPool;
  /** Time source for the SSE heartbeat. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/** Keep-alive comment cadence (ms). */
const HEARTBEAT_MS = 15_000;

const encoder = new TextEncoder();

/**
 * One event's SSE frame. A watcher publishes the same event object to
 * every viewer of a run, so it is serialized and encoded once, not once per
 * viewer; the cache entry goes with the event.
 */
const frames = new WeakMap<RunEvent, Uint8Array>();
function encodeEvent(ev: RunEvent): Uint8Array {
  let frame = frames.get(ev);
  if (!frame) {
    frame = encoder.encode(`data: ${JSON.stringify(ev)}\n\n`);
    frames.set(ev, frame);
  }
  return frame;
}

export function streamRunEvents(deps: SseDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const state = await deps.storage.loadWorkflow(id);
    if (!state) return jsonError(404, "not_found");

    let unsub: (() => void) | undefined;
    const clock = deps.clock ?? SystemWallClock;
    let heartbeatHandle: TimerHandle | undefined;
    let closed = false;

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const close = (): void => {
          teardown();
          try {
            controller.close();
          } catch {
            // already closed
          }
        };
        const send = (ev: RunEvent): void => {
          if (closed) return;
          try {
            controller.enqueue(encodeEvent(ev));
          } catch {
            // Controller closed.
            teardown();
            return;
          }
          if (ev.type === "end") close();
        };
        unsub = deps.watchers.subscribe({ state, listener: send });
        if (closed) {
          // Terminal run: `end` arrived during subscribe.
          unsub();
          return;
        }
        heartbeatHandle = clock.setInterval(() => {
          try {
            controller.enqueue(encoder.encode(`: heartbeat\n\n`));
          } catch {
            teardown();
          }
        }, HEARTBEAT_MS);
      },
      cancel() {
        teardown();
      },
    });

    function teardown(): void {
      closed = true;
      unsub?.();
      unsub = undefined;
      if (heartbeatHandle !== undefined) {
        heartbeatHandle.clear();
        heartbeatHandle = undefined;
      }
    }

    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      },
    });
  };
}
