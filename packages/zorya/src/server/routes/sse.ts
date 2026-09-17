// ---------------------------------------------------------------------------
// SSE route — /api/runs/:id/events
//
// Opens a Server-Sent Events stream for a single workflow. Starts a
// RunPollWatcher that emits a snapshot followed by per-step / status diffs
// until the run reaches a terminal state.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import { jsonError } from "../router.ts";
import { RunEventBus } from "../run-event-bus.ts";
import { RunPollWatcher } from "../run-poll-watcher.ts";
import type { RunEvent } from "../api-types.ts";

export interface SseDeps {
  storage: WorkflowStorage;
  bus: RunEventBus;
  /** Watcher poll interval in ms. Default 1000. */
  pollIntervalMs?: number;
}

export function streamRunEvents(deps: SseDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");

    const exists = await deps.storage.loadWorkflow(id);
    if (!exists) return jsonError(404, "not_found");

    const watcher = new RunPollWatcher({
      storage: deps.storage,
      bus: deps.bus,
      workflowId: id,
      intervalMs: deps.pollIntervalMs ?? 1000,
    });

    const encoder = new TextEncoder();
    let unsub: (() => void) | undefined;
    let heartbeatHandle: ReturnType<typeof setInterval> | undefined;

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const send = (ev: RunEvent) => {
          try {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(ev)}\n\n`));
          } catch {
            // Controller closed.
          }
          if (ev.type === "end") {
            teardown();
            try {
              controller.close();
            } catch {
              // already closed
            }
          }
        };
        unsub = deps.bus.subscribe(id, send);
        void watcher.start();
        heartbeatHandle = setInterval(() => {
          try {
            controller.enqueue(encoder.encode(`: heartbeat\n\n`));
          } catch {
            teardown();
          }
        }, 15_000);
      },
      cancel() {
        teardown();
      },
    });

    function teardown(): void {
      unsub?.();
      unsub = undefined;
      watcher.stop();
      if (heartbeatHandle !== undefined) {
        clearInterval(heartbeatHandle);
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
