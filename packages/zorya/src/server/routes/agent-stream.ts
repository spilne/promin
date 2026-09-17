// ---------------------------------------------------------------------------
// GET /api/runs/:id/agent-stream — SSE feed for live agent events.
//
// Routes through AgentStreamHub: subscribe an SSE sink for the workflowId,
// the hub broadcasts a stream-start command over the worker WS, the
// hosting worker begins forwarding SessionEvents which fan out to every
// connected SSE client.
//
// Wire format follows the standard `text/event-stream`:
//   event: agent-event
//   data: {"type":"token.delta","turn":0,"delta":"Hello"}
//
// The browser uses fetch+ReadableStream parsing (the same shape ChatGPT /
// Claude.ai use) to consume custom event types alongside the auth header.
// ---------------------------------------------------------------------------

import type { AgentStreamHub } from "../services/agent-stream-hub.ts";

export function streamAgentEvents(hub: AgentStreamHub) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const workflowId = params.id;
    if (!workflowId) {
      return new Response("missing_id", { status: 400 });
    }

    // Capture unsubscribe in a closure shared by start + cancel so the
    // hub can clean up when the browser disconnects.
    let unsubscribe: (() => void) | undefined;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const enc = new TextEncoder();
        const push = (event: { type: string; data: unknown }) => {
          try {
            controller.enqueue(
              enc.encode(`event: ${event.type}\ndata: ${JSON.stringify(event.data)}\n\n`),
            );
          } catch {
            // Controller already closed — best-effort.
          }
        };
        const close = () => {
          try {
            controller.close();
          } catch {
            // Already closed.
          }
        };

        // Send a hello event immediately so proxies don't buffer the
        // headers waiting for the first real event.
        push({ type: "ready", data: { workflowId } });

        unsubscribe = hub.subscribe(workflowId, { push, close });
      },
      cancel() {
        unsubscribe?.();
      },
    });

    return new Response(stream, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
        // Disable Nginx / Cloud Run buffering so the dashboard sees
        // events as they happen.
        "x-accel-buffering": "no",
      },
    });
  };
}
