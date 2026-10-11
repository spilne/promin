// ---------------------------------------------------------------------------
// SSE plumbing for agent turns: a guarded channel writer, the shared
// `fullStream` → SSE fan-out for thread turns, and error payloads that carry
// a request id instead of the error text.
// ---------------------------------------------------------------------------

import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  TurnKeyConflictError,
  type AgentRunOutput,
} from "@promin/agent";
import { WorkflowSuspendedError } from "@promin/workflow";

const SSE_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
} as const;

export function sseResponse(stream: ReadableStream<Uint8Array>): Response {
  return new Response(stream, { status: 200, headers: SSE_HEADERS });
}

/**
 * Guarded SSE writer: after the client disconnects (or the stream is
 * closed) every send is a no-op instead of a throw.
 */
export function createSseChannel() {
  const enc = new TextEncoder();
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let closed = false;
  return {
    get closed() {
      return closed;
    },
    attach(c: ReadableStreamDefaultController<Uint8Array>) {
      controller = c;
    },
    send(event: string | null, data: unknown) {
      if (closed || !controller) return;
      const prefix = event ? `event: ${event}\n` : "";
      try {
        controller.enqueue(enc.encode(`${prefix}data: ${JSON.stringify(data)}\n\n`));
      } catch {
        closed = true;
      }
    },
    markClosed() {
      closed = true;
    },
    close() {
      if (closed) return;
      closed = true;
      try {
        controller?.close();
      } catch {
        // already closed
      }
    },
  };
}

/**
 * Attach a no-op rejection handler to every promise on a run output so
 * a failed run that nobody awaits (the client went away, or the stream
 * reported the failure through `fullStream`) never surfaces as an
 * unhandled rejection.
 */
export function silenceOutput(out: AgentRunOutput): void {
  for (const value of Object.values(out)) {
    if (value instanceof Promise) value.catch(() => {});
  }
}

/** An `event: error` payload: a request id, never the error's text. */
export function streamErrorPayload(err: unknown): { error: string; requestId: string } {
  const requestId = crypto.randomUUID();
  try {
    console.error(`[zorya] agent stream failed requestId=${requestId}`, err);
  } catch {
    // ignore
  }
  return { error: "stream_failed", requestId };
}

/**
 * Shared SSE writer for thread.stream / thread.resumeStream — fans the
 * agent's `fullStream` into typed SSE events so the client sees text
 * deltas, approval requests, and finish/suspend signals in one channel.
 *
 * Suspension semantics: when the workflow suspends on an approval gate
 * the underlying `out.text` promise rejects with `WorkflowSuspendedError`.
 * The `approval-requested` event already fired through `fullStream`, so
 * we map the rejection to a clean `event: suspended` and let the client
 * react (render the approval banner). A resumed turn replays its journal
 * without re-emitting that request, so it ends with `event: finish` (or
 * the next `suspended`). Any other error becomes `event: error`.
 *
 * `onClose` runs exactly once, when the run has settled (completed,
 * failed or suspended) — also when the client disconnected earlier: the
 * run keeps writing to the thread until it settles, so the turn lease is
 * held until then.
 */
export function streamAgentRunResponse(params: {
  out: AgentRunOutput;
  threadMeta: Record<string, unknown>;
  onClose?: () => Promise<void>;
}): Response {
  const { out, threadMeta, onClose } = params;
  silenceOutput(out);
  const runSettled = Promise.allSettled([out.text]);
  let cleaned = false;
  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    await onClose?.().catch(() => {});
  };
  const cleanupWhenSettled = () => {
    void runSettled.then(cleanup);
  };
  const sse = createSseChannel();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      sse.attach(controller);
      sse.send("thread", threadMeta);

      // The last approval the run asked for — reported if the run suspends.
      let pendingApproval: { toolCallId: string; toolName: string } | null = null;
      try {
        for await (const event of out.fullStream) {
          if (sse.closed) break;
          switch (event.type) {
            case "text-delta":
              sse.send(null, { delta: event.delta });
              break;
            case "approval-requested":
              pendingApproval = { toolCallId: event.toolCallId, toolName: event.toolName };
              sse.send("approval-requested", { ...pendingApproval, input: event.input });
              break;
            case "error":
              sse.send("error", { message: event.error.message });
              break;
            default:
              break;
          }
        }
        // A closed channel means the client went away mid-run; nothing
        // left to report. Otherwise the run's outcome decides the frame:
        // an approval request alone doesn't mean the run suspended (an
        // inline `onApprovalRequired` hook answers it in-process).
        if (!sse.closed) {
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          sse.send("finish", { ...threadMeta, text, finishReason, usage });
        }
      } catch (err) {
        if (err instanceof WorkflowSuspendedError) {
          sse.send("suspended", { ...threadMeta, ...pendingApproval });
        } else if (err instanceof ApprovalNotFoundError) {
          sse.send("error", { error: "approval_not_found" });
        } else if (err instanceof ApprovalAlreadyDecidedError) {
          sse.send("error", { error: "approval_already_decided" });
        } else if (err instanceof ApprovalExpiredError) {
          sse.send("error", { error: "approval_expired" });
        } else if (err instanceof ApprovalNotReadyError) {
          sse.send("error", { error: "approval_not_ready" });
        } else if (err instanceof TurnKeyConflictError) {
          sse.send("error", { error: "idempotency_key_mismatch" });
        } else {
          sse.send("error", streamErrorPayload(err));
        }
      } finally {
        sse.close();
        cleanupWhenSettled();
      }
    },
    cancel: async () => {
      sse.markClosed();
      await out.cancel().catch(() => {});
      cleanupWhenSettled();
    },
  });
  return sseResponse(stream);
}
