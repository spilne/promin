// ---------------------------------------------------------------------------
// AgentStreamHub — bridges WS frames from workers to SSE clients on the
// dashboard side.
//
// Architecture:
//
//   browser ──SSE── ZoryaServer ──WS── worker
//        GET /api/runs/:id/agent-stream         AgentSession events
//
// On SSE connect:
//   1. Mint a unique streamId.
//   2. Broadcast `cmd: agent-stream-start { streamId, workflowId }` to
//      every connected worker. Workers that aren't hosting the workflow
//      ignore it; the one that is starts forwarding session events as
//      `frame { streamId, payload }` over the existing WS.
//   3. Server's WorkerWebSocketServer.onFrame routes by streamId — the
//      hub forwards the payload to every SSE sink registered for that
//      streamId (multiple browser tabs on the same run all fan out from
//      one worker source).
//
// On SSE disconnect:
//   1. Broadcast `cmd: agent-stream-stop { streamId }` so the hosting
//      worker tears down its bus subscription.
//   2. Drop the SSE sink.
//
// The hub is generic — payloads are `unknown`; the wire schema for a
// specific stream type (agent SessionEvent, future workflow lifecycle
// stream, etc.) is the producer's contract with its consumer. This keeps
// the hub free of @promin/agent imports.
// ---------------------------------------------------------------------------

import type { WorkerWebSocketServer } from "./worker-ws-server.ts";

/** A single SSE client subscribed to a stream. */
interface StreamSink {
  /** Push one event to the client. */
  push(event: { type: string; data: unknown }): void;
  /** Close the SSE connection. */
  close(): void;
}

export class AgentStreamHub {
  private nextStreamId = 1;
  /** streamId → workflowId. Stable for the life of an SSE connection. */
  private streamToWorkflow = new Map<string, string>();
  /** streamId → set of subscribed SSE sinks. */
  private sinks = new Map<string, Set<StreamSink>>();
  private unsubFrame?: () => void;

  constructor(private readonly workerWs: WorkerWebSocketServer) {}

  /**
   * Wire the hub to the WS frame stream. Idempotent — safe to call from
   * `ZoryaServer.listen`.
   */
  start(): void {
    if (this.unsubFrame) return;
    this.unsubFrame = this.workerWs.onFrame((_workerId, streamId, payload) => {
      const subs = this.sinks.get(streamId);
      if (!subs || subs.size === 0) return;
      for (const sink of subs) {
        try {
          sink.push({ type: "agent-event", data: payload });
        } catch {
          // Drop misbehaving sink — preserves the rest.
          subs.delete(sink);
        }
      }
    });
  }

  stop(): void {
    if (this.unsubFrame) {
      this.unsubFrame();
      this.unsubFrame = undefined;
    }
    for (const subs of this.sinks.values()) {
      for (const sink of subs) {
        try {
          sink.close();
        } catch {
          /* swallow */
        }
      }
    }
    this.sinks.clear();
    this.streamToWorkflow.clear();
  }

  /**
   * Subscribe an SSE sink to a workflow's agent stream. Returns an
   * unsubscribe function. On first subscriber for a workflow, the hub
   * broadcasts a stream-start command to all workers; subsequent
   * subscribers reuse the existing stream.
   */
  subscribe(workflowId: string, sink: StreamSink): () => void {
    const streamId = this.findExistingStreamId(workflowId) ?? this.openStream(workflowId);
    let subs = this.sinks.get(streamId);
    if (!subs) {
      subs = new Set();
      this.sinks.set(streamId, subs);
    }
    subs.add(sink);

    return () => {
      const set = this.sinks.get(streamId);
      if (!set) return;
      set.delete(sink);
      if (set.size === 0) {
        this.sinks.delete(streamId);
        this.streamToWorkflow.delete(streamId);
        this.broadcast({
          kind: "cmd",
          requestId: "fire-and-forget",
          cmd: "agent-stream-stop",
          args: { streamId },
        });
      }
    };
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private findExistingStreamId(workflowId: string): string | undefined {
    for (const [sid, wid] of this.streamToWorkflow) {
      if (wid === workflowId) return sid;
    }
    return undefined;
  }

  private openStream(workflowId: string): string {
    const streamId = `s-${this.nextStreamId++}`;
    this.streamToWorkflow.set(streamId, workflowId);
    // Fire-and-forget broadcast: send the start command to every connected
    // worker. The one hosting the workflow subscribes its session bus and
    // begins emitting frames; others ignore it. We don't await replies —
    // the cmd carries an unused requestId because the worker shape expects
    // one, but we treat reply absence as fine.
    this.broadcast({
      kind: "cmd",
      requestId: "fire-and-forget",
      cmd: "agent-stream-start",
      args: { streamId, workflowId },
    });
    return streamId;
  }

  private broadcast(message: { kind: "cmd"; requestId: string; cmd: string; args: unknown }): void {
    for (const workerId of this.workerWs.connectedWorkers()) {
      this.workerWs.send(workerId, message);
    }
  }
}
