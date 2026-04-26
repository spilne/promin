// ---------------------------------------------------------------------------
// WorkerWebSocketServer — persistent server-side multiplexer for worker
// control sockets.
//
// Foundation for the agentic chain:
//   - promin-o8dj — agent event streaming (worker → server frames per
//     workflowId, fanned out to dashboard SSE clients)
//   - promin-i0wi — workflow query handlers (server → worker request/reply
//     for in-memory state queries)
//   - promin-eg0d — AgentWorker (uses both)
//
// Wire shape (JSON over a single WebSocket per worker):
//
//   Worker → Server
//     { kind: "identify", workerId, capabilities? }
//     { kind: "pong" }
//     { kind: "frame", streamId, payload }   // unidirectional push
//     { kind: "reply", requestId, ok: true|false, result?, error? }
//
//   Server → Worker
//     { kind: "ping" }
//     { kind: "cmd", requestId, cmd, args? }   // request/reply
//
// Heartbeat: server pings every `pingIntervalMs` (default 10s); worker is
// expected to pong within `pongTimeoutMs` (default 30s, three missed
// pings). On timeout the server closes the socket; the worker reconnects.
//
// Multi-server: workers identify with a workerId. Each server tracks the
// sockets it has accepted; cross-server routing (when the workflow lives
// on a worker connected to a different server) is the same problem space
// as promin-bor and is out of scope for this layer.
// ---------------------------------------------------------------------------

import type { ServerWebSocket, WebSocketHandler } from "bun";

export type WorkerWsInbound =
  | { kind: "identify"; workerId: string; capabilities?: readonly string[] }
  | { kind: "pong" }
  | { kind: "frame"; streamId: string; payload: unknown }
  | {
      kind: "reply";
      requestId: string;
      ok: true;
      result?: unknown;
    }
  | {
      kind: "reply";
      requestId: string;
      ok: false;
      error: string;
    };

export type WorkerWsOutbound =
  | { kind: "ping" }
  | { kind: "cmd"; requestId: string; cmd: string; args?: unknown };

interface WorkerWsData {
  workerId?: string;
  /** Last `pong` arrival time (ms). Updated on pong; checked by heartbeat tick. */
  lastPongAt: number;
  /** Last `frame` / `reply` arrival time. Used to break ties on duplicate workerId. */
  lastSeenAt: number;
}

export interface WorkerWebSocketServerConfig {
  /** Ping cadence in ms. Default: 10_000. */
  pingIntervalMs?: number;
  /**
   * Time without a pong after which the server closes the socket. Default:
   * 30_000 (three missed pings at the default cadence). Lower for tests.
   */
  pongTimeoutMs?: number;
  /**
   * Optional auth check on the upgrade request. Returns true to allow the
   * upgrade. Pair with the server's `workerAuth` keys for parity with
   * `/rpc/worker`. When omitted, the upgrade is open.
   */
  authorize?: (req: Request) => boolean;
}

export interface WorkerCommandRequest {
  workerId: string;
  cmd: string;
  args?: unknown;
  /** Reply timeout. Default: 30_000ms. */
  timeoutMs?: number;
}

export class WorkerWebSocketServer {
  /** workerId → socket. Last-write-wins on duplicate ids — matches the
   * worker-registry's "I crashed and reconnected" pattern. */
  private workers = new Map<string, ServerWebSocket<WorkerWsData>>();
  /** Pending request/reply futures keyed by requestId. */
  private pending = new Map<
    string,
    { resolve: (result: unknown) => void; reject: (err: Error) => void }
  >();
  /** Subscribers that want every inbound `frame` from any worker. */
  private frameSubs = new Set<(workerId: string, streamId: string, payload: unknown) => void>();
  /** Subscribers that want connect / disconnect lifecycle events. */
  private connectionSubs = new Set<(event: { workerId: string; connected: boolean }) => void>();
  private readonly pingIntervalMs: number;
  private readonly pongTimeoutMs: number;
  private readonly authorize?: (req: Request) => boolean;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private nextRequestId = 1;

  constructor(config: WorkerWebSocketServerConfig = {}) {
    this.pingIntervalMs = config.pingIntervalMs ?? 10_000;
    this.pongTimeoutMs = config.pongTimeoutMs ?? 30_000;
    this.authorize = config.authorize;
  }

  /**
   * Wire-up helper for `Bun.serve`. The fetch handler routes upgrade
   * requests on `/ws/worker` to `Bun.serve`'s `server.upgrade(req, ...)`;
   * the websocket handlers run the wire protocol.
   */
  // `Bun.serve`'s server is generic in the WS data type so the structural
  // type would force this method to be generic too. Loosen to `any` —
  // `server.upgrade` is the only call site and it's a thin pass-through.
  // biome-ignore lint/suspicious/noExplicitAny: see comment above
  upgradeIfWorkerWs(req: Request, server: any): Response | undefined {
    const url = new URL(req.url);
    if (url.pathname !== "/ws/worker") return undefined;
    if (this.authorize && !this.authorize(req)) {
      return new Response("unauthorized_worker", { status: 401 });
    }
    const data: WorkerWsData = {
      lastPongAt: Date.now(),
      lastSeenAt: Date.now(),
    };
    const upgraded = server.upgrade(req, { data });
    if (!upgraded) return new Response("upgrade_failed", { status: 400 });
    return undefined;
  }

  websocketHandlers(): WebSocketHandler<WorkerWsData> {
    return {
      open: (ws) => {
        // Wait for identify before adding to the workers map. Until then
        // the socket exists but isn't routable; it gets dropped on close
        // if identify never arrives.
        ws.data.lastPongAt = Date.now();
        ws.data.lastSeenAt = Date.now();
      },
      message: (ws, raw) => {
        const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
        let msg: WorkerWsInbound;
        try {
          msg = JSON.parse(text) as WorkerWsInbound;
        } catch {
          // Malformed frame — drop silently. Worker-side bug shouldn't
          // crash the server.
          return;
        }
        ws.data.lastSeenAt = Date.now();
        this.handleInbound(ws, msg);
      },
      close: (ws) => {
        if (ws.data.workerId) {
          // Only evict if the entry is still pointing at THIS socket — a
          // reconnect that beats the close event shouldn't get evicted.
          const existing = this.workers.get(ws.data.workerId);
          if (existing === ws) {
            this.workers.delete(ws.data.workerId);
            for (const sub of this.connectionSubs) {
              try {
                sub({ workerId: ws.data.workerId, connected: false });
              } catch {
                /* swallow */
              }
            }
          }
        }
      },
    };
  }

  start(): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => this.tickHeartbeat(), this.pingIntervalMs);
  }

  stop(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    // Close all open sockets so workers reconnect when the server comes
    // back. Iterating a copy because close → close handler → workers.delete
    // would mutate the map mid-iteration.
    for (const ws of [...this.workers.values()]) {
      try {
        ws.close(1001, "server_shutdown");
      } catch {
        /* swallow */
      }
    }
    this.workers.clear();
    for (const { reject } of this.pending.values()) reject(new Error("server_shutdown"));
    this.pending.clear();
  }

  /**
   * Send a request to a specific worker and await its reply. Throws when:
   *   - the worker isn't connected
   *   - the worker disconnects before replying
   *   - the reply times out
   *   - the worker replies with `{ ok: false, error }`.
   */
  async request<T = unknown>(req: WorkerCommandRequest): Promise<T> {
    const ws = this.workers.get(req.workerId);
    if (!ws) {
      throw new Error(`Worker "${req.workerId}" is not connected`);
    }
    const requestId = `r-${this.nextRequestId++}`;
    const timeoutMs = req.timeoutMs ?? 30_000;
    const promise = new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(requestId)) {
          reject(new Error(`Worker "${req.workerId}" request "${req.cmd}" timed out`));
        }
      }, timeoutMs);
      this.pending.set(requestId, {
        resolve: (result) => {
          clearTimeout(timer);
          resolve(result as T);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });
    });
    const out: WorkerWsOutbound = { kind: "cmd", requestId, cmd: req.cmd, args: req.args };
    try {
      ws.send(JSON.stringify(out));
    } catch (err) {
      this.pending.delete(requestId);
      throw err instanceof Error ? err : new Error(String(err));
    }
    return promise;
  }

  /**
   * Push a one-way command to a worker. Returns `false` when the worker
   * isn't connected. Used by callers that don't need a reply (heartbeats,
   * shutdown signals, stream-stop notices).
   */
  send(workerId: string, message: WorkerWsOutbound): boolean {
    const ws = this.workers.get(workerId);
    if (!ws) return false;
    try {
      ws.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  /** Subscribe to inbound `frame` messages across every worker. */
  onFrame(handler: (workerId: string, streamId: string, payload: unknown) => void): () => void {
    this.frameSubs.add(handler);
    return () => {
      this.frameSubs.delete(handler);
    };
  }

  /** Subscribe to worker connect / disconnect transitions. */
  onConnection(handler: (event: { workerId: string; connected: boolean }) => void): () => void {
    this.connectionSubs.add(handler);
    return () => {
      this.connectionSubs.delete(handler);
    };
  }

  /** Whether the server has a live socket for this worker. */
  isConnected(workerId: string): boolean {
    return this.workers.has(workerId);
  }

  /** Snapshot of currently connected worker ids. */
  connectedWorkers(): string[] {
    return [...this.workers.keys()];
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private handleInbound(ws: ServerWebSocket<WorkerWsData>, msg: WorkerWsInbound): void {
    switch (msg.kind) {
      case "identify": {
        ws.data.workerId = msg.workerId;
        // Replace any prior socket for the same workerId. Old sockets will
        // hit their close handler later but won't evict the new entry
        // because the close handler checks `existing === ws`.
        const prior = this.workers.get(msg.workerId);
        if (prior && prior !== ws) {
          try {
            prior.close(4000, "replaced");
          } catch {
            /* swallow */
          }
        }
        this.workers.set(msg.workerId, ws);
        for (const sub of this.connectionSubs) {
          try {
            sub({ workerId: msg.workerId, connected: true });
          } catch {
            /* swallow */
          }
        }
        return;
      }
      case "pong":
        ws.data.lastPongAt = Date.now();
        return;
      case "frame": {
        if (!ws.data.workerId) return; // unidentified frame — ignore
        for (const sub of this.frameSubs) {
          try {
            sub(ws.data.workerId, msg.streamId, msg.payload);
          } catch {
            /* swallow */
          }
        }
        return;
      }
      case "reply": {
        const pending = this.pending.get(msg.requestId);
        if (!pending) return; // stale or unknown — drop
        this.pending.delete(msg.requestId);
        if (msg.ok) pending.resolve(msg.result);
        else pending.reject(new Error(msg.error));
        return;
      }
    }
  }

  private tickHeartbeat(): void {
    const now = Date.now();
    const ping: WorkerWsOutbound = { kind: "ping" };
    for (const ws of [...this.workers.values()]) {
      if (now - ws.data.lastPongAt > this.pongTimeoutMs) {
        // Worker hasn't pong'd in too long — close, the worker will
        // reconnect.
        try {
          ws.close(4001, "heartbeat_timeout");
        } catch {
          /* swallow */
        }
        continue;
      }
      try {
        ws.send(JSON.stringify(ping));
      } catch {
        /* swallow — close handler will clean up */
      }
    }
  }
}
