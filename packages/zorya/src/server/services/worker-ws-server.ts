// ---------------------------------------------------------------------------
// WorkerWebSocketServer — persistent server-side multiplexer for worker
// control sockets.
//
// Carries two things:
//   - agent event streaming: worker → server frames per workflowId, fanned
//     out to dashboard SSE clients (AgentStreamHub)
//   - workflow query handlers: server → worker request/reply for in-memory
//     state queries (`POST /api/runs/:id/query`)
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
//     { kind: "work" }                         // starts the worker can run were queued
//     { kind: "error", error, message? }       // protocol violation; socket closes
//
// Every inbound frame is validated against `WorkerWsInboundSchema`. A frame
// that is too large, not JSON, or not a known message is answered with
// `{ kind: "error" }` and the socket is closed. Replies are accepted only
// from the socket the command was sent to.
//
// Auth: the upgrade request must carry `Authorization: Bearer <worker key>`
// when worker keys are configured (`authorize`). A socket that does not
// identify within `identifyTimeoutMs` (default 10s) is closed with 4002.
//
// Heartbeat: server pings every `pingIntervalMs` (default 10s); worker is
// expected to pong within `pongTimeoutMs` (default 30s, three missed
// pings). On timeout the server closes the socket; the worker reconnects.
//
// Multi-server: workers identify with a workerId. Each server tracks only
// the sockets it has accepted; a query or agent stream for a worker
// connected to another instance is not routed across instances (run the
// instances behind sticky routing, see DEPLOYMENT.md).
// ---------------------------------------------------------------------------

import type { ServerWebSocket, WebSocketHandler } from "bun";
import { z } from "zod";
import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";

/** Default cap on one inbound frame (bytes). */
export const DEFAULT_MAX_WORKER_FRAME_BYTES = 1_048_576;

const IdSchema = z.string().min(1).max(256);

/**
 * Every frame a worker may send. Anything else (non-JSON, a non-object, an
 * unknown `kind`, a missing or mistyped field) is a protocol error: the
 * server answers with `{ kind: "error" }` and closes the socket.
 */
export const WorkerWsInboundSchema = z.union([
  z.object({
    kind: z.literal("identify"),
    workerId: IdSchema,
    capabilities: z.array(z.string().max(256)).max(256).optional(),
  }),
  z.object({ kind: z.literal("pong") }),
  z.object({
    kind: z.literal("frame"),
    streamId: z.string().min(1).max(1024),
    payload: z.unknown(),
  }),
  z.object({
    kind: z.literal("reply"),
    requestId: IdSchema,
    ok: z.literal(true),
    result: z.unknown().optional(),
  }),
  z.object({
    kind: z.literal("reply"),
    requestId: IdSchema,
    ok: z.literal(false),
    error: z.string().max(65_536),
  }),
]);

export type WorkerWsInbound = z.infer<typeof WorkerWsInboundSchema>;

export type WorkerWsOutbound =
  | { kind: "ping" }
  | { kind: "cmd"; requestId: string; cmd: string; args?: unknown }
  /** Work available: claim now instead of at the next poll (`WorkerWakeups`). */
  | { kind: "work" }
  | { kind: "error"; error: "protocol_error" | "frame_too_large"; message?: string };

/** Close codes the server uses on worker sockets. */
const CLOSE_POLICY_VIOLATION = 1008;
const CLOSE_MESSAGE_TOO_BIG = 1009;
const CLOSE_INTERNAL_ERROR = 1011;
const CLOSE_GOING_AWAY = 1001;
const CLOSE_IDENTIFY_TIMEOUT = 4002;

/** Default time a socket has to send `identify` after the upgrade (ms). */
export const DEFAULT_WORKER_IDENTIFY_TIMEOUT_MS = 10_000;

interface WorkerWsData {
  workerId?: string;
  /** Last `pong` arrival time (ms). Updated on pong; checked by heartbeat tick. */
  lastPongAt: number;
  /** Closes the socket if `identify` does not arrive in time. */
  identifyTimer?: TimerHandle;
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
   * Time a socket has to send `identify` after the upgrade before the server
   * closes it (code 4002). Default: `DEFAULT_WORKER_IDENTIFY_TIMEOUT_MS`.
   */
  identifyTimeoutMs?: number;
  /**
   * Optional auth check on the upgrade request. Returns true to allow the
   * upgrade. Pair with the server's `workerAuth` keys for parity with
   * `/rpc/worker`. When omitted, the upgrade is open.
   */
  authorize?: (req: Request) => boolean;
  /**
   * Time source for pong / last-seen stamps, the heartbeat cadence and
   * request reply timeouts. Default: `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Largest inbound frame accepted, in bytes. Bun drops larger frames at
   * the transport (`maxPayloadLength`); the handler re-checks. Default:
   * `DEFAULT_MAX_WORKER_FRAME_BYTES` (1 MiB).
   */
  maxFrameBytes?: number;
  /** Reports errors the socket handlers caught. Default: `console.error`. */
  onError?: (message: string, err: unknown) => void;
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
  /** Open sockets that have not identified yet. */
  private unidentified = new Set<ServerWebSocket<WorkerWsData>>();
  /** Pending request/reply futures keyed by requestId. */
  private pending = new Map<
    string,
    {
      /** Socket the command went to; only it may answer. */
      ws: ServerWebSocket<WorkerWsData>;
      resolve: (result: unknown) => void;
      reject: (err: Error) => void;
    }
  >();
  /** Subscribers that want every inbound `frame` from any worker. */
  private frameSubs = new Set<(workerId: string, streamId: string, payload: unknown) => void>();
  /** Subscribers that want connect / disconnect lifecycle events. */
  private connectionSubs = new Set<(event: { workerId: string; connected: boolean }) => void>();
  private readonly pingIntervalMs: number;
  private readonly pongTimeoutMs: number;
  private readonly identifyTimeoutMs: number;
  private readonly authorize?: (req: Request) => boolean;
  private readonly clock: WallClock;
  private readonly maxFrameBytes: number;
  private readonly onError: (message: string, err: unknown) => void;
  private heartbeatTimer?: TimerHandle;
  private nextRequestId = 1;
  /** Set by `stop()`: new upgrades are refused until `start()`. */
  private stopped = false;

  constructor(config: WorkerWebSocketServerConfig = {}) {
    this.pingIntervalMs = config.pingIntervalMs ?? 10_000;
    this.pongTimeoutMs = config.pongTimeoutMs ?? 30_000;
    this.identifyTimeoutMs = config.identifyTimeoutMs ?? DEFAULT_WORKER_IDENTIFY_TIMEOUT_MS;
    this.authorize = config.authorize;
    this.clock = config.clock ?? SystemWallClock;
    this.maxFrameBytes = config.maxFrameBytes ?? DEFAULT_MAX_WORKER_FRAME_BYTES;
    this.onError = config.onError ?? ((message, err) => console.error(message, err));
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
  upgradeIfWorkerWs(params: { req: Request; server: any }): Response | undefined {
    const { req, server } = params;
    const url = new URL(req.url);
    if (url.pathname !== "/ws/worker") return undefined;
    if (this.stopped) return Response.json({ error: "shutting_down" }, { status: 503 });
    if (this.authorize && !this.authorize(req)) {
      return Response.json({ error: "unauthorized_worker" }, { status: 401 });
    }
    const data: WorkerWsData = { lastPongAt: this.clock.currentTimeMs() };
    const upgraded = server.upgrade(req, { data });
    if (!upgraded) return Response.json({ error: "upgrade_failed" }, { status: 400 });
    return undefined;
  }

  /**
   * Bun websocket handlers. Each one is a crash barrier: nothing a worker
   * sends can throw out of them (Bun exits the process on a throw in a
   * websocket handler).
   */
  websocketHandlers(): WebSocketHandler<WorkerWsData> {
    return {
      maxPayloadLength: this.maxFrameBytes,
      open: (ws) => {
        // Wait for identify before adding to the workers map. Until then
        // the socket exists but isn't routable; it gets dropped on close
        // if identify never arrives.
        ws.data.lastPongAt = this.clock.currentTimeMs();
        if (this.stopped) {
          this.closeQuietly(ws, CLOSE_GOING_AWAY, "server_shutdown");
          return;
        }
        this.unidentified.add(ws);
        ws.data.identifyTimer = this.clock.setTimeout(() => {
          ws.data.identifyTimer = undefined;
          if (!this.unidentified.delete(ws)) return;
          this.closeQuietly(ws, CLOSE_IDENTIFY_TIMEOUT, "identify_timeout");
        }, this.identifyTimeoutMs);
      },
      message: (ws, raw) => {
        try {
          this.handleRaw(ws, raw);
        } catch (err) {
          this.report("[zorya] worker socket message handler failed", err);
          this.closeQuietly(ws, CLOSE_INTERNAL_ERROR, "internal_error");
        }
      },
      close: (ws) => {
        try {
          this.handleClose(ws);
        } catch (err) {
          this.report("[zorya] worker socket close handler failed", err);
        }
      },
    };
  }

  start(): void {
    this.stopped = false;
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = this.clock.setInterval(() => this.tickHeartbeat(), this.pingIntervalMs);
  }

  stop(): void {
    this.stopped = true;
    if (this.heartbeatTimer) {
      this.heartbeatTimer.clear();
      this.heartbeatTimer = undefined;
    }
    // Close all open sockets so workers reconnect when the server comes
    // back. Iterating a copy because close → close handler → workers.delete
    // would mutate the map mid-iteration.
    for (const ws of [...this.workers.values(), ...this.unidentified]) {
      this.clearIdentifyTimer(ws);
      this.closeQuietly(ws, CLOSE_GOING_AWAY, "server_shutdown");
    }
    this.workers.clear();
    this.unidentified.clear();
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
      const timer = this.clock.setTimeout(() => {
        if (this.pending.delete(requestId)) {
          reject(new Error(`Worker "${req.workerId}" request "${req.cmd}" timed out`));
        }
      }, timeoutMs);
      this.pending.set(requestId, {
        ws,
        resolve: (result) => {
          timer.clear();
          resolve(result as T);
        },
        reject: (err) => {
          timer.clear();
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
  send(params: { workerId: string; message: WorkerWsOutbound }): boolean {
    const { workerId, message } = params;
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

  /** Open sockets, identified or not. */
  openSocketCount(): number {
    return this.workers.size + this.unidentified.size;
  }

  /** Whether the server has a live socket for this worker. */
  isConnected(workerId: string): boolean {
    return this.workers.has(workerId);
  }

  /** Snapshot of currently connected worker ids. */
  connectedWorkers(): string[] {
    return [...this.workers.keys()];
  }

  /** Number of identified worker sockets. */
  connectedCount(): number {
    return this.workers.size;
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private handleRaw(ws: ServerWebSocket<WorkerWsData>, raw: string | Buffer): void {
    const size = typeof raw === "string" ? Buffer.byteLength(raw, "utf8") : raw.byteLength;
    if (size > this.maxFrameBytes) {
      this.reject(ws, {
        error: "frame_too_large",
        code: CLOSE_MESSAGE_TOO_BIG,
        message: `Frame exceeds ${this.maxFrameBytes} bytes`,
      });
      return;
    }
    const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
    let decoded: unknown;
    try {
      decoded = JSON.parse(text);
    } catch {
      this.reject(ws, { error: "protocol_error", message: "Frame is not valid JSON" });
      return;
    }
    const parsed = WorkerWsInboundSchema.safeParse(decoded);
    if (!parsed.success) {
      this.reject(ws, { error: "protocol_error", message: "Unrecognized worker message" });
      return;
    }
    this.handleInbound(ws, parsed.data);
  }

  /** Tell the worker why, then close its socket. */
  private reject(
    ws: ServerWebSocket<WorkerWsData>,
    params: {
      error: "protocol_error" | "frame_too_large";
      message: string;
      code?: number;
    },
  ): void {
    const out: WorkerWsOutbound = { kind: "error", error: params.error, message: params.message };
    try {
      ws.send(JSON.stringify(out));
    } catch {
      /* socket already gone */
    }
    this.closeQuietly(ws, params.code ?? CLOSE_POLICY_VIOLATION, params.error);
  }

  private closeQuietly(ws: ServerWebSocket<WorkerWsData>, code: number, reason: string): void {
    try {
      ws.close(code, reason);
    } catch {
      /* already closed */
    }
  }

  private report(message: string, err: unknown): void {
    try {
      this.onError(message, err);
    } catch {
      /* a broken reporter must not break the barrier */
    }
  }

  private clearIdentifyTimer(ws: ServerWebSocket<WorkerWsData>): void {
    ws.data.identifyTimer?.clear();
    ws.data.identifyTimer = undefined;
  }

  private handleClose(ws: ServerWebSocket<WorkerWsData>): void {
    this.clearIdentifyTimer(ws);
    this.unidentified.delete(ws);
    // Commands in flight on this socket can no longer be answered.
    for (const [requestId, entry] of this.pending) {
      if (entry.ws !== ws) continue;
      this.pending.delete(requestId);
      entry.reject(new Error(`Worker "${ws.data.workerId ?? "unidentified"}" disconnected`));
    }
    if (!ws.data.workerId) return;
    // Only evict if the entry is still pointing at THIS socket — a
    // reconnect that beats the close event shouldn't get evicted.
    const existing = this.workers.get(ws.data.workerId);
    if (existing !== ws) return;
    this.workers.delete(ws.data.workerId);
    for (const sub of this.connectionSubs) {
      try {
        sub({ workerId: ws.data.workerId, connected: false });
      } catch {
        /* swallow */
      }
    }
  }

  private handleInbound(ws: ServerWebSocket<WorkerWsData>, msg: WorkerWsInbound): void {
    switch (msg.kind) {
      case "identify": {
        this.clearIdentifyTimer(ws);
        this.unidentified.delete(ws);
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
        ws.data.lastPongAt = this.clock.currentTimeMs();
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
        // Stale, unknown, or answered by a socket the command didn't go to.
        if (!pending || pending.ws !== ws) return;
        this.pending.delete(msg.requestId);
        if (msg.ok) pending.resolve(msg.result);
        else pending.reject(new Error(msg.error));
        return;
      }
    }
  }

  private tickHeartbeat(): void {
    const now = this.clock.currentTimeMs();
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
