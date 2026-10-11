// ---------------------------------------------------------------------------
// WorkerControlSocket — persistent WebSocket from a worker process to the
// Zorya server.
//
// Mirrors the server-side WorkerWebSocketServer protocol. Workers open this
// socket alongside their HTTP-RPC client; it's the channel the server
// pushes commands over (start agent stream, run query handler, drain) and
// "work available" notices (starts this worker can run were queued), and
// the channel the worker pushes asynchronous frames back through (token
// deltas, structured agent events).
//
// Lifecycle: open → identify → ping/pong → close → reconnect with
// exponential backoff (250ms → 30s capped). The reconnect is bounded so a
// permanent server outage doesn't burn unbounded CPU; consumers can wire
// their own up-to-date reconnect-attempt observer if needed.
//
// Wire shape mirrors `WorkerWsInbound` / `WorkerWsOutbound` in the server
// module verbatim. Frame payloads are unknown — callers serialize their
// own typed wire schema (e.g. SessionEvent for agent streams).
// ---------------------------------------------------------------------------

import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";

export type ServerToWorker =
  | { kind: "ping" }
  | { kind: "cmd"; requestId: string; cmd: string; args?: unknown }
  /** Starts this worker can run were queued: claim now (`onWork`). */
  | { kind: "work" };

export type WorkerToServer =
  | {
      kind: "identify";
      workerId: string;
      capabilities?: readonly string[];
      claimsStarts?: boolean;
    }
  | { kind: "pong" }
  | { kind: "frame"; streamId: string; payload: unknown }
  | { kind: "reply"; requestId: string; ok: true; result?: unknown }
  | { kind: "reply"; requestId: string; ok: false; error: string };

/** The Bun / Node `WebSocket` constructor form that sends upgrade headers. */
type WebSocketWithHeaders = new (
  url: string,
  options: { headers: Record<string, string> },
) => WebSocket;

export type CommandHandler = (args: unknown) => Promise<unknown> | unknown;

export interface WorkerControlSocketConfig {
  /** Zorya server URL (e.g. `http://localhost:4100` or `ws://...`). */
  url: string;
  /** Stable worker identifier. */
  workerId: string;
  /** Capabilities reported on identify. */
  capabilities?: readonly string[];
  /**
   * The worker claims queued starts, so the server sends it work-available
   * wake-ups. Default false: a socket used only for control commands,
   * queries or agent streams is never woken for work.
   */
  claimsStarts?: boolean;
  /** Worker API key, sent as `Authorization: Bearer ...` on the upgrade request. */
  apiKey?: string;
  /**
   * Initial reconnect delay in ms. Doubles on each consecutive failure up
   * to `maxReconnectDelayMs`. Default 250.
   */
  reconnectDelayMs?: number;
  /** Cap on reconnect delay. Default 30_000. */
  maxReconnectDelayMs?: number;
  /**
   * Optional WebSocket constructor override. Defaults to the global
   * `WebSocket` (Bun + browser + Node 22+). Tests pass a fake.
   */
  WebSocketImpl?: typeof WebSocket;
  /** Time source for reconnect backoff. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * Hook fired on connection state transitions — useful for surfacing
   * "disconnected from server" in the dashboard worker registry.
   */
  onConnectionChange?: (state: "open" | "closed" | "reconnecting") => void;
}

export class WorkerControlSocket {
  private ws?: WebSocket;
  private readonly url: string;
  private readonly workerId: string;
  private readonly capabilities?: readonly string[];
  private readonly claimsStarts: boolean;
  private readonly apiKey?: string;
  private readonly initialDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly WebSocketImpl: typeof WebSocket;
  private readonly onConnectionChange?: (state: "open" | "closed" | "reconnecting") => void;
  private readonly handlers = new Map<string, CommandHandler>();
  private readonly workListeners = new Set<() => void>();
  private currentDelayMs: number;
  private reconnectTimer?: TimerHandle;
  private readonly clock: WallClock;
  private stopped = false;
  private state: "closed" | "connecting" | "open" = "closed";

  constructor(config: WorkerControlSocketConfig) {
    // Convert http(s):// to ws(s):// — Bun's WebSocket accepts either,
    // but the explicit conversion keeps logs readable.
    this.url =
      config.url.replace(/^http:\/\//, "ws://").replace(/^https:\/\//, "wss://") + "/ws/worker";
    this.workerId = config.workerId;
    this.capabilities = config.capabilities;
    this.claimsStarts = config.claimsStarts ?? false;
    this.apiKey = config.apiKey;
    this.initialDelayMs = config.reconnectDelayMs ?? 250;
    this.maxDelayMs = config.maxReconnectDelayMs ?? 30_000;
    this.currentDelayMs = this.initialDelayMs;
    this.WebSocketImpl = config.WebSocketImpl ?? globalThis.WebSocket;
    this.clock = config.clock ?? SystemWallClock;
    this.onConnectionChange = config.onConnectionChange;
  }

  /**
   * Register a handler for a server-issued command. The handler's return
   * value is sent back as a `reply { ok: true, result }`; thrown errors
   * are sent as `reply { ok: false, error }`. Handlers can be sync or
   * async.
   *
   * Returns an unsubscribe function. Re-registering the same `cmd`
   * replaces the previous handler (last-write-wins).
   */
  onCommand(cmd: string, handler: CommandHandler): () => void {
    this.handlers.set(cmd, handler);
    return () => {
      if (this.handlers.get(cmd) === handler) this.handlers.delete(cmd);
    };
  }

  /**
   * Listen for "work available" messages: the server queued starts this
   * worker can run. Returns the unsubscribe function.
   */
  onWork(listener: () => void): () => void {
    this.workListeners.add(listener);
    return () => {
      this.workListeners.delete(listener);
    };
  }

  /**
   * Push a frame to the server. Returns `true` when the message was
   * dispatched on an open socket; `false` when the socket is closed
   * (caller decides whether to buffer / drop). No queuing here — agent
   * token streams should drop on disconnect, not flood on reconnect.
   */
  sendFrame(streamId: string, payload: unknown): boolean {
    return this.send({ kind: "frame", streamId, payload });
  }

  /**
   * Open the socket. Idempotent — calling twice is a no-op. After `stop()`
   * it opens the socket again (a restarted worker).
   */
  start(): void {
    this.stopped = false;
    if (this.state !== "closed") return;
    this.currentDelayMs = this.initialDelayMs;
    this.connect();
  }

  /** Close the socket and cancel any pending reconnect. Idempotent. */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) {
      this.reconnectTimer.clear();
      this.reconnectTimer = undefined;
    }
    if (this.ws) {
      try {
        this.ws.close(1000, "client_shutdown");
      } catch {
        /* swallow */
      }
      this.ws = undefined;
    }
    this.state = "closed";
  }

  /** Whether the socket is currently open. */
  isOpen(): boolean {
    return this.state === "open";
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private connect(): void {
    this.state = "connecting";
    let ws: WebSocket;
    try {
      // The key goes in the upgrade's `Authorization` header, never the URL
      // (URLs land in access logs). Bun's and Node's WebSocket accept
      // `{ headers }` as the second argument; the server reads only the
      // header.
      ws = this.apiKey
        ? new (this.WebSocketImpl as unknown as WebSocketWithHeaders)(this.url, {
            headers: { authorization: `Bearer ${this.apiKey}` },
          })
        : new this.WebSocketImpl(this.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.addEventListener("open", () => {
      if (this.ws !== ws) return; // a socket replaced by stop() and a restart
      // Identify before anything else.
      this.send({
        kind: "identify",
        workerId: this.workerId,
        capabilities: this.capabilities,
        claimsStarts: this.claimsStarts,
      });
      this.state = "open";
      this.currentDelayMs = this.initialDelayMs;
      this.onConnectionChange?.("open");
    });
    ws.addEventListener("message", (event: MessageEvent) => {
      if (this.ws !== ws) return;
      let msg: ServerToWorker;
      try {
        const text =
          typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data);
        msg = JSON.parse(text) as ServerToWorker;
      } catch {
        return; // malformed
      }
      this.handleInbound(msg);
    });
    ws.addEventListener("close", () => {
      if (this.ws !== ws) return;
      this.ws = undefined;
      const wasOpen = this.state === "open";
      if (wasOpen) this.onConnectionChange?.("closed");
      if (this.stopped) {
        this.state = "closed";
        return;
      }
      this.scheduleReconnect();
    });
    ws.addEventListener("error", () => {
      // Errors fire alongside close in WS — let close handle the
      // reconnect path. Avoid double-reconnect.
    });
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    this.state = "closed";
    this.onConnectionChange?.("reconnecting");
    const delay = this.currentDelayMs + Math.floor(Math.random() * this.currentDelayMs * 0.25);
    this.reconnectTimer = this.clock.setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
    this.currentDelayMs = Math.min(this.currentDelayMs * 2, this.maxDelayMs);
  }

  private handleInbound(msg: ServerToWorker): void {
    switch (msg.kind) {
      case "ping":
        this.send({ kind: "pong" });
        return;
      case "cmd":
        void this.dispatchCommand(msg);
        return;
      case "work":
        for (const listener of this.workListeners) {
          try {
            listener();
          } catch {
            /* a throwing listener must not break the socket */
          }
        }
        return;
    }
  }

  private async dispatchCommand(msg: {
    requestId: string;
    cmd: string;
    args?: unknown;
  }): Promise<void> {
    const handler = this.handlers.get(msg.cmd);
    if (!handler) {
      this.send({
        kind: "reply",
        requestId: msg.requestId,
        ok: false,
        error: `Unknown command "${msg.cmd}"`,
      });
      return;
    }
    try {
      const result = await handler(msg.args);
      this.send({ kind: "reply", requestId: msg.requestId, ok: true, result });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      this.send({ kind: "reply", requestId: msg.requestId, ok: false, error });
    }
  }

  private send(message: WorkerToServer): boolean {
    const ws = this.ws;
    if (!ws || ws.readyState !== this.WebSocketImpl.OPEN) return false;
    try {
      ws.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }
}
