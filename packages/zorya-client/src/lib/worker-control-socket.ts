// ---------------------------------------------------------------------------
// WorkerControlSocket — persistent WebSocket from a worker process to the
// Zorya server.
//
// Mirrors the server-side WorkerWebSocketServer protocol. Workers open this
// socket alongside their HTTP-RPC client; it's the channel the server
// pushes commands over (start agent stream, run query handler, drain),
// and the channel the worker pushes asynchronous frames back through
// (token deltas, structured agent events).
//
// Lifecycle: open → identify → ping/pong → close → reconnect with
// exponential backoff (250ms → 30s capped). The reconnect is bounded so a
// permanent server outage doesn't burn unbounded CPU; consumers can wire
// their own up-to-date reconnect-attempt observer if needed.
//
// Wire shape mirrors `WorkerWsInbound` / `WorkerWsOutbound` in the server
// module verbatim. Frame payloads are unknown — callers serialize their
// own typed wire schema (e.g. SessionEvent for promin-o8dj).
// ---------------------------------------------------------------------------

export type ServerToWorker =
  | { kind: "ping" }
  | { kind: "cmd"; requestId: string; cmd: string; args?: unknown };

export type WorkerToServer =
  | { kind: "identify"; workerId: string; capabilities?: readonly string[] }
  | { kind: "pong" }
  | { kind: "frame"; streamId: string; payload: unknown }
  | { kind: "reply"; requestId: string; ok: true; result?: unknown }
  | { kind: "reply"; requestId: string; ok: false; error: string };

export type CommandHandler = (args: unknown) => Promise<unknown> | unknown;

export interface WorkerControlSocketConfig {
  /** Zorya server URL (e.g. `http://localhost:4100` or `ws://...`). */
  url: string;
  /** Stable worker identifier. */
  workerId: string;
  /** Capabilities reported on identify. */
  capabilities?: readonly string[];
  /** Optional bearer token, sent as `Authorization: Bearer ...` on the upgrade. */
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
  private readonly apiKey?: string;
  private readonly initialDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly WebSocketImpl: typeof WebSocket;
  private readonly onConnectionChange?: (state: "open" | "closed" | "reconnecting") => void;
  private readonly handlers = new Map<string, CommandHandler>();
  private currentDelayMs: number;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private stopped = false;
  private state: "closed" | "connecting" | "open" = "closed";

  constructor(config: WorkerControlSocketConfig) {
    // Convert http(s):// to ws(s):// — Bun's WebSocket accepts either,
    // but the explicit conversion keeps logs readable.
    this.url =
      config.url.replace(/^http:\/\//, "ws://").replace(/^https:\/\//, "wss://") + "/ws/worker";
    this.workerId = config.workerId;
    this.capabilities = config.capabilities;
    this.apiKey = config.apiKey;
    this.initialDelayMs = config.reconnectDelayMs ?? 250;
    this.maxDelayMs = config.maxReconnectDelayMs ?? 30_000;
    this.currentDelayMs = this.initialDelayMs;
    this.WebSocketImpl = config.WebSocketImpl ?? globalThis.WebSocket;
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
   * Push a frame to the server. Returns `true` when the message was
   * dispatched on an open socket; `false` when the socket is closed
   * (caller decides whether to buffer / drop). No queuing here — agent
   * token streams should drop on disconnect, not flood on reconnect.
   */
  sendFrame(streamId: string, payload: unknown): boolean {
    return this.send({ kind: "frame", streamId, payload });
  }

  /** Open the socket. Idempotent — calling twice is a no-op. */
  start(): void {
    if (this.stopped || this.state !== "closed") return;
    this.connect();
  }

  /** Close the socket and cancel any pending reconnect. Idempotent. */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
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
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["Authorization"] = `Bearer ${this.apiKey}`;
    let ws: WebSocket;
    try {
      // Bun + browser: WebSocket constructor doesn't accept headers
      // directly. The Bun-specific 3rd-arg form does, but we keep this
      // portable. apiKey is also sent in the URL search params as a
      // fallback — server-side `authorize(req)` reads either.
      const urlWithKey = this.apiKey
        ? `${this.url}?apiKey=${encodeURIComponent(this.apiKey)}`
        : this.url;
      ws = new this.WebSocketImpl(urlWithKey);
    } catch (err) {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.addEventListener("open", () => {
      // Identify before anything else.
      this.send({
        kind: "identify",
        workerId: this.workerId,
        capabilities: this.capabilities,
      });
      this.state = "open";
      this.currentDelayMs = this.initialDelayMs;
      this.onConnectionChange?.("open");
    });
    ws.addEventListener("message", (event: MessageEvent) => {
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
    this.reconnectTimer = setTimeout(() => {
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
