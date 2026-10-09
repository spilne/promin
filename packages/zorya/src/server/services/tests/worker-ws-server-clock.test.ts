// ---------------------------------------------------------------------------
// WorkerWebSocketServer — heartbeat cadence, pong-timeout eviction and
// request reply timeouts run on the injected clock. Drives the websocket
// handlers directly with in-memory socket doubles (no Bun.serve port).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { ServerWebSocket } from "bun";
import { FakeWallClock } from "@promin/workflow";
import { WorkerWebSocketServer } from "../worker-ws-server.ts";

interface SocketDouble {
  data: { workerId?: string; lastPongAt: number; lastSeenAt: number };
  sent: unknown[];
  closed: { code: number; reason: string } | null;
  send(text: string): void;
  close(code: number, reason: string): void;
}

function connect(server: WorkerWebSocketServer, workerId: string): SocketDouble {
  const handlers = server.websocketHandlers();
  const ws: SocketDouble = {
    data: { lastPongAt: 0, lastSeenAt: 0 },
    sent: [],
    closed: null,
    send(text) {
      ws.sent.push(JSON.parse(text));
    },
    close(code, reason) {
      ws.closed = { code, reason };
      handlers.close?.(asBun(ws), code, reason);
    },
  };
  handlers.open?.(asBun(ws));
  handlers.message(asBun(ws), JSON.stringify({ kind: "identify", workerId }));
  return ws;
}

function message(server: WorkerWebSocketServer, ws: SocketDouble, msg: unknown): void {
  server.websocketHandlers().message(asBun(ws), JSON.stringify(msg));
}

function asBun(ws: SocketDouble): ServerWebSocket<any> {
  return ws as unknown as ServerWebSocket<any>;
}

const pings = (ws: SocketDouble) =>
  ws.sent.filter((m) => (m as { kind: string }).kind === "ping").length;

describe("WorkerWebSocketServer — heartbeat on an injected clock", () => {
  it("pings every pingIntervalMs and evicts a worker silent for longer than pongTimeoutMs", () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const server = new WorkerWebSocketServer({
      pingIntervalMs: 10_000,
      pongTimeoutMs: 30_000,
      clock,
    });
    const ws = connect(server, "w-1");
    expect(ws.data.lastPongAt).toBe(clock.currentTimeMs());

    server.start();
    expect(clock.pendingCount()).toBe(1);

    clock.advance(9_999);
    expect(pings(ws)).toBe(0);
    clock.advance(1);
    expect(pings(ws)).toBe(1);

    // 30s without a pong is not yet "longer than" the timeout.
    clock.advance(20_000);
    expect(pings(ws)).toBe(3);
    expect(ws.closed).toBeNull();
    expect(server.isConnected("w-1")).toBe(true);

    clock.advance(10_000);
    expect(ws.closed).toEqual({ code: 4001, reason: "heartbeat_timeout" });
    expect(server.isConnected("w-1")).toBe(false);

    server.stop();
    expect(clock.pendingCount()).toBe(0);
  });

  it("a pong stamped by the clock keeps the worker alive", () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const server = new WorkerWebSocketServer({
      pingIntervalMs: 10_000,
      pongTimeoutMs: 30_000,
      clock,
    });
    const ws = connect(server, "w-2");
    server.start();

    clock.advance(25_000);
    message(server, ws, { kind: "pong" });
    expect(ws.data.lastPongAt).toBe(clock.currentTimeMs());

    // 40s since connect but only 15s since the pong.
    clock.advance(15_000);
    expect(ws.closed).toBeNull();
    expect(server.isConnected("w-2")).toBe(true);
    server.stop();
  });
});

describe("WorkerWebSocketServer.request — reply timeout on an injected clock", () => {
  it("rejects once the clock passes timeoutMs and clears the timer on reply", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const server = new WorkerWebSocketServer({ clock });
    const ws = connect(server, "w-3");

    const timedOut = server.request({ workerId: "w-3", cmd: "query", timeoutMs: 5_000 });
    const outcome = timedOut.then(
      () => "resolved",
      (err: Error) => err.message,
    );
    expect(clock.pendingCount()).toBe(1);
    clock.advance(5_000);
    expect(await outcome).toBe('Worker "w-3" request "query" timed out');

    const answered = server.request<string>({ workerId: "w-3", cmd: "query", timeoutMs: 5_000 });
    expect(clock.pendingCount()).toBe(1);
    const cmd = ws.sent.at(-1) as { requestId: string };
    message(server, ws, { kind: "reply", requestId: cmd.requestId, ok: true, result: "hi" });
    expect(await answered).toBe("hi");
    expect(clock.pendingCount()).toBe(0);
  });
});
