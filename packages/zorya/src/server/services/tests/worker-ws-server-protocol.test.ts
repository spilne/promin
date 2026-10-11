// ---------------------------------------------------------------------------
// WorkerWebSocketServer — inbound frame validation and the handler crash
// barrier, driven with in-memory socket doubles.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { ServerWebSocket } from "bun";
import { FakeWallClock } from "@promin/workflow";
import { WorkerWebSocketServer } from "../worker-ws-server.ts";

interface SocketDouble {
  data: { workerId?: string; lastPongAt: number; lastSeenAt: number };
  sent: Array<Record<string, unknown>>;
  closed: { code: number; reason: string } | null;
  send(text: string): void;
  close(code: number, reason: string): void;
}

function open(server: WorkerWebSocketServer): SocketDouble {
  const handlers = server.websocketHandlers();
  const ws: SocketDouble = {
    data: { lastPongAt: 0, lastSeenAt: 0 },
    sent: [],
    closed: null,
    send(text) {
      ws.sent.push(JSON.parse(text));
    },
    close(code, reason) {
      if (ws.closed) return;
      ws.closed = { code, reason };
      handlers.close?.(asBun(ws), code, reason);
    },
  };
  handlers.open?.(asBun(ws));
  return ws;
}

function raw(server: WorkerWebSocketServer, ws: SocketDouble, frame: string | Buffer): void {
  server.websocketHandlers().message(asBun(ws), frame);
}

function asBun(ws: SocketDouble): ServerWebSocket<any> {
  return ws as unknown as ServerWebSocket<any>;
}

describe("WorkerWebSocketServer — inbound validation", () => {
  const hostile: Array<[string, string]> = [
    ["null", "null"],
    ["a number", "5"],
    ["a string", '"x"'],
    ["an array", "[]"],
    ["invalid JSON", "{"],
    ["an unknown kind", '{"kind":"nope"}'],
    ["identify with a numeric workerId", '{"kind":"identify","workerId":5}'],
    ["frame without streamId", '{"kind":"frame","payload":1}'],
    ["reply with ok:false and no error", '{"kind":"reply","requestId":"r-1","ok":false}'],
  ];

  for (const [label, frame] of hostile) {
    it(`rejects ${label} with a protocol error and closes without throwing`, () => {
      const server = new WorkerWebSocketServer({ clock: FakeWallClock.create(0) });
      const ws = open(server);
      expect(() => raw(server, ws, frame)).not.toThrow();
      expect(ws.sent).toEqual([
        { kind: "error", error: "protocol_error", message: expect.any(String) },
      ]);
      expect(ws.closed?.code).toBe(1008);
      expect(server.connectedWorkers()).toEqual([]);
    });
  }

  it("rejects a frame over maxFrameBytes with frame_too_large / 1009", () => {
    const server = new WorkerWebSocketServer({ clock: FakeWallClock.create(0), maxFrameBytes: 64 });
    const ws = open(server);
    raw(
      server,
      ws,
      Buffer.from(JSON.stringify({ kind: "frame", streamId: "s", payload: "x".repeat(100) })),
    );
    expect(ws.sent[0]).toMatchObject({ kind: "error", error: "frame_too_large" });
    expect(ws.closed?.code).toBe(1009);
  });

  it("advertises maxFrameBytes to Bun as maxPayloadLength", () => {
    const server = new WorkerWebSocketServer({ maxFrameBytes: 4_096 });
    expect(server.websocketHandlers().maxPayloadLength).toBe(4_096);
  });

  it("accepts binary frames that decode to a valid message", () => {
    const server = new WorkerWebSocketServer({ clock: FakeWallClock.create(0) });
    const ws = open(server);
    raw(server, ws, Buffer.from(JSON.stringify({ kind: "identify", workerId: "w-1" })));
    expect(ws.closed).toBeNull();
    expect(server.isConnected("w-1")).toBe(true);
  });

  it("contains a throwing subscriber and reports nothing fatal", () => {
    const errors: unknown[] = [];
    const server = new WorkerWebSocketServer({
      clock: FakeWallClock.create(0),
      onError: (_m, err) => errors.push(err),
    });
    server.onConnection(() => {
      throw new Error("subscriber bug");
    });
    const ws = open(server);
    expect(() => raw(server, ws, '{"kind":"identify","workerId":"w-1"}')).not.toThrow();
    expect(server.isConnected("w-1")).toBe(true);
  });
});

describe("WorkerWebSocketServer — replies", () => {
  it("accepts a reply only from the socket the command went to", async () => {
    const clock = FakeWallClock.create(0);
    const server = new WorkerWebSocketServer({ clock });
    const worker = open(server);
    raw(server, worker, '{"kind":"identify","workerId":"w-1"}');
    const intruder = open(server);
    raw(server, intruder, '{"kind":"identify","workerId":"w-2"}');

    const result = server.request({ workerId: "w-1", cmd: "echo", timeoutMs: 1_000 });
    const cmd = worker.sent.find((m) => m.kind === "cmd")!;

    raw(
      server,
      intruder,
      JSON.stringify({ kind: "reply", requestId: cmd.requestId, ok: true, result: "spoofed" }),
    );
    raw(
      server,
      worker,
      JSON.stringify({ kind: "reply", requestId: cmd.requestId, ok: true, result: "real" }),
    );
    expect(await result).toBe("real");
  });

  it("rejects in-flight commands when their socket closes", async () => {
    const clock = FakeWallClock.create(0);
    const server = new WorkerWebSocketServer({ clock });
    const worker = open(server);
    raw(server, worker, '{"kind":"identify","workerId":"w-1"}');
    const result = server.request({ workerId: "w-1", cmd: "echo", timeoutMs: 60_000 });
    worker.close(1000, "bye");
    await expect(result).rejects.toThrow('Worker "w-1" disconnected');
    expect(clock.pendingCount()).toBe(0);
  });
});
