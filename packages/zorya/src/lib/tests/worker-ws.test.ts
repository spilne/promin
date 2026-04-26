// ---------------------------------------------------------------------------
// Worker control socket — end-to-end test against a real Bun.serve port.
//
// Boots a ZoryaServer, opens a worker WS via the WorkerControlSocket
// client, and exercises the full protocol: identify handshake, server →
// worker request/reply, worker → server frame fan-out, ping/pong
// keepalive, and reconnect after server-side close.
//
// Uses a real port (random) rather than a fake handler because Bun's
// in-process handle() doesn't carry the upgrade negotiation — the WS is
// only addressable via Bun.serve.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { WorkerControlSocket } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";

function listenServer(): { server: ZoryaServer; url: string; close: () => void } {
  const server = new ZoryaServer({ storage: new InMemoryWorkflowStorage() });
  const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
  return {
    server,
    url: `http://127.0.0.1:${handle.port}`,
    close: () => handle.stop(),
  };
}

async function waitFor<T>(
  fn: () => T | Promise<T>,
  predicate: (v: T) => boolean,
  timeoutMs = 2_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (predicate(v)) return v;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe("WorkerControlSocket — loopback against real ZoryaServer", () => {
  it("identifies, exchanges request/reply, and pushes frames in both directions", async () => {
    const { server, url, close } = listenServer();
    try {
      const ws = new WorkerControlSocket({
        url,
        workerId: "w-rt-1",
        capabilities: ["gpu"],
        reconnectDelayMs: 50,
      });

      // Worker registers a command handler BEFORE start so the server's
      // first request lands cleanly.
      ws.onCommand("echo", (args) => ({ echoed: args }));
      ws.start();

      // Wait for the server to accept the identify frame.
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("w-rt-1"),
      );
      expect(ws.isOpen()).toBe(true);

      // Server → worker request/reply round-trip.
      const reply = await server.workerWs.request<{ echoed: { msg: string } }>({
        workerId: "w-rt-1",
        cmd: "echo",
        args: { msg: "hello" },
      });
      expect(reply).toEqual({ echoed: { msg: "hello" } });

      // Worker → server frame fan-out via onFrame.
      const frames: Array<{ workerId: string; streamId: string; payload: unknown }> = [];
      const unsub = server.workerWs.onFrame((workerId, streamId, payload) =>
        frames.push({ workerId, streamId, payload }),
      );
      ws.sendFrame("s1", { tick: 1 });
      ws.sendFrame("s1", { tick: 2 });
      await waitFor(
        () => frames.length,
        (n) => n >= 2,
      );
      expect(frames).toEqual([
        { workerId: "w-rt-1", streamId: "s1", payload: { tick: 1 } },
        { workerId: "w-rt-1", streamId: "s1", payload: { tick: 2 } },
      ]);
      unsub();

      // Unknown command → reply with ok: false.
      await expect(
        server.workerWs.request({ workerId: "w-rt-1", cmd: "no-such-cmd" }),
      ).rejects.toThrow(/Unknown command/);

      await ws.stop();
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => !xs.includes("w-rt-1"),
      );
    } finally {
      close();
    }
  });

  it("reconnects after a server-side close (exponential backoff capped at maxReconnectDelayMs)", async () => {
    const { server, url, close } = listenServer();
    try {
      const transitions: string[] = [];
      const ws = new WorkerControlSocket({
        url,
        workerId: "w-recon-1",
        reconnectDelayMs: 30,
        maxReconnectDelayMs: 100,
        onConnectionChange: (s) => transitions.push(s),
      });
      ws.start();
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("w-recon-1"),
      );

      // Force-close from the server side. Worker should detect, schedule
      // a reconnect, and re-identify on the new socket.
      const wasIdentifiedBefore = server.workerWs.isConnected("w-recon-1");
      expect(wasIdentifiedBefore).toBe(true);

      // Issue a request that closes the socket via heartbeat-timeout
      // semantics — easier than reaching into private state: just call
      // server.workerWs.stop() then start() with the same instance.
      // Stop drops every active socket so the worker sees a close.
      server.workerWs.stop();
      // Restart for the reconnect to land on.
      server.workerWs.start();

      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("w-recon-1"),
        3_000,
      );
      expect(ws.isOpen()).toBe(true);
      // Transitions: open → reconnecting → open.
      expect(transitions[0]).toBe("open");
      expect(transitions).toContain("reconnecting");
      expect(transitions[transitions.length - 1]).toBe("open");

      await ws.stop();
    } finally {
      close();
    }
  });

  it("rejects request() when the worker isn't connected", async () => {
    const { server, close } = listenServer();
    try {
      await expect(server.workerWs.request({ workerId: "ghost", cmd: "anything" })).rejects.toThrow(
        /not connected/,
      );
    } finally {
      close();
    }
  });

  it("emits onConnection events on identify + close", async () => {
    const { server, url, close } = listenServer();
    try {
      const events: Array<{ workerId: string; connected: boolean }> = [];
      const unsub = server.workerWs.onConnection((e) => events.push(e));

      const ws = new WorkerControlSocket({ url, workerId: "w-conn-1", reconnectDelayMs: 50 });
      ws.start();
      await waitFor(
        () => events,
        (xs) => xs.some((e) => e.connected),
      );

      await ws.stop();
      await waitFor(
        () => events,
        (xs) => xs.some((e) => !e.connected),
      );

      expect(events.find((e) => e.workerId === "w-conn-1" && e.connected)).toBeDefined();
      expect(events.find((e) => e.workerId === "w-conn-1" && !e.connected)).toBeDefined();
      unsub();
    } finally {
      close();
    }
  });
});
