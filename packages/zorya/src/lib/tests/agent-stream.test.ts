// ---------------------------------------------------------------------------
// Agent event streaming — end-to-end through ZoryaServer.
//
// Browser ──SSE── Server ──WS── Worker
//   /api/runs/:id/agent-stream         worker.streams[workflowId]
//
// Boots a ZoryaServer on a real port, opens a worker WS, registers a
// stream source on the worker (a tiny event emitter standing in for
// SessionEventBus), then opens an SSE connection from a "browser" via
// fetch + ReadableStream parsing (the shape ChatGPT / Claude.ai use).
//
// Pins:
//   - The hub broadcasts `agent-stream-start` to workers when the SSE
//     opens. The worker subscribes to its registered stream.
//   - Events emitted on the source land on the SSE client as
//     `event: agent-event` lines.
//   - Disconnecting the SSE client triggers `agent-stream-stop` to the
//     worker, which tears down the source subscription.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { WorkerControlSocket } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
import { LocalWorkflows } from "../../index.ts";

function listenServer(): { server: ZoryaServer; url: string; close: () => void } {
  const storage = new InMemoryWorkflowStorage();
  const server = new ZoryaServer({
    workflows: new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    }),
  });
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

/** Tiny stand-in for SessionEventBus — same subscribe/emit shape. */
function makeBus<T>(): {
  emit: (event: T) => void;
  subscribe: (observer: (event: T) => void) => () => void;
} {
  const subs = new Set<(event: T) => void>();
  return {
    emit: (event) => {
      for (const fn of subs) fn(event);
    },
    subscribe: (fn) => {
      subs.add(fn);
      return () => subs.delete(fn);
    },
  };
}

/** Parse a `text/event-stream` body, yielding `{ event, data }` per record. */
async function* readSSE(
  body: ReadableStream<Uint8Array>,
): AsyncGenerator<{ event: string; data: string }> {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) return;
    buf += dec.decode(value, { stream: true });
    // SSE records are separated by a blank line. Loop while we have one.
    let sep = buf.indexOf("\n\n");
    while (sep !== -1) {
      const record = buf.slice(0, sep);
      buf = buf.slice(sep + 2);
      sep = buf.indexOf("\n\n");
      let event = "message";
      let data = "";
      for (const line of record.split("\n")) {
        if (line.startsWith("event: ")) event = line.slice(7);
        else if (line.startsWith("data: "))
          data = data ? data + "\n" + line.slice(6) : line.slice(6);
      }
      yield { event, data };
    }
  }
}

describe("Agent event streaming — server SSE relayed from worker WS frames", () => {
  it("broadcasts stream-start, fans events through, tears down on disconnect", async () => {
    const { server, url, close } = listenServer();
    try {
      // 1. Worker connects + registers a stream for workflow "wf-stream-1".
      const ws = new WorkerControlSocket({ url, workerId: "w-stream-1", reconnectDelayMs: 50 });
      ws.start();
      await waitFor(
        () => server.workerWs.connectedWorkers(),
        (xs) => xs.includes("w-stream-1"),
      );

      // Stream source — when the server sends `agent-stream-start`, the
      // worker subscribes here and forwards each emit as a WS frame.
      const bus = makeBus<{ type: string; payload?: unknown }>();
      const subs = new Set<(event: unknown) => void>();
      ws.onCommand("agent-stream-start", (args) => {
        const { streamId, workflowId } = args as { streamId: string; workflowId: string };
        if (workflowId !== "wf-stream-1") return { hosted: false };
        const observer = (event: unknown) => {
          ws.sendFrame(streamId, event);
        };
        subs.add(observer);
        const unsub = bus.subscribe((e) => observer(e));
        // Map the server-assigned streamId to the unsub for stop.
        cleanups.set(streamId, () => {
          unsub();
          subs.delete(observer);
        });
        return { hosted: true };
      });
      const cleanups = new Map<string, () => void>();
      ws.onCommand("agent-stream-stop", (args) => {
        const { streamId } = args as { streamId: string };
        cleanups.get(streamId)?.();
        cleanups.delete(streamId);
        return { ok: true };
      });

      // 2. Browser-side: open the SSE endpoint via fetch + ReadableStream
      // parsing (same pattern ChatGPT / Claude.ai use to consume their
      // chat streams).
      const ac = new AbortController();
      const sseRes = await fetch(`${url}/api/runs/wf-stream-1/agent-stream`, {
        signal: ac.signal,
      });
      expect(sseRes.ok).toBe(true);
      expect(sseRes.headers.get("content-type")).toContain("text/event-stream");

      const events: Array<{ event: string; data: string }> = [];
      const sseIter = readSSE(sseRes.body!);

      // First record: the immediate `ready` event so proxies don't buffer.
      const ready = await sseIter.next();
      expect(ready.done).toBe(false);
      events.push(ready.value!);
      expect(ready.value!.event).toBe("ready");
      expect(JSON.parse(ready.value!.data)).toEqual({ workflowId: "wf-stream-1" });

      // 3. The server should have broadcasted stream-start by now. Emit
      // events on the bus and watch them arrive over SSE.
      // Brief wait so the WS reply path settles.
      await waitFor(
        () => subs.size,
        (n) => n >= 1,
      );
      bus.emit({ type: "token.delta", payload: { delta: "Hello" } });
      bus.emit({ type: "token.delta", payload: { delta: " world" } });
      bus.emit({ type: "turn.end", payload: { answer: "Hello world" } });

      for (let i = 0; i < 3; i++) {
        const next = await sseIter.next();
        expect(next.done).toBe(false);
        events.push(next.value!);
        expect(next.value!.event).toBe("agent-event");
      }
      expect(events.slice(1).map((e) => JSON.parse(e.data))).toEqual([
        { type: "token.delta", payload: { delta: "Hello" } },
        { type: "token.delta", payload: { delta: " world" } },
        { type: "turn.end", payload: { answer: "Hello world" } },
      ]);

      // 4. Disconnect the SSE — server should fire stream-stop, worker
      // tears down the bus subscription.
      // Release the reader so the ReadableStream isn't locked, then
      // cancel — same pattern as `EventSource.close()`.
      ac.abort();
      await waitFor(
        () => subs.size,
        (n) => n === 0,
        3_000,
      );
      expect(subs.size).toBe(0);

      await ws.stop();
    } finally {
      close();
    }
  });

  it("opens cleanly when no worker is hosting the workflow (empty stream, no events)", async () => {
    const { url, close } = listenServer();
    try {
      const ac = new AbortController();
      const sseRes = await fetch(`${url}/api/runs/no-such-workflow/agent-stream`, {
        signal: ac.signal,
      });
      expect(sseRes.ok).toBe(true);
      const iter = readSSE(sseRes.body!);
      // Should still get the `ready` event.
      const first = await iter.next();
      expect(first.done).toBe(false);
      expect(first.value!.event).toBe("ready");
      // Release the reader so the ReadableStream isn't locked, then
      // cancel — same pattern as `EventSource.close()`.
      ac.abort();
    } finally {
      close();
    }
  });
});
