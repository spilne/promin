// ---------------------------------------------------------------------------
// ZoryaServer.listen over a real socket: body limits answer with a JSON
// 413, errors never render Bun's dev page, hostile worker WebSocket frames
// close only their own socket, and a storage outage during an open SSE
// stream ends that stream instead of the process. If any of these crashed
// the server, this test process would die with it.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows } from "../services/workflows/local-workflows.ts";
import { ZoryaServer, type ListenOptions, type Logger } from "../server.ts";

const quietLogger: Logger = { log() {}, warn() {}, error() {} };

/** Storage that can be switched into failing reads mid-test. */
class FlakyStorage extends InMemoryWorkflowStorage {
  failing = false;
  override async loadWorkflow(workflowId: string) {
    if (this.failing) throw new Error("connection reset");
    return super.loadWorkflow(workflowId);
  }
}

let running: { server: ZoryaServer; stop: () => void } | undefined;

afterEach(() => {
  running?.stop();
  running = undefined;
});

function start(params: { storage?: InMemoryWorkflowStorage; listen?: ListenOptions } = {}): {
  base: string;
  storage: InMemoryWorkflowStorage;
} {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const server = new ZoryaServer({ workflows, logger: quietLogger, sseIntervalMs: 10 });
  const handle = server.listen({ port: 0, hostname: "127.0.0.1", ...params.listen });
  running = { server, stop: handle.stop };
  return { base: `http://127.0.0.1:${handle.port}`, storage };
}

async function healthy(base: string): Promise<boolean> {
  const res = await fetch(`${base}/api/health`);
  return res.status === 200;
}

describe("request body limits", () => {
  it("answers a declared oversized body with a JSON 413", async () => {
    const { base } = start({ listen: { maxRequestBodySize: 1_000 } });
    const res = await fetch(`${base}/api/runs/trigger/x`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ input: "x".repeat(1_500) }),
    });
    expect(res.status).toBe(413);
    expect(await res.json()).toEqual({
      error: "payload_too_large",
      message: "Request body exceeds 1000 bytes",
    });
    expect(await healthy(base)).toBe(true);
  });

  it("answers an oversized chunked body with a JSON 413", async () => {
    const { base } = start({ listen: { maxRequestBodySize: 1_000 } });
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        for (let i = 0; i < 5; i++) c.enqueue(new TextEncoder().encode("y".repeat(400)));
        c.close();
      },
    });
    const res = await fetch(`${base}/api/runs/trigger/x`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      duplex: "half",
    } as RequestInit);
    expect(res.status).toBe(413);
    expect(((await res.json()) as { error: string }).error).toBe("payload_too_large");
  });

  it("passes a chunked body under the limit through to the handler", async () => {
    const { base, storage } = start({ listen: { maxRequestBodySize: 10_000 } });
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "w", input: {} });
    const json = JSON.stringify({ expiresInMs: 60_000 });
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode(json));
        c.close();
      },
    });
    const res = await fetch(`${base}/api/runs/wf-1/signals/go/token`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      duplex: "half",
    } as RequestInit);
    expect(res.status).toBe(201);
  });
});

describe("error responses", () => {
  it("never renders Bun's dev error page", async () => {
    const storage = new FlakyStorage();
    storage.failing = true;
    const { base } = start({ storage });
    const res = await fetch(`${base}/api/runs/wf-1`);
    expect(res.status).toBe(500);
    expect(res.headers.get("content-type")).toBe("application/json");
    const text = await res.text();
    expect(text).not.toContain("connection reset");
    expect(text).not.toContain(".ts");
    expect(JSON.parse(text).error).toBe("internal_error");
  });

  it("answers a malformed URL escape with a JSON 400", async () => {
    const { base } = start();
    const res = await fetch(`${base}/api/runs/%E0%A4%A`);
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toBe("bad_path");
  });
});

/** Open a worker socket, send `frame`, and collect what comes back until close. */
async function sendHostileFrame(params: {
  base: string;
  frame: string | Uint8Array;
}): Promise<{ messages: string[]; closeCode: number }> {
  const ws = new WebSocket(`${params.base.replace("http", "ws")}/ws/worker`);
  const messages: string[] = [];
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("socket was not closed")), 5_000);
    ws.onopen = () => ws.send(params.frame);
    ws.onmessage = (e) => messages.push(String(e.data));
    ws.onclose = (e) => {
      clearTimeout(timer);
      resolve({ messages, closeCode: e.code });
    };
  });
}

describe("worker WebSocket", () => {
  const hostile: Array<[string, string]> = [
    ["null", "null"],
    ["a number", "5"],
    ["a string", '"x"'],
    ["a boolean", "true"],
    ["an array", "[1,2]"],
    ["invalid JSON", "{nope"],
    ["an unknown kind", JSON.stringify({ kind: "nope" })],
    ["a non-string kind", JSON.stringify({ kind: 7 })],
    ["identify without workerId", JSON.stringify({ kind: "identify" })],
    ["reply without requestId", JSON.stringify({ kind: "reply", ok: true })],
  ];

  for (const [label, frame] of hostile) {
    it(`closes the socket with a protocol error on ${label} and keeps serving`, async () => {
      const { base } = start();
      const { messages, closeCode } = await sendHostileFrame({ base, frame });
      expect(closeCode).toBe(1008);
      expect(messages).toHaveLength(1);
      expect(JSON.parse(messages[0]!)).toMatchObject({ kind: "error", error: "protocol_error" });
      expect(await healthy(base)).toBe(true);
    });
  }

  it("drops an oversized frame and keeps serving", async () => {
    const { base } = start();
    const huge = JSON.stringify({ kind: "frame", streamId: "s", payload: "x".repeat(2_000_000) });
    const { closeCode } = await sendHostileFrame({ base, frame: huge });
    // Bun drops it at the transport (maxPayloadLength); the client sees an
    // abnormal close. The handler-level check is covered in the unit test.
    expect([1006, 1009]).toContain(closeCode);
    expect(await healthy(base)).toBe(true);
  });

  it("accepts a well-formed identify", async () => {
    const { base } = start();
    const ws = new WebSocket(`${base.replace("http", "ws")}/ws/worker`);
    await new Promise<void>((r) => (ws.onopen = () => r()));
    ws.send(JSON.stringify({ kind: "identify", workerId: "w-1" }));
    for (let i = 0; i < 200 && !running!.server.workerWs.isConnected("w-1"); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    expect(running!.server.workerWs.isConnected("w-1")).toBe(true);
    ws.close();
  });
});

describe("SSE /api/runs/:id/events under a storage outage", () => {
  it("ends the stream with an error event and keeps the process alive", async () => {
    const storage = new FlakyStorage();
    const { base } = start({ storage });
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "w", input: {} });

    const res = await fetch(`${base}/api/runs/wf-1/events`);
    expect(res.status).toBe(200);
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let text = "";
    const first = await reader.read();
    text += decoder.decode(first.value);
    expect(text).toContain('"type":"snapshot"');

    // Storage goes down while the tab is open. With sseIntervalMs = 10 the
    // watcher fails 5 times (10, 20, 40, 80, 160 ms of backoff) and gives up.
    storage.failing = true;
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      text += decoder.decode(next.value, { stream: true });
    }
    expect(text).toContain('"type":"error","error":"watch_failed"');
    expect(text).toContain('"type":"end"');

    storage.failing = false;
    expect(await healthy(base)).toBe(true);
  });
});
