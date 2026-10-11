// ---------------------------------------------------------------------------
// Secure defaults, per-plane auth, readiness and graceful shutdown, through
// ZoryaServer.handle and a real listen on port 0.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  type WorkflowStorage,
  type WorkflowStatus,
} from "@promin/workflow";
import { WorkerControlSocket } from "@promin/zorya-client";
import { QueuedWorkflows } from "../services/workflows/queued-workflows.ts";
import { ZoryaWorkflows, type TriggerResult } from "../services/workflows/zorya-workflows.ts";
import { ZoryaServer } from "../server.ts";
import { ZoryaInsecureConfigError, type Logger, type ZoryaServerConfig } from "../server-config.ts";
import { readSse, SseHttpError, type SseMessage } from "../../ui/api/sse.ts";

const OPERATOR_KEY = "operator-key-0123456789";
const WORKER_KEY = "worker-key-0123456789";

const quietLogger: Logger = { log() {}, warn() {}, error() {} };

/** Storage whose reads fail while `down` is set. */
class FlakyStorage extends InMemoryWorkflowStorage {
  down = false;
  override async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatus | null> {
    if (this.down) throw new Error("connection refused");
    return super.loadWorkflowStatus(workflowId);
  }
}

/**
 * Workflows service whose start touches storage (so it fails while storage
 * is down) and whose trigger can be held open to test draining.
 */
class ProbeWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  startCalls = 0;
  stopCalls = 0;
  /** While set, `trigger` waits on it. */
  hold?: Promise<void>;
  dispatching = false;

  constructor(storage: WorkflowStorage) {
    super();
    this.storage = storage;
  }
  protected canHandle(): boolean {
    return true;
  }
  protected async dispatch(): Promise<TriggerResult> {
    this.dispatching = true;
    await this.hold;
    return { workflowId: "held-run" };
  }
  protected override async onStart(): Promise<void> {
    this.startCalls += 1;
    await this.storage.loadWorkflowStatus("boot-check");
  }
  protected override async onStop(): Promise<void> {
    this.stopCalls += 1;
  }
}

function queued(storage: WorkflowStorage = new InMemoryWorkflowStorage()) {
  return new QueuedWorkflows({ storage });
}

function keyedServer(overrides: Partial<ZoryaServerConfig> = {}): ZoryaServer {
  return new ZoryaServer({
    workflows: queued(),
    apiKeys: [OPERATOR_KEY],
    remoteWorkers: { apiKeys: [WORKER_KEY] },
    logger: quietLogger,
    ...overrides,
  });
}

function bearer(key: string): Record<string, string> {
  return { authorization: `Bearer ${key}` };
}

function rpc(params: { path: string; method: string; body?: unknown; key?: string }) {
  return new Request(`http://zorya${params.path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(params.key ? bearer(params.key) : {}) },
    body: JSON.stringify({ method: params.method, params: params.body ?? {} }),
  });
}

/** Yield to the event loop until `predicate` holds, bounded by iterations. */
async function until(predicate: () => boolean, maxTurns = 2_000): Promise<void> {
  for (let i = 0; i < maxTurns; i++) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 1));
  }
  throw new Error("condition not reached");
}

const running: Array<{ stop: () => Promise<void> }> = [];
afterEach(async () => {
  await Promise.all(running.splice(0).map((r) => r.stop()));
});

function listen(server: ZoryaServer): string {
  const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
  running.push(handle);
  return `http://127.0.0.1:${handle.port}`;
}

describe("secure by default", () => {
  it("refuses to construct without operator keys", () => {
    expect(() => new ZoryaServer({ workflows: queued(), logger: quietLogger })).toThrow(
      ZoryaInsecureConfigError,
    );
  });

  it("refuses remote workers without worker keys", () => {
    expect(
      () =>
        new ZoryaServer({
          workflows: queued(),
          apiKeys: [OPERATOR_KEY],
          remoteWorkers: {},
          logger: quietLogger,
        }),
    ).toThrow(/remoteWorkers\.apiKeys/);
  });

  it("starts without keys only with insecure, and warns loudly", () => {
    const warnings: string[] = [];
    new ZoryaServer({
      workflows: queued(),
      remoteWorkers: {},
      insecure: true,
      logger: { ...quietLogger, warn: (m) => warnings.push(m) },
    });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("INSECURE");
  });

  it("never puts a key in the insecure warning or the refusal", () => {
    const warnings: string[] = [];
    new ZoryaServer({
      workflows: queued(),
      apiKeys: [OPERATOR_KEY],
      remoteWorkers: {},
      insecure: true,
      logger: { ...quietLogger, warn: (m) => warnings.push(m) },
    });
    expect(warnings.join("\n")).not.toContain(OPERATOR_KEY);
  });

  it("rejects empty keys", () => {
    expect(() => keyedServer({ apiKeys: [""] })).toThrow(/non-empty/);
  });
});

describe("every plane is authenticated", () => {
  const cases: Array<{ name: string; req: (key?: string) => Request }> = [
    {
      name: "/api/runs",
      req: (key) => new Request("http://zorya/api/runs", { headers: key ? bearer(key) : {} }),
    },
    {
      name: "/api/advertisements",
      req: (key) =>
        new Request("http://zorya/api/advertisements", { headers: key ? bearer(key) : {} }),
    },
    { name: "/rpc/worker", req: (key) => rpc({ path: "/rpc/worker", method: "listWorkers", key }) },
    {
      name: "/rpc/storage",
      req: (key) =>
        rpc({ path: "/rpc/storage", method: "loadWorkflow", body: { workflowId: "x" }, key }),
    },
  ];

  for (const c of cases) {
    it(`${c.name} rejects a request without credentials`, async () => {
      const res = await keyedServer().handle(c.req());
      expect(res.status).toBe(401);
    });
    it(`${c.name} rejects a wrong key`, async () => {
      const res = await keyedServer().handle(c.req("not-a-key"));
      expect(res.status).toBe(401);
    });
  }

  it("keeps operator and worker keys on their own planes", async () => {
    const server = keyedServer();
    expect((await server.handle(cases[0]!.req(WORKER_KEY))).status).toBe(401);
    expect((await server.handle(cases[0]!.req(OPERATOR_KEY))).status).toBe(200);
    expect((await server.handle(cases[2]!.req(OPERATOR_KEY))).status).toBe(401);
    expect((await server.handle(cases[2]!.req(WORKER_KEY))).status).toBe(200);
  });

  it("rejects a worker WebSocket upgrade without the worker key", async () => {
    const base = listen(keyedServer());
    const res = await fetch(`${base}/ws/worker`, {
      headers: { upgrade: "websocket", connection: "Upgrade", ...bearer(OPERATOR_KEY) },
    });
    expect(res.status).toBe(401);
    const anonymous = await fetch(`${base}/ws/worker?apiKey=${WORKER_KEY}`, {
      headers: { upgrade: "websocket", connection: "Upgrade" },
    });
    // The key is read from the Authorization header only, never the URL.
    expect(anonymous.status).toBe(401);
  });

  it("serves health without credentials", async () => {
    const res = await keyedServer().handle(new Request("http://zorya/api/health"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });
});

describe("/rpc/storage scoping", () => {
  async function call(server: ZoryaServer, method: string, body: unknown, key: string) {
    return server.handle(rpc({ path: "/rpc/storage", method, body, key }));
  }

  it("gives worker keys the runner methods only", async () => {
    const server = keyedServer();
    expect((await call(server, "loadWorkflow", { workflowId: "x" }, WORKER_KEY)).status).toBe(200);
    for (const method of [
      "purgeCompleted",
      "cancelWorkflow",
      "resetSteps",
      "findSignalTokenById",
      "listSignalTokensForWorkflow",
      "completeSignalToken",
      "createSignalToken",
    ]) {
      const res = await call(server, method, { workflowId: "x", tokenId: "t" }, WORKER_KEY);
      expect({ method, status: res.status }).toEqual({ method, status: 403 });
    }
  });

  it("never hands a token bearer to a worker key; operators get the full contract", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "demo", input: {} });
    await storage.createSignalToken({
      tokenId: "tok-1",
      workflowId: "wf-1",
      signalName: "approve",
      bearer: "secret-bearer",
      tags: [],
      idempotencyKey: null,
      expiresAt: new Date(Date.UTC(2100, 0, 1)),
    });
    const server = keyedServer({ workflows: queued(storage) });
    const asWorker = await call(server, "findSignalTokenById", { tokenId: "tok-1" }, WORKER_KEY);
    expect(asWorker.status).toBe(403);
    expect(await asWorker.text()).not.toContain("secret-bearer");
    const asOperator = await call(
      server,
      "findSignalTokenById",
      { tokenId: "tok-1" },
      OPERATOR_KEY,
    );
    expect(asOperator.status).toBe(200);
  });
});

describe("worker WebSocket auth", () => {
  it("connects a worker that sends its key", async () => {
    const server = keyedServer();
    const base = listen(server);
    const socket = new WorkerControlSocket({ url: base, workerId: "w-keyed", apiKey: WORKER_KEY });
    socket.start();
    try {
      await until(() => server.workerWs.isConnected("w-keyed"));
    } finally {
      await socket.stop();
    }
  });

  it("never connects a worker with the wrong key", async () => {
    const server = keyedServer();
    const base = listen(server);
    const states: string[] = [];
    const socket = new WorkerControlSocket({
      url: base,
      workerId: "w-bad",
      apiKey: "wrong",
      reconnectDelayMs: 5,
      onConnectionChange: (s) => states.push(s),
    });
    socket.start();
    try {
      await until(() => states.filter((s) => s === "reconnecting").length >= 2);
      expect(server.workerWs.isConnected("w-bad")).toBe(false);
      expect(server.workerWs.openSocketCount()).toBe(0);
    } finally {
      await socket.stop();
    }
  });

  it("closes a socket that never identifies", async () => {
    const clock = FakeWallClock.create(0);
    const server = keyedServer({ clock });
    const base = listen(server);
    const ws = new WebSocket(`${base.replace("http", "ws")}/ws/worker`, {
      headers: bearer(WORKER_KEY),
    } as unknown as string[]);
    const closed = new Promise<number>((resolve) =>
      ws.addEventListener("close", (e) => resolve(e.code)),
    );
    await until(() => server.workerWs.openSocketCount() === 1);
    clock.advance(10_000);
    expect(await closed).toBe(4002);
    await until(() => server.workerWs.openSocketCount() === 0);
  });
});

describe("readiness", () => {
  it("is public and reports storage down, then up after the start retry", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new FlakyStorage();
    storage.down = true;
    const workflows = new ProbeWorkflows(storage);
    const server = new ZoryaServer({
      workflows,
      apiKeys: [OPERATOR_KEY],
      clock,
      logger: quietLogger,
      startRetry: { initialDelayMs: 1_000, maxDelayMs: 8_000 },
    });
    listen(server);

    await until(() => server.serviceStatuses()[0]!.state === "retrying");
    const down = await server.handle(new Request("http://zorya/api/ready"));
    expect(down.status).toBe(503);
    const downBody = (await down.json()) as { storage: { ok: boolean }; services: unknown[] };
    expect(downBody.storage.ok).toBe(false);
    expect(downBody.services).toEqual([
      { name: "workflows", state: "retrying", attempts: 1, lastError: "connection refused" },
    ]);

    // Still down at the first retry: backoff doubles.
    clock.advance(1_000);
    await until(
      () => workflows.startCalls === 2 && server.serviceStatuses()[0]!.state === "retrying",
    );

    storage.down = false;
    clock.advance(1_999);
    expect(workflows.startCalls).toBe(2);
    clock.advance(1);
    await server.ready();
    expect(workflows.startCalls).toBe(3);

    const up = await server.handle(new Request("http://zorya/api/ready"));
    expect(up.status).toBe(200);
    expect(((await up.json()) as { ready: boolean }).ready).toBe(true);
  });

  it("reports a hung storage probe as not ready within the timeout", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage();
    const server = new ZoryaServer({
      workflows: new ProbeWorkflows(storage),
      apiKeys: [OPERATOR_KEY],
      clock,
      logger: quietLogger,
      readinessTimeoutMs: 500,
    });
    listen(server);
    await server.ready();
    storage.loadWorkflowStatus = () => new Promise(() => {});
    const pending = server.handle(new Request("http://zorya/api/ready"));
    await until(() => clock.pendingCount() > 1);
    clock.advance(500);
    const res = await pending;
    expect(res.status).toBe(503);
  });

  it("is not ready before the server starts", async () => {
    const res = await keyedServer().handle(new Request("http://zorya/api/ready"));
    expect(res.status).toBe(503);
  });
});

describe("browser auth", () => {
  it("streams run events over fetch-based SSE with an API key", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-sse", workflowName: "demo", input: {} });
    const server = new ZoryaServer({
      workflows: new ProbeWorkflows(storage),
      apiKeys: [OPERATOR_KEY],
      logger: quietLogger,
      sseIntervalMs: 10,
    });
    const base = listen(server);

    await expect(
      readSse({ url: `${base}/api/runs/wf-sse/events`, onMessage: () => {} }),
    ).rejects.toEqual(new SseHttpError(401));

    const ctrl = new AbortController();
    const messages: SseMessage[] = [];
    const reading = readSse({
      url: `${base}/api/runs/wf-sse/events`,
      headers: bearer(OPERATOR_KEY),
      signal: ctrl.signal,
      onMessage: (m) => {
        messages.push(m);
        ctrl.abort();
      },
    }).catch(() => {});
    await reading;
    expect(JSON.parse(messages[0]!.data).type).toBe("snapshot");
  });

  it("serves the public signal page with the token bearer alone", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-sig", workflowName: "demo", input: {} });
    const server = new ZoryaServer({
      workflows: new ProbeWorkflows(storage),
      apiKeys: [OPERATOR_KEY],
      logger: quietLogger,
    });
    const minted = await server.handle(
      new Request("http://zorya/api/runs/wf-sig/signals/approve/token", {
        method: "POST",
        headers: { "content-type": "application/json", ...bearer(OPERATOR_KEY) },
        body: JSON.stringify({ expiresInMs: 60_000 }),
      }),
    );
    expect(minted.status).toBe(201);
    const { tokenId, bearer: tokenBearer } = (await minted.json()) as {
      tokenId: string;
      bearer: string;
    };

    const describe = (key: string) =>
      server.handle(
        new Request(`http://zorya/api/signal-tokens/${tokenId}/describe`, { headers: bearer(key) }),
      );
    expect((await describe(tokenBearer)).status).toBe(200);
    // The operator key is not a token bearer.
    expect((await describe(OPERATOR_KEY)).status).toBe(401);

    const complete = await server.handle(
      new Request(`http://zorya/api/signal-tokens/${tokenId}/complete`, {
        method: "POST",
        headers: { "content-type": "application/json", ...bearer(tokenBearer) },
        body: JSON.stringify({ value: { ok: true } }),
      }),
    );
    expect(complete.status).toBe(202);
  });
});

describe("graceful stop", () => {
  it("leaves no timers, sockets or open streams", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "wf-open", workflowName: "demo", input: {} });
    const workflows = new ProbeWorkflows(storage);
    const server = new ZoryaServer({
      workflows,
      apiKeys: [OPERATOR_KEY],
      clock,
      logger: quietLogger,
    });
    const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
    const base = `http://127.0.0.1:${handle.port}`;
    await server.ready();

    // An open SSE stream and a worker socket that never identified.
    let gotSnapshot = false;
    const stream = readSse({
      url: `${base}/api/runs/wf-open/events`,
      headers: bearer(OPERATOR_KEY),
      onMessage: () => {
        gotSnapshot = true;
      },
    });
    const ws = new WebSocket(`${base.replace("http", "ws")}/ws/worker`);
    const wsClosed = new Promise<void>((r) => ws.addEventListener("close", () => r()));
    await until(() => gotSnapshot && server.workerWs.openSocketCount() === 1);
    expect(clock.pendingCount()).toBeGreaterThan(0);

    await handle.stop();

    await stream; // the server ended the stream
    await wsClosed;
    expect(server.workerWs.openSocketCount()).toBe(0);
    expect(clock.pendingCount()).toBe(0);
    expect(workflows.stopCalls).toBe(1);
    await expect(fetch(`${base}/api/health`)).rejects.toThrow();
    const ready = await server.handle(new Request("http://zorya/api/ready"));
    expect(ready.status).toBe(503);
  });

  it("waits for in-flight requests before stopping services", async () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new ProbeWorkflows(storage);
    let release!: () => void;
    workflows.hold = new Promise<void>((r) => (release = r));
    const server = new ZoryaServer({ workflows, apiKeys: [OPERATOR_KEY], logger: quietLogger });
    const handle = server.listen({ port: 0, hostname: "127.0.0.1" });
    const base = `http://127.0.0.1:${handle.port}`;
    await server.ready();

    const triggered = fetch(`${base}/api/workflows/demo/runs`, {
      method: "POST",
      headers: { "content-type": "application/json", ...bearer(OPERATOR_KEY) },
      body: JSON.stringify({ input: {} }),
    });
    await until(() => workflows.dispatching);

    let stopped = false;
    const stopping = handle.stop().then(() => {
      stopped = true;
    });
    // New requests are refused while draining; probes still answer.
    expect(
      (await server.handle(new Request("http://zorya/api/runs", { headers: bearer(OPERATOR_KEY) })))
        .status,
    ).toBe(503);
    await new Promise((r) => setTimeout(r, 10));
    expect(stopped).toBe(false);
    expect(workflows.stopCalls).toBe(0);

    release();
    const res = await triggered;
    expect(res.status).toBeLessThan(300);
    await stopping;
    expect(workflows.stopCalls).toBe(1);
  });
});
