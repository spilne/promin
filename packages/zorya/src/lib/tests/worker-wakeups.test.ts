// ---------------------------------------------------------------------------
// Wake-ups end to end, in process: ZoryaServer + QueuedWorkflows +
// ZoryaWorkers whose control sockets are bridged straight into the server's
// worker-socket handlers (no port). One FakeWallClock drives everything.
//
// Covered: a notified worker claims without waiting for its poll; a lost
// notification falls back to the poll; a burst is coalesced; wake-ups
// follow the workflow name and version; a worker with every slot busy
// ignores the wake and claims once a slot frees.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import type { ServerWebSocket } from "bun";
import { succeed } from "@spilne/perfect-core";
import { FakeWallClock, InMemoryWorkflowStorage, workflow, type Workflow } from "@promin/workflow";
import {
  InMemoryWorkerRegistry,
  InMemoryWorkflowAdvertisementRegistry,
  InMemoryWorkflowStartQueue,
} from "@promin/workflow/distributed";
import { ZoryaClient, ZoryaWorker, type ZoryaWorkerErrorEvent } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
import type { Logger } from "../../server/server-config.ts";
import { QueuedWorkflows } from "../../server/services/workflows/index.ts";

const quietLogger: Logger = { log() {}, warn() {}, error() {} };
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

/** Yield until the code under test has parked on clock timers (bounded by iterations). */
async function settle(clock: FakeWallClock): Promise<void> {
  let last = -1;
  let stable = 0;
  for (let i = 0; i < 1_000 && stable < 5; i++) {
    await new Promise<void>((r) => setImmediate(r));
    const pending = clock.pendingCount();
    stable = pending > 0 && pending === last ? stable + 1 : 0;
    last = pending;
  }
}

/** Settle, then advance `stepMs`, until `done()` holds; resolves with the clock time spent. */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxSteps?: number;
}): Promise<number> {
  const { clock, done, stepMs = 5, maxSteps = 4_000 } = params;
  const start = clock.currentTimeMs();
  for (let i = 0; i < maxSteps; i++) {
    await settle(clock);
    if (await done()) return clock.currentTimeMs() - start;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
  return clock.currentTimeMs() - start;
}

/**
 * A `WebSocket` class whose instances talk to `server.workerWs` through its
 * Bun handlers. `drop(message)` true loses a server → worker message.
 */
function bridgedWebSocket(params: {
  server: ZoryaServer;
  drop?: (message: { kind: string }) => boolean;
}): typeof WebSocket {
  const handlers = params.server.workerWs.websocketHandlers();
  const deliver = (fn: () => void) => setImmediate(fn);
  class BridgedWebSocket extends EventTarget {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    readyState = 0;
    private readonly serverSide: ServerWebSocket<any>;
    constructor(_url: string) {
      super();
      this.serverSide = {
        data: { lastPongAt: 0 },
        send: (text: string) => {
          const msg = JSON.parse(text) as { kind: string };
          if (params.drop?.(msg)) return 0;
          deliver(() => this.dispatchEvent(new MessageEvent("message", { data: text })));
          return text.length;
        },
        close: () => this.shut(),
      } as unknown as ServerWebSocket<any>;
      deliver(() => {
        handlers.open?.(this.serverSide);
        this.readyState = 1;
        this.dispatchEvent(new Event("open"));
      });
    }
    send(text: string): void {
      handlers.message(this.serverSide, text);
    }
    close(): void {
      this.shut();
    }
    private shut(): void {
      if (this.readyState === 3) return;
      this.readyState = 3;
      handlers.close?.(this.serverSide, 1000, "");
      deliver(() => this.dispatchEvent(new Event("close")));
    }
  }
  return BridgedWebSocket as unknown as typeof WebSocket;
}

const POLL_MS = 10_000;

function deploy() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const workflowStarts = new InMemoryWorkflowStartQueue({ clock });
  const queued = new QueuedWorkflows({
    storage,
    workflowStarts,
    advertisements: new InMemoryWorkflowAdvertisementRegistry({ clock }),
    workerRegistry: new InMemoryWorkerRegistry({ clock }),
    clock,
    wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
    maintenance: false,
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows: queued,
    remoteWorkers: { wakeups: { coalesceMs: 20 } },
    clock,
    logger: quietLogger,
  });
  server.start();
  cleanups.push(() => server.stop());
  let dropWork = false;
  const WebSocketImpl = bridgedWebSocket({ server, drop: (m) => m.kind === "work" && dropWork });

  const worker = (params: {
    workflows: Workflow<unknown, unknown>[];
    workerId: string;
    concurrency?: number;
    webSocket?: typeof WebSocket;
    errors?: ZoryaWorkerErrorEvent[];
  }) => {
    const client = new ZoryaClient({
      url: "http://zorya.test",
      clock,
      fetch: (req) => server.handle(req),
    });
    const w = new ZoryaWorker({
      client,
      workflows: params.workflows,
      workerId: params.workerId,
      clock,
      concurrency: params.concurrency ?? 10,
      heartbeatIntervalMs: 60_000,
      pollWorkflowStarts: { intervalMs: POLL_MS, jitter: 0 },
      controlSocket: { WebSocketImpl: params.webSocket ?? WebSocketImpl, reconnectDelayMs: 1_000 },
      ...(params.errors && { onError: (e: ZoryaWorkerErrorEvent) => params.errors!.push(e) }),
    });
    w.start();
    cleanups.push(async () => {
      let stopped = false;
      const stopping = w.stop({ timeoutMs: 0 }).then(() => (stopped = true));
      await driveUntil({ clock, done: () => stopped, stepMs: 50 });
      await stopping;
    });
    return w;
  };

  /** Workers advertised, connected and idle (their first claim done). */
  const ready = async (workers: ZoryaWorker[]) => {
    await Promise.all(workers.map((w) => w.ready()));
    await driveUntil({
      clock,
      done: () =>
        server.workerWs.connectedCount() === workers.length &&
        workers.every((w) => w.stats.claimCalls > 0),
    });
    await settle(clock);
  };

  const status = async (workflowId: string) =>
    (await storage.loadWorkflowStatus(workflowId))?.status;

  return {
    clock,
    storage,
    workflowStarts,
    queued,
    server,
    worker,
    ready,
    status,
    loseWorkMessages: (lose: boolean) => {
      dropWork = lose;
    },
  };
}

const asDefs = (...wfs: Workflow<unknown, unknown>[]) => wfs;
const quick = (name = "quick", version?: string) =>
  workflow<number>({ name, ...(version !== undefined && { version }) })
    .step("only", ({ input }) => succeed(input * 2))
    .build();

describe("worker wake-ups", () => {
  it("a notified worker claims at once, without waiting for its poll", async () => {
    const dep = deploy();
    const w = dep.worker({ workflows: asDefs(quick()), workerId: "w1" });
    await dep.ready([w]);
    const before = w.stats;

    await dep.queued.trigger("quick", 21, { workflowId: "q-1" });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("q-1")) === "completed",
    });

    expect(spent).toBeLessThan(100);
    expect((await dep.storage.loadWorkflow("q-1"))?.result).toBe(42);
    expect(w.stats.wakeups - before.wakeups).toBe(1);
    expect(w.stats.claimCalls - before.claimCalls).toBeGreaterThanOrEqual(1);
  });

  it("a lost notification falls back to the poll", async () => {
    const dep = deploy();
    const w = dep.worker({ workflows: asDefs(quick()), workerId: "w1" });
    await dep.ready([w]);
    dep.loseWorkMessages(true);
    const before = w.stats.wakeups;

    await dep.queued.trigger("quick", 1, { workflowId: "lost-1" });
    await driveUntil({
      clock: dep.clock,
      done: () => dep.clock.currentTimeMs() >= Date.parse("2026-01-01T00:00:00Z") + POLL_MS - 500,
      stepMs: 100,
    });
    expect(await dep.status("lost-1")).toBe("pending");
    expect(w.stats.wakeups - before).toBe(0);

    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("lost-1")) === "completed",
      stepMs: 100,
    });
  });

  it("coalesces a burst of triggers into a few messages per worker", async () => {
    const dep = deploy();
    const workers = [
      dep.worker({ workflows: asDefs(quick()), workerId: "w1", concurrency: 50 }),
      dep.worker({ workflows: asDefs(quick()), workerId: "w2", concurrency: 50 }),
    ];
    await dep.ready(workers);
    const before = workers.map((w) => w.stats.wakeups);
    const sentBefore = dep.server.workerWakeups!.sent;

    for (let i = 0; i < 200; i++) await dep.queued.trigger("quick", i, { workflowId: `b-${i}` });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => {
        const all = await Promise.all(Array.from({ length: 200 }, (_, i) => dep.status(`b-${i}`)));
        return all.every((s) => s === "completed");
      },
    });

    expect(spent).toBeLessThan(POLL_MS);
    workers.forEach((w, i) => expect(w.stats.wakeups - before[i]!).toBeLessThanOrEqual(3));
    expect(dep.server.workerWakeups!.sent - sentBefore).toBeLessThanOrEqual(6);
    // Both workers shared the burst, each claiming up to its free slots.
    expect(workers.reduce((n, w) => n + w.stats.claimed, 0)).toBe(200);
  });

  it("wakes only workers that host the run's workflow name and version", async () => {
    const dep = deploy();
    const v1 = dep.worker({ workflows: asDefs(quick("greet", "1")), workerId: "v1" });
    const v2 = dep.worker({ workflows: asDefs(quick("greet", "2")), workerId: "v2" });
    const other = dep.worker({ workflows: asDefs(quick("other")), workerId: "other" });
    await dep.ready([v1, v2, other]);
    const base = [v1, v2, other].map((w) => w.stats.wakeups);
    const woken = () => [v1, v2, other].map((w, i) => w.stats.wakeups - base[i]!);

    await dep.queued.trigger("greet", 1, { workflowId: "g-2", version: "2" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("g-2")) === "completed",
    });
    expect(woken()).toEqual([0, 1, 0]);

    dep.clock.advance(50);
    await dep.queued.trigger("other", 1, { workflowId: "o-1" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("o-1")) === "completed",
    });
    expect(woken()).toEqual([0, 1, 1]);
  });

  it("a start released with a delay is claimed again when the delay is up, not at the poll", async () => {
    const dep = deploy();
    const w = dep.worker({ workflows: asDefs(quick()), workerId: "w1" });
    await dep.ready([w]);
    // Another driver holds the run's lock for one more second: the worker
    // releases the start for `lockedRetryDelayMs` (5 s) and claims it then.
    await dep.queued.trigger("quick", 1, { workflowId: "held-1" });
    await dep.storage.tryLock({ workflowId: "held-1", lockDurationMs: 1_000 });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("held-1")) === "completed",
      stepMs: 100,
    });
    expect(spent).toBeGreaterThanOrEqual(5_000);
    expect(spent).toBeLessThan(POLL_MS);
  });

  it("a worker with every slot busy ignores the wake, then claims when a slot frees", async () => {
    const dep = deploy();
    const gates: Array<() => void> = [];
    const gated = workflow<number>({ name: "gated" })
      .step("wait", async () => {
        await new Promise<void>((r) => gates.push(r));
        return "ok";
      })
      .build();
    const w = dep.worker({ workflows: asDefs(gated), workerId: "w1", concurrency: 1 });
    await dep.ready([w]);

    await dep.queued.trigger("gated", 1, { workflowId: "g-1" });
    await driveUntil({ clock: dep.clock, done: () => gates.length === 1 });
    const busy = w.stats;

    await dep.queued.trigger("gated", 2, { workflowId: "g-2" });
    await driveUntil({ clock: dep.clock, done: () => w.stats.wakeups > busy.wakeups });
    expect(w.stats.wakeupsIgnored - busy.wakeupsIgnored).toBe(1);
    expect(w.stats.claimCalls).toBe(busy.claimCalls);

    gates.shift()!();
    const spent = await driveUntil({ clock: dep.clock, done: () => gates.length === 1 });
    expect(spent).toBeLessThan(100);
    gates.shift()!();
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("g-2")) === "completed",
    });
  });

  it("wakes a few of many idle workers per trigger, not the whole fleet", async () => {
    const dep = deploy();
    const fleet = Array.from({ length: 12 }, (_, i) =>
      dep.worker({ workflows: asDefs(quick()), workerId: `f${i}` }),
    );
    await dep.ready(fleet);
    const claimsBefore = fleet.reduce((n, w) => n + w.stats.claimCalls, 0);

    for (let i = 0; i < 6; i++) {
      await dep.queued.trigger("quick", i, { workflowId: `few-${i}` });
      await driveUntil({
        clock: dep.clock,
        done: async () => (await dep.status(`few-${i}`)) === "completed",
      });
      dep.clock.advance(100);
    }
    const claims = fleet.reduce((n, w) => n + w.stats.claimCalls, 0) - claimsBefore;
    // Per trigger: two woken (1 + margin), not twelve, plus the claim the
    // driving worker makes when its slot frees (it picks up a start that was
    // queued again while it held it).
    expect(claims).toBeLessThanOrEqual(6 * 3);
    // Round-robin: the work is spread over the fleet.
    expect(fleet.filter((w) => w.stats.claimed > 0).length).toBeGreaterThanOrEqual(6);
  });

  it("a start whose woken workers are all busy is followed up to an idle one", async () => {
    const dep = deploy();
    const gates: Array<() => void> = [];
    const gated = workflow<number>({ name: "gated" })
      .step("wait", async ({ input }) => {
        if (input !== 0) await new Promise<void>((r) => gates.push(r));
        return "ok";
      })
      .build();
    const workers = ["a", "b", "c"].map((id) =>
      dep.worker({ workflows: asDefs(gated), workerId: id, concurrency: 1 }),
    );
    await dep.ready(workers);
    // Two of the three are busy.
    await dep.queued.trigger("gated", 1, { workflowId: "busy-1" });
    await dep.queued.trigger("gated", 2, { workflowId: "busy-2" });
    await driveUntil({ clock: dep.clock, done: () => gates.length === 2 });
    const idle = workers.find((w) => w.activeCount === 0)!;

    // Whichever pair the wake-up picks, the idle worker runs the start well
    // before its poll: at once, or at the follow-up.
    dep.clock.advance(100);
    await dep.queued.trigger("gated", 0, { workflowId: "free" });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("free")) === "completed",
    });
    expect(spent).toBeLessThan(1_000);
    expect(idle.stats.claimed).toBeGreaterThanOrEqual(1);
    while (gates.length > 0) gates.shift()!();
  });

  it("polls at the short cadence and reports it while the control socket can't connect", async () => {
    const dep = deploy();
    class RefusedWebSocket extends EventTarget {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSING = 2;
      static readonly CLOSED = 3;
      readyState = 3;
      constructor(_url: string) {
        super();
        setImmediate(() => this.dispatchEvent(new Event("close")));
      }
      send(): void {}
      close(): void {}
    }
    const errors: ZoryaWorkerErrorEvent[] = [];
    const w = dep.worker({
      workflows: asDefs(quick()),
      workerId: "no-socket",
      webSocket: RefusedWebSocket as unknown as typeof WebSocket,
      errors,
    });
    await w.ready();
    await driveUntil({ clock: dep.clock, done: () => w.stats.claimCalls > 0 });

    await dep.queued.trigger("quick", 1, { workflowId: "ns-1" });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("ns-1")) === "completed",
      stepMs: 50,
    });
    // The 1 s fallback cadence, not the 10 s configured for an open socket.
    expect(spent).toBeLessThanOrEqual(1_100);
    expect(w.stats.controlSocketOpen).toBe(false);
    expect(w.stats.controlSocketFailures).toBeGreaterThan(0);
    // Reported once for the outage, not once per reconnect attempt.
    expect(errors.filter((e) => e.phase === "control")).toHaveLength(1);
  });

  it("a stopped worker that starts again reopens its control socket and gets wake-ups", async () => {
    const dep = deploy();
    const w = dep.worker({ workflows: asDefs(quick()), workerId: "again" });
    await dep.ready([w]);
    let stopped = false;
    const stopping = w.stop().then(() => (stopped = true));
    await driveUntil({ clock: dep.clock, done: () => stopped, stepMs: 50 });
    await stopping;
    expect(dep.server.workerWs.connectedCount()).toBe(0);

    w.start();
    await w.ready();
    await driveUntil({
      clock: dep.clock,
      done: () => dep.server.workerWs.connectedCount() === 1 && w.stats.controlSocketOpen,
    });
    await settle(dep.clock);
    const before = w.stats.wakeups;
    await dep.queued.trigger("quick", 2, { workflowId: "again-1" });
    const spent = await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.status("again-1")) === "completed",
    });
    expect(spent).toBeLessThan(100);
    expect(w.stats.wakeups - before).toBeGreaterThanOrEqual(1);
  });

  it("a start whose run is gone is dropped, not driven or retried", async () => {
    const dep = deploy();
    const errors: ZoryaWorkerErrorEvent[] = [];
    const w = dep.worker({ workflows: asDefs(quick()), workerId: "w1", errors });
    await dep.ready([w]);
    // A start with no run behind it (the run was deleted while queued).
    await dep.workflowStarts.enqueue({ workflowId: "ghost", workflowName: "quick" });
    await dep.queued.trigger("quick", 1, { workflowId: "nudge" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.workflowStarts.list()).length === 0,
      stepMs: 100,
    });
    expect(await dep.storage.loadWorkflow("ghost")).toBeNull();
    expect(errors.some((e) => e.workflowId === "ghost" && e.phase === "run")).toBe(true);
  });
});
