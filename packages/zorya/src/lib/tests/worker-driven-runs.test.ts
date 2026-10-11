// ---------------------------------------------------------------------------
// Worker-driven runs end to end, in process: ZoryaServer + QueuedWorkflows
// + ZoryaWorkers talking over the server's handler (no sockets). One
// FakeWallClock drives the server's stores, Zorya's scanners and recovery,
// the workers' loops and their runners, so every wait is clock-driven.
//
// Covered: concurrency is enforced; version routing during a rolling
// deploy; a start whose run is locked elsewhere is released, not acked;
// a lost claim aborts the run; stop({ timeoutMs }) hands runs to another
// worker; sleeps, signals and child workflows resume through Zorya; a
// worker started before the server, and across a server restart,
// recovers and re-advertises; a dead worker's run is recovered by another.
// ---------------------------------------------------------------------------

import { afterEach, describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { FakeWallClock, InMemoryWorkflowStorage, workflow, type Workflow } from "@promin/workflow";
import { InMemoryWorkerRegistry } from "@promin/workflow/distributed";
import { ZoryaClient, ZoryaWorker, type ZoryaWorkerErrorEvent } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
import type { Logger } from "../../server/server-config.ts";
import { QueuedWorkflows } from "../../server/services/workflows/index.ts";
import { InMemoryWorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";

const quietLogger: Logger = { log() {}, warn() {}, error() {} };
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

/**
 * Yield until the code under test has parked on clock timers: the clock's
 * pending timer count is non-zero and unchanged over several event-loop
 * turns. Bounded by an iteration count, never by wall time.
 */
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

/** Settle work, then advance `stepMs`, until `done()` holds (bounded by `maxSteps`). */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxSteps?: number;
}): Promise<void> {
  const { clock, done, stepMs = 50, maxSteps = 2_000 } = params;
  for (let i = 0; i < maxSteps; i++) {
    await settle(clock);
    if (await done()) return;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

/** Let `ms` of clock time pass in `stepMs` steps, settling work after each. */
async function elapse(params: {
  clock: FakeWallClock;
  ms: number;
  stepMs?: number;
}): Promise<void> {
  const stepMs = params.stepMs ?? 50;
  for (let t = 0; t < params.ms; t += stepMs) {
    await settle(params.clock);
    params.clock.advance(stepMs);
  }
  await settle(params.clock);
}

/** A connection-refused error, as Bun's fetch throws it. */
function refused(): Error {
  return Object.assign(new Error("Unable to connect. Is the computer able to access the url?"), {
    code: "ConnectionRefused",
  });
}

interface Deployment {
  clock: FakeWallClock;
  storage: InMemoryWorkflowStorage;
  queued: QueuedWorkflows;
  server: ZoryaServer;
  workflowStarts: InMemoryWorkflowStartQueue;
  /** Simulate the server being unreachable. */
  down: { value: boolean };
  /** Simulate `kill -9` of a worker: it is cut off from the server and never stops cleanly. */
  crash: (worker: ZoryaWorker) => void;
  /** Replace the server (a restart with the same storage). */
  restart: () => Promise<void>;
  worker: (params: {
    workflows: Workflow<unknown, unknown>[];
    workerId: string;
    concurrency?: number;
    errors?: ZoryaWorkerErrorEvent[];
  }) => ZoryaWorker;
}

function deploy(params: { reclaimAfterMs?: number } = {}): Deployment {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const workflowStarts = new InMemoryWorkflowStartQueue({
    clock,
    reclaimAfterMs: params.reclaimAfterMs ?? 10_000,
  });
  const down = { value: false };
  const dead = new Set<string>();
  const build = () => {
    const queued = new QueuedWorkflows({
      storage,
      workflowStarts,
      advertisements: new InMemoryWorkflowAdvertisementRegistry({ clock }),
      workerRegistry: new InMemoryWorkerRegistry({ clock }),
      clock,
      wakeups: { sleepScanIntervalMs: 100, signalScanIntervalMs: 100 },
      maintenance: { intervalMs: 500, orphanGraceMs: 1_000 },
    });
    const server = new ZoryaServer({
      insecure: true,
      workflows: queued,
      remoteWorkers: {},
      clock,
      logger: quietLogger,
    });
    return { queued, server };
  };
  const d = build() as { queued: QueuedWorkflows; server: ZoryaServer };
  void d.queued.start();
  const dep: Deployment = {
    clock,
    storage,
    workflowStarts,
    down,
    get queued() {
      return d.queued;
    },
    get server() {
      return d.server;
    },
    crash: (worker) => {
      dead.add(worker.workerId);
    },
    restart: async () => {
      await d.queued.stop();
      const next = build();
      d.queued = next.queued;
      d.server = next.server;
      void d.queued.start();
    },
    worker: ({ workflows, workerId, concurrency, errors }) => {
      const client = new ZoryaClient({
        url: "http://zorya.test",
        clock,
        // A dead worker's requests never arrive; its retries give up fast.
        retry: { maxElapsedMs: 2_000 },
        fetch: async (req) => {
          if (down.value || dead.has(workerId)) throw refused();
          return d.server.handle(req);
        },
      });
      const worker = new ZoryaWorker({
        client,
        workflows,
        workerId,
        clock,
        concurrency: concurrency ?? 10,
        heartbeatIntervalMs: 1_000,
        // No sockets here: the workers find work by polling alone.
        controlSocket: false,
        pollWorkflowStarts: { intervalMs: 100, jitter: 0, claimHeartbeatMs: 2_000 },
        lockedRetryDelayMs: 500,
        lock: { durationMs: 6_000, heartbeatIntervalMs: 2_000 },
        onError: (e) => errors?.push(e),
      });
      worker.start();
      cleanups.push(async () => {
        if (dead.has(workerId)) return;
        let stopped = false;
        const stopping = worker.stop({ timeoutMs: 0 }).then(() => (stopped = true));
        await driveUntil({ clock, done: () => stopped });
        await stopping;
      });
      return worker;
    },
  };
  cleanups.push(() => d.queued.stop());
  return dep;
}

const asDefs = (...wfs: Workflow<unknown, unknown>[]) => wfs;

describe("worker-driven runs", () => {
  it("never drives more than `concurrency` runs at once, and completes them all", async () => {
    const dep = deploy();
    let active = 0;
    let peak = 0;
    const gates: Array<() => void> = [];
    const slow = workflow<number>({ name: "slow" })
      .step("work", async () => {
        active++;
        peak = Math.max(peak, active);
        await new Promise<void>((r) => gates.push(r));
        active--;
        return "ok";
      })
      .build();
    const worker = dep.worker({ workflows: asDefs(slow), workerId: "w1", concurrency: 2 });
    await driveUntil({ clock: dep.clock, done: () => worker.activeCount === 0 && true });
    await worker.ready();
    for (let i = 0; i < 12; i++) await dep.queued.trigger("slow", i, { workflowId: `s-${i}` });

    await driveUntil({
      clock: dep.clock,
      done: async () => {
        while (gates.length > 0) gates.shift()!();
        const statuses = await Promise.all(
          Array.from({ length: 12 }, (_, i) => dep.storage.loadWorkflowStatus(`s-${i}`)),
        );
        return statuses.every((s) => s?.status === "completed");
      },
    });
    expect(peak).toBe(2);
    expect(await dep.workflowStarts.list()).toHaveLength(0);
  });

  it("routes a run to a worker hosting its version during a rolling deploy", async () => {
    const dep = deploy();
    const greet = (version: string) =>
      workflow<string>({ name: "greet", version })
        .step("say", () => succeed(`v${version}-code`))
        .build();
    const v1 = dep.worker({ workflows: asDefs(greet("1")), workerId: "old" });
    const v2 = dep.worker({ workflows: asDefs(greet("2")), workerId: "new" });
    await driveUntil({ clock: dep.clock, done: async () => true });
    await Promise.all([v1.ready(), v2.ready()]);

    // Default: the newest advertised version. Pinned: exactly that one.
    await dep.queued.trigger("greet", "x", { workflowId: "g-default" });
    await dep.queued.trigger("greet", "x", { workflowId: "g-v1", version: "1" });
    await driveUntil({
      clock: dep.clock,
      done: async () =>
        (await dep.storage.loadWorkflowStatus("g-default"))?.status === "completed" &&
        (await dep.storage.loadWorkflowStatus("g-v1"))?.status === "completed",
    });
    expect((await dep.storage.loadWorkflow("g-default"))?.result).toBe("v2-code");
    expect((await dep.storage.loadWorkflow("g-v1"))?.result).toBe("v1-code");
  });

  it("releases (never acks) a start whose run is locked elsewhere, and drives it once free", async () => {
    const dep = deploy();
    const quick = workflow<string>({ name: "quick" })
      .step("only", () => succeed("done"))
      .build();
    const worker = dep.worker({ workflows: asDefs(quick), workerId: "w1" });
    await driveUntil({ clock: dep.clock, done: async () => true, maxSteps: 10 });
    await worker.ready();
    await dep.queued.trigger("quick", "x", { workflowId: "q-1" });
    // A dead driver holds the run's lock for 3 more seconds.
    await dep.storage.tryLock({ workflowId: "q-1", lockDurationMs: 3_000 });
    await elapse({ clock: dep.clock, ms: 1_000 });
    // Still queued while the lock is held.
    expect((await dep.storage.loadWorkflowStatus("q-1"))?.status).toBe("pending");
    expect((await dep.workflowStarts.list()).map((s) => s.workflowId)).toEqual(["q-1"]);

    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("q-1"))?.status === "completed",
    });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.workflowStarts.list()).length === 0,
    });
  });

  it("resumes sleeps, signals and child workflows through Zorya's wake-ups", async () => {
    const dep = deploy();
    const child = workflow<{ n: number }>({ name: "child" })
      .sleep("child-nap", 2_000)
      .step("double", ({ input }) => succeed(input.n * 2))
      .build();
    const parent = workflow<{ n: number }>({ name: "parent" })
      .sleep("nap", 1_000)
      .waitForSignal<{ add: number }, "approval">("approval", { signalName: "approve" })
      .subworkflow("kid", child, {
        input: (prev) => ({ n: prev.add }),
        workflowId: () => "kid-1",
      })
      .step("finish", ({ prev }) => succeed(prev))
      .build();
    const worker = dep.worker({ workflows: asDefs(parent), workerId: "w1" });
    await driveUntil({ clock: dep.clock, done: async () => true, maxSteps: 5 });
    await worker.ready();
    await dep.queued.trigger("parent", { n: 1 }, { workflowId: "p-1" });

    await driveUntil({
      clock: dep.clock,
      done: async () => {
        const state = await dep.storage.loadWorkflow("p-1");
        return state?.steps.approval?.status === "waiting_for_signal";
      },
    });
    await dep.storage.deliverSignal({
      workflowId: "p-1",
      signalName: "approve",
      payload: { add: 21 },
    });
    await driveUntil({
      clock: dep.clock,
      done: async () => {
        return (await dep.storage.loadWorkflowStatus("p-1"))?.status === "completed";
      },
    });
    expect((await dep.storage.loadWorkflow("p-1"))?.result).toBe(42);
    expect((await dep.storage.loadWorkflowStatus("kid-1"))?.status).toBe("completed");
  });

  it("a run whose worker died is recovered and completed by another worker", async () => {
    const dep = deploy({ reclaimAfterMs: 6_000 });
    const ran: string[] = [];
    let hang = true;
    const wf = workflow<string>({ name: "two-steps" })
      .step("first", () => {
        ran.push("first");
        return succeed(1);
      })
      .step("second", async () => {
        ran.push("second");
        // The first worker "dies" here: its promise never settles.
        if (hang) await new Promise(() => {});
        return 2;
      })
      .build();
    const dying = dep.worker({ workflows: asDefs(wf), workerId: "dying" });
    await driveUntil({ clock: dep.clock, done: async () => true, maxSteps: 5 });
    await dying.ready();
    await dep.queued.trigger("two-steps", "x", { workflowId: "t-1" });
    await driveUntil({ clock: dep.clock, done: () => ran.includes("second") });

    // Crash: the dying worker is cut off and never stops cleanly.
    hang = false;
    dep.crash(dying);
    const survivor = dep.worker({ workflows: asDefs(wf), workerId: "survivor" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("t-1"))?.status === "completed",
    });
    expect((await dep.storage.loadWorkflow("t-1"))?.result).toBe(2);
    // "first" ran once: its result was checkpointed before the crash.
    expect(ran.filter((s) => s === "first")).toHaveLength(1);
    expect(survivor.activeCount).toBe(0);
  });

  it("stop({ timeoutMs }) abandons runs in flight and releases them to another worker", async () => {
    const dep = deploy();
    let hold = true;
    const wf = workflow<string>({ name: "long" })
      .step("a", async () => {
        while (hold) await new Promise<void>((r) => setImmediate(r));
        return 1;
      })
      .step("b", () => succeed(2))
      .build();
    const first = dep.worker({ workflows: asDefs(wf), workerId: "first" });
    await driveUntil({ clock: dep.clock, done: async () => true, maxSteps: 5 });
    await first.ready();
    await dep.queued.trigger("long", "x", { workflowId: "l-1" });
    await driveUntil({ clock: dep.clock, done: () => first.activeCount === 1 });

    let stopped = false;
    const stopping = first.stop({ timeoutMs: 1_000 }).then(() => (stopped = true));
    await elapse({ clock: dep.clock, ms: 1_500 });
    hold = false;
    await driveUntil({ clock: dep.clock, done: () => stopped });
    await stopping;
    // The start went back to the queue at once; another worker finishes it.
    const second = dep.worker({ workflows: asDefs(wf), workerId: "second" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("l-1"))?.status === "completed",
    });
    expect(second.activeCount).toBe(0);
  });

  it("a worker started before the server comes up, and across a restart, (re)advertises on its own", async () => {
    const dep = deploy();
    const quick = workflow<string>({ name: "quick" })
      .step("only", () => succeed("done"))
      .build();
    dep.down.value = true;
    const errors: ZoryaWorkerErrorEvent[] = [];
    const worker = dep.worker({ workflows: asDefs(quick), workerId: "early", errors });
    let ready = false;
    void worker.ready().then(() => (ready = true));
    await elapse({ clock: dep.clock, ms: 5_000 });
    expect(ready).toBe(false);
    expect(errors.some((e) => e.phase === "advertise")).toBe(true);

    dep.down.value = false;
    await driveUntil({ clock: dep.clock, done: () => ready });

    // Restart with fresh in-memory advertisements: the worker re-advertises
    // within its heartbeat interval, and triggers work again.
    await dep.restart();
    expect(await dep.queued.advertisements.list()).toHaveLength(0);
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.queued.advertisements.list()).length === 1,
    });
    await dep.queued.trigger("quick", "x", { workflowId: "after-restart" });
    await driveUntil({
      clock: dep.clock,
      done: async () =>
        (await dep.storage.loadWorkflowStatus("after-restart"))?.status === "completed",
    });
  });

  it("resume re-drives a worker-driven run from a step on a hosting worker", async () => {
    const dep = deploy();
    const ran: string[] = [];
    const abc = workflow<string>({ name: "abc", version: "4" })
      .step("a", () => succeed(ran.push("a")))
      .step("b", () => succeed(ran.push("b")))
      .step("c", () => succeed(ran.push("c")))
      .build();
    const worker = dep.worker({ workflows: asDefs(abc), workerId: "w1" });
    await elapse({ clock: dep.clock, ms: 100 });
    await worker.ready();
    await dep.queued.trigger("abc", "x", { workflowId: "abc-1" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("abc-1"))?.status === "completed",
    });
    await dep.queued.resume({ workflowId: "abc-1", fromStep: "b" });
    await driveUntil({
      clock: dep.clock,
      done: async () =>
        ran.length === 5 && (await dep.storage.loadWorkflowStatus("abc-1"))?.status === "completed",
    });
    expect(ran).toEqual(["a", "b", "c", "b", "c"]);
    const run = await dep.storage.loadWorkflow("abc-1");
    expect(run?.run).toBe(1);
    expect(run?.version).toBe("4");
  });

  it("a run parked on a sleep due soon keeps its start until the wake time (no wake-up churn)", async () => {
    const dep = deploy();
    const napper = workflow<string>({ name: "napper" })
      .sleep("nap", 5_000)
      .step("after", () => succeed("awake"))
      .build();
    const worker = dep.worker({ workflows: asDefs(napper), workerId: "w1" });
    await elapse({ clock: dep.clock, ms: 100 });
    await worker.ready();
    await dep.queued.trigger("napper", "x", { workflowId: "nap-1" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("nap-1"))?.status === "suspended",
    });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.workflowStarts.list())[0]?.claimToken === undefined,
    });
    // Released to pending, claimable at the wake time — not completed and
    // enqueued again by every scan once Zorya's clock finds the sleep due.
    const [held] = await dep.workflowStarts.list();
    const wakeAt = (await dep.storage.loadWorkflow("nap-1"))?.steps.nap?.wakeAt?.getTime();
    expect(held?.workflowId).toBe("nap-1");
    expect(held?.availableAt).toBeGreaterThanOrEqual(wakeAt! - 50);
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("nap-1"))?.status === "completed",
    });
    expect(await dep.workflowStarts.list()).toHaveLength(0);
  });

  it("a continued-as-new run resumed by another worker gets the continued input", async () => {
    const dep = deploy();
    const finals: Array<{ worker: string; count: number }> = [];
    const chained = (worker: string) =>
      workflow<{ count: number }>({ name: "chained" })
        .journaled("loop", function* (ctx) {
          const count = yield* ctx.activity("read", () => ctx.input.count);
          if (count < 1) ctx.continueAsNew({ count: count + 1 });
          return count;
        })
        .sleep("nap", 2_000)
        .step("final", ({ input }) => {
          finals.push({ worker, count: input.count });
          return succeed(input.count);
        })
        .build();
    const first = dep.worker({ workflows: asDefs(chained("first")), workerId: "first" });
    await elapse({ clock: dep.clock, ms: 100 });
    await first.ready();
    await dep.queued.trigger("chained", { count: 0 }, { workflowId: "can-1" });
    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("can-1"))?.status === "suspended",
    });
    expect((await dep.storage.loadWorkflow("can-1"))?.run).toBe(2);
    // The first worker goes away while the run sleeps; another resumes it.
    let stopped = false;
    const stopping = first.stop({ timeoutMs: 0 }).then(() => (stopped = true));
    await driveUntil({ clock: dep.clock, done: () => stopped });
    await stopping;
    dep.worker({ workflows: asDefs(chained("second")), workerId: "second" });

    await driveUntil({
      clock: dep.clock,
      done: async () => (await dep.storage.loadWorkflowStatus("can-1"))?.status === "completed",
    });
    expect(finals).toEqual([{ worker: "second", count: 1 }]);
    expect((await dep.storage.loadWorkflow("can-1"))?.result).toBe(1);
  });

  it("the Workers page lists the workers registered over /rpc/worker", async () => {
    const dep = deploy();
    const quick = workflow<string>({ name: "quick" })
      .step("only", () => succeed("done"))
      .build();
    const worker = dep.worker({ workflows: asDefs(quick), workerId: "listed" });
    await driveUntil({ clock: dep.clock, done: async () => true, maxSteps: 5 });
    await worker.ready();
    const res = await dep.server.handle(new Request("http://zorya.test/api/workers"));
    const body = (await res.json()) as { workers: Array<{ workerId: string; status: string }> };
    expect(body.workers.map((w) => [w.workerId, w.status])).toEqual([["listed", "online"]]);
  });
});
