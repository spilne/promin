// ---------------------------------------------------------------------------
// ZoryaScheduler end-to-end tests against ZoryaServer.
//
// Pins the contracts that matter for horizontal-scaling safety:
//   1. tickOnce dispatches due schedules through the configured trigger.
//   2. Single-leader semantics — when two ZoryaServers share the same
//      InMemorySchedulerStorage they alternate (or one wins), but the
//      same tick never fires twice.
//   3. Deterministic workflowId belt-and-suspenders.
//   4. Custom `fire` callback overrides the default trigger routing.
//   5. Multi-namespace scaling proof.
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { describe, it, expect } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  workflow,
  createWorkflowRunner,
} from "@promin/workflow";
import { InMemoryStepQueue, InMemoryWorkerRegistry } from "@promin/workflow/distributed";
import {
  InMemorySchedulerStorage,
  isStaleLeaseError,
  schedulePartition,
} from "@promin/workflow/scheduler";
import type { ScheduleTick, SchedulerErrorEvent } from "@promin/workflow/scheduler";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
import { SchedulerLoop } from "../../server/services/scheduler-loop.ts";
import { DistributedWorkflows, LocalWorkflows, ZoryaScheduler } from "../../index.ts";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function pollUntil(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  intervalMs = 25,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await sleep(intervalMs);
  }
  return false;
}

describe("ZoryaServer scheduling — embedded ZoryaScheduler", () => {
  it("tickOnce fires due schedules through the configured trigger", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const workerRegistry = new InMemoryWorkerRegistry();
    const schedStore = new InMemorySchedulerStorage();

    const workflows = new DistributedWorkflows({
      storage,
      stepQueue,
      workerRegistry,
      pollIntervalMs: 25,
    });
    const scheduler = new ZoryaScheduler({
      storage: schedStore,
      workflows,
      pollIntervalMs: 50,
      leaderLockTtlMs: 1_000,
    });

    const server = new ZoryaServer({
      workflows,
      scheduler,
      remoteWorkers: {},
    });
    expect(server.scheduler).toBeDefined();
    await workflows.start();

    const fetch = (req: Request) => server.handle(req);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const wf = workflow<{ id: number }>({ name: "scheduled-wf" })
      .step("a", ({ input }) => succeed(`run-${input.id}`))
      .build();

    const worker = new ZoryaWorker({
      client,
      workflows: [wf],
      mode: "step",
      stepPolling: { intervalMs: 25, heartbeatMs: 1_000 },
      heartbeatIntervalMs: 1_000,
    });
    await worker.start();

    await schedStore.upsertSchedule({
      id: "every-second",
      intervalMs: 1_000,
      enabled: true,
      startAt: new Date(Date.now() - 1_000),
      metadata: { workflowName: "scheduled-wf", input: { id: 42 } },
    });
    await schedStore.setNextRun("every-second", new Date(Date.now() - 100));

    const ticks = await scheduler.tickOnce();
    expect(ticks.length).toBeGreaterThanOrEqual(1);
    expect(ticks[0]!.scheduleId).toBe("every-second");
    expect(ticks[0]!.tickNumber).toBe(0);

    const wfId = `every-second.${ticks[0]!.tickNumber}`;
    const ok = await pollUntil(
      async () => (await storage.loadWorkflow(wfId))?.status === "completed",
      5_000,
    );
    expect(ok).toBe(true);

    const state = await storage.loadWorkflow(wfId);
    expect(state?.status).toBe("completed");

    await worker.stop();
    await server.stop();
  });

  it("two leaders contending on the same scheduler storage never double-fire", async () => {
    const schedStore = new InMemorySchedulerStorage();
    const dispatched = new Map<string, number>();
    const trackFire = async (tick: { scheduleId: string; tickNumber: number }) => {
      const key = `${tick.scheduleId}.${tick.tickNumber}`;
      dispatched.set(key, (dispatched.get(key) ?? 0) + 1);
    };

    const mkServer = (instanceId: string) => {
      const storage = new InMemoryWorkflowStorage();
      const workflows = new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
        sleepScanIntervalMs: 0,
      });
      const scheduler = new ZoryaScheduler({
        storage: schedStore,
        workflows,
        pollIntervalMs: 30,
        leaderLockTtlMs: 200,
        instanceId,
        fire: trackFire,
      });
      return { server: new ZoryaServer({ workflows, scheduler }), scheduler };
    };

    const a = mkServer("server-a");
    const b = mkServer("server-b");

    await schedStore.upsertSchedule({
      id: "one-shot",
      intervalMs: 60_000,
      enabled: true,
      startAt: new Date(Date.now() - 1_000),
      metadata: { workflowName: "noop", input: null },
    });
    await schedStore.setNextRun("one-shot", new Date(Date.now() - 100));

    const [ticksA, ticksB] = await Promise.all([a.scheduler.tickOnce(), b.scheduler.tickOnce()]);

    const total = ticksA.length + ticksB.length;
    expect(total).toBeGreaterThanOrEqual(1);

    for (const [, count] of dispatched) {
      expect(count).toBeLessThanOrEqual(1);
    }

    await a.server.stop();
    await b.server.stop();
  });

  it("custom fire callback overrides default trigger routing", async () => {
    const schedStore = new InMemorySchedulerStorage();
    const fired: Array<{ id: string; tick: number }> = [];
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const scheduler = new ZoryaScheduler({
      storage: schedStore,
      workflows,
      pollIntervalMs: 30,
      leaderLockTtlMs: 1_000,
      fire: async (tick) => {
        fired.push({ id: tick.scheduleId, tick: tick.tickNumber });
      },
    });
    const server = new ZoryaServer({ workflows, scheduler });

    await schedStore.upsertSchedule({
      id: "custom",
      intervalMs: 100,
      enabled: true,
      startAt: new Date(Date.now() - 200),
      metadata: {},
    });
    await schedStore.setNextRun("custom", new Date(Date.now() - 50));

    await scheduler.tickOnce();
    expect(fired.length).toBeGreaterThanOrEqual(1);
    expect(fired[0]!.id).toBe("custom");
    await server.stop();
  });

  it("namespaces: 'all' fires schedules across every namespace in one tick", async () => {
    const schedStore = new InMemorySchedulerStorage();
    const fired: Array<{ id: string; ns: string | undefined }> = [];
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const scheduler = new ZoryaScheduler({
      storage: schedStore,
      workflows,
      pollIntervalMs: 50,
      leaderLockTtlMs: 1_000,
      namespaces: "all",
      fire: async (tick, sched) => {
        fired.push({ id: tick.scheduleId, ns: sched.namespace });
      },
    });
    const server = new ZoryaServer({ workflows, scheduler });

    const seed = async (id: string, namespace: string | undefined) => {
      await schedStore.upsertSchedule({
        id,
        namespace,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() - 1_000),
        metadata: {},
      });
      await schedStore.setNextRun(id, new Date(Date.now() - 100));
    };
    await seed("tenant-a-job", "tenant-a");
    await seed("tenant-b-job", "tenant-b");
    await seed("tenant-c-job", "tenant-c");
    await seed("global-job", undefined);

    await scheduler.tickOnce();

    const namespacesFired = new Set(fired.map((f) => f.ns));
    expect(namespacesFired.has("tenant-a")).toBe(true);
    expect(namespacesFired.has("tenant-b")).toBe(true);
    expect(namespacesFired.has("tenant-c")).toBe(true);
    expect(namespacesFired.has(undefined)).toBe(true);

    await server.stop();
  });

  it("idle namespaces cost zero leader-lock RPCs (1000-tenant scaling proof)", async () => {
    const inner = new InMemorySchedulerStorage();
    const calls = { tryAcquireLeader: 0, findDue: 0, findDueAcross: 0, commitPoll: 0 };
    const counting = new Proxy(inner, {
      get(target, prop, receiver) {
        const value = Reflect.get(target, prop, receiver);
        if (typeof value !== "function") return value;
        const name = prop as string;
        return (...args: unknown[]) => {
          if (name in calls) (calls as Record<string, number>)[name]++;
          return (value as (...args: unknown[]) => unknown).apply(target, args);
        };
      },
    }) as unknown as typeof inner;

    const fired: string[] = [];
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const scheduler = new ZoryaScheduler({
      storage: counting,
      workflows,
      pollIntervalMs: 50,
      leaderLockTtlMs: 1_000,
      namespaces: "all",
      fire: async (tick) => {
        fired.push(tick.scheduleId);
      },
    });
    const server = new ZoryaServer({ workflows, scheduler });

    for (let i = 0; i < 1000; i++) {
      await inner.upsertSchedule({
        id: `tenant-${i}-job`,
        namespace: `tenant-${i}`,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() + 60_000),
        metadata: {},
      });
    }

    const seedActive = async (id: string, ns: string) => {
      await inner.upsertSchedule({
        id,
        namespace: ns,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() - 1_000),
        metadata: {},
      });
      await inner.setNextRun(id, new Date(Date.now() - 100));
    };
    await seedActive("hot-1", "hot-tenant-a");
    await seedActive("hot-2", "hot-tenant-b");
    await seedActive("hot-3", "hot-tenant-c");

    calls.tryAcquireLeader = 0;
    calls.findDue = 0;
    calls.findDueAcross = 0;
    calls.commitPoll = 0;

    await scheduler.tickOnce();

    expect(calls.findDueAcross).toBe(1);
    expect(calls.findDue).toBe(0);
    expect(calls.tryAcquireLeader).toBe(3);
    expect(calls.commitPoll).toBe(3);
    expect(fired.length).toBe(3);

    await server.stop();
  });

  it("dispatchConcurrency caps per-tick fan-out", async () => {
    const schedStore = new InMemorySchedulerStorage();
    let inFlight = 0;
    let peakInFlight = 0;
    const release: Array<() => void> = [];

    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const scheduler = new ZoryaScheduler({
      storage: schedStore,
      workflows,
      pollIntervalMs: 50,
      leaderLockTtlMs: 1_000,
      dispatchConcurrency: 3,
      fire: async () => {
        inFlight++;
        peakInFlight = Math.max(peakInFlight, inFlight);
        await new Promise<void>((r) => release.push(r));
        inFlight--;
      },
    });
    const server = new ZoryaServer({ workflows, scheduler });

    for (let i = 0; i < 12; i++) {
      await schedStore.upsertSchedule({
        id: `job-${i}`,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() - 1_000),
        metadata: {},
      });
      await schedStore.setNextRun(`job-${i}`, new Date(Date.now() - 100));
    }

    const tickPromise = scheduler.tickOnce();
    await new Promise<void>((r) => setTimeout(r, 25));
    while (release.length > 0 || inFlight > 0) {
      release.shift()?.();
      await new Promise<void>((r) => setTimeout(r, 1));
    }
    await tickPromise;

    expect(peakInFlight).toBeLessThanOrEqual(3);
    expect(peakInFlight).toBeGreaterThanOrEqual(2);

    await server.stop();
  });

  it("rejects passing both `namespace` and `namespaces` simultaneously", () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    expect(
      () =>
        new ZoryaScheduler({
          storage: new InMemorySchedulerStorage(),
          workflows,
          namespace: "tenant-a",
          namespaces: "all",
        }),
    ).toThrow(/namespace|namespaces/);
  });
});

// ---------------------------------------------------------------------------
// SchedulerLoop on an injected WallClock — due ticks, nextRun, fireOnce
// stamps and the poll cadence all follow the clock.
// ---------------------------------------------------------------------------

describe("SchedulerLoop — injected WallClock", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  /** Loop + clock-aware storage, recording dispatched ticks and poll attempts. */
  function setup(params: { pollIntervalMs?: number } = {}) {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    let polls = 0;
    const acquire = storage.tryAcquireLeader.bind(storage);
    storage.tryAcquireLeader = (p) => {
      polls++;
      return acquire(p);
    };
    const fired: ScheduleTick[] = [];
    const loop = new SchedulerLoop({
      storage,
      clock,
      pollIntervalMs: params.pollIntervalMs ?? 1_000,
      fire: async (tick) => {
        fired.push(tick);
      },
    });
    return { clock, storage, loop, fired, polls: () => polls };
  }

  /**
   * Wait (in real time) until the loop is parked on its fake-clock poll
   * timer with `polls` polls done. Advancing earlier would fire nothing.
   */
  async function untilWaiting(params: {
    clock: FakeWallClock;
    polls: () => number;
    count: number;
  }): Promise<void> {
    const deadline = Date.now() + 4_000;
    while (params.clock.pendingCount() === 0 || params.polls() < params.count) {
      if (Date.now() > deadline) throw new Error("loop never waited on the fake clock");
      await sleep(1);
    }
  }

  it("tickOnce: due ticks, firedAt and nextRun come from the clock", async () => {
    const { clock, storage, loop } = setup();
    await storage.upsertSchedule({ id: "iv", intervalMs: 1_000 });

    const first = await loop.tickOnce();
    expect(first.map((t) => [t.scheduledAt.getTime(), t.firedAt.getTime(), t.tickNumber])).toEqual([
      [T0, T0, 0],
    ]);

    // nextRun = T0 + 1000 on the clock: not due a millisecond earlier.
    clock.advance(999);
    expect(await loop.tickOnce()).toEqual([]);

    clock.advance(1);
    const second = await loop.tickOnce();
    expect(second.map((t) => [t.scheduledAt.getTime(), t.firedAt.getTime(), t.tickNumber])).toEqual(
      [[T0 + 1_000, T0 + 1_000, 1]],
    );
  });

  it("tickOnce: multi-namespace mode reads now from the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const loop = new SchedulerLoop({ storage, clock, namespaces: "all", fire: async () => {} });
    await storage.upsertSchedule({ id: "a", intervalMs: 5_000, namespace: "tenant-a" });

    expect((await loop.tickOnce()).map((t) => t.firedAt.getTime())).toEqual([T0]);
    clock.advance(4_999);
    expect(await loop.tickOnce()).toEqual([]);
    clock.advance(1);
    expect((await loop.tickOnce()).map((t) => t.scheduledAt.getTime())).toEqual([T0 + 5_000]);
  });

  it("fireOnce stamps the tick and slides nextRun on the clock", async () => {
    const { clock, storage, loop, fired } = setup();
    await storage.upsertSchedule({ id: "iv", intervalMs: 1_000 });
    await loop.tickOnce(); // first fire at T0

    clock.advance(400);
    const manual = await loop.fireOnce("iv");
    expect(manual!.firedAt.getTime()).toBe(T0 + 400);
    expect(manual!.scheduledAt.getTime()).toBe(T0 + 400);
    expect(fired.map((t) => t.tickNumber)).toEqual([0, 1]);

    // Cadence resumes from the manual fire: T0 + 400 + 1000.
    clock.advance(999);
    expect(await loop.tickOnce()).toEqual([]);
    clock.advance(1);
    expect((await loop.tickOnce()).map((t) => t.scheduledAt.getTime())).toEqual([T0 + 1_400]);
  });

  it("start(): polls once per pollIntervalMs on the clock and stop() clears the timer", async () => {
    const { clock, storage, loop, fired, polls } = setup({ pollIntervalMs: 500 });
    await storage.upsertSchedule({ id: "iv", intervalMs: 1_000 });

    loop.start();
    await untilWaiting({ clock, polls, count: 1 });
    expect(fired.map((t) => t.scheduledAt.getTime())).toEqual([T0]);

    clock.advance(499);
    await sleep(10);
    expect(polls()).toBe(1);

    clock.advance(1); // T0 + 500: poll, nothing due yet
    await untilWaiting({ clock, polls, count: 2 });
    expect(fired).toHaveLength(1);

    clock.advance(500); // T0 + 1000: poll, interval due
    await untilWaiting({ clock, polls, count: 3 });
    expect(fired.map((t) => t.scheduledAt.getTime())).toEqual([T0, T0 + 1_000]);

    await loop.stop();
    expect(clock.pendingCount()).toBe(0);
    expect(polls()).toBe(3);
  });

  it("stop() returns without waiting out the poll interval", async () => {
    const { clock, loop, polls } = setup({ pollIntervalMs: 60_000 });
    loop.start();
    await untilWaiting({ clock, polls, count: 1 });

    // The fake clock never advances: stop() must cut the wait itself.
    await loop.stop();
    expect(clock.pendingCount()).toBe(0);
    expect(polls()).toBe(1);
  });
});

describe("SchedulerLoop — delivery and isolation", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  function setup() {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const fired: ScheduleTick[] = [];
    const errors: SchedulerErrorEvent[] = [];
    const loop = new SchedulerLoop({
      storage,
      clock,
      fire: async (tick) => {
        fired.push(tick);
      },
      onError: (e) => void errors.push(e),
    });
    return { clock, storage, loop, fired, errors };
  }

  it("dispatches before committing: a failed commit re-fires the same tick next poll", async () => {
    const { storage, loop, fired, errors } = setup();
    await storage.upsertSchedule({ id: "a", intervalMs: 60_000 });
    const commit = storage.commitPoll.bind(storage);
    let failNext = true;
    storage.commitPoll = async (updates) => {
      if (failNext) {
        failNext = false;
        throw new Error("commit blip");
      }
      return commit(updates);
    };

    await loop.tickOnce();
    await loop.tickOnce();
    await loop.tickOnce();

    expect(fired.map((t) => [t.scheduleId, t.tickNumber])).toEqual([
      ["a", 0],
      ["a", 0],
    ]);
    expect(errors.map((e) => e.phase)).toEqual(["commit"]);
    expect((await storage.loadScheduleState("a"))?.tickCount).toBe(1);
  });

  it("an invalid stored schedule is reported and disabled; the rest of the poll fires", async () => {
    const { storage, loop, fired, errors } = setup();
    await storage.upsertSchedule({ id: "bad", cron: "not a cron" });
    await storage.recordFire("bad", new Date(T0 - 60_000));
    await storage.upsertSchedule({ id: "good", intervalMs: 1_000 });

    await loop.tickOnce();

    expect(fired.map((t) => t.scheduleId)).toEqual(["good"]);
    expect(errors.map((e) => [e.phase, e.scheduleId])).toEqual([["schedule", "bad"]]);
    expect((await storage.loadSchedule("bad"))?.enabled).toBe(false);
  });

  it("a storage error in a poll is reported and the loop keeps polling", async () => {
    const { clock, storage, loop, fired, errors } = setup();
    await storage.upsertSchedule({ id: "a", intervalMs: 60_000 });
    const findDue = storage.findDue.bind(storage);
    let failNext = true;
    storage.findDue = async (params) => {
      if (failNext) {
        failNext = false;
        throw new Error("db blip");
      }
      return findDue(params);
    };

    loop.start();
    expect(await pollUntil(() => errors.length === 1 && clock.pendingCount() === 1, 2_000)).toBe(
      true,
    );
    clock.advance(1_000);
    expect(await pollUntil(() => fired.length === 1, 2_000)).toBe(true);
    await loop.stop();

    expect(errors.map((e) => e.phase)).toEqual(["poll"]);
    expect(fired.map((t) => t.scheduleId)).toEqual(["a"]);
  });
});

describe("SchedulerLoop — leader leases", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  function loopOn(params: {
    storage: InMemorySchedulerStorage;
    clock: FakeWallClock;
    instanceId: string;
    partition?: { index: number; count: number };
    onError?: (e: SchedulerErrorEvent) => void;
  }) {
    const fired: ScheduleTick[] = [];
    const loop = new SchedulerLoop({
      storage: params.storage,
      clock: params.clock,
      instanceId: params.instanceId,
      partition: params.partition,
      leaderLockTtlMs: 60_000,
      fire: async (tick) => {
        fired.push(tick);
      },
      onError: params.onError,
    });
    return { loop, fired };
  }

  it("stop() releases the lease so another instance leads at once", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const a = loopOn({ storage, clock, instanceId: "A" });
    const b = loopOn({ storage, clock, instanceId: "B" });
    await storage.upsertSchedule({ id: "x", intervalMs: 60_000 });

    await a.loop.tickOnce();
    await storage.upsertSchedule({ id: "y", intervalMs: 60_000 });
    expect(await b.loop.tickOnce()).toEqual([]);

    await a.loop.stop();
    expect((await b.loop.tickOnce()).map((t) => t.scheduleId)).toEqual(["y"]);
  });

  it("partitioned loops each lead their own partition", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const loops = [0, 1].map((index) =>
      loopOn({ storage, clock, instanceId: `p${index}`, partition: { index, count: 2 } }),
    );
    const ids = Array.from({ length: 10 }, (_, i) => `job-${i}`);
    for (const id of ids) await storage.upsertSchedule({ id, intervalMs: 60_000 });

    const fired = await Promise.all(loops.map((l) => l.loop.tickOnce()));

    expect(
      fired
        .flat()
        .map((t) => t.scheduleId)
        .sort(),
    ).toEqual([...ids].sort());
    for (const [index, ticks] of fired.entries()) {
      expect(ticks.every((t) => schedulePartition({ id: t.scheduleId, count: 2 }) === index)).toBe(
        true,
      );
    }
  });

  it("a loop whose lease was taken over can't commit the poll it dispatched", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const errors: SchedulerErrorEvent[] = [];
    await storage.upsertSchedule({ id: "x", intervalMs: 60_000 });
    let b: ReturnType<typeof loopOn> | undefined;
    const fired: ScheduleTick[] = [];
    // A's dispatch takes longer than its lease; B takes over meanwhile.
    const a = new SchedulerLoop({
      storage,
      clock,
      instanceId: "A",
      leaderLockTtlMs: 1_000,
      fire: async (tick) => {
        fired.push(tick);
        clock.advance(2_000);
        await b!.loop.tickOnce();
      },
      onError: (e) => void errors.push(e),
    });
    b = loopOn({ storage, clock, instanceId: "B" });

    await a.tickOnce();

    expect(errors.map((e) => [e.phase, isStaleLeaseError(e.error)])).toEqual([["commit", true]]);
    expect(fired.map((t) => t.tickNumber)).toEqual([0]);
    expect(b.fired.map((t) => t.tickNumber)).toEqual([0]);
    expect((await storage.loadScheduleState("x"))?.tickCount).toBe(1);
  });

  it("concurrent fireOnce calls take distinct tick numbers", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const { loop } = loopOn({ storage, clock, instanceId: "A" });
    await storage.upsertSchedule({ id: "m", intervalMs: 60_000 });

    const ticks = await Promise.all(Array.from({ length: 4 }, () => loop.fireOnce("m")));

    expect(ticks.map((t) => t!.tickNumber).sort()).toEqual([0, 1, 2, 3]);
    expect((await storage.loadScheduleState("m"))?.tickCount).toBe(4);
  });
});
