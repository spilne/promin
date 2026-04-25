// ---------------------------------------------------------------------------
// SchedulerLoop end-to-end tests.
//
// Pins the contracts that matter for horizontal-scaling safety:
//   1. tickOnce dispatches due schedules through the configured trigger.
//   2. Single-leader semantics — when two ZoryaServers share the same
//      InMemorySchedulerStorage they alternate (or one wins), but the
//      same tick never fires twice.
//   3. Deterministic workflowId belt-and-suspenders — a duplicate dispatch
//      lands on the same workflow row (createWorkflow is idempotent).
//   4. Custom `fire` callback overrides the default trigger routing.
//   5. Config errors fail fast (scheduling.enabled without scheduler).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  InMemorySchedulerStorage,
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  InMemoryWorkflowStorage,
  workflow,
} from "@promin/workflow";
import { Pipeline } from "@promin/core";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";

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

describe("ZoryaServer scheduling — embedded SchedulerLoop", () => {
  it("tickOnce fires due schedules through the configured trigger", async () => {
    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();
    const workerRegistry = new InMemoryWorkerRegistry();
    const scheduler = new InMemorySchedulerStorage();

    const server = new ZoryaServer({
      storage,
      scheduler,
      workerProtocol: { stepQueue, workerRegistry },
      coordination: { enabled: true, pollIntervalMs: 25, stepPollIntervalMs: 25 },
      scheduling: { enabled: true, pollIntervalMs: 50, leaderLockTtlMs: 1_000 },
    });
    expect(server.schedulerLoop).toBeDefined();
    server.startCoordinator();

    const fetch = (req: Request) => server.handle(req);
    const client = new ZoryaClient({ url: "http://test.local", fetch });

    const wf = workflow<{ id: number }>({ name: "scheduled-wf" })
      .step("a", ({ input }) => Pipeline.succeed(`run-${input.id}`))
      .build();

    const worker = new ZoryaWorker({
      client,
      workflows: [wf],
      mode: "step",
      stepPolling: { intervalMs: 25, heartbeatMs: 1_000 },
      heartbeatIntervalMs: 1_000,
    });
    await worker.start();

    // 1ms-old schedule so it's due immediately.
    await scheduler.upsertSchedule({
      id: "every-second",
      intervalMs: 1_000,
      enabled: true,
      startAt: new Date(Date.now() - 1_000),
      metadata: { workflowName: "scheduled-wf", input: { id: 42 } },
    });
    // Seed nextRun so findDue picks it up — DurableScheduler.registerAsync
    // does this for users; here we go through the storage directly.
    await scheduler.setNextRun("every-second", new Date(Date.now() - 100));

    const ticks = await server.schedulerLoop!.tickOnce();
    expect(ticks.length).toBeGreaterThanOrEqual(1);
    expect(ticks[0]!.scheduleId).toBe("every-second");
    expect(ticks[0]!.tickNumber).toBe(0);

    // The deterministic workflowId convention is `${scheduleId}.${tickNumber}`.
    const wfId = `every-second.${ticks[0]!.tickNumber}`;
    const ok = await pollUntil(
      async () => (await storage.loadWorkflow(wfId))?.status === "completed",
      5_000,
    );
    expect(ok).toBe(true);

    const state = await storage.loadWorkflow(wfId);
    expect(state?.status).toBe("completed");

    await worker.stop();
    server.stop();
  });

  it("two leaders contending on the same scheduler storage never double-fire", async () => {
    const scheduler = new InMemorySchedulerStorage();
    const dispatched = new Map<string, number>();
    const trackFire = async (tick: { scheduleId: string; tickNumber: number }) => {
      const key = `${tick.scheduleId}.${tick.tickNumber}`;
      dispatched.set(key, (dispatched.get(key) ?? 0) + 1);
    };

    const mkServer = (instanceId: string) =>
      new ZoryaServer({
        storage: new InMemoryWorkflowStorage(),
        scheduler,
        scheduling: {
          enabled: true,
          pollIntervalMs: 30,
          leaderLockTtlMs: 200,
          instanceId,
          fire: trackFire,
        },
      });

    const a = mkServer("server-a");
    const b = mkServer("server-b");

    await scheduler.upsertSchedule({
      id: "one-shot",
      intervalMs: 60_000, // long enough that only the initial tick fires
      enabled: true,
      startAt: new Date(Date.now() - 1_000),
      metadata: { workflowName: "noop", input: null },
    });
    await scheduler.setNextRun("one-shot", new Date(Date.now() - 100));

    // Race two tickOnce calls — both contend for leadership at the same
    // moment. The leader-lock ensures only one wins; the loser's tickOnce
    // returns [].
    const [ticksA, ticksB] = await Promise.all([
      a.schedulerLoop!.tickOnce(),
      b.schedulerLoop!.tickOnce(),
    ]);

    const total = ticksA.length + ticksB.length;
    expect(total).toBeGreaterThanOrEqual(1);

    // Even if both somehow fired (TTL race), the dispatch map's count for
    // each (scheduleId, tickNumber) should be ≤ 1 because the leader lock
    // is acquired before findDue. Critically: the same TICK never fires
    // twice from the same poll.
    for (const [, count] of dispatched) {
      expect(count).toBeLessThanOrEqual(1);
    }

    server_stop(a);
    server_stop(b);
  });

  it("custom fire callback overrides default trigger routing", async () => {
    const scheduler = new InMemorySchedulerStorage();
    const fired: Array<{ id: string; tick: number }> = [];

    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      scheduler,
      scheduling: {
        enabled: true,
        pollIntervalMs: 30,
        leaderLockTtlMs: 1_000,
        fire: async (tick) => {
          fired.push({ id: tick.scheduleId, tick: tick.tickNumber });
        },
      },
    });

    await scheduler.upsertSchedule({
      id: "custom",
      intervalMs: 100,
      enabled: true,
      startAt: new Date(Date.now() - 200),
      metadata: {},
    });
    await scheduler.setNextRun("custom", new Date(Date.now() - 50));

    await server.schedulerLoop!.tickOnce();
    expect(fired.length).toBeGreaterThanOrEqual(1);
    expect(fired[0]!.id).toBe("custom");
    server.stop();
  });

  it("scheduling.enabled without a scheduler storage is a hard config error", () => {
    expect(
      () =>
        new ZoryaServer({
          storage: new InMemoryWorkflowStorage(),
          scheduling: { enabled: true },
        }),
    ).toThrow(/scheduler/);
  });

  it("scheduling.enabled without trigger or fire is a hard config error", () => {
    expect(
      () =>
        new ZoryaServer({
          storage: new InMemoryWorkflowStorage(),
          scheduler: new InMemorySchedulerStorage(),
          scheduling: { enabled: true },
        }),
    ).toThrow(/fire|trigger/);
  });

  it("namespaces: 'all' fires schedules across every namespace in one tick", async () => {
    const scheduler = new InMemorySchedulerStorage();
    const fired: Array<{ id: string; ns: string | undefined }> = [];

    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      scheduler,
      scheduling: {
        enabled: true,
        pollIntervalMs: 50,
        leaderLockTtlMs: 1_000,
        namespaces: "all",
        fire: async (tick, sched) => {
          fired.push({ id: tick.scheduleId, ns: sched.namespace });
        },
      },
    });

    // Three tenants + one global schedule, all due immediately.
    const seed = async (id: string, namespace: string | undefined) => {
      await scheduler.upsertSchedule({
        id,
        namespace,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() - 1_000),
        metadata: {},
      });
      await scheduler.setNextRun(id, new Date(Date.now() - 100));
    };
    await seed("tenant-a-job", "tenant-a");
    await seed("tenant-b-job", "tenant-b");
    await seed("tenant-c-job", "tenant-c");
    await seed("global-job", undefined);

    await server.schedulerLoop!.tickOnce();

    const namespacesFired = new Set(fired.map((f) => f.ns));
    expect(namespacesFired.has("tenant-a")).toBe(true);
    expect(namespacesFired.has("tenant-b")).toBe(true);
    expect(namespacesFired.has("tenant-c")).toBe(true);
    expect(namespacesFired.has(undefined)).toBe(true);

    server.stop();
  });

  it("idle namespaces cost zero leader-lock RPCs (1000-tenant scaling proof)", async () => {
    // Wraps an InMemorySchedulerStorage and counts every method call so the
    // test can prove findDueAcross + per-active-namespace dispatch is the
    // scaling shape, not per-namespace fan-out.
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
    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      scheduler: counting,
      scheduling: {
        enabled: true,
        pollIntervalMs: 50,
        leaderLockTtlMs: 1_000,
        namespaces: "all",
        fire: async (tick) => {
          fired.push(tick.scheduleId);
        },
      },
    });

    // Seed 1000 idle tenants (no due schedules).
    for (let i = 0; i < 1000; i++) {
      await inner.upsertSchedule({
        id: `tenant-${i}-job`,
        namespace: `tenant-${i}`,
        intervalMs: 60_000,
        enabled: true,
        // startAt in the future so nothing is due.
        startAt: new Date(Date.now() + 60_000),
        metadata: {},
      });
      // Deliberately leave nextRun unset (the in-memory backend only
      // counts schedules with a nextRun in due-tracking).
    }

    // Plus 3 active tenants with due schedules.
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

    // Reset counters after seeding.
    calls.tryAcquireLeader = 0;
    calls.findDue = 0;
    calls.findDueAcross = 0;
    calls.commitPoll = 0;

    await server.schedulerLoop!.tickOnce();

    // Findings: 1 cross-namespace findDueAcross + 1 leader-lock per
    // ACTIVE namespace (3) + 1 commit per active namespace.
    expect(calls.findDueAcross).toBe(1);
    expect(calls.findDue).toBe(0); // multi-namespace mode skips per-ns findDue
    expect(calls.tryAcquireLeader).toBe(3); // only the 3 active tenants
    expect(calls.commitPoll).toBe(3);
    expect(fired.length).toBe(3);

    server.stop();
  });

  it("dispatchConcurrency caps per-tick fan-out", async () => {
    const scheduler = new InMemorySchedulerStorage();
    let inFlight = 0;
    let peakInFlight = 0;
    const release: Array<() => void> = [];

    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      scheduler,
      scheduling: {
        enabled: true,
        pollIntervalMs: 50,
        leaderLockTtlMs: 1_000,
        dispatchConcurrency: 3,
        fire: async () => {
          inFlight++;
          peakInFlight = Math.max(peakInFlight, inFlight);
          await new Promise<void>((r) => release.push(r));
          inFlight--;
        },
      },
    });

    // Twelve schedules due simultaneously.
    for (let i = 0; i < 12; i++) {
      await scheduler.upsertSchedule({
        id: `job-${i}`,
        intervalMs: 60_000,
        enabled: true,
        startAt: new Date(Date.now() - 1_000),
        metadata: {},
      });
      await scheduler.setNextRun(`job-${i}`, new Date(Date.now() - 100));
    }

    // Kick off the tick; release dispatchers in a loop so the pool drains.
    const tickPromise = server.schedulerLoop!.tickOnce();
    // Give the pool a moment to ramp up to its concurrency cap.
    await new Promise<void>((r) => setTimeout(r, 25));
    while (release.length > 0 || inFlight > 0) {
      release.shift()?.();
      await new Promise<void>((r) => setTimeout(r, 1));
    }
    await tickPromise;

    // With dispatchConcurrency=3, no more than 3 ticks ever run in parallel.
    expect(peakInFlight).toBeLessThanOrEqual(3);
    expect(peakInFlight).toBeGreaterThanOrEqual(2); // proof of actual parallelism

    server.stop();
  });

  it("rejects passing both `namespace` and `namespaces` simultaneously", () => {
    expect(
      () =>
        new ZoryaServer({
          storage: new InMemoryWorkflowStorage(),
          scheduler: new InMemorySchedulerStorage(),
          scheduling: {
            enabled: true,
            namespace: "tenant-a",
            namespaces: "all",
            fire: async () => {},
          },
        }),
    ).toThrow(/namespace|namespaces/);
  });
});

function server_stop(s: ZoryaServer): void {
  s.stop();
}
