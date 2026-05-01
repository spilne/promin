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

import { describe, it, expect } from "bun:test";
import {
  InMemorySchedulerStorage,
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  InMemoryWorkflowStorage,
  workflow,
  createWorkflowRunner,
} from "@promin/workflow";
import { Pipeline } from "@promin/core";
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { ZoryaServer } from "../../server/server.ts";
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
