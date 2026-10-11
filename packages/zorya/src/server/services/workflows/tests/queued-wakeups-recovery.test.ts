// ---------------------------------------------------------------------------
// QueuedWorkflows background services: wake-ups re-enqueue due and
// signalled runs for a hosting worker, orphan recovery re-enqueues runs
// nobody drives (never one with a live lock), dead workers lose their
// advertisements, version routing picks numerically, and clustered setups
// refuse in-memory stores. One FakeWallClock drives everything.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import {
  SqliteLeaderLeaseStore,
  SqliteWorkerRegistry,
  SqliteWorkflowAdvertisementRegistry,
  SqliteWorkflowStartQueue,
  SqliteWorkflowStorage,
} from "@promin/sqlite";
import { succeed } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { InMemoryWorkerRegistry } from "@promin/workflow/distributed";
import { InMemoryLeaderLeases } from "@promin/workflow/scheduler";
import { InMemoryWorkflowAdvertisementRegistry } from "@promin/workflow/distributed";
import { InMemoryWorkflowStartQueue } from "@promin/workflow/distributed";
import { childEndedSignalName } from "@promin/workflow/storage-kit";
import {
  LocalWorkflows,
  QueuedWorkflows,
  RunCancelledError,
  UnknownWorkflowVersionError,
  ZoryaClusterConfigError,
} from "../index.ts";

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Settle work, then advance `stepMs`, until `done()` holds (bounded by `maxSteps`). */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxSteps?: number;
}): Promise<void> {
  const { clock, done, stepMs = 100, maxSteps = 500 } = params;
  for (let i = 0; i < maxSteps; i++) {
    await flush();
    if (await done()) return;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

const steps = [{ name: "s", kind: "single", dependsOn: [] as string[] }];

function setup(params: { maintenance?: boolean } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const workflowStarts = new InMemoryWorkflowStartQueue({ clock });
  const advertisements = new InMemoryWorkflowAdvertisementRegistry({ clock });
  const workerRegistry = new InMemoryWorkerRegistry({ clock });
  const queued = new QueuedWorkflows({
    storage,
    workflowStarts,
    advertisements,
    workerRegistry,
    clock,
    acceptAny: true,
    wakeups: { sleepScanIntervalMs: 100, signalScanIntervalMs: 100 },
    maintenance: params.maintenance === false ? false : { intervalMs: 500, orphanGraceMs: 1_000 },
  });
  // A worker-side runner on the same storage, to put runs in real states.
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, workflowStarts, advertisements, workerRegistry, queued, runner };
}

const napper = workflow<string>({ name: "napper", version: "3" })
  .sleep("nap", 1_000)
  .step("after", () => succeed("done"))
  .build();

const waiter = workflow<string>({ name: "waiter" })
  .waitForSignal<{ ok: boolean }, "go">("go", { signalName: "go" })
  .step("after", ({ prev }) => succeed(prev.ok))
  .build();

describe("QueuedWorkflows wake-ups", () => {
  it("re-enqueues a run whose sleep is due, with its stored version, once", async () => {
    const { clock, storage, workflowStarts, queued, runner } = setup({ maintenance: false });
    await storage.createWorkflow({
      workflowId: "n-1",
      workflowName: "napper",
      input: "x",
      version: "3",
    });
    await runner.runSafe({ workflow: napper, workflowId: "n-1", input: "x" });
    expect((await storage.loadWorkflowStatus("n-1"))?.status).toBe("suspended");
    await queued.start();

    await driveUntil({ clock, done: async () => (await workflowStarts.list()).length === 1 });
    for (let i = 0; i < 10; i++) {
      clock.advance(100);
      await flush();
    }
    const starts = await workflowStarts.list();
    expect(starts).toHaveLength(1);
    expect(starts[0]).toMatchObject({ workflowId: "n-1", workflowName: "napper", version: "3" });
    await queued.stop();
  });

  it("re-enqueues a run once its signal is delivered", async () => {
    const { clock, storage, workflowStarts, queued, runner } = setup({ maintenance: false });
    await runner.runSafe({ workflow: waiter, workflowId: "w-1", input: "x" });
    await queued.start();
    for (let i = 0; i < 5; i++) {
      clock.advance(100);
      await flush();
    }
    expect(await workflowStarts.list()).toHaveLength(0);

    await storage.deliverSignal({ workflowId: "w-1", signalName: "go", payload: { ok: true } });
    await driveUntil({ clock, done: async () => (await workflowStarts.list()).length === 1 });
    await queued.stop();

    // The worker that claims it resumes the run with the delivered value.
    expect(await runner.run({ workflow: waiter, workflowId: "w-1", input: "x" })).toBe(true);
  });

  it("leaves runs of workflows an outer local layer drives to that layer", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const workflowStarts = new InMemoryWorkflowStartQueue({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const queued = new QueuedWorkflows({
      storage,
      workflowStarts,
      clock,
      acceptAny: true,
      wakeups: { sleepScanIntervalMs: 100, signalScanIntervalMs: 0 },
      maintenance: false,
    });
    const local = new LocalWorkflows({
      storage,
      runner,
      clock,
      definitions: { napper },
      fallback: queued,
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    await runner.runSafe({ workflow: napper, workflowId: "n-2", input: "x" });
    await local.start();
    for (let i = 0; i < 30; i++) {
      clock.advance(100);
      await flush();
    }
    expect(await workflowStarts.list()).toHaveLength(0);
    await local.stop();
  });
});

describe("QueuedWorkflows orphan recovery", () => {
  it("re-enqueues a pending run whose start was lost, and a running run whose driver died", async () => {
    const { clock, storage, workflowStarts, queued } = setup({ maintenance: false });
    // A trigger that crashed between the create and the enqueue.
    await storage.createWorkflow({ workflowId: "lost", workflowName: "napper", input: "x" });
    // A run whose worker died mid-run: its lock expires.
    await storage.createWorkflow({ workflowId: "dead", workflowName: "napper", input: "y" });
    const lock = await storage.tryLock({ workflowId: "dead", lockDurationMs: 60_000 });
    await storage.setWorkflowMetadata({
      workflowId: "dead",
      patch: { touched: true },
      guard: { fenceToken: lock.token! },
    });

    // Past the default 10 s grace: "lost" is an orphan, "dead" is still locked.
    clock.advance(11_000);
    expect((await queued.maintain()).requeued).toBe(1);
    expect((await workflowStarts.list()).map((s) => s.workflowId)).toEqual(["lost"]);

    clock.advance(60_000);
    const report = await queued.maintain();
    expect(report.requeued).toBe(1);
    expect((await workflowStarts.list()).map((s) => s.workflowId).sort()).toEqual(["dead", "lost"]);
    // Idempotent: a second sweep adds nothing.
    expect((await queued.maintain()).requeued).toBe(0);
  });

  it("never touches a run another worker drives (live lock), however old", async () => {
    const { clock, storage, workflowStarts, queued } = setup({ maintenance: false });
    await storage.createWorkflow({ workflowId: "live", workflowName: "napper", input: "x" });
    const lock = await storage.tryLock({ workflowId: "live", lockDurationMs: 60_000 });
    const guard = { fenceToken: lock.token! };
    // The driver keeps heartbeating for ten minutes: the lock never lapses.
    for (let i = 0; i < 20; i++) {
      clock.advance(30_000);
      await storage.heartbeat({ workflowId: "live", lockDurationMs: 60_000, guard });
    }
    const report = await queued.maintain();
    expect(report.requeued).toBe(0);
    expect(await workflowStarts.list()).toHaveLength(0);
    expect((await storage.loadWorkflowStatus("live"))?.status).toBe("pending");
  });

  it("a retried trigger of a pending run whose start was lost queues it again", async () => {
    const { storage, workflowStarts, queued } = setup({ maintenance: false });
    await storage.createWorkflow({ workflowId: "t-1", workflowName: "wf", input: { a: 1 } });
    const res = await queued.trigger("wf", { a: 1 }, { workflowId: "t-1" });
    expect(res).toEqual({ workflowId: "t-1", existing: true });
    expect((await workflowStarts.list()).map((s) => s.workflowId)).toEqual(["t-1"]);
  });

  it("drops the advertisements of workers the registry declares dead, and expired ones", async () => {
    const { clock, advertisements, workerRegistry, queued } = setup({ maintenance: false });
    await workerRegistry.register({ workerId: "w-dead", capabilities: [], concurrency: 1 });
    await advertisements.upsert({ workerId: "w-dead", workflows: [{ name: "a", steps }] });
    clock.advance(31_000);
    await workerRegistry.register({ workerId: "w-live", capabilities: [], concurrency: 1 });
    await advertisements.upsert({ workerId: "w-live", workflows: [{ name: "b", steps }] });

    const report = await queued.maintain();
    expect(report.expiredWorkers).toBe(1);
    expect((await advertisements.list()).map((e) => e.workerId)).toEqual(["w-live"]);
  });

  it("runs the sweep periodically once started", async () => {
    const { clock, storage, workflowStarts, queued } = setup();
    await storage.createWorkflow({ workflowId: "p-1", workflowName: "wf", input: null });
    await queued.start();
    await driveUntil({ clock, done: async () => (await workflowStarts.list()).length === 1 });
    expect(queued.lastMaintenance?.requeued).toBe(1);
    await queued.stop();
  });
});

describe("QueuedWorkflows version routing", () => {
  async function advertise(
    advertisements: InMemoryWorkflowAdvertisementRegistry,
    versions: string[],
  ) {
    await advertisements.upsert({
      workerId: "w",
      workflows: versions.map((version) => ({ name: "greet", version, steps })),
    });
  }

  it("defaults to the newest advertised version in numeric order", async () => {
    const { storage, advertisements } = setup({ maintenance: false });
    const queued = new QueuedWorkflows({ storage, advertisements, maintenance: false });
    await advertise(advertisements, ["9", "10", "2"]);
    const { workflowId } = await queued.trigger("greet", {});
    expect((await storage.loadWorkflow(workflowId))?.version).toBe("10");
  });

  it("prefers the promoted version when a worker advertises it", async () => {
    const { storage, advertisements } = setup({ maintenance: false });
    const versionRegistry = new InMemoryWorkflowVersionRegistry();
    for (const version of ["1", "2"]) {
      versionRegistry.register(
        workflow<unknown>({ name: "greet", version })
          .step("s", () => succeed(1))
          .build(),
      );
    }
    await versionRegistry.promote({ name: "greet", version: "1" });
    const queued = new QueuedWorkflows({
      storage,
      advertisements,
      versionRegistry,
      maintenance: false,
    });
    await advertise(advertisements, ["1", "2"]);
    const { workflowId } = await queued.trigger("greet", {});
    expect((await storage.loadWorkflow(workflowId))?.version).toBe("1");
  });

  it("rejects a version no live worker hosts, creating nothing", async () => {
    const { storage, advertisements, workflowStarts } = setup({ maintenance: false });
    const queued = new QueuedWorkflows({
      storage,
      advertisements,
      workflowStarts,
      maintenance: false,
    });
    await advertise(advertisements, ["1"]);
    await expect(
      queued.trigger("greet", {}, { version: "99", workflowId: "v-99" }),
    ).rejects.toBeInstanceOf(UnknownWorkflowVersionError);
    expect(await storage.loadWorkflow("v-99")).toBeNull();
    expect(await workflowStarts.list()).toHaveLength(0);
  });
});

describe("chain ownership, both directions", () => {
  it("a worker-driven layer above a local one leaves the local layer's runs alone", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const workflowStarts = new InMemoryWorkflowStartQueue({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const local = new LocalWorkflows({
      storage,
      runner,
      clock,
      definitions: { napper },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    const queued = new QueuedWorkflows({
      storage,
      workflowStarts,
      advertisements: new InMemoryWorkflowAdvertisementRegistry({ clock }),
      clock,
      fallback: local,
      wakeups: { sleepScanIntervalMs: 100, signalScanIntervalMs: 0 },
      maintenance: false,
    });
    // No worker advertises "napper": its triggers fall through to the local layer.
    await runner.runSafe({ workflow: napper, workflowId: "n-3", input: "x" });
    await storage.createWorkflow({ workflowId: "n-4", workflowName: "napper", input: "x" });
    await queued.start();
    for (let i = 0; i < 30; i++) {
      clock.advance(100);
      await flush();
    }
    clock.advance(11_000);
    await queued.maintain();
    expect(await workflowStarts.list()).toHaveLength(0);
    await queued.stop();
  });

  it("refuses a chain whose layers run with different clusters", () => {
    const storage = new InMemoryWorkflowStorage();
    const local = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      orphanRecovery: false,
    });
    expect(
      () =>
        new QueuedWorkflows({
          storage,
          fallback: local,
          cluster: { instanceId: "a", leases: new InMemoryLeaderLeases() },
        }),
    ).toThrow(ZoryaClusterConfigError);
  });
});

describe("QueuedWorkflows.resume", () => {
  const abc = workflow<string>({ name: "abc" })
    .step("a", () => succeed(1))
    .sleep("nap", 1_000)
    .step("c", () => succeed(3))
    .build();
  const abcAd = {
    workerId: "w",
    workflows: [
      {
        name: "abc",
        steps: abc.dag.steps.map((s) => ({
          name: s.name,
          kind: s.kind,
          dependsOn: [...s.dependsOn],
        })),
      },
    ],
  };

  it("a resume that lands while a worker still holds the start is not lost", async () => {
    const { storage, workflowStarts, advertisements, queued, runner } = setup({
      maintenance: false,
    });
    await advertisements.upsert(abcAd);
    await queued.trigger("abc", "x", { workflowId: "abc-1" });
    // The worker claims the start and drives the run to its sleep.
    const [start] = await workflowStarts.claim({
      workflowSpecs: [{ name: "abc", versions: [] }],
      limit: 1,
    });
    await runner.runSafe({ workflow: abc, workflowId: "abc-1", input: "x" });
    expect((await storage.loadWorkflowStatus("abc-1"))?.status).toBe("suspended");

    // An operator resumes from the sleep before the worker settles its start.
    await queued.resume({ workflowId: "abc-1", fromStep: "nap" });
    expect(await workflowStarts.complete({ id: start!.id, claimToken: start!.claimToken! })).toBe(
      true,
    );
    // The start is pending again: a worker drives the resumed run.
    const [again] = await workflowStarts.claim({
      workflowSpecs: [{ name: "abc", versions: [] }],
      limit: 1,
    });
    expect(again?.workflowId).toBe("abc-1");
  });

  it("refuses a cancelled run, and hands a run it has no steps for to its fallback", async () => {
    const { storage, advertisements, queued } = setup({ maintenance: false });
    await advertisements.upsert(abcAd);
    await storage.createWorkflow({ workflowId: "c-1", workflowName: "abc", input: "x" });
    await storage.cancelWorkflow({ workflowId: "c-1" });
    await expect(queued.resume({ workflowId: "c-1", fromStep: "a" })).rejects.toBeInstanceOf(
      RunCancelledError,
    );

    const handedOver: string[] = [];
    const local = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      orphanRecovery: false,
    });
    local.resume = async (params) => {
      handedOver.push(params.workflowId);
    };
    const withFallback = new QueuedWorkflows({ storage, fallback: local, maintenance: false });
    await storage.createWorkflow({ workflowId: "x-1", workflowName: "unknown", input: "x" });
    await withFallback.resume({ workflowId: "x-1", fromStep: "a" });
    expect(handedOver).toEqual(["x-1"]);
  });
});

describe("lost child-ended wake-ups", () => {
  it("the sweep re-delivers the wake-up of a parent parked on a child that ended", async () => {
    const { clock, storage, queued } = setup({ maintenance: false });
    const wake = childEndedSignalName({ childWorkflowId: "kid-1", run: 1 });
    await storage.createWorkflow({ workflowId: "par-1", workflowName: "par", input: "x" });
    await storage.createWorkflow({
      workflowId: "kid-1",
      workflowName: "kid",
      input: "x",
      parentWorkflowId: "par-1",
    });
    await storage.suspendWorkflow({
      workflowId: "par-1",
      stepName: "child",
      stepUpdate: { status: "waiting_for_signal", signalName: wake },
    });
    // The child ends, but its driver died before delivering the wake-up.
    await storage.completeWorkflow({ workflowId: "kid-1", result: 2 });
    expect(await storage.loadSignals("par-1")).toEqual([]);

    const report = await queued.maintain();
    expect(report.childWakes).toBe(1);
    expect((await storage.loadSignals("par-1")).map((s) => s.signalName)).toEqual([wake]);
    // Already delivered: the next check leaves it.
    clock.advance(31_000);
    expect(await queued.maintain()).toMatchObject({ childWakes: 0 });
  });
});

describe("clustered configuration", () => {
  it("refuses in-memory stores when several instances share the work", () => {
    expect(
      () =>
        new QueuedWorkflows({
          storage: new InMemoryWorkflowStorage(),
          cluster: { instanceId: "a", leases: new InMemoryLeaderLeases() },
        }),
    ).toThrow(ZoryaClusterConfigError);
  });

  it("a direct sweep on an instance without the leadership says so instead of sweeping", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const db = new Database(":memory:");
    const storage = SqliteWorkflowStorage.make({ db, clock });
    const leases = SqliteLeaderLeaseStore.make({ db, clock });
    const queuedOn = (instanceId: string) =>
      new QueuedWorkflows({
        storage,
        workflowStarts: SqliteWorkflowStartQueue.make({ db, clock }),
        advertisements: SqliteWorkflowAdvertisementRegistry.make({ db, clock }),
        workerRegistry: SqliteWorkerRegistry.make({ db, clock }),
        clock,
        acceptAny: true,
        cluster: { instanceId, leases },
      });
    const localOn = (instanceId: string) =>
      new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage, clock }),
        clock,
        definitions: { napper },
        cluster: { instanceId, leases },
      });

    // Before any loop tick, a direct call takes the leadership itself.
    const [a, b] = [queuedOn("a"), queuedOn("b")];
    expect((await a.maintain()).skipped).toBeUndefined();
    expect(await b.maintain()).toEqual({
      orphans: 0,
      requeued: 0,
      expiredWorkers: 0,
      childWakes: 0,
      skipped: "not_leader",
    });
    expect((await a.maintain()).skipped).toBeUndefined();

    const [la, lb] = [localOn("a"), localOn("b")];
    expect(await la.recoverOrphans()).toEqual({ resumed: 0 });
    expect(await lb.recoverOrphans()).toEqual({ resumed: 0, skipped: "not_leader" });
    db.close();
  });
});
