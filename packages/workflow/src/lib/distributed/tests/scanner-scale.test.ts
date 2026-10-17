// ---------------------------------------------------------------------------
// Sleep / signal scanners at scale: keyset paging finds every due run in one
// scan, resumes run concurrently (a long one doesn't hold up the rest), only
// the leader scans, and repeated signal names follow the store's
// last-delivery-wins semantics.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { createSleepScanner } from "../sleep-scanner.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { scannerLeaderKey } from "../leader-election.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { workflow, type Workflow } from "../../durable/durable-pipeline.ts";
import { createWorkflowRunner, type WorkflowRunner } from "../../durable/workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { InMemoryLeaderLeases, LeaseLeaderElection } from "../../scheduler/leader-lease.ts";

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

const anyWorkflow = { name: "napper" } as unknown as Workflow<unknown, unknown>;

/** Suspend `count` runs on a sleep that is due at `wakeAt`. */
async function sleepingRuns(params: {
  storage: InMemoryWorkflowStorage;
  count: number;
  wakeAt: Date;
}): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < params.count; i++) {
    const workflowId = `run-${String(i).padStart(3, "0")}`;
    await params.storage.createWorkflow({ workflowId, workflowName: "napper", input: { i } });
    await params.storage.suspendWorkflow(workflowId, "nap", {
      status: "sleeping",
      stepType: "sleep",
      wakeAt: params.wakeAt,
    });
    ids.push(workflowId);
  }
  return ids;
}

/** A runner whose resume finishes the run in storage, unless `hold` gates it. */
function recordingRunner(params: {
  storage: InMemoryWorkflowStorage;
  hold?: Map<string, Promise<void>>;
}): { runner: WorkflowRunner; resumed: string[] } {
  const resumed: string[] = [];
  const runner = {
    run: async ({ workflowId }: { workflowId: string }) => {
      resumed.push(workflowId);
      await params.hold?.get(workflowId);
      await params.storage.completeWorkflow(workflowId, "woke");
      return "woke";
    },
  } as unknown as WorkflowRunner;
  return { runner, resumed };
}

describe("sleep scanner at scale", () => {
  it("150 due sleeps are all resumed in one scan (keyset paging skips none)", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const ids = await sleepingRuns({ storage, count: 150, wakeAt: new Date(1_000) });
    const { runner, resumed } = recordingRunner({ storage });
    let scans = 0;
    const list = storage.listDueTimers.bind(storage);
    storage.listDueTimers = (p) => (scans++, list(p));

    clock.advance(1_000);
    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 60_000,
      resolveWorkflow: () => anyWorkflow,
      clock,
    });
    void scanner.start();
    await waitFor(() => resumed.length === 150 && clock.pendingCount() === 1);
    // Two pages (100 + 50), one scan, every run exactly once.
    expect(scans).toBe(2);
    expect([...resumed].sort()).toEqual(ids);
    await scanner.stop();
    for (const id of ids) expect((await storage.loadWorkflow(id))?.status).toBe("completed");
  });

  it("a long resume holds one slot; the other due runs are resumed meanwhile", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const ids = await sleepingRuns({ storage, count: 10, wakeAt: new Date(0) });
    let releaseSlow!: () => void;
    const hold = new Map([[ids[0]!, new Promise<void>((r) => (releaseSlow = r))]]);
    const { runner, resumed } = recordingRunner({ storage, hold });

    const scanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 60_000,
      resolveWorkflow: () => anyWorkflow,
      clock,
      resumeConcurrency: 3,
    });
    void scanner.start();
    await waitFor(async () => {
      const done = await storage.listWorkflows({ status: "completed", limit: 20 });
      return done.length === 9;
    });
    expect((await storage.loadWorkflow(ids[0]!))?.status).toBe("suspended");

    // The next scan doesn't resume the still-running one a second time.
    clock.advance(60_000);
    await waitFor(() => clock.pendingCount() === 1);
    expect(resumed.filter((id) => id === ids[0]).length).toBe(1);

    releaseSlow();
    await scanner.stop();
    expect((await storage.loadWorkflow(ids[0]!))?.status).toBe("completed");
  });

  it("only the lease holder scans", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    await sleepingRuns({ storage, count: 5, wakeAt: new Date(0) });
    const leases = new InMemoryLeaderLeases({ clock });
    const key = scannerLeaderKey({ scanner: "sleep" });
    const make = (instanceId: string) => {
      const { runner, resumed } = recordingRunner({ storage });
      const scanner = createSleepScanner({
        storage,
        runner,
        scanIntervalMs: 1_000,
        resolveWorkflow: () => anyWorkflow,
        clock,
        leaderElection: new LeaseLeaderElection({ store: leases, key, instanceId, ttlMs: 5_000 }),
      });
      return { scanner, resumed };
    };
    const a = make("a");
    const b = make("b");
    void a.scanner.start();
    await waitFor(() => a.resumed.length === 5);
    void b.scanner.start();
    await waitFor(() => clock.pendingCount() === 2);
    expect(b.resumed).toEqual([]);

    // A stops and releases; B takes over on its next scan.
    await a.scanner.stop();
    await storage.createWorkflow({ workflowId: "late", workflowName: "napper", input: {} });
    await storage.suspendWorkflow("late", "nap", {
      status: "sleeping",
      stepType: "sleep",
      wakeAt: new Date(0),
    });
    clock.advance(1_000);
    await waitFor(() => b.resumed.includes("late"));
    expect(a.resumed).not.toContain("late");
    await b.scanner.stop();
  });
});

describe("signal scanner and repeated signal names", () => {
  const twoWaits = workflow<number>({ name: "two-waits" })
    .step("start", ({ input }) => succeed(input))
    .waitForSignal<number>("first", { signalName: "go" })
    .waitForSignal<number>("second", { signalName: "go" })
    .step("sum", { dependsOn: ["first", "second"] }, ({ deps }) =>
      succeed(
        (deps as Record<string, number>)["first"]! + (deps as Record<string, number>)["second"]!,
      ),
    )
    .build();

  it("the latest delivery under a name wins, and both waits on it see that value", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    await runner.runSafe({ workflow: twoWaits, workflowId: "r", input: 0 });
    expect((await storage.loadWorkflow("r"))?.status).toBe("suspended");

    await storage.deliverSignal("r", "go", 1);
    await storage.deliverSignal("r", "go", 5);
    const wakeups = await storage.listSignalWakeups({ limit: 10 });
    expect(wakeups.map((w) => [w.workflowId, w.stepName, w.signalPayload])).toEqual([
      ["r", "first", 5],
    ]);

    const scanner = createSignalScanner({
      storage,
      runner,
      scanIntervalMs: 1_000,
      resolveWorkflow: () => twoWaits as never,
      clock,
    });
    void scanner.start();
    await waitFor(async () => (await storage.loadWorkflow("r"))?.status === "completed");
    await scanner.stop();
    expect((await storage.loadWorkflow("r"))?.result).toBe(10);
  });

  it("a fresh run starts with no signals, so it waits again", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    await runner.runSafe({ workflow: twoWaits, workflowId: "r", input: 0 });
    await storage.deliverSignal("r", "go", 1);
    await storage.startFreshRun("r");
    await runner.runSafe({ workflow: twoWaits, workflowId: "r", input: 0 });
    expect((await storage.loadWorkflow("r"))?.status).toBe("suspended");
    expect(await storage.listSignalWakeups({ limit: 10 })).toEqual([]);
  });
});
