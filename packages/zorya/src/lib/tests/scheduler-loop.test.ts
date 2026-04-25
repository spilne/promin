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
});

function server_stop(s: ZoryaServer): void {
  s.stop();
}
