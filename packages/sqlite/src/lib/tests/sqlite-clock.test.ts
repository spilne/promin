// ---------------------------------------------------------------------------
// SQLite workflow stores read time from the injected WallClock: stored
// timestamps, lock expiry, heartbeat cutoffs and start-queue reclaim all
// follow FakeWallClock.advance().
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock } from "@promin/workflow";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";
import { SqliteWorkerRegistry } from "../sqlite-worker-registry.ts";
import { SqliteWorkflowStartQueue } from "../sqlite-workflow-start-queue.ts";
import { SqliteWorkflowAdvertisementRegistry } from "../sqlite-workflow-advertisements.ts";

const T0 = "2026-01-01T00:00:00.000Z";

describe("SqliteWorkflowStorage on a FakeWallClock", () => {
  it("stamps created / completed times from the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = SqliteWorkflowStorage.make({ db: new Database(":memory:"), clock });

    await storage.createWorkflow({ workflowId: "wf", workflowName: "n", input: {} });
    clock.advance(5_000);
    await storage.completeWorkflow("wf", "ok");

    const wf = await storage.loadWorkflow("wf");
    expect(wf!.createdAt.toISOString()).toBe(T0);
    expect(wf!.completedAt!.toISOString()).toBe("2026-01-01T00:00:05.000Z");
  });

  it("a lock expires when the clock passes its duration", async () => {
    const clock = FakeWallClock.create(T0);
    const db = new Database(":memory:");
    const a = SqliteWorkflowStorage.make({ db, clock });
    const b = SqliteWorkflowStorage.make({ db, clock });
    await a.createWorkflow({ workflowId: "wf", workflowName: "n", input: {} });

    expect((await a.tryLock("wf", 1_000)).acquired).toBe(true);
    clock.advance(999);
    expect((await b.tryLock("wf", 1_000)).acquired).toBe(false);
    clock.advance(2);
    expect((await b.tryLock("wf", 1_000)).acquired).toBe(true);
  });
});

describe("SqliteWorkerRegistry on a FakeWallClock", () => {
  it("detectDead uses the clock for the heartbeat cutoff", async () => {
    const clock = FakeWallClock.create(T0);
    const registry = SqliteWorkerRegistry.make({ db: new Database(":memory:"), clock });
    await registry.register({ workerId: "w1", capabilities: [], concurrency: 1 });

    clock.advance(9_000);
    expect(await registry.detectDead(10_000)).toEqual([]);
    clock.advance(2_000);
    expect((await registry.detectDead(10_000)).map((w) => w.workerId)).toEqual(["w1"]);
  });
});

describe("SqliteWorkflowStartQueue on a FakeWallClock", () => {
  it("re-offers a stale claim once the clock passes reclaimAfterMs", async () => {
    const clock = FakeWallClock.create(T0);
    const queue = SqliteWorkflowStartQueue.make({
      db: new Database(":memory:"),
      reclaimAfterMs: 1_000,
      clock,
    });
    const specs = [{ name: "n", versions: [] }];
    await queue.enqueue({ workflowId: "wf", workflowName: "n", input: {} });

    expect(await queue.claim({ workflowSpecs: specs, workerId: "a", limit: 1 })).toHaveLength(1);
    clock.advance(500);
    expect(await queue.claim({ workflowSpecs: specs, workerId: "b", limit: 1 })).toHaveLength(0);
    clock.advance(1_000);
    expect(await queue.claim({ workflowSpecs: specs, workerId: "b", limit: 1 })).toHaveLength(1);
  });
});

describe("SqliteWorkflowAdvertisementRegistry on a FakeWallClock", () => {
  it("stamps advertisedAt from the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const registry = SqliteWorkflowAdvertisementRegistry.make({
      db: new Database(":memory:"),
      clock,
    });
    clock.advance(1_000);
    await registry.upsert({ workerId: "w1", workflows: [{ name: "n", steps: [] }] });

    const [entry] = await registry.list();
    expect(entry!.advertisedAt.toISOString()).toBe("2026-01-01T00:00:01.000Z");
  });
});
