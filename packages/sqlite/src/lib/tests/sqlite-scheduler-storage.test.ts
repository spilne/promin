import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { schedulerStorageTestSuite } from "@promin/workflow/testing";
import { SqliteSchedulerStorage } from "../sqlite-scheduler-storage.ts";

// Each factory call gets a fresh :memory: database — natural per-test
// isolation so the shared conformance suite (which expects a clean
// slate) works without TRUNCATE.
schedulerStorageTestSuite(async () =>
  SqliteSchedulerStorage.make({ db: new Database(":memory:") }),
);

// ---- SQLite-specific tests ----

describe("SqliteSchedulerStorage", () => {
  it("persists state across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const a = SqliteSchedulerStorage.make({ db });
    await a.upsertSchedule({
      id: "persists",
      intervalMs: 60_000,
      enabled: true,
      metadata: { workflowName: "demo" },
    });
    await a.setNextRun("persists", new Date(1000));
    await a.recordFire("persists", new Date(2000), 3);

    // A fresh storage instance pointed at the same db sees the persisted
    // state — this is the property that makes "pickup from where we left"
    // work across server restarts.
    const b = SqliteSchedulerStorage.make({ db });
    const cfg = await b.loadSchedule("persists");
    expect(cfg?.intervalMs).toBe(60_000);
    expect(cfg?.metadata).toEqual({ workflowName: "demo" });
    const state = await b.loadScheduleState("persists");
    expect(state?.tickCount).toBe(3);
    expect(state?.lastFired?.getTime()).toBe(2000);
  });

  it("custom table prefix avoids conflicts when two storages share a db", async () => {
    const db = new Database(":memory:");
    const tenantA = SqliteSchedulerStorage.make({ db, tablePrefix: "sched_a" });
    const tenantB = SqliteSchedulerStorage.make({ db, tablePrefix: "sched_b" });
    await tenantA.upsertSchedule({ id: "same", intervalMs: 1_000, enabled: true });
    await tenantB.upsertSchedule({ id: "same", intervalMs: 2_000, enabled: true });
    expect((await tenantA.loadSchedule("same"))?.intervalMs).toBe(1_000);
    expect((await tenantB.loadSchedule("same"))?.intervalMs).toBe(2_000);
  });

  it("expired leader lock can be claimed by a different instance", async () => {
    // Burn the conformance suite's "TTL refresh" guarantee from the other
    // direction — once expires_at is in the past, a different instanceId
    // wins the next acquire.
    const s = SqliteSchedulerStorage.make({ db: new Database(":memory:") });
    const first = await s.tryAcquireLeader({
      instanceId: "expired-leader",
      namespace: "ns",
      ttlMs: 0, // immediate expiry
    });
    expect(first).toBe(true);
    // ttlMs=0 means expires_at = now, which is NOT > now in tryAcquire's
    // strict-greater check, so a fresh instance wins immediately.
    const second = await s.tryAcquireLeader({
      instanceId: "new-leader",
      namespace: "ns",
      ttlMs: 10_000,
    });
    expect(second).toBe(true);
  });
});
