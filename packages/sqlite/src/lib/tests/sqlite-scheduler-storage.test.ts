import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock, isStaleLeaseError, schedulerLeaderKey } from "@promin/workflow";
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

  it("lease expiry follows the injected clock", async () => {
    const clock = FakeWallClock.create(0);
    const s = SqliteSchedulerStorage.make({ db: new Database(":memory:"), clock });
    const key = schedulerLeaderKey({ namespace: "ns" });
    const first = await s.tryAcquireLeader({ key, instanceId: "a", ttlMs: 1_000 });
    clock.advance(999);
    expect(await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 1_000 })).toBeNull();
    clock.advance(1);
    const second = await s.tryAcquireLeader({ key, instanceId: "b", ttlMs: 1_000 });
    expect(second!.epoch).toBe(first!.epoch + 1);
  });

  it("two storages on one database file share leases and fencing", async () => {
    const db = new Database(":memory:");
    const a = SqliteSchedulerStorage.make({ db });
    const b = SqliteSchedulerStorage.make({ db });
    const key = schedulerLeaderKey({});
    await a.upsertSchedule({ id: "shared", intervalMs: 1_000 });

    const leaseA = await a.tryAcquireLeader({ key, instanceId: "A", ttlMs: 60_000 });
    expect(await b.tryAcquireLeader({ key, instanceId: "B", ttlMs: 60_000 })).toBeNull();
    await a.releaseLeader({ lease: leaseA! });
    expect(await b.tryAcquireLeader({ key, instanceId: "B", ttlMs: 60_000 })).not.toBeNull();

    const stale = await a
      .commitPoll({ updates: [{ id: "shared", nextRun: null }], lease: leaseA! })
      .then(
        () => null,
        (error: unknown) => error,
      );
    expect(isStaleLeaseError(stale)).toBe(true);
  });
});
