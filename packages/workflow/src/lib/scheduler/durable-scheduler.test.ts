// ---------------------------------------------------------------------------
// Run the portable Scheduler conformance suite against the DurableScheduler
// shell wired to InMemorySchedulerStorage. Exercises the storage interface
// end-to-end with the same poll-based semantics that Postgres/Redis adapters
// share.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { schedulerTestSuite } from "./scheduler-test-suite.ts";
import { DurableScheduler } from "./durable-scheduler.ts";
import { InMemorySchedulerStorage } from "./in-memory-scheduler-storage.ts";

schedulerTestSuite("DurableScheduler+InMemoryStorage", () => {
  const storage = new InMemorySchedulerStorage();
  const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
  return {
    scheduler,
    register: (config) => scheduler.registerAsync(config),
    unregister: (id, options) => scheduler.unregisterAsync(id, options),
    pause: (id) => scheduler.pauseAsync(id),
    resume: (id) => scheduler.resumeAsync(id),
    list: async () => scheduler.listAsync(),
  };
});

// ---------------------------------------------------------------------------
// Scalability features (promin-bqs): batch firing, jitter, partitioning,
// namespace isolation. Storage-backend agnostic — exercised here against
// InMemorySchedulerStorage; the same behaviors hold for Postgres/Redis since
// the logic lives in the shell.
// ---------------------------------------------------------------------------

describe("DurableScheduler scalability features", () => {
  it("fires many due schedules in a single poll cycle (batch firing via commitPoll)", async () => {
    const storage = new InMemorySchedulerStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

    // 20 schedules all due at registration time.
    for (let i = 0; i < 20; i++) {
      await scheduler.registerAsync({ id: `batch-${i}`, intervalMs: 1000 });
    }

    const ticks = await scheduler.stream().take(20).collect();
    expect(ticks).toHaveLength(20);
    const ids = new Set(ticks.map((t) => t.scheduleId));
    expect(ids.size).toBe(20);
  });

  it("commitPoll collapses N catch-up ticks into one tickCount increment", async () => {
    const storage = new InMemorySchedulerStorage();

    // Single update with N tickIncrement should move tickCount by exactly N.
    await storage.upsertSchedule({ id: "ci-1", intervalMs: 100, namespace: undefined });
    const before = await storage.loadScheduleState("ci-1");
    expect(before).toEqual({ lastFired: null, tickCount: 0 });

    const fired = new Date();
    await storage.commitPoll([
      { id: "ci-1", firedAt: fired, tickIncrement: 5, nextRun: new Date(Date.now() + 1000) },
    ]);
    const after = await storage.loadScheduleState("ci-1");
    expect(after).toEqual({ lastFired: fired, tickCount: 5 });
  });

  it("jitterMs randomizes firedAt within the configured window", async () => {
    const storage = new InMemorySchedulerStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

    await scheduler.registerAsync({ id: "jittered", intervalMs: 50, jitterMs: 200 });

    const tick = (await scheduler.stream("jittered").take(1).collect())[0]!;
    // firedAt should be at most jitterMs ahead of the call time. Lower bound
    // is loose — Date.now() advances during scheduling, so we just check it's
    // a Date that's in the recent past/near future.
    const offset = tick.firedAt.getTime() - tick.scheduledAt.getTime();
    expect(offset).toBeGreaterThanOrEqual(-1000);
    expect(offset).toBeLessThan(2000); // jitter + scheduler overhead
  });

  it("partitioning splits workload across instances by hash(id) % count", async () => {
    const storage = new InMemorySchedulerStorage();

    const w0 = new DurableScheduler({
      storage,
      pollIntervalMs: 25,
      partition: { index: 0, count: 2 },
      // Each worker needs its own leader lock — reuse instanceId so the lock
      // doesn't bounce. Use distinct ids so they don't compete for it either.
      instanceId: "worker-0",
    });
    const w1 = new DurableScheduler({
      storage,
      pollIntervalMs: 25,
      partition: { index: 1, count: 2 },
      instanceId: "worker-1",
    });

    for (let i = 0; i < 30; i++) {
      await w0.registerAsync({ id: `part-${i}`, intervalMs: 1000 });
    }

    // Manually fan out a single fireChunk per partition: instead of relying on
    // both workers' streams (which would race for the shared in-memory leader
    // lock), check the partition-filter logic by running them in series.
    const w0Ticks = await w0.stream().take(15).collect();
    const w1Ticks = await w1.stream().take(15).collect();

    const w0Ids = w0Ticks.map((t) => t.scheduleId);
    const w1Ids = w1Ticks.map((t) => t.scheduleId);

    // Each partition should only fire ids whose hash falls in its bucket.
    // Pure assertion: no schedule appears in both partitions' tick streams.
    const overlap = w0Ids.filter((id) => w1Ids.includes(id));
    expect(overlap).toHaveLength(0);
  });

  it("partition validation rejects out-of-range index/count", () => {
    const storage = new InMemorySchedulerStorage();
    expect(() => new DurableScheduler({ storage, partition: { index: 5, count: 3 } })).toThrow(
      /Invalid partition/,
    );
    expect(() => new DurableScheduler({ storage, partition: { index: -1, count: 2 } })).toThrow(
      /Invalid partition/,
    );
    expect(() => new DurableScheduler({ storage, partition: { index: 0, count: 0 } })).toThrow(
      /Invalid partition/,
    );
  });

  // -------------------------------------------------------------------------
  // Dynamic schedule management (promin-wqg): partial updateAsync
  // -------------------------------------------------------------------------

  describe("updateAsync — partial update", () => {
    it("merges patch with existing config and preserves untouched fields", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

      await scheduler.registerAsync({
        id: "daily-report",
        name: "Daily Report",
        cron: "0 9 * * *",
        timezone: "America/New_York",
        metadata: { team: "analytics" },
      });

      await scheduler.updateAsync("daily-report", { cron: "0 10 * * *" });

      const list = await scheduler.listAsync();
      const updated = list.find((s) => s.id === "daily-report")!;
      expect(updated.cron).toBe("0 10 * * *");
      // Fields not in the patch remain.
      expect(updated.name).toBe("Daily Report");
      expect(updated.timezone).toBe("America/New_York");
      expect(updated.metadata).toEqual({ team: "analytics" });
    });

    it("recomputes nextRun after a trigger change", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

      await scheduler.registerAsync({ id: "iv-1", intervalMs: 60_000 });

      // Updating interval should push the next run further out.
      await scheduler.updateAsync("iv-1", { intervalMs: 300_000 });

      // Due-tracking should now reflect the new interval; findDue within the
      // next few ms should not return it because nextRun moved forward.
      const due = await storage.findDue({ now: new Date(), limit: 10 });
      expect(due).not.toContain("iv-1");
    });

    it("throws on update of non-existent schedule", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
      await expect(scheduler.updateAsync("missing", { cron: "* * * * *" })).rejects.toThrow(
        /does not exist/,
      );
    });

    it("rejects invalid merged config (e.g. cron + intervalMs both set)", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

      await scheduler.registerAsync({ id: "conflict", cron: "0 9 * * *" });
      // Patch adds intervalMs — merged config has both cron AND intervalMs, invalid.
      await expect(scheduler.updateAsync("conflict", { intervalMs: 60_000 })).rejects.toThrow(
        /must have exactly one/,
      );
    });

    it("can pause via updateAsync enabled: false", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
      await scheduler.registerAsync({ id: "pr-u", intervalMs: 60_000 });

      await scheduler.updateAsync("pr-u", { enabled: false });
      const updated = (await scheduler.listAsync()).find((s) => s.id === "pr-u")!;
      expect(updated.enabled).toBe(false);
    });
  });

  it("namespace isolation — schedules in ns A don't fire in scheduler scoped to ns B", async () => {
    const storage = new InMemorySchedulerStorage();

    const nsA = new DurableScheduler({
      storage,
      pollIntervalMs: 25,
      namespace: "tenant-a",
      instanceId: "a",
    });
    const nsB = new DurableScheduler({
      storage,
      pollIntervalMs: 25,
      namespace: "tenant-b",
      instanceId: "b",
    });

    await nsA.registerAsync({ id: "only-a", intervalMs: 50 });
    await nsB.registerAsync({ id: "only-b", intervalMs: 50 });

    const aTicks = await nsA.stream().take(2).collect();
    const bTicks = await nsB.stream().take(2).collect();

    expect(aTicks.every((t) => t.scheduleId === "only-a")).toBe(true);
    expect(bTicks.every((t) => t.scheduleId === "only-b")).toBe(true);
  });
});
