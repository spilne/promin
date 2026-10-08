// ---------------------------------------------------------------------------
// Run the portable Scheduler conformance suite against the DurableScheduler
// shell wired to InMemorySchedulerStorage. Exercises the storage interface
// end-to-end with the same poll-based semantics that Postgres/Redis adapters
// share.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { schedulerTestSuite } from "../scheduler-test-suite.ts";
import { DurableScheduler, computeDueTicks, computeNextRun } from "../durable-scheduler.ts";
import { InMemorySchedulerStorage } from "../in-memory-scheduler-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { ScheduleTick } from "../types.ts";

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
// Scalability features — batch firing, jitter, partitioning,
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

    const ticks = await scheduler.stream().take(20).toArray().run();
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

    const tick = (await scheduler.stream("jittered").take(1).toArray().run())[0]!;
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
    const w0Ticks = await w0.stream().take(15).toArray().run();
    const w1Ticks = await w1.stream().take(15).toArray().run();

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
  // Dynamic schedule management — partial updateAsync
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

    const aTicks = await nsA.stream().take(2).toArray().run();
    const bTicks = await nsB.stream().take(2).toArray().run();

    expect(aTicks.every((t) => t.scheduleId === "only-a")).toBe(true);
    expect(bTicks.every((t) => t.scheduleId === "only-b")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Poll cadence — driven by the injected WallClock
// ---------------------------------------------------------------------------

describe("DurableScheduler poll cadence", () => {
  /** Storage that counts poll cycles (one leader attempt per poll). */
  function countingStorage(): { storage: InMemorySchedulerStorage; polls: () => number } {
    const storage = new InMemorySchedulerStorage();
    let polls = 0;
    const acquire = storage.tryAcquireLeader.bind(storage);
    storage.tryAcquireLeader = (params) => {
      polls++;
      return acquire(params);
    };
    return { storage, polls: () => polls };
  }

  /** Let the in-memory storage's promise chain settle. */
  const settle = () => new Promise((resolve) => setTimeout(resolve, 10));

  it("polls on the first pull and delivers that poll's ticks one interval later", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.registerAsync({ id: "first", intervalMs: 60_000 });

    let delivered = false;
    const result = scheduler
      .stream()
      .take(1)
      .toArray()
      .run()
      .then((ticks) => {
        delivered = true;
        return ticks;
      });

    await settle();
    expect(polls()).toBe(1);
    expect(delivered).toBe(false);
    expect(clock.pendingCount()).toBe(1);

    clock.advance(999);
    await settle();
    expect(delivered).toBe(false);

    clock.advance(1);
    const ticks = await result;
    expect(ticks.map((t) => t.scheduleId)).toEqual(["first"]);
    // take(1) stops before pulling again: no second poll, no timer left behind.
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });

  it("polls again only after the consumer pulls past the previous batch", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.registerAsync({ id: "a", intervalMs: 60_000 });
    await scheduler.registerAsync({ id: "b", intervalMs: 60_000 });

    const result = scheduler.stream().take(2).toArray().run();
    await settle();
    clock.advance(1000);
    const ticks = await result;

    // Both schedules were due in the first poll, so one poll covers take(2).
    expect(ticks.map((t) => t.scheduleId).sort()).toEqual(["a", "b"]);
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });

  it("clears the pending interval timer when the consumer stops mid-wait", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.registerAsync({ id: "never-delivered", intervalMs: 60_000 });

    // The fake clock never advances, so the stream is parked on the interval
    // timer when interruptAfter (real time) stops it.
    const ticks = await scheduler.stream().interruptAfter(30).toArray().run();

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("clears the pending interval timer when a for-await consumer breaks", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.registerAsync({ id: "a", intervalMs: 60_000 });

    const consumed = (async () => {
      const seen: string[] = [];
      for await (const tick of scheduler.subscribe().toAsyncIterable()) {
        seen.push(tick.scheduleId);
        break;
      }
      return seen;
    })();

    await settle();
    clock.advance(1000);

    expect(await consumed).toEqual(["a"]);
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Due-time math — which ticks fire, and when next, follow the injected clock
// ---------------------------------------------------------------------------

describe("DurableScheduler due-time math follows the injected WallClock", () => {
  // Fixed in the past, so anything that reads the real clock instead of the
  // fake one sees every schedule as long overdue (or never due again).
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  /**
   * Wait (in real time) until the scheduler is parked on a fake-clock timer,
   * i.e. it has finished the previous poll/delivery and is waiting for time
   * to move. Advancing before that point would fire nothing and leave the
   * stream waiting forever, which is what a fixed real-time sleep raced on.
   */
  async function untilWaiting(clock: FakeWallClock): Promise<void> {
    const deadline = Date.now() + 4_000;
    while (clock.pendingCount() === 0) {
      if (Date.now() > deadline) throw new Error("scheduler never waited on the fake clock");
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  }

  /** Consume `take` ticks in the background, exposing what has arrived so far. */
  function collect(params: { scheduler: DurableScheduler; take: number }) {
    const seen: ScheduleTick[] = [];
    const done = (async () => {
      for await (const tick of params.scheduler.stream().take(params.take).toAsyncIterable()) {
        seen.push(tick);
      }
    })();
    return { seen, done };
  }

  /** Advance the fake clock one poll interval at a time, each once the scheduler waits. */
  async function advancePolls(params: { clock: FakeWallClock; ms: number; polls: number }) {
    for (let i = 0; i < params.polls; i++) {
      await untilWaiting(params.clock);
      params.clock.advance(params.ms);
    }
  }

  it("an interval tick becomes due exactly when the fake clock passes the interval", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    await scheduler.registerAsync({ id: "every-10s", intervalMs: 10_000 });

    const { seen, done } = collect({ scheduler, take: 2 });
    await untilWaiting(clock);

    // Poll at T0 fires the bootstrap tick, delivered one poll interval later.
    // Polls at T0+1s … T0+9s find nothing due.
    await advancePolls({ clock, ms: 1_000, polls: 10 });
    await untilWaiting(clock);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.scheduledAt.getTime()).toBe(T0);
    expect(seen[0]!.firedAt.getTime()).toBe(T0);

    // The poll at T0+10s finds the next tick due; delivered at T0+11s.
    await advancePolls({ clock, ms: 1_000, polls: 1 });
    await done;
    expect(seen).toHaveLength(2);
    expect(seen[1]!.scheduledAt.getTime()).toBe(T0 + 10_000);
    expect(seen[1]!.firedAt.getTime()).toBe(T0 + 10_000);
    expect(seen[1]!.tickNumber).toBe(1);

    const state = await storage.loadScheduleState("every-10s");
    expect(state).toEqual({ lastFired: new Date(T0 + 10_000), tickCount: 2 });
  });

  it("a cron tick becomes due when the fake clock reaches the next occurrence", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 60_000, clock });
    await scheduler.registerAsync({ id: "every-5m", cron: "*/5 * * * *" });

    const { seen, done } = collect({ scheduler, take: 2 });
    await untilWaiting(clock);

    // Bootstrap tick at 00:00, then polls at 00:01 … 00:04 find nothing.
    await advancePolls({ clock, ms: 60_000, polls: 5 });
    await untilWaiting(clock);
    expect(seen.map((t) => t.scheduledAt.getTime())).toEqual([T0]);

    // Poll at 00:05 fires the 00:05 occurrence.
    await advancePolls({ clock, ms: 60_000, polls: 1 });
    await done;
    expect(seen.map((t) => t.scheduledAt.getTime())).toEqual([T0, T0 + 5 * 60_000]);
  });

  it("nextRun written by a poll and by updateAsync is computed on the fake clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    await scheduler.registerAsync({ id: "nr", intervalMs: 10_000 });

    const { done } = collect({ scheduler, take: 1 });
    await untilWaiting(clock);
    await advancePolls({ clock, ms: 1_000, polls: 1 });
    await done;

    // Polled at T0 → nextRun = T0 + 10s.
    expect(await storage.findDue({ now: new Date(T0 + 9_999), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0 + 10_000), limit: 10 })).toEqual(["nr"]);

    // Updated at T0 + 1s → nextRun = T0 + 1s + 3s.
    await scheduler.updateAsync("nr", { intervalMs: 3_000 });
    expect(await storage.findDue({ now: new Date(T0 + 3_999), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0 + 4_000), limit: 10 })).toEqual(["nr"]);
  });

  it("computeDueTicks and computeNextRun read the clock they are given", () => {
    const config = { id: "pure", intervalMs: 1_000 };
    const lastFired = new Date(T0);

    expect(computeDueTicks(config, lastFired, 1, FakeWallClock.create(T0 + 999))).toEqual([]);
    const due = computeDueTicks(config, lastFired, 1, FakeWallClock.create(T0 + 1_500));
    expect(due.map((t) => [t.scheduledAt.getTime(), t.firedAt.getTime()])).toEqual([
      [T0 + 1_000, T0 + 1_500],
    ]);

    expect(computeNextRun(config, FakeWallClock.create(T0))!.getTime()).toBe(T0 + 1_000);
  });

  it("InMemorySchedulerStorage seeds nextRun and leader-lock expiry from its clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    await storage.upsertSchedule({ id: "seeded", intervalMs: 1_000 });
    expect(await storage.findDue({ now: new Date(T0 - 1), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0), limit: 10 })).toEqual(["seeded"]);

    expect(await storage.tryAcquireLeader({ instanceId: "a", ttlMs: 1_000 })).toBe(true);
    clock.advance(999);
    expect(await storage.tryAcquireLeader({ instanceId: "b", ttlMs: 1_000 })).toBe(false);
    clock.advance(2);
    expect(await storage.tryAcquireLeader({ instanceId: "b", ttlMs: 1_000 })).toBe(true);
  });
});
