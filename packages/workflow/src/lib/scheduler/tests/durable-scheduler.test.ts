// ---------------------------------------------------------------------------
// Run the portable Scheduler conformance suite against the DurableScheduler
// shell wired to InMemorySchedulerStorage. Exercises the storage interface
// end-to-end with the same poll-based semantics that Postgres/Redis adapters
// share.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Stream } from "@spilne/perfect-core";
import { schedulerTestSuite } from "../scheduler-test-suite.ts";
import {
  DurableScheduler,
  computeDueTicks,
  computeNextRun,
  planDueTicks,
  schedulePartition,
  type SchedulerErrorEvent,
} from "../durable-scheduler.ts";
import { InMemorySchedulerStorage } from "../in-memory-scheduler-storage.ts";
import { schedulerLeaderKey } from "../leader-lease.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { DurableScheduleConfig, ScheduleTick } from "../types.ts";

schedulerTestSuite("DurableScheduler+InMemoryStorage", () => {
  const storage = new InMemorySchedulerStorage();
  const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
  return {
    scheduler,
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
      await scheduler.register({ id: `batch-${i}`, intervalMs: 1000 });
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
    await storage.commitPoll({
      updates: [
        { id: "ci-1", firedAt: fired, tickIncrement: 5, nextRun: new Date(Date.now() + 1000) },
      ],
    });
    const after = await storage.loadScheduleState("ci-1");
    expect(after).toEqual({ lastFired: fired, tickCount: 5 });
  });

  it("jitterMs delays the next run, so the tick is emitted late with its nominal scheduledAt", async () => {
    const T0 = Date.parse("2026-01-01T00:00:00Z");
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    // random() = 0.5 → a 2.5s delay out of the 5s jitter window.
    const scheduler = new DurableScheduler({
      storage,
      pollIntervalMs: 1_000,
      clock,
      random: () => 0.5,
    });
    await scheduler.register({ id: "jittered", intervalMs: 10_000, jitterMs: 5_000 });

    const consumer = consume({ scheduler });
    await untilWaiting(clock);
    // Bootstrap tick at T0; the next run is T0 + 10s nominal + 2.5s jitter.
    expect(await storage.findDue({ now: new Date(T0 + 12_499), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0 + 12_500), limit: 10 })).toEqual(["jittered"]);

    await advancePolls({ clock, ms: 1_000, polls: 12 });
    await untilWaiting(clock);
    expect(consumer.seen).toHaveLength(1);
    await advancePolls({ clock, ms: 1_000, polls: 1 });
    await untilWaiting(clock);
    await consumer.stop();

    expect(consumer.seen).toHaveLength(2);
    expect(consumer.seen[1]!.scheduledAt.getTime()).toBe(T0 + 10_000);
    // Emitted by the first poll at or after the jittered next run, never early.
    expect(consumer.seen[1]!.firedAt.getTime()).toBe(T0 + 13_000);
  });

  it("partitioned instances each lead their own partition and fire concurrently", async () => {
    const storage = new InMemorySchedulerStorage();
    const workers = [0, 1].map(
      (index) =>
        new DurableScheduler({
          storage,
          pollIntervalMs: 25,
          partition: { index, count: 2 },
          instanceId: `worker-${index}`,
        }),
    );

    const ids = Array.from({ length: 20 }, (_, i) => `part-${i}`);
    for (const id of ids) await workers[0]!.register({ id, intervalMs: 60_000 });
    const expected = [0, 1].map((index) =>
      ids.filter((id) => schedulePartition({ id, count: 2 }) === index).sort(),
    );
    expect(expected[0]!.length).toBeGreaterThan(0);
    expect(expected[1]!.length).toBeGreaterThan(0);

    // Both streams run at once: a shared leader lock would let only one fire.
    const fired = await Promise.all(
      workers.map((w, i) => w.stream().take(expected[i]!.length).toArray().run()),
    );

    expect(fired.map((ticks) => ticks.map((t) => t.scheduleId).sort())).toEqual(expected);
  });

  it("partitioned polls look past the other partitions' due schedules", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemorySchedulerStorage({ clock });
    const pick = (partition: number) =>
      Array.from({ length: 200 }, (_, i) => `deep-${i}`)
        .filter((id) => schedulePartition({ id, count: 2 }) === partition)
        .slice(0, 10);
    const [others, mine] = [pick(0), pick(1)];
    const scheduler = new DurableScheduler({
      storage,
      clock,
      pollIntervalMs: 1_000,
      batchSize: 10,
      partition: { index: 1, count: 2 },
    });
    // Partition 0 has no running instance; its schedules are due first.
    for (const id of others) await scheduler.register({ id, intervalMs: 60_000 });
    clock.advance(1);
    for (const id of mine) await scheduler.register({ id, intervalMs: 60_000 });

    const ticks = await scheduler.stream().take(mine.length).toArray().run();
    expect(ticks.map((t) => t.scheduleId).sort()).toEqual([...mine].sort());
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
  // Dynamic schedule management — partial update
  // -------------------------------------------------------------------------

  describe("update — partial update", () => {
    it("merges patch with existing config and preserves untouched fields", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

      await scheduler.register({
        id: "daily-report",
        name: "Daily Report",
        cron: "0 9 * * *",
        timezone: "America/New_York",
        metadata: { team: "analytics" },
      });

      await scheduler.update("daily-report", { cron: "0 10 * * *" });

      const list = await scheduler.list();
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

      await scheduler.register({ id: "iv-1", intervalMs: 60_000 });

      // Updating interval should push the next run further out.
      await scheduler.update("iv-1", { intervalMs: 300_000 });

      // Due-tracking should now reflect the new interval; findDue within the
      // next few ms should not return it because nextRun moved forward.
      const due = await storage.findDue({ now: new Date(), limit: 10 });
      expect(due).not.toContain("iv-1");
    });

    it("throws on update of non-existent schedule", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
      await expect(scheduler.update("missing", { cron: "* * * * *" })).rejects.toThrow(
        /does not exist/,
      );
    });

    it("rejects invalid merged config (e.g. cron + intervalMs both set)", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });

      await scheduler.register({ id: "conflict", cron: "0 9 * * *" });
      // Patch adds intervalMs — merged config has both cron AND intervalMs, invalid.
      await expect(scheduler.update("conflict", { intervalMs: 60_000 })).rejects.toThrow(
        /must have exactly one/,
      );
    });

    it("can pause via update enabled: false", async () => {
      const storage = new InMemorySchedulerStorage();
      const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
      await scheduler.register({ id: "pr-u", intervalMs: 60_000 });

      await scheduler.update("pr-u", { enabled: false });
      const updated = (await scheduler.list()).find((s) => s.id === "pr-u")!;
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

    await nsA.register({ id: "only-a", intervalMs: 50 });
    await nsB.register({ id: "only-b", intervalMs: 50 });

    const aTicks = await nsA.stream().take(2).toArray().run();
    const bTicks = await nsB.stream().take(2).toArray().run();

    expect(aTicks.every((t) => t.scheduleId === "only-a")).toBe(true);
    expect(bTicks.every((t) => t.scheduleId === "only-b")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Fake-clock helpers
// ---------------------------------------------------------------------------

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

/** Advance the fake clock one poll interval at a time, each once the scheduler waits. */
async function advancePolls(params: { clock: FakeWallClock; ms: number; polls: number }) {
  for (let i = 0; i < params.polls; i++) {
    await untilWaiting(params.clock);
    params.clock.advance(params.ms);
  }
}

/**
 * Drain a scheduler stream in the background, recording every tick. The
 * drain pulls past each tick, so every recorded tick is acknowledged.
 * `stop()` ends the stream and waits for its finalizers.
 */
function consume(params: { scheduler: DurableScheduler; scheduleId?: string }) {
  const seen: ScheduleTick[] = [];
  let fire: () => void = () => {};
  const signal = Stream.fromCallback<void>((emit) => {
    fire = () => emit(undefined);
  });
  const done = params.scheduler
    .stream(params.scheduleId)
    .takeUntil(signal)
    .tap((tick) => void seen.push(tick))
    .drain()
    .run();
  return {
    seen,
    stop: async () => {
      fire();
      await done;
    },
  };
}

/** Give a wrongly scheduled delivery a chance to happen before asserting it didn't. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 10));

/** Storage that counts poll cycles (one leader attempt per poll). */
function countingStorage(clock?: FakeWallClock): {
  storage: InMemorySchedulerStorage;
  polls: () => number;
} {
  const storage = new InMemorySchedulerStorage({ clock });
  let polls = 0;
  const acquire = storage.tryAcquireLeader.bind(storage);
  storage.tryAcquireLeader = (params) => {
    polls++;
    return acquire(params);
  };
  return { storage, polls: () => polls };
}

// ---------------------------------------------------------------------------
// Poll cadence — poll, emit, then wait on the injected WallClock
// ---------------------------------------------------------------------------

describe("DurableScheduler poll cadence", () => {
  it("polls on the first pull and delivers that poll's ticks right away", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.register({ id: "first", intervalMs: 60_000 });

    const ticks = await scheduler.stream().take(1).toArray().run();

    expect(ticks.map((t) => t.scheduleId)).toEqual(["first"]);
    // take(1) stops before pulling again: no second poll, no timer left behind.
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });

  it("waits pollIntervalMs on the clock after a batch before polling again", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage(clock);
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.register({ id: "a", intervalMs: 1_000 });

    let count = 0;
    const result = scheduler
      .stream()
      .tap(() => void count++)
      .take(2)
      .toArray()
      .run();

    await untilWaiting(clock);
    expect(count).toBe(1);
    expect(polls()).toBe(1);

    clock.advance(999);
    await settle();
    expect(polls()).toBe(1);

    clock.advance(1);
    const ticks = await result;
    expect(ticks.map((t) => t.tickNumber)).toEqual([0, 1]);
    expect(polls()).toBe(2);
    expect(clock.pendingCount()).toBe(0);
  });

  it("one poll covers every schedule due in it", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.register({ id: "a", intervalMs: 60_000 });
    await scheduler.register({ id: "b", intervalMs: 60_000 });

    const ticks = await scheduler.stream().take(2).toArray().run();

    expect(ticks.map((t) => t.scheduleId).sort()).toEqual(["a", "b"]);
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });

  it("clears the pending interval timer when the consumer stops mid-wait", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.register({ id: "once", intervalMs: 60_000 });

    // The fake clock never advances, so after the first batch the stream is
    // parked on the interval timer when interruptAfter (real time) stops it.
    const ticks = await scheduler.stream().interruptAfter(30).toArray().run();

    expect(ticks.map((t) => t.scheduleId)).toEqual(["once"]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("clears the pending interval timer when a for-await consumer breaks", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { storage, polls } = countingStorage();
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1000, clock });
    await scheduler.register({ id: "a", intervalMs: 60_000 });

    const seen: string[] = [];
    for await (const tick of scheduler.subscribe().toAsyncIterable()) {
      seen.push(tick.scheduleId);
      break;
    }

    expect(seen).toEqual(["a"]);
    expect(polls()).toBe(1);
    expect(clock.pendingCount()).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Delivery guarantee — at least once, redelivered with the same tickNumber
// ---------------------------------------------------------------------------

describe("DurableScheduler delivery", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  function setup() {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    return { clock, storage, scheduler };
  }

  async function tickCounts(storage: InMemorySchedulerStorage, ids: string[]) {
    const states = await storage.loadScheduleStates(ids);
    return ids.map((id) => states.get(id)?.tickCount ?? 0);
  }

  it("a consumer that stops mid-batch loses nothing: unacknowledged ticks are redelivered", async () => {
    const { storage, scheduler } = setup();
    for (const id of ["a", "b", "c"]) await scheduler.register({ id, intervalMs: 60_000 });

    // take(1) receives "a" and stops without pulling again, so nothing is acknowledged.
    const first = await scheduler.stream().take(1).toArray().run();
    expect(first.map((t) => t.scheduleId)).toEqual(["a"]);
    expect(await tickCounts(storage, ["a", "b", "c"])).toEqual([0, 0, 0]);

    // The next stream redelivers all three, "a" with the same tickNumber.
    const again = await scheduler.stream().take(3).toArray().run();
    expect(again.map((t) => [t.scheduleId, t.tickNumber])).toEqual([
      ["a", 0],
      ["b", 0],
      ["c", 0],
    ]);
  });

  it("stopping mid-batch commits the acknowledged schedules only", async () => {
    const { storage, scheduler } = setup();
    for (const id of ["a", "b", "c"]) await scheduler.register({ id, intervalMs: 60_000 });

    // take(2): pulling "b" acknowledges "a"; "b" itself is never acknowledged.
    const first = await scheduler.stream().take(2).toArray().run();
    expect(first.map((t) => t.scheduleId)).toEqual(["a", "b"]);
    expect(await tickCounts(storage, ["a", "b", "c"])).toEqual([1, 0, 0]);

    const again = await scheduler.stream().take(2).toArray().run();
    expect(again.map((t) => [t.scheduleId, t.tickNumber])).toEqual([
      ["b", 0],
      ["c", 0],
    ]);
  });

  it("a consumer that pulls past a batch commits all of it before waiting", async () => {
    const { clock, storage, scheduler } = setup();
    for (const id of ["a", "b", "c"]) await scheduler.register({ id, intervalMs: 60_000 });

    const consumer = consume({ scheduler });
    await untilWaiting(clock);
    expect(consumer.seen).toHaveLength(3);
    expect(await tickCounts(storage, ["a", "b", "c"])).toEqual([1, 1, 1]);
    await consumer.stop();

    // Nothing is due any more, so a new stream has nothing to redeliver.
    const later = consume({ scheduler });
    await untilWaiting(clock);
    await later.stop();
    expect(later.seen).toEqual([]);
  });

  it("a failed commit is reported and its ticks are redelivered with the same tickNumber", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const errors: SchedulerErrorEvent[] = [];
    const scheduler = new DurableScheduler({
      storage,
      pollIntervalMs: 1_000,
      clock,
      onError: (e) => void errors.push(e),
    });
    await scheduler.register({ id: "a", intervalMs: 60_000 });

    const commit = storage.commitPoll.bind(storage);
    let failNext = true;
    storage.commitPoll = async (updates) => {
      if (failNext) {
        failNext = false;
        throw new Error("commit blip");
      }
      return commit(updates);
    };

    const consumer = consume({ scheduler });
    await untilWaiting(clock);
    expect(errors.map((e) => [e.phase, (e.error as Error).message])).toEqual([
      ["commit", "commit blip"],
    ]);
    await advancePolls({ clock, ms: 1_000, polls: 1 });
    await untilWaiting(clock);
    await consumer.stop();

    expect(consumer.seen.map((t) => [t.scheduleId, t.tickNumber])).toEqual([
      ["a", 0],
      ["a", 0],
    ]);
    expect(await tickCounts(storage, ["a"])).toEqual([1]);
  });
});

// ---------------------------------------------------------------------------
// Robustness — storage errors and bad stored schedules don't end the stream
// ---------------------------------------------------------------------------

describe("DurableScheduler robustness", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  it("survives storage errors: reports them and retries with backoff on the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const errors: SchedulerErrorEvent[] = [];
    const scheduler = new DurableScheduler({
      storage,
      pollIntervalMs: 1_000,
      maxErrorBackoffMs: 3_000,
      clock,
      onError: (e) => void errors.push(e),
    });
    await scheduler.register({ id: "z", intervalMs: 60_000 });

    const findDue = storage.findDue.bind(storage);
    let failures = 3;
    const pollTimes: number[] = [];
    storage.findDue = async (params) => {
      pollTimes.push(clock.currentTimeMs() - T0);
      if (failures > 0) {
        failures--;
        throw new Error("db blip");
      }
      return findDue(params);
    };

    const consumer = consume({ scheduler });
    // Backoff: 2s, then 4s capped to 3s, then 3s.
    await advancePolls({ clock, ms: 2_000, polls: 1 });
    await advancePolls({ clock, ms: 3_000, polls: 2 });
    await untilWaiting(clock);
    await consumer.stop();

    expect(pollTimes).toEqual([0, 2_000, 5_000, 8_000]);
    expect(errors.map((e) => e.phase)).toEqual(["poll", "poll", "poll"]);
    expect(consumer.seen.map((t) => t.scheduleId)).toEqual(["z"]);
  });

  it("a throwing onError hook doesn't end the stream", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({
      storage,
      pollIntervalMs: 1_000,
      clock,
      onError: () => {
        throw new Error("hook bug");
      },
    });
    await scheduler.register({ id: "z", intervalMs: 60_000 });
    const findDue = storage.findDue.bind(storage);
    let failed = false;
    storage.findDue = async (params) => {
      if (!failed) {
        failed = true;
        throw new Error("db blip");
      }
      return findDue(params);
    };

    const consumer = consume({ scheduler });
    await advancePolls({ clock, ms: 2_000, polls: 1 });
    await untilWaiting(clock);
    await consumer.stop();
    expect(consumer.seen.map((t) => t.scheduleId)).toEqual(["z"]);
  });

  it("an invalid schedule in storage is reported, disabled and skipped; the others keep firing", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const errors: SchedulerErrorEvent[] = [];
    const scheduler = new DurableScheduler({
      storage,
      pollIntervalMs: 1_000,
      clock,
      onError: (e) => void errors.push(e),
    });
    // Written straight to storage, bypassing register()'s validation; it has
    // fired before, so evaluating it parses the cron.
    await storage.upsertSchedule({ id: "bad", cron: "not a cron" });
    await storage.recordFire("bad", new Date(T0 - 60_000));
    await scheduler.register({ id: "good", intervalMs: 1_000 });

    const consumer = consume({ scheduler });
    await advancePolls({ clock, ms: 1_000, polls: 2 });
    await untilWaiting(clock);
    await consumer.stop();

    expect(consumer.seen.map((t) => t.scheduleId)).toEqual(["good", "good", "good"]);
    expect(errors.map((e) => [e.phase, e.scheduleId])).toEqual([["schedule", "bad"]]);
    expect((await storage.loadSchedule("bad"))?.enabled).toBe(false);
    expect(await storage.findDue({ now: new Date(T0 + 60_000), limit: 10 })).not.toContain("bad");
  });

  it("paused schedules don't starve an active one under a small batchSize", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, batchSize: 5, clock });
    for (let i = 0; i < 5; i++) {
      await scheduler.register({ id: `paused-${i}`, intervalMs: 60_000 });
      await scheduler.pause(`paused-${i}`);
    }
    clock.advance(1);
    await scheduler.register({ id: "active", intervalMs: 60_000 });

    const ticks = await scheduler.stream().take(1).toArray().run();
    expect(ticks.map((t) => t.scheduleId)).toEqual(["active"]);
  });

  it("pause drops the schedule from due-tracking; resume makes it due again", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    await scheduler.register({ id: "p", intervalMs: 60_000 });

    await scheduler.pause("p");
    expect(await storage.findDue({ now: new Date(T0 + 3_600_000), limit: 10 })).toEqual([]);

    await scheduler.resume("p");
    expect(await storage.findDue({ now: new Date(T0), limit: 10 })).toEqual(["p"]);
  });
});

// ---------------------------------------------------------------------------
// Catch-up — the newest maxCatchUp missed occurrences fire, for every trigger
// ---------------------------------------------------------------------------

describe("computeDueTicks catch-up", () => {
  const lastFired = new Date("2026-01-01T00:00:00Z");
  const clock = FakeWallClock.create(Date.parse("2026-01-01T10:00:00Z"));
  const hhmm = (ticks: ScheduleTick[]) =>
    ticks.map((t) => t.scheduledAt.toISOString().slice(11, 16));

  it("cron, rrule and interval all fire the newest maxCatchUp missed occurrences", () => {
    const configs: DurableScheduleConfig[] = [
      { id: "c", cron: "0 * * * *", maxCatchUp: 2 },
      { id: "r", rrule: "DTSTART:20260101T000000Z\nRRULE:FREQ=HOURLY", maxCatchUp: 2 },
      { id: "i", intervalMs: 3_600_000, maxCatchUp: 2 },
    ];
    for (const config of configs) {
      const ticks = computeDueTicks(config, lastFired, 7, clock);
      expect(hhmm(ticks)).toEqual(["09:00", "10:00"]);
      expect(ticks.map((t) => t.tickNumber)).toEqual([7, 8]);
    }
  });

  it("maxCatchUp 0 fires only the most recent missed occurrence", () => {
    const configs: DurableScheduleConfig[] = [
      { id: "c", cron: "0 * * * *" },
      { id: "r", rrule: "DTSTART:20260101T000000Z\nRRULE:FREQ=HOURLY" },
      { id: "i", intervalMs: 3_600_000 },
    ];
    for (const config of configs) {
      expect(hhmm(computeDueTicks(config, lastFired, 0, clock))).toEqual(["10:00"]);
    }
  });

  it("fires fewer when fewer were missed, and nothing before the next occurrence", () => {
    const at = (iso: string) => FakeWallClock.create(Date.parse(iso));
    const cron = { id: "c", cron: "0 * * * *", maxCatchUp: 5 };
    expect(hhmm(computeDueTicks(cron, lastFired, 0, at("2026-01-01T02:30:00Z")))).toEqual([
      "01:00",
      "02:00",
    ]);
    expect(computeDueTicks(cron, lastFired, 0, at("2026-01-01T00:59:59Z"))).toEqual([]);
  });
});

describe("planDueTicks", () => {
  const T0 = Date.parse("2026-01-01T00:00:00Z");
  const clock = FakeWallClock.create(T0);

  it("isolates a schedule that throws and drops disabled or deleted ones from due-tracking", () => {
    const configs = new Map<string, DurableScheduleConfig>([
      ["ok", { id: "ok", intervalMs: 1_000 }],
      ["bad", { id: "bad", cron: "not a cron" }],
      ["off", { id: "off", intervalMs: 1_000, enabled: false }],
    ]);
    const states = new Map([
      ["ok", { lastFired: null, tickCount: 0 }],
      ["bad", { lastFired: new Date(T0 - 1_000), tickCount: 3 }],
      ["off", { lastFired: null, tickCount: 0 }],
    ]);
    const plans = planDueTicks({ ids: ["ok", "bad", "off", "gone"], configs, states, clock });

    expect(plans.map((p) => [p.id, p.ticks.length, p.commit.nextRun?.getTime() ?? null])).toEqual([
      ["ok", 1, T0 + 1_000],
      ["bad", 0, null],
      ["off", 0, null],
      ["gone", 0, null],
    ]);
    expect(plans[1]!.error).toBeInstanceOf(Error);
    expect(plans.filter((p) => p.error !== undefined).map((p) => p.id)).toEqual(["bad"]);
  });

  it("jitter delays nextRun by random() × jitterMs, never past endAt", () => {
    const configs = new Map<string, DurableScheduleConfig>([
      ["j", { id: "j", intervalMs: 10_000, jitterMs: 4_000 }],
      ["end", { id: "end", intervalMs: 10_000, jitterMs: 4_000, endAt: new Date(T0 + 11_000) }],
    ]);
    const states = new Map([
      ["j", { lastFired: new Date(T0), tickCount: 1 }],
      ["end", { lastFired: new Date(T0), tickCount: 1 }],
    ]);
    const plans = planDueTicks({
      ids: ["j", "end"],
      configs,
      states,
      clock,
      random: () => 0.75,
    });
    expect(plans.map((p) => p.commit.nextRun?.getTime())).toEqual([T0 + 13_000, T0 + 11_000]);
  });
});

// ---------------------------------------------------------------------------
// Due-time math — which ticks fire, and when next, follow the injected clock
// ---------------------------------------------------------------------------

describe("DurableScheduler due-time math follows the injected WallClock", () => {
  // Fixed in the past, so anything that reads the real clock instead of the
  // fake one sees every schedule as long overdue (or never due again).
  const T0 = Date.parse("2026-01-01T00:00:00Z");

  it("an interval tick becomes due exactly when the fake clock passes the interval", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    await scheduler.register({ id: "every-10s", intervalMs: 10_000 });

    const consumer = consume({ scheduler });
    await untilWaiting(clock);

    // The poll at T0 fires the bootstrap tick right away. Polls at
    // T0+1s … T0+9s find nothing due.
    expect(consumer.seen).toHaveLength(1);
    expect(consumer.seen[0]!.scheduledAt.getTime()).toBe(T0);
    expect(consumer.seen[0]!.firedAt.getTime()).toBe(T0);
    await advancePolls({ clock, ms: 1_000, polls: 9 });
    await untilWaiting(clock);
    expect(consumer.seen).toHaveLength(1);

    // The poll at T0+10s finds the next tick due.
    await advancePolls({ clock, ms: 1_000, polls: 1 });
    await untilWaiting(clock);
    await consumer.stop();
    expect(consumer.seen).toHaveLength(2);
    expect(consumer.seen[1]!.scheduledAt.getTime()).toBe(T0 + 10_000);
    expect(consumer.seen[1]!.firedAt.getTime()).toBe(T0 + 10_000);
    expect(consumer.seen[1]!.tickNumber).toBe(1);

    const state = await storage.loadScheduleState("every-10s");
    expect(state).toEqual({ lastFired: new Date(T0 + 10_000), tickCount: 2 });
  });

  it("a cron tick becomes due when the fake clock reaches the next occurrence", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 60_000, clock });
    await scheduler.register({ id: "every-5m", cron: "*/5 * * * *" });

    const consumer = consume({ scheduler });
    await untilWaiting(clock);

    // Bootstrap tick at 00:00, then polls at 00:01 … 00:04 find nothing.
    await advancePolls({ clock, ms: 60_000, polls: 4 });
    await untilWaiting(clock);
    expect(consumer.seen.map((t) => t.scheduledAt.getTime())).toEqual([T0]);

    // Poll at 00:05 fires the 00:05 occurrence.
    await advancePolls({ clock, ms: 60_000, polls: 1 });
    await untilWaiting(clock);
    await consumer.stop();
    expect(consumer.seen.map((t) => t.scheduledAt.getTime())).toEqual([T0, T0 + 5 * 60_000]);
  });

  it("nextRun written by a poll and by update is computed on the fake clock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, pollIntervalMs: 1_000, clock });
    await scheduler.register({ id: "nr", intervalMs: 10_000 });

    const consumer = consume({ scheduler });
    await untilWaiting(clock);
    await consumer.stop();

    // Polled at T0 → nextRun = T0 + 10s.
    expect(await storage.findDue({ now: new Date(T0 + 9_999), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0 + 10_000), limit: 10 })).toEqual(["nr"]);

    // Updated at T0 + 1s → nextRun = T0 + 1s + 3s.
    clock.advance(1_000);
    await scheduler.update("nr", { intervalMs: 3_000 });
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

    const key = schedulerLeaderKey({});
    expect(await storage.tryAcquireLeader({ key, instanceId: "a", ttlMs: 1_000 })).not.toBeNull();
    clock.advance(999);
    expect(await storage.tryAcquireLeader({ key, instanceId: "b", ttlMs: 1_000 })).toBeNull();
    clock.advance(2);
    expect(await storage.tryAcquireLeader({ key, instanceId: "b", ttlMs: 1_000 })).not.toBeNull();
  });
});
