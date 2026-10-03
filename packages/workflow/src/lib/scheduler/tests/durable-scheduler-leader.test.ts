// ---------------------------------------------------------------------------
// DurableScheduler — leader leases, fencing, handover, manual-fire numbering
// and fire-time previews. Time runs on a FakeWallClock throughout.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Stream } from "@spilne/perfect-core";
import { DurableScheduler, planDueTicks, type SchedulerErrorEvent } from "../durable-scheduler.ts";
import { InMemorySchedulerStorage } from "../in-memory-scheduler-storage.ts";
import { isStaleLeaseError, schedulerLeaderKey } from "../leader-lease.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { ScheduleTick } from "../types.ts";

const T0 = Date.parse("2026-01-01T00:00:00Z");

/** Wait (in real time) until at least `count` timers are pending on the fake clock. */
async function untilPending(clock: FakeWallClock, count: number): Promise<void> {
  const deadline = Date.now() + 4_000;
  while (clock.pendingCount() < count) {
    if (Date.now() > deadline) throw new Error(`fewer than ${count} timers ever pending`);
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

/** Drain a scheduler stream in the background; `stop()` ends it and waits for its finalizers. */
function consume(params: { scheduler: DurableScheduler }) {
  const seen: ScheduleTick[] = [];
  let fire: () => void = () => {};
  const signal = Stream.fromCallback<void>((emit) => {
    fire = () => emit(undefined);
  });
  const done = params.scheduler
    .stream()
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

/** Storage view whose first `commitPoll` waits until `resume()` is called. */
function pausedAtFirstCommit(storage: InMemorySchedulerStorage) {
  let resume!: () => void;
  const gate = new Promise<void>((r) => (resume = r));
  let entered!: () => void;
  const commitEntered = new Promise<void>((r) => (entered = r));
  let paused = false;
  const view = new Proxy(storage, {
    get(target, prop) {
      if (prop === "commitPoll") {
        return async (params: Parameters<InMemorySchedulerStorage["commitPoll"]>[0]) => {
          if (!paused) {
            paused = true;
            entered();
            await gate;
          }
          return target.commitPoll(params);
        };
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { view, commitEntered, resume };
}

describe("DurableScheduler leader election", () => {
  it("a leader paused past its TTL can't commit: no tick number is reused for another occurrence", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const paused = pausedAtFirstCommit(storage);
    const errorsA: SchedulerErrorEvent[] = [];
    const common = { pollIntervalMs: 1_000, leaderLockTtlMs: 3_000, clock };
    const a = new DurableScheduler({
      ...common,
      storage: paused.view,
      instanceId: "A",
      onError: (e) => void errorsA.push(e),
    });
    const b = new DurableScheduler({ ...common, storage, instanceId: "B" });
    // Deep catch-up keeps the numbering deterministic: tick k <-> T0 + (k-1)s.
    await a.register({ id: "s", intervalMs: 1_000, maxCatchUp: 10 });
    await storage.recordFire("s", new Date(T0 - 1_000));

    // A leads, delivers tick 1 (T0), then stalls inside its commit.
    const consumerA = consume({ scheduler: a });
    await paused.commitEntered;
    const consumerB = consume({ scheduler: b });

    // B takes over once A's lease lapses at T0 + 3s, then keeps leading.
    for (let i = 0; i < 4; i++) {
      await untilPending(clock, 1);
      clock.advance(1_000);
    }
    await untilPending(clock, 1);

    // A wakes up and tries to commit its stale poll.
    paused.resume();
    await untilPending(clock, 2);
    expect(errorsA.map((e) => [e.phase, isStaleLeaseError(e.error)])).toEqual([["commit", true]]);

    clock.advance(1_000);
    await untilPending(clock, 2);
    await consumerA.stop();
    await consumerB.stop();

    // Redelivering a tick under its own number is fine; one number for two
    // occurrences (or two numbers for one occurrence) is not.
    const byNumber = new Map<number, Set<number>>();
    const byOccurrence = new Map<number, Set<number>>();
    for (const t of [...consumerA.seen, ...consumerB.seen]) {
      const at = t.scheduledAt.getTime();
      byNumber.set(t.tickNumber, (byNumber.get(t.tickNumber) ?? new Set()).add(at));
      byOccurrence.set(at, (byOccurrence.get(at) ?? new Set()).add(t.tickNumber));
    }
    expect([...byNumber.values()].every((s) => s.size === 1)).toBe(true);
    expect([...byOccurrence.values()].every((s) => s.size === 1)).toBe(true);

    expect(consumerA.seen.map((t) => t.tickNumber)).toEqual([1]);
    expect(consumerB.seen.map((t) => [t.tickNumber, t.scheduledAt.getTime() - T0])).toEqual([
      [1, 0],
      [2, 1_000],
      [3, 2_000],
      [4, 3_000],
      [5, 4_000],
      [6, 5_000],
    ]);
    expect((await storage.loadScheduleState("s"))?.tickCount).toBe(7);
  });

  it("stopping the leader's stream releases its lease: another instance takes over at once", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const common = { storage, pollIntervalMs: 1_000, leaderLockTtlMs: 60_000, clock };
    const a = new DurableScheduler({ ...common, instanceId: "A" });
    const b = new DurableScheduler({ ...common, instanceId: "B" });
    await a.register({ id: "x", intervalMs: 60_000 });
    await a.register({ id: "y", intervalMs: 60_000 });

    const fromA = await a.stream().take(1).toArray().run();
    expect(fromA.map((t) => t.scheduleId)).toEqual(["x"]);

    // No clock movement: B's first poll only succeeds if A handed the lease over.
    const fromB = await b.stream().take(2).toArray().run();
    expect(fromB.map((t) => t.scheduleId)).toEqual(["x", "y"]);
  });

  it("a stream that stops while another stream of the same instance runs keeps the lease", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const a = new DurableScheduler({ storage, clock, pollIntervalMs: 1_000, instanceId: "A" });
    await a.register({ id: "x", intervalMs: 60_000 });
    const key = schedulerLeaderKey({});

    const long = consume({ scheduler: a });
    const short = consume({ scheduler: a });
    await untilPending(clock, 2);
    await short.stop();
    expect(await storage.tryAcquireLeader({ key, instanceId: "B", ttlMs: 1 })).toBeNull();

    await long.stop();
    expect(await storage.tryAcquireLeader({ key, instanceId: "B", ttlMs: 1 })).not.toBeNull();
  });

  it("namespaced schedulers lead their namespaces independently", async () => {
    const storage = new InMemorySchedulerStorage();
    const nsA = new DurableScheduler({ storage, pollIntervalMs: 25, namespace: "a" });
    const nsB = new DurableScheduler({ storage, pollIntervalMs: 25, namespace: "b" });
    await nsA.register({ id: "in-a", intervalMs: 60_000 });
    await nsB.register({ id: "in-b", intervalMs: 60_000 });

    const [ta, tb] = await Promise.all([
      nsA.stream().take(1).toArray().run(),
      nsB.stream().take(1).toArray().run(),
    ]);
    expect([ta[0]!.scheduleId, tb[0]!.scheduleId]).toEqual(["in-a", "in-b"]);
  });
});

describe("DurableScheduler manual fires", () => {
  it("concurrent triggerNow calls take distinct tick numbers", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, clock });
    await scheduler.register({ id: "m", intervalMs: 60_000 });

    const ticks = await Promise.all(Array.from({ length: 5 }, () => scheduler.triggerNow("m")));

    expect(ticks.map((t) => t!.tickNumber).sort()).toEqual([0, 1, 2, 3, 4]);
    expect((await storage.loadScheduleState("m"))?.tickCount).toBe(5);
    expect(await storage.countTicks({ scheduleId: "m" })).toBe(5);
  });

  it("a poll planned before a manual fire doesn't commit over it", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, clock, pollIntervalMs: 1_000 });
    await scheduler.register({ id: "m", intervalMs: 60_000 });

    // The poll plans tick 0, then a manual fire takes number 0 before it commits.
    const states = await storage.loadScheduleStates(["m"]);
    const configs = await storage.loadSchedules(["m"]);
    const plans = planDueTicks({ ids: ["m"], configs, states, clock });
    const manual = await scheduler.triggerNow("m");
    const result = await storage.commitPoll({ updates: plans.map((p) => p.commit) });

    expect(manual?.tickNumber).toBe(0);
    expect(result.conflicts).toEqual(["m"]);
    expect((await storage.loadScheduleState("m"))?.tickCount).toBe(1);
  });

  it("triggerNow leaves nextRun alone and returns null for unknown schedules", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, clock });
    await scheduler.register({ id: "m", intervalMs: 60_000 });
    await storage.setNextRun("m", new Date(T0 + 30_000));

    await scheduler.triggerNow("m");
    expect(await storage.findDue({ now: new Date(T0 + 29_999), limit: 10 })).toEqual([]);
    expect(await storage.findDue({ now: new Date(T0 + 30_000), limit: 10 })).toEqual(["m"]);
    expect(await scheduler.triggerNow("missing")).toBeNull();
  });

  it("backfill numbers its ticks after the current count", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, clock });
    await scheduler.register({ id: "h", cron: "0 * * * *" });
    await scheduler.triggerNow("h");

    const hour = 3_600_000;
    const ticks = await scheduler.backfill("h", {
      from: new Date(T0 - 3 * hour),
      to: new Date(T0),
    });
    expect(ticks.map((t) => [t.tickNumber, (t.scheduledAt.getTime() - T0) / hour])).toEqual([
      [1, -2],
      [2, -1],
    ]);
    expect((await storage.loadScheduleState("h"))?.tickCount).toBe(3);
  });
});

describe("DurableScheduler nextFireTimes", () => {
  function setup() {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemorySchedulerStorage({ clock });
    const scheduler = new DurableScheduler({ storage, clock });
    return { storage, scheduler };
  }
  const offsets = (times: Date[], unit = 1) => times.map((t) => (t.getTime() - T0) / unit);

  it("interval schedules continue the cadence from the last fire", async () => {
    const { storage, scheduler } = setup();
    await scheduler.register({ id: "i", intervalMs: 10_000 });
    await storage.recordFire("i", new Date(T0 - 4_000));

    expect(offsets(await scheduler.nextFireTimes("i", 3))).toEqual([6_000, 16_000, 26_000]);
  });

  it("a never-fired interval schedule starts now, or at startAt", async () => {
    const { scheduler } = setup();
    await scheduler.register({ id: "now", intervalMs: 5_000 });
    await scheduler.register({ id: "later", intervalMs: 5_000, startAt: new Date(T0 + 60_000) });

    expect(offsets(await scheduler.nextFireTimes("now", 2))).toEqual([0, 5_000]);
    expect(offsets(await scheduler.nextFireTimes("later", 2))).toEqual([60_000, 65_000]);
  });

  it("cron and rrule previews start at startAt and stop before endAt", async () => {
    const { scheduler } = setup();
    const hour = 3_600_000;
    const window = { startAt: new Date(T0 + 5 * hour), endAt: new Date(T0 + 7 * hour) };
    await scheduler.register({ id: "c", cron: "0 * * * *", ...window });
    await scheduler.register({
      id: "r",
      rrule: "DTSTART:20260101T000000Z\nRRULE:FREQ=HOURLY",
      ...window,
    });

    expect(offsets(await scheduler.nextFireTimes("c", 10), hour)).toEqual([5, 6]);
    expect(offsets(await scheduler.nextFireTimes("r", 10), hour)).toEqual([5, 6]);
  });

  it("interval previews stop before endAt", async () => {
    const { scheduler } = setup();
    await scheduler.register({ id: "done", intervalMs: 1_000, endAt: new Date(T0 + 2_500) });
    expect(offsets(await scheduler.nextFireTimes("done", 5))).toEqual([0, 1_000, 2_000]);
  });
});
