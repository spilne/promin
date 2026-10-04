// ---------------------------------------------------------------------------
// InMemoryScheduler on an injected WallClock — fire times, firedAt stamps,
// the paused recheck and the startAt / endAt waits all follow the clock,
// and a stopped consumer leaves no timer pending on it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryScheduler } from "../in-memory-scheduler.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { ScheduleTick } from "../types.ts";

const T0 = Date.parse("2026-01-01T00:00:00Z");

/**
 * Wait (in real time) until the scheduler has parked at least `count`
 * timers on the fake clock, i.e. it finished its previous step and is
 * waiting for time to move. Advancing earlier would fire nothing.
 */
async function untilWaiting(params: { clock: FakeWallClock; count?: number }): Promise<void> {
  const count = params.count ?? 1;
  const deadline = Date.now() + 4_000;
  while (params.clock.pendingCount() < count) {
    if (Date.now() > deadline) throw new Error("scheduler never waited on the fake clock");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

/** Give a wrongly scheduled delivery a chance to happen before asserting it didn't. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 10));

/** Track delivery of a `take(n)` run so tests can assert "not yet". */
function track(run: Promise<ScheduleTick[]>): {
  result: Promise<ScheduleTick[]>;
  delivered: () => boolean;
} {
  let delivered = false;
  const result = run.then((ticks) => {
    delivered = true;
    return ticks;
  });
  return { result, delivered: () => delivered };
}

describe("InMemoryScheduler — fire times follow the injected WallClock", () => {
  it("interval: waits exactly intervalMs on the clock between ticks", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "iv", intervalMs: 1_000 });

    const run = track(scheduler.stream("iv").take(2).toArray().run());

    await untilWaiting({ clock });
    expect(clock.pendingCount()).toBe(1);
    clock.advance(999);
    await settle();
    expect(run.delivered()).toBe(false);

    clock.advance(1);
    await untilWaiting({ clock });
    clock.advance(1_000);
    const ticks = await run.result;

    expect(ticks.map((t) => t.scheduledAt.getTime())).toEqual([T0 + 1_000, T0 + 2_000]);
    expect(ticks.map((t) => t.firedAt.getTime())).toEqual([T0 + 1_000, T0 + 2_000]);
    expect(ticks.map((t) => t.tickNumber)).toEqual([0, 1]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("cron: fires at the next cron boundary computed from the clock's now", async () => {
    // 30s into the minute — next "every minute" boundary is 30s away.
    const clock = FakeWallClock.create(T0 + 30_000);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "cron", cron: "* * * * *" });

    const run = track(scheduler.stream("cron").take(2).toArray().run());

    await untilWaiting({ clock });
    clock.advance(29_999);
    await settle();
    expect(run.delivered()).toBe(false);

    clock.advance(1);
    await untilWaiting({ clock });
    clock.advance(60_000);
    const ticks = await run.result;

    expect(ticks.map((t) => t.scheduledAt.toISOString())).toEqual([
      "2026-01-01T00:01:00.000Z",
      "2026-01-01T00:02:00.000Z",
    ]);
    expect(ticks.map((t) => t.firedAt.getTime())).toEqual([T0 + 60_000, T0 + 120_000]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("jitter: delays the emission past the nominal fire time", async () => {
    const clock = FakeWallClock.create(T0);
    // random() = 0.5 → a 100ms delay out of the 200ms jitter window.
    const scheduler = new InMemoryScheduler({ clock, random: () => 0.5 });
    await scheduler.register({ id: "jit", intervalMs: 1_000, jitterMs: 200 });

    const run = track(scheduler.stream("jit").take(1).toArray().run());
    await untilWaiting({ clock });
    clock.advance(1_000);
    await settle();
    // The nominal fire time has passed, but the jitter delay hasn't.
    expect(run.delivered()).toBe(false);

    clock.advance(100);
    const [tick] = await run.result;
    expect(tick!.scheduledAt.getTime()).toBe(T0 + 1_000);
    expect(tick!.firedAt.getTime()).toBe(T0 + 1_100);
  });

  it("paused: rechecks on the clock and resumes from the clock's now", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "p", intervalMs: 500, enabled: false });

    const run = track(scheduler.stream("p").take(1).toArray().run());

    // Still paused at the first recheck: parks another recheck timer.
    await untilWaiting({ clock });
    clock.advance(1_000);
    await untilWaiting({ clock });
    expect(run.delivered()).toBe(false);

    await scheduler.resume("p");
    // The recheck sees it resumed at T0+2000 and waits one interval from there.
    clock.advance(1_000);
    await untilWaiting({ clock });
    await settle();
    expect(run.delivered()).toBe(false);
    clock.advance(500);

    const ticks = await run.result;
    expect(ticks.map((t) => t.scheduledAt.getTime())).toEqual([T0 + 2_500]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("startAt: waits on the clock until startAt, then one interval", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "s", intervalMs: 1_000, startAt: new Date(T0 + 5_000) });

    const run = track(scheduler.stream("s").take(1).toArray().run());

    await untilWaiting({ clock });
    clock.advance(5_000);
    await untilWaiting({ clock });
    expect(run.delivered()).toBe(false);
    clock.advance(1_000);

    const ticks = await run.result;
    expect(ticks.map((t) => t.scheduledAt.getTime())).toEqual([T0 + 6_000]);
  });

  it("endAt: ends the stream once the next fire time reaches endAt on the clock", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "e", intervalMs: 1_000, endAt: new Date(T0 + 2_500) });

    const run = scheduler.stream("e").toArray().run();
    await untilWaiting({ clock });
    clock.advance(1_000);
    await untilWaiting({ clock });
    clock.advance(1_000);

    const ticks = await run;
    expect(ticks.map((t) => t.scheduledAt.getTime())).toEqual([T0 + 1_000, T0 + 2_000]);
    expect(clock.pendingCount()).toBe(0);
  });
});

describe("InMemoryScheduler — stopping a consumer clears its clock timers", () => {
  it("interrupting a single-schedule stream mid-wait leaves nothing pending", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "never", intervalMs: 60_000 });

    // The fake clock never advances, so the stream is parked on its timer
    // when interruptAfter (real time) stops it.
    const ticks = await scheduler.stream("never").interruptAfter(30).toArray().run();

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("interrupting a paused schedule's recheck wait leaves nothing pending", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "paused", intervalMs: 1_000, enabled: false });

    const ticks = await scheduler.stream("paused").interruptAfter(30).toArray().run();

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("breaking out of a merged subscribe() clears every schedule's timer", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "fast", intervalMs: 1_000 });
    await scheduler.register({ id: "slow", intervalMs: 60_000 });

    const consumed = (async () => {
      const seen: string[] = [];
      for await (const tick of scheduler.subscribe().toAsyncIterable()) {
        seen.push(tick.scheduleId);
        break;
      }
      return seen;
    })();

    await untilWaiting({ clock, count: 2 });
    clock.advance(1_000);

    expect(await consumed).toEqual(["fast"]);
    expect(clock.pendingCount()).toBe(0);
  });
});

describe("InMemoryScheduler — changes made while a stream waits", () => {
  it("pause() during the wait suppresses the pending tick", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "y", intervalMs: 1_000 });

    const ticks: ScheduleTick[] = [];
    const run = scheduler
      .stream("y")
      .tap((t) => void ticks.push(t))
      .interruptAfter(200)
      .drain()
      .run();
    await untilWaiting({ clock });
    await scheduler.pause("y");
    clock.advance(1_000);
    await settle();
    await run;

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("unregister() during the wait ends the stream without emitting", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "gone", intervalMs: 1_000 });

    const run = scheduler.stream("gone").toArray().run();
    await untilWaiting({ clock });
    await scheduler.unregister({ scheduleId: "gone" });
    clock.advance(1_000);

    expect(await run).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("re-registering during the wait restarts the wait with the new config", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "r", intervalMs: 1_000, metadata: { v: 1 } });

    const run = track(scheduler.stream("r").take(1).toArray().run());
    await untilWaiting({ clock });
    await scheduler.register({ id: "r", intervalMs: 5_000, metadata: { v: 2 } });
    // The old 1s wait fires but emits nothing; the stream re-waits 5s from T0+1s.
    clock.advance(1_000);
    await untilWaiting({ clock });
    await settle();
    expect(run.delivered()).toBe(false);
    clock.advance(5_000);

    const [tick] = await run.result;
    expect(tick!.scheduledAt.getTime()).toBe(T0 + 6_000);
    expect(tick!.metadata).toEqual({ v: 2 });
  });

  it("re-registering while subscribed doesn't start a second stream for the schedule", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = new InMemoryScheduler({ clock });
    await scheduler.register({ id: "x", intervalMs: 1_000 });

    const ticks: ScheduleTick[] = [];
    const run = scheduler
      .subscribe()
      .tap((t) => void ticks.push(t))
      .take(3)
      .drain()
      .run();
    await untilWaiting({ clock });
    await scheduler.register({ id: "x", intervalMs: 1_000 });
    for (let i = 0; i < 4; i++) {
      await untilWaiting({ clock });
      expect(clock.pendingCount()).toBe(1);
      clock.advance(1_000);
      if (ticks.length === 3) break;
      await settle();
    }
    await run;

    expect(ticks.map((t) => t.scheduledAt.getTime())).toEqual([T0 + 2_000, T0 + 3_000, T0 + 4_000]);
  });
});
