// ---------------------------------------------------------------------------
// InMemoryScheduler on an injected WallClock — fire times, firedAt stamps,
// the paused recheck and the startAt / endAt waits all follow the clock,
// and a stopped consumer leaves no timer pending on it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { createScheduler } from "../in-memory-scheduler.ts";
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
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "iv", intervalMs: 1_000 });

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
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "cron", cron: "* * * * *" });

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

  it("jitter: firedAt is the clock's time plus at most jitterMs", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "jit", intervalMs: 1_000, jitterMs: 200 });

    const run = scheduler.stream("jit").take(1).toArray().run();
    await untilWaiting({ clock });
    clock.advance(1_000);
    const [tick] = await run;

    expect(tick!.scheduledAt.getTime()).toBe(T0 + 1_000);
    const offset = tick!.firedAt.getTime() - (T0 + 1_000);
    expect(offset).toBeGreaterThanOrEqual(0);
    expect(offset).toBeLessThan(200);
  });

  it("paused: rechecks on the clock and resumes from the clock's now", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "p", intervalMs: 500, enabled: false });

    const run = track(scheduler.stream("p").take(1).toArray().run());

    // Still paused at the first recheck: parks another recheck timer.
    await untilWaiting({ clock });
    clock.advance(1_000);
    await untilWaiting({ clock });
    expect(run.delivered()).toBe(false);

    scheduler.resume("p");
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
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "s", intervalMs: 1_000, startAt: new Date(T0 + 5_000) });

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
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "e", intervalMs: 1_000, endAt: new Date(T0 + 2_500) });

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
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "never", intervalMs: 60_000 });

    // The fake clock never advances, so the stream is parked on its timer
    // when interruptAfter (real time) stops it.
    const ticks = await scheduler.stream("never").interruptAfter(30).toArray().run();

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("interrupting a paused schedule's recheck wait leaves nothing pending", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "paused", intervalMs: 1_000, enabled: false });

    const ticks = await scheduler.stream("paused").interruptAfter(30).toArray().run();

    expect(ticks).toEqual([]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("breaking out of a merged subscribe() clears every schedule's timer", async () => {
    const clock = FakeWallClock.create(T0);
    const scheduler = createScheduler({ clock });
    scheduler.register({ id: "fast", intervalMs: 1_000 });
    scheduler.register({ id: "slow", intervalMs: 60_000 });

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
