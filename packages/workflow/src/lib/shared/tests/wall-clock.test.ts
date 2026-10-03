// ---------------------------------------------------------------------------
// WallClock tests — FakeWallClock time semantics + scheduling (setTimeout /
// setInterval advance deterministically alongside the clock).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeWallClock, SystemWallClock } from "../wall-clock.ts";

describe("FakeWallClock — time source", () => {
  it("starts at the configured time and advances only when told", () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const start = clock.currentTimeMs();
    expect(clock.now().toISOString()).toBe("2026-01-01T00:00:00.000Z");

    clock.advance(5_000);
    expect(clock.currentTimeMs()).toBe(start + 5_000);
    expect(clock.now().toISOString()).toBe("2026-01-01T00:00:05.000Z");
  });

  it("set() jumps to an absolute time without firing pending callbacks", () => {
    const clock = FakeWallClock.create(0);
    let fired = false;
    clock.setTimeout(() => (fired = true), 100);

    clock.set("2026-06-01T00:00:00Z");
    expect(clock.now().toISOString()).toBe("2026-06-01T00:00:00.000Z");
    // Jumps time forward, but doesn't execute pending callbacks — advance()
    // is the only API that fires them. Keeps `set` a pure time-setter.
    expect(fired).toBe(false);
  });

  it("SystemWallClock tracks wall time (within tolerance)", () => {
    const before = Date.now();
    const mid = SystemWallClock.currentTimeMs();
    const after = Date.now();
    expect(mid).toBeGreaterThanOrEqual(before);
    expect(mid).toBeLessThanOrEqual(after);
  });
});

describe("FakeWallClock — scheduler", () => {
  it("setTimeout fires exactly once at the due time", () => {
    const clock = FakeWallClock.create(0);
    let fired = 0;
    clock.setTimeout(() => fired++, 100);

    clock.advance(50);
    expect(fired).toBe(0);
    expect(clock.currentTimeMs()).toBe(50);

    clock.advance(50);
    expect(fired).toBe(1);
    expect(clock.currentTimeMs()).toBe(100);

    clock.advance(500);
    expect(fired).toBe(1);
  });

  it("setInterval fires every `ms` and re-arms within a single advance", () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    clock.setInterval(() => ticks++, 100);

    clock.advance(350);
    expect(ticks).toBe(3); // 100, 200, 300
    expect(clock.currentTimeMs()).toBe(350);

    clock.advance(150);
    expect(ticks).toBe(5);
  });

  it("clear() cancels a scheduled timeout before it fires", () => {
    const clock = FakeWallClock.create(0);
    let fired = false;
    const handle = clock.setTimeout(() => (fired = true), 100);
    handle.clear();

    clock.advance(1_000);
    expect(fired).toBe(false);
  });

  it("clear() cancels an interval mid-stream", () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const handle = clock.setInterval(() => ticks++, 100);

    clock.advance(250);
    expect(ticks).toBe(2);

    handle.clear();
    clock.advance(1_000);
    expect(ticks).toBe(2);
  });

  it("callbacks observe the clock at their due time, not at advance start", () => {
    const clock = FakeWallClock.create(0);
    const observed: number[] = [];
    clock.setTimeout(() => observed.push(clock.currentTimeMs()), 100);
    clock.setTimeout(() => observed.push(clock.currentTimeMs()), 250);

    clock.advance(500);
    expect(observed).toEqual([100, 250]);
    expect(clock.currentTimeMs()).toBe(500);
  });

  it("concurrent timeouts fire in due-time order", () => {
    const clock = FakeWallClock.create(0);
    const order: string[] = [];
    clock.setTimeout(() => order.push("c"), 300);
    clock.setTimeout(() => order.push("a"), 100);
    clock.setTimeout(() => order.push("b"), 200);

    clock.advance(400);
    expect(order).toEqual(["a", "b", "c"]);
  });

  it("pendingCount reflects still-scheduled callbacks", () => {
    const clock = FakeWallClock.create(0);
    clock.setTimeout(() => {}, 100);
    clock.setInterval(() => {}, 50);
    expect(clock.pendingCount()).toBe(2);

    clock.advance(150);
    expect(clock.pendingCount()).toBe(1); // timeout done, interval re-armed
  });
});

describe("SystemWallClock — real timers", () => {
  it("setTimeout fires via the real event loop", async () => {
    let fired = false;
    SystemWallClock.setTimeout(() => (fired = true), 5);
    await new Promise((r) => globalThis.setTimeout(r, 25));
    expect(fired).toBe(true);
  });

  it("setInterval handle.clear stops real ticks", async () => {
    let ticks = 0;
    const handle = SystemWallClock.setInterval(() => ticks++, 5);
    await new Promise((r) => globalThis.setTimeout(r, 25));
    handle.clear();
    const afterClear = ticks;
    await new Promise((r) => globalThis.setTimeout(r, 25));
    expect(ticks).toBe(afterClear);
    expect(ticks).toBeGreaterThanOrEqual(2);
  });

  it("handles expose unref(); an unref'd timer still fires while the loop is alive", async () => {
    let fired = false;
    const handle = SystemWallClock.setTimeout(() => (fired = true), 5);
    expect(typeof handle.unref).toBe("function");
    handle.unref?.();
    await new Promise((r) => globalThis.setTimeout(r, 25));
    expect(fired).toBe(true);

    const interval = SystemWallClock.setInterval(() => {}, 1_000);
    expect(typeof interval.unref).toBe("function");
    interval.unref?.();
    interval.clear();
  });

  it("FakeWallClock handles omit unref() — fake timers never hold the process", () => {
    const clock = FakeWallClock.create(0);
    expect(clock.setTimeout(() => {}, 10).unref).toBeUndefined();
    expect(clock.setInterval(() => {}, 10).unref).toBeUndefined();
  });
});
