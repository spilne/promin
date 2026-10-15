import { describe, it, expect } from "bun:test";
import { PollLoop, type PollLoopErrorInfo, type PollTickResult } from "../poll-loop.ts";
import { FakeWallClock } from "../wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !predicate(); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(predicate()).toBe(true);
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

describe("PollLoop", () => {
  it("waits intervalMs of clock time between iterations", async () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 100,
      clock,
      tick: async () => {
        ticks++;
      },
    });

    const done = loop.start();
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);
    clock.advance(99);
    expect(ticks).toBe(1);
    clock.advance(1);
    await waitFor(() => ticks === 2 && clock.pendingCount() === 1);

    await loop.stop();
    await done;
    expect(clock.pendingCount()).toBe(0);
  });

  it("a throwing iteration is reported and backed off, and the loop keeps going", async () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const errors: PollLoopErrorInfo[] = [];
    const loop = new PollLoop({
      name: "flaky",
      intervalMs: 100,
      maxBackoffMs: 300,
      clock,
      tick: async () => {
        ticks++;
        if (ticks <= 4) throw new Error(`blip ${ticks}`);
      },
      onError: (_err, info) => errors.push(info),
    });

    void loop.start();
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);
    expect(errors.map((e) => e.nextDelayMs)).toEqual([100]);

    clock.advance(100);
    await waitFor(() => ticks === 2 && clock.pendingCount() === 1);
    expect(errors.map((e) => e.nextDelayMs)).toEqual([100, 200]);

    clock.advance(199);
    expect(ticks).toBe(2);
    clock.advance(1);
    await waitFor(() => ticks === 3 && clock.pendingCount() === 1);
    // Capped at maxBackoffMs.
    clock.advance(300);
    await waitFor(() => ticks === 4 && clock.pendingCount() === 1);
    expect(errors.map((e) => e.nextDelayMs)).toEqual([100, 200, 300, 300]);
    expect(errors.map((e) => e.consecutiveFailures)).toEqual([1, 2, 3, 4]);
    expect(errors[0]!.name).toBe("flaky");

    // Recovery resets the cadence to intervalMs.
    clock.advance(300);
    await waitFor(() => ticks === 5 && clock.pendingCount() === 1);
    clock.advance(100);
    await waitFor(() => ticks === 6);

    await loop.stop();
  });

  it("a throwing onError hook doesn't take the loop down", async () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 10,
      clock,
      tick: async () => {
        ticks++;
        throw new Error("tick failed");
      },
      onError: () => {
        throw new Error("hook failed too");
      },
    });

    void loop.start();
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);
    clock.advance(10);
    await waitFor(() => ticks === 2 && clock.pendingCount() === 1);
    await loop.stop();
  });

  it("stop() cancels a pending wait and resolves without advancing the clock", async () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 60_000,
      clock,
      tick: async () => {
        ticks++;
      },
    });

    const done = loop.start();
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);
    await loop.stop();
    await done;
    expect(clock.pendingCount()).toBe(0);
    expect(loop.running).toBe(false);
    expect(ticks).toBe(1);
  });

  it("stop() waits for the in-flight iteration to finish", async () => {
    const clock = FakeWallClock.create(0);
    const gate = deferred();
    let finished = false;
    let started = false;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 10,
      clock,
      tick: async () => {
        started = true;
        await gate.promise;
        finished = true;
      },
    });

    void loop.start();
    await waitFor(() => started);
    let stopped = false;
    const stopping = loop.stop().then(() => (stopped = true));
    await new Promise<void>((r) => setImmediate(r));
    expect(stopped).toBe(false);

    gate.resolve();
    await stopping;
    expect(finished).toBe(true);
    expect(clock.pendingCount()).toBe(0);
  });

  it("wake() cuts the wait short; a wake during an iteration is not lost", async () => {
    const clock = FakeWallClock.create(0);
    const gate = deferred();
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 60_000,
      clock,
      tick: async () => {
        ticks++;
        if (ticks === 2) await gate.promise;
      },
    });

    void loop.start();
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);

    loop.wake();
    await waitFor(() => ticks === 2);
    // Wake while iteration 2 is still running: the next wait is skipped.
    loop.wake();
    gate.resolve();
    await waitFor(() => ticks === 3 && clock.pendingCount() === 1);
    expect(clock.currentTimeMs()).toBe(0);

    await loop.stop();
  });

  it('"again" re-runs at once and "stop" ends the loop', async () => {
    const clock = FakeWallClock.create(0);
    const results: PollTickResult[] = ["again", "again", "stop"];
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 1_000,
      clock,
      tick: async () => results[ticks++],
    });

    await loop.start();
    expect(ticks).toBe(3);
    expect(clock.currentTimeMs()).toBe(0);
    expect(clock.pendingCount()).toBe(0);
  });

  it("start() while running returns the same loop; it can restart after stop()", async () => {
    const clock = FakeWallClock.create(0);
    let ticks = 0;
    const loop = new PollLoop({
      name: "t",
      intervalMs: 1_000,
      clock,
      tick: async () => {
        ticks++;
      },
    });

    const a = loop.start();
    const b = loop.start();
    expect(a).toBe(b);
    await waitFor(() => ticks === 1 && clock.pendingCount() === 1);
    await loop.stop();

    void loop.start();
    await waitFor(() => ticks === 2 && clock.pendingCount() === 1);
    await loop.stop();
    expect(clock.pendingCount()).toBe(0);
  });
});
