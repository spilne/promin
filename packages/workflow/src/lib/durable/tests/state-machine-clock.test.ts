// ---------------------------------------------------------------------------
// State machine timers, rate limit and retry backoff run on the injected
// clock — in-process timeouts fire on FakeWallClock.advance(), the
// per-second rate window slides with the clock, and transition / middleware
// retry waits are clock timers.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { retryMiddleware, stateMachine } from "../state-machine.ts";
import { InMemoryStateMachineStorage } from "../state-machine-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00Z";

type ApprovalStates = {
  pending: { context: { item: string }; transitions: { approve: "approved" } };
  approved: { context: { item: string }; transitions: {} };
  timedOut: { context: { item: string }; transitions: {} };
};

type Toggle = {
  off: { context: { n: number }; transitions: { flip: "on" } };
  on: { context: { n: number }; transitions: { flip: "off" } };
};

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 1_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

describe("state timeouts — auto-scheduled timers use the clock", () => {
  it("schedules the timeout on the clock and fires it on advance()", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const m = stateMachine<ApprovalStates>({ name: "approval-clock", storage, clock })
      .state("pending", { timeout: { ms: 100, target: "timedOut" } })
      .state("approved", { terminal: true })
      .state("timedOut", { terminal: true })
      .on("approve", { from: "pending", to: "approved" })
      .initial("pending")
      .build();

    await m.start({ id: "a-1", context: { item: "x" } });
    expect(clock.pendingCount()).toBe(1);

    clock.advance(99);
    expect((await m.getState("a-1"))!.current).toBe("pending");

    clock.advance(1);
    await waitFor(async () => (await m.getState("a-1"))!.current === "timedOut");
    expect(clock.pendingCount()).toBe(0);
  });

  it("an explicit transition cancels the pending clock timer", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const m = stateMachine<ApprovalStates>({ name: "approval-cancel", storage, clock })
      .state("pending", { timeout: { ms: 100, target: "timedOut" } })
      .state("approved", { terminal: true })
      .state("timedOut", { terminal: true })
      .on("approve", { from: "pending", to: "approved" })
      .initial("pending")
      .build();

    await m.start({ id: "a-2", context: { item: "x" } });
    expect(clock.pendingCount()).toBe(1);
    await m.send({ id: "a-2", event: "approve" });
    expect(clock.pendingCount()).toBe(0);

    clock.advance(500);
    expect((await m.getState("a-2"))!.current).toBe("approved");
  });

  it("cancelAllTimeouts() clears the clock timers", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const m = stateMachine<ApprovalStates>({ name: "approval-cancel-all", storage, clock })
      .state("pending", { timeout: { ms: 100, target: "timedOut" } })
      .state("approved", { terminal: true })
      .state("timedOut", { terminal: true })
      .on("approve", { from: "pending", to: "approved" })
      .initial("pending")
      .build();

    await m.start({ id: "a-3", context: { item: "x" } });
    await m.start({ id: "a-4", context: { item: "y" } });
    expect(clock.pendingCount()).toBe(2);
    m.cancelAllTimeouts();
    expect(clock.pendingCount()).toBe(0);
  });
});

describe("maxTransitionsPerSecond — window slides with the clock", () => {
  it("rejects the third send inside one clock second and allows it after the window", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const m = stateMachine<Toggle>({
      name: "rated-clock",
      storage,
      clock,
      limits: { maxTransitionsPerSecond: 2 },
    })
      .state("off")
      .state("on")
      .on("flip", { from: "off", to: "on" })
      .on("flip", { from: "on", to: "off" })
      .initial("off")
      .build();

    await m.start({ id: "r-1", context: { n: 0 } });
    await m.send({ id: "r-1", event: "flip" });
    await m.send({ id: "r-1", event: "flip" });
    await expect(m.send({ id: "r-1", event: "flip" })).rejects.toThrow("exceeded rate limit");

    // Still inside the 1s window on the fake clock.
    clock.advance(999);
    await expect(m.send({ id: "r-1", event: "flip" })).rejects.toThrow("exceeded rate limit");

    clock.advance(1);
    await m.send({ id: "r-1", event: "flip" });
    expect((await m.getState("r-1"))!.current).toBe("on");
  });
});

describe("retry backoff — transition and middleware retries wait on the clock", () => {
  it("transition `retry` sleeps on clock timers between attempts", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    let attempts = 0;
    const m = stateMachine<Toggle>({ name: "retry-clock", storage, clock })
      .state("off")
      .state("on")
      .on("flip", {
        from: "off",
        to: "on",
        retry: { maxRetries: 2, baseDelayMs: 500 },
        action: (ctx: { n: number }) => {
          attempts++;
          if (attempts < 2) throw new Error("transient");
          return { n: ctx.n + 1 };
        },
      })
      .on("flip", { from: "on", to: "off" })
      .initial("off")
      .build();

    await m.start({ id: "rt-1", context: { n: 0 } });
    const sent = m.send({ id: "rt-1", event: "flip" });

    // The backoff timer plus the send's lock heartbeat.
    await waitFor(() => clock.pendingCount() === 2);
    expect(attempts).toBe(1);
    clock.advance(499);
    expect(attempts).toBe(1);
    clock.advance(1);

    await sent;
    expect(attempts).toBe(2);
    expect(await m.getState("rt-1")).toEqual({ current: "on", context: { n: 1 } });
  });

  it("retryMiddleware takes the clock for backoff and the time budget", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    let calls = 0;
    const m = stateMachine<Toggle>({ name: "retry-mw-clock", storage, clock })
      .use(retryMiddleware({ maxRetries: 5, baseDelayMs: 1_000, timeBudgetMs: 1_500, clock }))
      .use(async (_ctx, next) => {
        calls++;
        if (calls < 10) throw new Error("always failing");
        await next();
      })
      .state("off")
      .state("on")
      .on("flip", { from: "off", to: "on" })
      .on("flip", { from: "on", to: "off" })
      .initial("off")
      .build();

    await m.start({ id: "mw-1", context: { n: 0 } });
    const sent = m.send({ id: "mw-1", event: "flip" });
    const outcome = sent.then(
      () => "resolved",
      (err: Error) => err.message,
    );

    // Pending timers: the backoff plus the send's lock heartbeat.
    // 1st failure → 1s backoff on the clock.
    await waitFor(() => clock.pendingCount() === 2);
    clock.advance(1_000);
    // 2nd failure → 2s backoff.
    await waitFor(() => calls === 2 && clock.pendingCount() === 2);
    clock.advance(2_000);
    // 3rd failure happens 3s in — past the 1.5s budget, so no further retry.
    expect(await outcome).toBe("always failing");
    expect(calls).toBe(3);
    expect(clock.pendingCount()).toBe(0);
  });
});

describe("lock heartbeat — a send's lock is extended on the clock", () => {
  type Slow = {
    idle: { context: { n: number }; transitions: { work: "done" } };
    done: { context: { n: number }; transitions: {} };
  };

  function slowMachine(params: { clock: FakeWallClock; storage: InMemoryStateMachineStorage }) {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    let entered = false;
    const m = stateMachine<Slow>({ name: "slow", storage: params.storage, clock: params.clock })
      .state("idle")
      .state("done", { terminal: true })
      .on("work", {
        from: "idle",
        to: "done",
        action: async (ctx: { n: number }) => {
          entered = true;
          await gate;
          return { n: ctx.n + 1 };
        },
      })
      .initial("idle")
      .build();
    return { m, release: () => release(), entered: () => entered };
  }

  it("a send that outlives the 30s lease keeps its lock and saves", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const { m, release, entered } = slowMachine({ clock, storage });
    await m.start({ id: "hb-1", context: { n: 0 } });

    const sent = m.send({ id: "hb-1", event: "work" });
    await waitFor(entered);
    // Two minutes in 10s beats: each beat pushes the lease 30s ahead.
    for (let i = 0; i < 12; i++) {
      clock.advance(10_000);
      await Promise.resolve();
    }
    expect(await storage.tryLock({ id: "hb-1", durationMs: 30_000 })).toBeNull();

    release();
    await sent;
    expect(await m.getState("hb-1")).toEqual({ current: "done", context: { n: 1 } });
    // Released after the send, and the heartbeat stopped with it.
    expect(clock.pendingCount()).toBe(0);
    expect(await storage.tryLock({ id: "hb-1", durationMs: 30_000 })).not.toBeNull();
  });

  it("a send whose heartbeat finds the lock lost does not save", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    storage.extendLock = async () => false;
    const { m, release, entered } = slowMachine({ clock, storage });
    await m.start({ id: "hb-2", context: { n: 0 } });

    const sent = m.send({ id: "hb-2", event: "work" });
    const outcome = sent.then(
      () => "resolved",
      (err: Error) => err.message,
    );
    await waitFor(entered);
    clock.advance(10_000);
    await Promise.resolve();

    release();
    expect(await outcome).toBe("Machine hb-2 lock expired before the transition was saved");
    expect(await m.getState("hb-2")).toEqual({ current: "idle", context: { n: 0 } });
    expect(await m.getHistory("hb-2")).toEqual([]);
  });

  it("a holder whose lease lapsed can neither extend nor release the next holder's lock", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    const stale = await storage.tryLock({ id: "hb-3", durationMs: 30_000 });
    clock.advance(30_000);
    const current = await storage.tryLock({ id: "hb-3", durationMs: 30_000 });
    expect(current).not.toBeNull();

    expect(await storage.extendLock({ id: "hb-3", token: stale!, durationMs: 30_000 })).toBe(false);
    await storage.releaseLock({ id: "hb-3", token: stale! });
    expect(await storage.tryLock({ id: "hb-3", durationMs: 30_000 })).toBeNull();
  });
});
