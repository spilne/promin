// ---------------------------------------------------------------------------
// State machine behaviour when a send does not go through cleanly:
//   - a rejected send keeps the state's timeout armed,
//   - a hook failure after the transition was saved does not re-run the
//     saved transition under retrying middleware,
//   - the per-second rate limit is counted per machine id.
// All timing runs on FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { retryMiddleware, stateMachine } from "../state-machine.ts";
import { InMemoryStateMachineStorage } from "../state-machine-storage.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00Z";

type Approval = {
  pending: { context: {}; transitions: { approve: "approved"; noop: "pending" } };
  approved: { context: {}; transitions: {} };
  timedOut: { context: {}; transitions: {} };
};

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 1_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

function approvalMachine(params: {
  clock: FakeWallClock;
  guard?: () => boolean | Promise<boolean>;
}) {
  const storage = new InMemoryStateMachineStorage({ clock: params.clock });
  const m = stateMachine<Approval>({ name: "approval", storage, clock: params.clock })
    .state("pending", { timeout: { ms: 100, target: "timedOut" } })
    .state("approved", { terminal: true })
    .state("timedOut", { terminal: true })
    .on("approve", { from: "pending", to: "approved", guard: params.guard })
    .initial("pending")
    .build();
  return { m, storage };
}

describe("rejected send keeps the state timeout", () => {
  it("a guard rejection leaves the timeout armed", async () => {
    const clock = FakeWallClock.create(T0);
    const { m } = approvalMachine({ clock, guard: () => false });
    await m.start({ id: "g", context: {} });

    clock.advance(40);
    await expect(m.send({ id: "g", event: "approve" })).rejects.toThrow("Guard rejected");
    expect(clock.pendingCount()).toBe(1);

    // Still due at the original time, not re-armed for a full period.
    clock.advance(60);
    await waitFor(async () => (await m.getState("g"))!.current === "timedOut");
  });

  it("an event with no matching transition leaves the timeout armed", async () => {
    const clock = FakeWallClock.create(T0);
    const { m } = approvalMachine({ clock });
    await m.start({ id: "n", context: {} });

    await expect(m.send({ id: "n", event: "unknown" as "approve" })).rejects.toThrow(
      "No transition",
    );
    clock.advance(100);
    await waitFor(async () => (await m.getState("n"))!.current === "timedOut");
  });

  it("a timeout that came due while a failing send held the lock still fires", async () => {
    const clock = FakeWallClock.create(T0);
    let releaseGuard!: (allowed: boolean) => void;
    const guardGate = new Promise<boolean>((r) => (releaseGuard = r));
    const { m } = approvalMachine({ clock, guard: () => guardGate });
    await m.start({ id: "busy", context: {} });

    const send = m.send({ id: "busy", event: "approve" });
    await waitFor(() => true);
    // The timer fires while the send holds the lock — the firing is skipped.
    clock.advance(100);
    await waitFor(() => true);
    expect((await m.getState("busy"))!.current).toBe("pending");

    releaseGuard(false);
    await expect(send).rejects.toThrow("Guard rejected");

    // Re-armed with the time it had left (zero).
    clock.advance(0);
    await waitFor(async () => (await m.getState("busy"))!.current === "timedOut");
  });

  it("a committed send still replaces the timeout when onEnter throws", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    type Flow = {
      a: { context: {}; transitions: { go: "b" } };
      b: { context: {}; transitions: {} };
      late: { context: {}; transitions: {} };
    };
    const m = stateMachine<Flow>({ name: "flow", storage, clock })
      .state("a", { timeout: { ms: 50, target: "late" } })
      .state("b", {
        timeout: { ms: 200, target: "late" },
        onEnter: () => {
          throw new Error("hook down");
        },
      })
      .state("late", { terminal: true })
      .on("go", { from: "a", to: "b" })
      .initial("a")
      .build();

    await m.start({ id: "c", context: {} });
    await expect(m.send({ id: "c", event: "go" })).rejects.toThrow("hook down");
    expect((await m.getState("c"))!.current).toBe("b");

    // The old state's 50ms timeout is gone; b's 200ms timeout is armed.
    clock.advance(199);
    await waitFor(() => true);
    expect((await m.getState("c"))!.current).toBe("b");
    clock.advance(1);
    await waitFor(async () => (await m.getState("c"))!.current === "late");
  });
});

describe("retrying middleware after the transition was saved", () => {
  it("re-runs only onEnter; the send succeeds with one event", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    let exits = 0;
    let enters = 0;
    const m = stateMachine<any>({ name: "retry", storage, clock })
      .use(retryMiddleware({ maxRetries: 2, baseDelayMs: 10, clock }))
      .state("a", { onExit: () => void exits++ })
      .state("b", {
        terminal: true,
        onEnter: () => {
          enters++;
          if (enters === 1) throw new Error("hook flake");
        },
      })
      .on("go", { from: "a", to: "b" })
      .initial("a")
      .build();

    await m.start({ id: "y", context: {} });
    const send = m.send({ id: "y", event: "go" });
    await waitFor(() => enters === 1);
    clock.advance(10);
    await send;

    expect((await m.getState("y"))!.current).toBe("b");
    expect(exits).toBe(1);
    expect(enters).toBe(2);
    expect((await m.getHistory("y")).map((e) => e.event)).toEqual(["go"]);
  });

  it("a fired timeout under retrying middleware re-runs only onEnter", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryStateMachineStorage({ clock });
    let enters = 0;
    const m = stateMachine<any>({ name: "retry-timeout", storage, clock })
      .use(retryMiddleware({ maxRetries: 2, baseDelayMs: 10, clock }))
      .state("a", { timeout: { ms: 100, target: "late" } })
      .state("late", {
        terminal: true,
        onEnter: () => {
          enters++;
          if (enters === 1) throw new Error("hook flake");
        },
      })
      .initial("a")
      .build();

    await m.start({ id: "t", context: {} });
    clock.advance(100);
    await waitFor(() => enters === 1);
    clock.advance(10);
    await waitFor(() => enters === 2);

    expect((await m.getState("t"))!.current).toBe("late");
    expect((await m.getHistory("t")).map((e) => e.to)).toEqual(["late"]);
  });
});

describe("maxTransitionsPerSecond is per machine id", () => {
  type Go = {
    a: { context: {}; transitions: { go: "b" } };
    b: { context: {}; transitions: { back: "a" } };
  };

  function rated(clock: FakeWallClock) {
    const storage = new InMemoryStateMachineStorage({ clock });
    return stateMachine<Go>({
      name: "rated",
      storage,
      clock,
      limits: { maxTransitionsPerSecond: 2 },
    })
      .state("a")
      .state("b")
      .on("go", { from: "a", to: "b" })
      .on("back", { from: "b", to: "a" })
      .initial("a")
      .build();
  }

  it("sends to different machines do not share a window", async () => {
    const clock = FakeWallClock.create(T0);
    const m = rated(clock);
    for (const id of ["m1", "m2", "m3"]) {
      await m.start({ id, context: {} });
      await m.send({ id, event: "go" });
    }
    for (const id of ["m1", "m2", "m3"]) {
      expect((await m.getState(id))!.current).toBe("b");
    }
  });

  it("one machine over the limit is rejected under its own id", async () => {
    const clock = FakeWallClock.create(T0);
    const m = rated(clock);
    await m.start({ id: "hot", context: {} });
    await m.start({ id: "cold", context: {} });
    await m.send({ id: "hot", event: "go" });
    await m.send({ id: "hot", event: "back" });
    await expect(m.send({ id: "hot", event: "go" })).rejects.toThrow(
      "Machine hot exceeded rate limit",
    );
    await m.send({ id: "cold", event: "go" });

    clock.advance(1_000);
    await m.send({ id: "hot", event: "go" });
    expect((await m.getState("hot"))!.current).toBe("b");
  });
});
