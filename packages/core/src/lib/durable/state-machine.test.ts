import { describe, it, expect, beforeEach } from "bun:test";
import {
  stateMachine,
  composeMachineMiddleware,
  retryMiddleware,
  type StateMachineInstance,
  type MachineMiddleware,
} from "./state-machine.ts";
import { InMemoryStateMachineStorage } from "./state-machine-storage.ts";
import { FakeClock } from "../clock.ts";

// ---------------------------------------------------------------------------
// Test state definitions
// ---------------------------------------------------------------------------

type TrafficLight = {
  red: { context: { count: number }; transitions: { next: "green" } };
  green: { context: { count: number }; transitions: { next: "yellow" } };
  yellow: { context: { count: number }; transitions: { next: "red" } };
};

type OrderStates = {
  draft: {
    context: { items: string[] };
    transitions: { submit: "submitted"; cancel: "cancelled" };
  };
  submitted: {
    context: { items: string[]; submittedAt: Date };
    transitions: { approve: "approved"; reject: "rejected" };
  };
  approved: {
    context: { items: string[]; approvedBy: string };
    transitions: { ship: "shipped" };
  };
  rejected: {
    context: { items: string[]; reason: string };
    transitions: { resubmit: "draft" };
  };
  shipped: { context: { items: string[]; trackingId: string }; transitions: {} };
  cancelled: { context: { reason: string }; transitions: {} };
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createTrafficLight(
  storage: InMemoryStateMachineStorage,
): StateMachineInstance<TrafficLight> {
  return stateMachine<TrafficLight>({ name: "traffic-light", storage })
    .state("red")
    .state("green")
    .state("yellow")
    .on("next", {
      from: "red",
      to: "green",
      action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
    })
    .on("next", {
      from: "green",
      to: "yellow",
      action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
    })
    .on("next", {
      from: "yellow",
      to: "red",
      action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
    })
    .initial("red")
    .build();
}

function createOrderMachine(
  storage: InMemoryStateMachineStorage,
): StateMachineInstance<OrderStates> {
  const now = new Date("2025-01-01T00:00:00Z");
  return stateMachine<OrderStates>({ name: "order", storage })
    .state("draft")
    .state("submitted")
    .state("approved")
    .state("rejected")
    .state("shipped", { terminal: true })
    .state("cancelled", { terminal: true })
    .on("submit", {
      from: "draft",
      to: "submitted",
      action: (ctx: { items: string[] }) => ({ items: ctx.items, submittedAt: now }),
    })
    .on("approve", {
      from: "submitted",
      to: "approved",
      action: (ctx: { items: string[] }) => ({ items: ctx.items, approvedBy: "admin" }),
    })
    .on("reject", {
      from: "submitted",
      to: "rejected",
      action: (ctx: { items: string[] }) => ({ items: ctx.items, reason: "out of stock" }),
    })
    .on("ship", {
      from: "approved",
      to: "shipped",
      action: (ctx: { items: string[] }) => ({ items: ctx.items, trackingId: "TRK-123" }),
    })
    .on("resubmit", {
      from: "rejected",
      to: "draft",
      action: (ctx: { items: string[]; reason: string }) => ({ items: ctx.items }),
    })
    .on("cancel", {
      from: ["draft", "submitted"],
      to: "cancelled",
      action: () => ({ reason: "user cancelled" }),
    })
    .initial("draft")
    .build();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("StateMachine", () => {
  let storage: InMemoryStateMachineStorage;

  beforeEach(() => {
    storage = new InMemoryStateMachineStorage();
  });

  it("creates and loads machine", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "tl-1", context: { count: 0 } });

    const state = await machine.getState("tl-1");
    expect(state).toEqual({ current: "red", context: { count: 0 } });
  });

  it("simple transition", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "tl-1", context: { count: 0 } });

    await machine.send({ id: "tl-1", event: "next" });
    const state = await machine.getState("tl-1");
    expect(state).toEqual({ current: "green", context: { count: 1 } });
  });

  it("cyclic transitions", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "tl-1", context: { count: 0 } });

    // red → green → yellow → red
    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });

    const state = await machine.getState("tl-1");
    expect(state).toEqual({ current: "red", context: { count: 3 } });

    // Another full cycle
    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });

    const state2 = await machine.getState("tl-1");
    expect(state2).toEqual({ current: "red", context: { count: 6 } });
  });

  it("guards reject transition", async () => {
    const machine = stateMachine<TrafficLight>({ name: "guarded", storage })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        guard: (ctx: { count: number }) => ctx.count < 3,
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "g-1", context: { count: 5 } });
    await expect(machine.send({ id: "g-1", event: "next" })).rejects.toThrow(
      'Guard rejected event "next"',
    );

    // State unchanged
    const state = await machine.getState("g-1");
    expect(state).toEqual({ current: "red", context: { count: 5 } });
  });

  it("conditional transitions via TransitionTo", async () => {
    type ReviewStates = {
      pending: {
        context: { score: number };
        transitions: { review: "approved" | "rejected" };
      };
      approved: { context: { score: number }; transitions: {} };
      rejected: { context: { score: number; reason: string }; transitions: {} };
    };

    const machine = stateMachine<ReviewStates>({ name: "review", storage })
      .state("pending")
      .state("approved", { terminal: true })
      .state("rejected", { terminal: true })
      .on("review", {
        from: "pending",
        action: (ctx: { score: number }, transition: any) => {
          if (ctx.score >= 70) {
            return transition("approved", { score: ctx.score });
          }
          return transition("rejected", { score: ctx.score, reason: "score too low" });
        },
      })
      .initial("pending")
      .build();

    // High score → approved
    await machine.start({ id: "r-1", context: { score: 85 } });
    await machine.send({ id: "r-1", event: "review" });
    expect(await machine.getState("r-1")).toEqual({
      current: "approved",
      context: { score: 85 },
    });

    // Low score → rejected
    await machine.start({ id: "r-2", context: { score: 40 } });
    await machine.send({ id: "r-2", event: "review" });
    expect(await machine.getState("r-2")).toEqual({
      current: "rejected",
      context: { score: 40, reason: "score too low" },
    });
  });

  it("multi-source transitions", async () => {
    const machine = createOrderMachine(storage);

    // Cancel from draft
    await machine.start({ id: "o-1", context: { items: ["a"] } });
    await machine.send({ id: "o-1", event: "cancel" });
    expect(await machine.getState("o-1")).toEqual({
      current: "cancelled",
      context: { reason: "user cancelled" },
    });

    // Cancel from submitted
    await machine.start({ id: "o-2", context: { items: ["b"] } });
    await machine.send({ id: "o-2", event: "submit" });
    await machine.send({ id: "o-2", event: "cancel" });
    expect(await machine.getState("o-2")).toEqual({
      current: "cancelled",
      context: { reason: "user cancelled" },
    });
  });

  it("terminal states reject transitions", async () => {
    const machine = createOrderMachine(storage);
    await machine.start({ id: "o-1", context: { items: ["a"] } });
    await machine.send({ id: "o-1", event: "cancel" });

    // No transitions from cancelled
    await expect(machine.send({ id: "o-1", event: "submit" })).rejects.toThrow(
      'No transition for event "submit" from state "cancelled"',
    );
  });

  it("event history", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "tl-1", context: { count: 0 } });

    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });
    await machine.send({ id: "tl-1", event: "next" });

    const history = await machine.getHistory("tl-1");
    expect(history).toHaveLength(3);
    expect(history[0]!.from).toBe("red");
    expect(history[0]!.to).toBe("green");
    expect(history[1]!.from).toBe("green");
    expect(history[1]!.to).toBe("yellow");
    expect(history[2]!.from).toBe("yellow");
    expect(history[2]!.to).toBe("red");

    // Each has a unique id
    const ids = new Set(history.map((e) => e.id));
    expect(ids.size).toBe(3);
  });

  it("invalid event throws", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "tl-1", context: { count: 0 } });

    await expect(machine.send({ id: "tl-1", event: "bogus" })).rejects.toThrow(
      'No transition for event "bogus" from state "red"',
    );
  });

  it("async actions", async () => {
    const machine = stateMachine<TrafficLight>({ name: "async-light", storage })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: async (ctx: { count: number }) => {
          await new Promise((resolve) => setTimeout(resolve, 5));
          return { count: ctx.count + 10 };
        },
      })
      .initial("red")
      .build();

    await machine.start({ id: "a-1", context: { count: 0 } });
    await machine.send({ id: "a-1", event: "next" });

    const state = await machine.getState("a-1");
    expect(state).toEqual({ current: "green", context: { count: 10 } });
  });

  it("locking prevents concurrent sends", async () => {
    // Manually lock the machine
    await storage.tryLock("locked-1", 60_000);

    const machine = createTrafficLight(storage);
    await machine.start({ id: "locked-1", context: { count: 0 } });

    await expect(machine.send({ id: "locked-1", event: "next" })).rejects.toThrow(
      "Machine locked-1 is locked",
    );

    // Release and retry
    await storage.releaseLock("locked-1");
    await machine.send({ id: "locked-1", event: "next" });

    const state = await machine.getState("locked-1");
    expect(state).toEqual({ current: "green", context: { count: 1 } });
  });

  it("machine not found throws", async () => {
    const machine = createTrafficLight(storage);

    await expect(machine.send({ id: "nonexistent", event: "next" })).rejects.toThrow(
      "Machine nonexistent not found",
    );
  });

  it("duplicate start throws", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "dup-1", context: { count: 0 } });

    await expect(machine.start({ id: "dup-1", context: { count: 0 } })).rejects.toThrow(
      "Machine dup-1 already exists",
    );
  });

  // -------------------------------------------------------------------------
  // Limits
  // -------------------------------------------------------------------------

  it("maxTransitions prevents infinite loops", async () => {
    const machine = stateMachine<TrafficLight>({
      name: "limited",
      storage,
      limits: { maxTransitions: 3 },
    })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "lim-1", context: { count: 0 } });
    await machine.send({ id: "lim-1", event: "next" }); // 1
    await machine.send({ id: "lim-1", event: "next" }); // 2
    await machine.send({ id: "lim-1", event: "next" }); // 3

    await expect(machine.send({ id: "lim-1", event: "next" })).rejects.toThrow(
      "exceeded max transitions limit",
    );
  });

  it("maxTransitionsPerSecond rate limits sends", async () => {
    const machine = stateMachine<TrafficLight>({
      name: "rated",
      storage,
      limits: { maxTransitionsPerSecond: 2 },
    })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "rate-1", context: { count: 0 } });
    await machine.send({ id: "rate-1", event: "next" }); // 1/sec
    await machine.send({ id: "rate-1", event: "next" }); // 2/sec

    await expect(machine.send({ id: "rate-1", event: "next" })).rejects.toThrow(
      "exceeded rate limit",
    );
  });

  // -------------------------------------------------------------------------
  // FakeClock
  // -------------------------------------------------------------------------

  it("uses FakeClock for deterministic timestamps", async () => {
    const clock = FakeClock.create("2026-01-01T00:00:00Z");
    const clockStorage = new InMemoryStateMachineStorage({ clock });
    const machine = createTrafficLight(clockStorage);

    await machine.start({ id: "clock-1", context: { count: 0 } });
    const state1 = await clockStorage.load("clock-1");
    expect(state1!.createdAt).toEqual(new Date("2026-01-01T00:00:00Z"));

    clock.advance(5000);
    await machine.send({ id: "clock-1", event: "next" });
    const state2 = await clockStorage.load("clock-1");
    expect(state2!.updatedAt).toEqual(new Date("2026-01-01T00:00:05Z"));

    const events = await machine.getHistory("clock-1");
    expect(events[0]!.createdAt).toEqual(new Date("2026-01-01T00:00:05Z"));
  });

  // -------------------------------------------------------------------------
  // Metadata
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // onEnter / onExit hooks
  // -------------------------------------------------------------------------

  it("calls onEnter when entering a state", async () => {
    const entered: string[] = [];
    const machine = stateMachine<TrafficLight>({ name: "hooks", storage })
      .state("red")
      .state("green", {
        onEnter: async () => {
          entered.push("green");
        },
      })
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "hook-1", context: { count: 0 } });
    await machine.send({ id: "hook-1", event: "next" }); // red → green
    expect(entered).toEqual(["green"]);
  });

  it("calls onExit when leaving a state", async () => {
    const exited: string[] = [];
    const machine = stateMachine<TrafficLight>({ name: "hooks-exit", storage })
      .state("red", {
        onExit: async () => {
          exited.push("red");
        },
      })
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "hook-2", context: { count: 0 } });
    await machine.send({ id: "hook-2", event: "next" }); // red → green, onExit(red)
    expect(exited).toEqual(["red"]);
  });

  it("calls onExit then onEnter in order", async () => {
    const calls: string[] = [];
    const machine = stateMachine<TrafficLight>({ name: "hooks-order", storage })
      .state("red", {
        onExit: async () => {
          calls.push("exit:red");
        },
      })
      .state("green", {
        onEnter: async () => {
          calls.push("enter:green");
        },
      })
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "hook-3", context: { count: 0 } });
    await machine.send({ id: "hook-3", event: "next" });
    expect(calls).toEqual(["exit:red", "enter:green"]);
  });

  // -------------------------------------------------------------------------
  // Snapshot / Restore
  // -------------------------------------------------------------------------

  it("getSnapshot returns current state for serialization", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "snap-1", context: { count: 0 } });
    await machine.send({ id: "snap-1", event: "next" });

    const snapshot = await machine.getSnapshot("snap-1");
    expect(snapshot).not.toBeNull();
    expect(snapshot!.current).toBe("green");
    expect(snapshot!.context).toEqual({ count: 1 });
    expect(snapshot!.name).toBe("traffic-light");
  });

  it("restore creates machine from snapshot", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({ id: "snap-src", context: { count: 5 } });
    await machine.send({ id: "snap-src", event: "next" }); // green

    const snapshot = await machine.getSnapshot("snap-src");

    await machine.restore({ id: "snap-dst", snapshot: snapshot! });
    const state = await machine.getState("snap-dst");
    expect(state!.current).toBe("green");
    expect(state!.context).toEqual({ count: 6 });
  });

  it("restore to non-existent machine works, duplicate throws", async () => {
    const machine = createTrafficLight(storage);
    await machine.restore({
      id: "snap-new",
      snapshot: { current: "yellow", context: { count: 99 }, name: "traffic-light" },
    });

    const state = await machine.getState("snap-new");
    expect(state!.current).toBe("yellow");

    await expect(
      machine.restore({ id: "snap-new", snapshot: { current: "red", context: { count: 0 } } }),
    ).rejects.toThrow("already exists");
  });

  // -------------------------------------------------------------------------
  // Metadata
  // -------------------------------------------------------------------------

  it("stores metadata on machine instance", async () => {
    const machine = createTrafficLight(storage);
    await machine.start({
      id: "meta-1",
      context: { count: 0 },
      metadata: { region: "us-east", createdBy: "test" },
    });

    const state = await storage.load("meta-1");
    expect(state!.metadata).toEqual({ region: "us-east", createdBy: "test" });
  });

  // -------------------------------------------------------------------------
  // Middleware
  // -------------------------------------------------------------------------

  it("middleware wraps transitions", async () => {
    const log: string[] = [];
    const machine = stateMachine<TrafficLight>({ name: "mw", storage })
      .use(async (ctx, next) => {
        log.push(`before:${ctx.from}->${ctx.to}`);
        await next();
        log.push(`after:${ctx.from}->${ctx.to}`);
      })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "mw-1", context: { count: 0 } });
    await machine.send({ id: "mw-1", event: "next" });
    expect(log).toEqual(["before:red->green", "after:red->green"]);
  });

  it("middleware can abort transition by throwing", async () => {
    const machine = stateMachine<TrafficLight>({ name: "mw-abort", storage })
      .use(async (_ctx, _next) => {
        throw new Error("Blocked by middleware");
      })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "mw-2", context: { count: 0 } });
    await expect(machine.send({ id: "mw-2", event: "next" })).rejects.toThrow(
      "Blocked by middleware",
    );
    // State should not have changed
    expect((await machine.getState("mw-2"))!.current).toBe("red");
  });

  it("multiple middleware compose in order", async () => {
    const calls: string[] = [];
    const machine = stateMachine<TrafficLight>({ name: "mw-compose", storage })
      .use(async (_ctx, next) => {
        calls.push("a:before");
        await next();
        calls.push("a:after");
      })
      .use(async (_ctx, next) => {
        calls.push("b:before");
        await next();
        calls.push("b:after");
      })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "mw-3", context: { count: 0 } });
    await machine.send({ id: "mw-3", event: "next" });
    expect(calls).toEqual(["a:before", "b:before", "b:after", "a:after"]);
  });

  it("composeMachineMiddleware composes standalone middleware", async () => {
    const calls: string[] = [];
    const mw1: MachineMiddleware = async (_ctx, next) => {
      calls.push("1");
      await next();
    };
    const mw2: MachineMiddleware = async (_ctx, next) => {
      calls.push("2");
      await next();
    };
    const composed = composeMachineMiddleware(mw1, mw2);

    const ctx = { machineId: "x", machineName: "x", event: "e", from: "a", to: "b", context: {} };
    await composed(ctx, async () => {
      calls.push("core");
    });
    expect(calls).toEqual(["1", "2", "core"]);
  });

  it("middleware can modify transition context", async () => {
    const machine = stateMachine<TrafficLight>({ name: "mw-modify", storage })
      .use(async (ctx, next) => {
        // Inject extra metadata
        ctx.metadata = { ...(ctx.metadata as any), injected: true };
        await next();
      })
      .state("red")
      .state("green")
      .state("yellow")
      .on("next", {
        from: "red",
        to: "green",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "green",
        to: "yellow",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .on("next", {
        from: "yellow",
        to: "red",
        action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
      })
      .initial("red")
      .build();

    await machine.start({ id: "mw-4", context: { count: 0 } });
    await machine.send({ id: "mw-4", event: "next" });

    const events = await machine.getHistory("mw-4");
    expect((events[0]!.metadata as any).injected).toBe(true);
  });

  // -------------------------------------------------------------------------
  // Retry
  // -------------------------------------------------------------------------

  it("per-transition retry retries failed actions", async () => {
    let attempts = 0;
    type Simple = {
      a: { context: { v: number }; transitions: { go: "b" } };
      b: { context: { v: number }; transitions: {} };
    };
    const machine = stateMachine<Simple>({ name: "retry", storage })
      .state("a")
      .state("b", { terminal: true })
      .on("go", {
        from: "a",
        to: "b",
        action: async (ctx: { v: number }) => {
          attempts++;
          if (attempts < 3) throw new Error("fail");
          return { v: ctx.v + 1 };
        },
        retry: { maxRetries: 5, baseDelayMs: 1 },
      })
      .initial("a")
      .build();

    await machine.start({ id: "retry-1", context: { v: 0 } });
    await machine.send({ id: "retry-1", event: "go" });
    expect(attempts).toBe(3);
    expect((await machine.getState("retry-1"))!.current).toBe("b");
  });

  it("retryMiddleware retries the full transition including persistence", async () => {
    let persistAttempts = 0;
    const retryMw = retryMiddleware({ maxRetries: 3, baseDelayMs: 1 });
    // Compose: a middleware that fails persistence on first attempt
    const failOnceMw: MachineMiddleware = async (ctx, next) => {
      persistAttempts++;
      if (persistAttempts < 2) throw new Error("transient persistence failure");
      await next();
    };
    type Simple = {
      a: { context: { v: number }; transitions: { go: "b" } };
      b: { context: { v: number }; transitions: {} };
    };
    const machine = stateMachine<Simple>({ name: "retry-mw", storage })
      .use(retryMw)
      .use(failOnceMw)
      .state("a")
      .state("b", { terminal: true })
      .on("go", {
        from: "a",
        to: "b",
        action: (ctx: { v: number }) => ({ v: ctx.v + 1 }),
      })
      .initial("a")
      .build();

    await machine.start({ id: "retry-mw-1", context: { v: 0 } });
    await machine.send({ id: "retry-mw-1", event: "go" });
    expect(persistAttempts).toBe(2);
    expect((await machine.getState("retry-mw-1"))!.current).toBe("b");
  });

  // -------------------------------------------------------------------------
  // onError transition
  // -------------------------------------------------------------------------

  it("onError routes to error state on action failure", async () => {
    type WithError = {
      pending: { context: { amount: number }; transitions: { charge: "charged" } };
      charged: { context: { amount: number; txId: string }; transitions: {} };
      payment_failed: { context: { amount: number; error: string }; transitions: {} };
    };
    const machine = stateMachine<WithError>({ name: "onerror", storage })
      .state("pending")
      .state("charged", { terminal: true })
      .state("payment_failed", { terminal: true })
      .on("charge", {
        from: "pending",
        to: "charged",
        action: async () => {
          throw new Error("card declined");
        },
        onError: "payment_failed",
      })
      .initial("pending")
      .build();

    await machine.start({ id: "err-1", context: { amount: 100 } });
    await machine.send({ id: "err-1", event: "charge" });

    const state = await machine.getState("err-1");
    expect(state!.current).toBe("payment_failed");
    expect((state!.context as any).error).toBe("card declined");
  });

  it("onError with retry — retries first, then routes to error state", async () => {
    let attempts = 0;
    type WithError = {
      pending: { context: { v: number }; transitions: { go: "done" } };
      done: { context: { v: number }; transitions: {} };
      failed: { context: { v: number; error: string }; transitions: {} };
    };
    const machine = stateMachine<WithError>({ name: "onerror-retry", storage })
      .state("pending")
      .state("done", { terminal: true })
      .state("failed", { terminal: true })
      .on("go", {
        from: "pending",
        to: "done",
        action: async () => {
          attempts++;
          throw new Error("always fails");
        },
        retry: { maxRetries: 2, baseDelayMs: 1 },
        onError: "failed",
      })
      .initial("pending")
      .build();

    await machine.start({ id: "err-2", context: { v: 0 } });
    await machine.send({ id: "err-2", event: "go" });

    expect(attempts).toBe(3); // 1 initial + 2 retries
    expect((await machine.getState("err-2"))!.current).toBe("failed");
  });
});
