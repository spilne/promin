import { describe, it, expect, beforeEach } from "bun:test";
import {
  stateMachine,
  machine,
  pureStateMachine,
  composeMachineMiddleware,
  retryMiddleware,
  EventDataValidationError,
  TIMEOUT_EVENT,
  type StateMachineInstance,
  type MachineMiddleware,
} from "./state-machine.ts";
import { InMemoryStateMachineStorage } from "./state-machine-storage.ts";
import { FakeClock, type SchemaParser } from "@promin/core";

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
        action: (ctx: { score: number }, _event: unknown, transition: any) => {
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

  // -------------------------------------------------------------------------
  // Typed event payloads
  // -------------------------------------------------------------------------

  describe("typed event payloads", () => {
    type ClaimStates = {
      pending: { context: { limit: number }; transitions: { approve: "approved"; deny: "denied" } };
      approved: {
        context: { limit: number; approvedBy: string; amount: number };
        transitions: {};
      };
      denied: { context: { limit: number; reason: string }; transitions: {} };
    };
    type ClaimEvents = {
      approve: { approvedBy: string; amount: number };
      deny: { reason: string };
    };

    it("guard receives typed event data", async () => {
      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          guard: (ctx, event) => event.amount <= ctx.limit,
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .on("deny", {
          from: "pending",
          to: "denied",
          action: (ctx, event) => ({ limit: ctx.limit, reason: event.reason }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-1", context: { limit: 1000 } });
      await machine.send({
        id: "c-1",
        event: "approve",
        data: { approvedBy: "alice", amount: 500 },
      });

      const state = await machine.getState("c-1");
      expect(state).toEqual({
        current: "approved",
        context: { limit: 1000, approvedBy: "alice", amount: 500 },
      });
    });

    it("guard rejects when event data fails predicate", async () => {
      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-deny", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          guard: (ctx, event) => event.amount <= ctx.limit,
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-2", context: { limit: 100 } });
      await expect(
        machine.send({ id: "c-2", event: "approve", data: { approvedBy: "bob", amount: 500 } }),
      ).rejects.toThrow('Guard rejected event "approve"');
    });

    it("event data persisted to history", async () => {
      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-hist", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-3", context: { limit: 1000 } });
      await machine.send({
        id: "c-3",
        event: "approve",
        data: { approvedBy: "alice", amount: 500 },
      });

      const history = await machine.getHistory("c-3");
      expect(history).toHaveLength(1);
      expect(history[0]!.eventData).toEqual({ approvedBy: "alice", amount: 500 });
    });

    it("onEnter / onExit receive event data", async () => {
      const seenEnter: unknown[] = [];
      const seenExit: unknown[] = [];

      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-hooks", storage })
        .state("pending", {
          onExit: (_ctx, event) => {
            seenExit.push(event);
          },
        })
        .state("approved", {
          terminal: true,
          onEnter: (_ctx, event) => {
            seenEnter.push(event);
          },
        })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-4", context: { limit: 1000 } });
      await machine.send({
        id: "c-4",
        event: "approve",
        data: { approvedBy: "alice", amount: 500 },
      });

      expect(seenExit).toEqual([{ approvedBy: "alice", amount: 500 }]);
      expect(seenEnter).toEqual([{ approvedBy: "alice", amount: 500 }]);
    });

    it("middleware sees event data via TransitionContext", async () => {
      let seen: unknown;
      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-mw", storage })
        .use((ctx, next) => {
          seen = ctx.eventData;
          return next();
        })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-5", context: { limit: 1000 } });
      await machine.send({
        id: "c-5",
        event: "approve",
        data: { approvedBy: "alice", amount: 500 },
      });

      expect(seen).toEqual({ approvedBy: "alice", amount: 500 });
    });

    it(".strict() validates event data and throws EventDataValidationError on failure", async () => {
      const approveSchema: SchemaParser<{ approvedBy: string; amount: number }> = {
        safeParse: (data) => {
          if (
            data &&
            typeof data === "object" &&
            "approvedBy" in data &&
            typeof (data as any).approvedBy === "string" &&
            "amount" in data &&
            typeof (data as any).amount === "number"
          ) {
            return { success: true, data: data as { approvedBy: string; amount: number } };
          }
          return { success: false, error: "invalid approve payload" };
        },
      };

      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-strict", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          action: (ctx, event) => ({
            limit: ctx.limit,
            approvedBy: event.approvedBy,
            amount: event.amount,
          }),
        })
        .strict({ approve: approveSchema })
        .initial("pending")
        .build();

      await machine.start({ id: "c-6", context: { limit: 1000 } });

      // Valid payload — succeeds
      await machine.send({
        id: "c-6",
        event: "approve",
        data: { approvedBy: "alice", amount: 500 },
      });
      expect((await machine.getState("c-6"))!.current).toBe("approved");

      // Invalid payload — rejected before transition
      await machine.start({ id: "c-7", context: { limit: 1000 } });
      await expect(
        machine.send({
          id: "c-7",
          event: "approve",
          data: { approvedBy: 42 as any, amount: "bad" as any },
        }),
      ).rejects.toBeInstanceOf(EventDataValidationError);
      expect((await machine.getState("c-7"))!.current).toBe("pending");
    });

    it(".strict() can be re-called to override per-event schemas (stricter)", async () => {
      const baseSchema: SchemaParser<{ reason: string }> = {
        safeParse: (data) => {
          if (data && typeof data === "object" && "reason" in data) {
            return { success: true, data: data as { reason: string } };
          }
          return { success: false, error: "missing reason" };
        },
      };
      const stricter: SchemaParser<{ reason: string }> = {
        safeParse: (data) => {
          if (
            data &&
            typeof data === "object" &&
            typeof (data as any).reason === "string" &&
            (data as any).reason.length >= 5
          ) {
            return { success: true, data: data as { reason: string } };
          }
          return { success: false, error: "reason must be >= 5 chars" };
        },
      };

      const machine = stateMachine<ClaimStates, ClaimEvents>({ name: "claim-override", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("deny", {
          from: "pending",
          to: "denied",
          action: (ctx, event) => ({ limit: ctx.limit, reason: event.reason }),
        })
        .strict({ deny: baseSchema })
        .strict({ deny: stricter })
        .initial("pending")
        .build();

      await machine.start({ id: "c-8", context: { limit: 1000 } });
      // Short reason — fails the stricter override
      await expect(
        machine.send({ id: "c-8", event: "deny", data: { reason: "no" } }),
      ).rejects.toBeInstanceOf(EventDataValidationError);

      // Longer reason — passes
      await machine.send({ id: "c-8", event: "deny", data: { reason: "fraud" } });
      expect((await machine.getState("c-8"))!.current).toBe("denied");
    });

    it("untyped Events=void machines still accept arbitrary events", async () => {
      // No Events generic — same as before
      const machine = stateMachine<ClaimStates>({ name: "claim-untyped", storage })
        .state("pending")
        .state("approved", { terminal: true })
        .state("denied", { terminal: true })
        .on("approve", {
          from: "pending",
          to: "approved",
          action: (ctx: { limit: number }) => ({
            limit: ctx.limit,
            approvedBy: "n/a",
            amount: 0,
          }),
        })
        .initial("pending")
        .build();

      await machine.start({ id: "c-9", context: { limit: 1000 } });
      await machine.send({ id: "c-9", event: "approve" });
      expect((await machine.getState("c-9"))!.current).toBe("approved");
    });
  });

  // -------------------------------------------------------------------------
  // machine() — quick in-memory shortcut
  // -------------------------------------------------------------------------

  describe("machine() shortcut", () => {
    it("auto-creates storage, auto-starts, and returns a single-instance handle", async () => {
      const handle = await machine<TrafficLight>({
        initial: "red",
        context: { count: 0 },
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
        .run();

      expect(handle.id).toMatch(/^m-/);

      await handle.send({ event: "next" });
      const state = await handle.getState();
      expect(state).toEqual({ current: "green", context: { count: 1 } });
    });

    it("accepts custom id and context override at run()", async () => {
      const handle = await machine<TrafficLight>({
        initial: "red",
        context: { count: 0 },
      })
        .state("red")
        .state("green")
        .state("yellow")
        .on("next", {
          from: "red",
          to: "green",
          action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
        })
        .run({ id: "custom-id", context: { count: 99 } });

      expect(handle.id).toBe("custom-id");
      const state = await handle.getState();
      expect(state).toEqual({ current: "red", context: { count: 99 } });
    });

    it("getHistory works through the handle", async () => {
      const handle = await machine<TrafficLight>({
        initial: "red",
        context: { count: 0 },
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
        .run();

      await handle.send({ event: "next" });
      await handle.send({ event: "next" });

      const history = await handle.getHistory();
      expect(history).toHaveLength(2);
      expect(history[0]!.from).toBe("red");
      expect(history[1]!.from).toBe("green");
    });
  });

  // -------------------------------------------------------------------------
  // pureStateMachine() — sync-only, deterministic
  // -------------------------------------------------------------------------

  describe("pureStateMachine() sync-only", () => {
    it("runs sync guards/actions identically to stateMachine()", async () => {
      const m = pureStateMachine<TrafficLight>({ name: "pure", storage })
        .state("red")
        .state("green")
        .state("yellow")
        .on("next", {
          from: "red",
          to: "green",
          guard: (ctx: { count: number }) => ctx.count < 10,
          action: (ctx: { count: number }) => ({ count: ctx.count + 1 }),
        })
        .initial("red")
        .build();

      await m.start({ id: "p-1", context: { count: 0 } });
      await m.send({ id: "p-1", event: "next" });
      expect(await m.getState("p-1")).toEqual({ current: "green", context: { count: 1 } });
    });

    it("type narrows action return — async action would be a compile error", async () => {
      // This test demonstrates the runtime works; the type-level constraint is
      // covered by the typecheck step. (Async action below would not compile in
      // pure mode because Promise<T> doesn't extend the narrowed sync return.)
      const m = pureStateMachine<TrafficLight>({ name: "pure-sync", storage })
        .state("red")
        .state("green")
        .state("yellow")
        .on("next", {
          from: "red",
          to: "green",
          action: (ctx: { count: number }) => ({ count: ctx.count + 5 }),
        })
        .initial("red")
        .build();

      await m.start({ id: "p-2", context: { count: 0 } });
      await m.send({ id: "p-2", event: "next" });
      expect(await m.getState("p-2")).toEqual({ current: "green", context: { count: 5 } });
    });
  });

  // -------------------------------------------------------------------------
  // Timed auto-transitions (.state(..., { timeout }))
  // -------------------------------------------------------------------------

  describe("state timeouts", () => {
    type ApprovalStates = {
      pending: { context: { item: string }; transitions: { approve: "approved" } };
      approved: { context: { item: string }; transitions: {} };
      timedOut: { context: { item: string }; transitions: {} };
    };

    function buildApprovalMachine(opts?: {
      clock?: FakeClock;
      autoScheduleTimeouts?: boolean;
    }): StateMachineInstance<ApprovalStates> {
      return stateMachine<ApprovalStates>({
        name: "approval",
        storage,
        clock: opts?.clock,
        autoScheduleTimeouts: opts?.autoScheduleTimeouts,
      })
        .state("pending", { timeout: { ms: 100, target: "timedOut" } })
        .state("approved", { terminal: true })
        .state("timedOut", { terminal: true })
        .on("approve", { from: "pending", to: "approved" })
        .initial("pending")
        .build();
    }

    it("checkTimeouts() fires due timeout deterministically with FakeClock", async () => {
      const clock = FakeClock.create("2026-01-01T00:00:00Z");
      const clockStorage = new InMemoryStateMachineStorage({ clock });
      storage = clockStorage;

      const m = buildApprovalMachine({ clock, autoScheduleTimeouts: false });
      await m.start({ id: "t-1", context: { item: "x" } });

      // Not due yet
      clock.advance(50);
      expect(await m.checkTimeouts("t-1")).toBe(false);
      expect((await m.getState("t-1"))!.current).toBe("pending");

      // Now due
      clock.advance(60);
      expect(await m.checkTimeouts("t-1")).toBe(true);
      expect((await m.getState("t-1"))!.current).toBe("timedOut");

      const history = await m.getHistory("t-1");
      expect(history).toHaveLength(1);
      expect(history[0]!.event).toBe(TIMEOUT_EVENT);
      expect(history[0]!.from).toBe("pending");
      expect(history[0]!.to).toBe("timedOut");
    });

    it("custom event label appears in history instead of TIMEOUT_EVENT default", async () => {
      const clock = FakeClock.create("2026-01-01T00:00:00Z");
      const clockStorage = new InMemoryStateMachineStorage({ clock });
      storage = clockStorage;

      const m = stateMachine<ApprovalStates>({
        name: "approval-labelled",
        storage,
        clock,
        autoScheduleTimeouts: false,
      })
        .state("pending", { timeout: { ms: 100, target: "timedOut", event: "expire" } })
        .state("approved", { terminal: true })
        .state("timedOut", { terminal: true })
        .on("approve", { from: "pending", to: "approved" })
        .initial("pending")
        .build();

      await m.start({ id: "t-label", context: { item: "x" } });
      clock.advance(150);
      expect(await m.checkTimeouts("t-label")).toBe(true);

      const history = await m.getHistory("t-label");
      expect(history).toHaveLength(1);
      expect(history[0]!.event).toBe("expire");
      expect(history[0]!.event).not.toBe(TIMEOUT_EVENT);
    });

    it("explicit transition cancels pending timeout", async () => {
      const clock = FakeClock.create("2026-01-01T00:00:00Z");
      const clockStorage = new InMemoryStateMachineStorage({ clock });
      storage = clockStorage;

      const m = buildApprovalMachine({ clock, autoScheduleTimeouts: false });
      await m.start({ id: "t-2", context: { item: "x" } });

      // Approve before timeout
      clock.advance(50);
      await m.send({ id: "t-2", event: "approve" });
      expect((await m.getState("t-2"))!.current).toBe("approved");

      // Even past the original timeout window, no auto-transition fires
      clock.advance(200);
      expect(await m.checkTimeouts("t-2")).toBe(false);
      expect((await m.getState("t-2"))!.current).toBe("approved");
    });

    it("timeout guard can veto firing — stays in source state", async () => {
      const clock = FakeClock.create("2026-01-01T00:00:00Z");
      const clockStorage = new InMemoryStateMachineStorage({ clock });
      storage = clockStorage;

      let allowFire = false;
      const m = stateMachine<ApprovalStates>({
        name: "approval-guarded",
        storage,
        clock,
        autoScheduleTimeouts: false,
      })
        .state("pending", {
          timeout: { ms: 100, target: "timedOut", guard: () => allowFire },
        })
        .state("approved", { terminal: true })
        .state("timedOut", { terminal: true })
        .on("approve", { from: "pending", to: "approved" })
        .initial("pending")
        .build();

      await m.start({ id: "t-3", context: { item: "x" } });
      clock.advance(150);

      // Guard returns false — no transition
      expect(await m.checkTimeouts("t-3")).toBe(false);
      expect((await m.getState("t-3"))!.current).toBe("pending");

      // Flip guard, retry — fires
      allowFire = true;
      expect(await m.checkTimeouts("t-3")).toBe(true);
      expect((await m.getState("t-3"))!.current).toBe("timedOut");
    });

    it("auto-scheduled timeout fires via setTimeout in real time", async () => {
      // Use a very short timeout (10ms) and real timers for the in-process path.
      const m = stateMachine<ApprovalStates>({ name: "approval-auto", storage })
        .state("pending", { timeout: { ms: 10, target: "timedOut" } })
        .state("approved", { terminal: true })
        .state("timedOut", { terminal: true })
        .on("approve", { from: "pending", to: "approved" })
        .initial("pending")
        .build();

      await m.start({ id: "t-4", context: { item: "x" } });
      expect((await m.getState("t-4"))!.current).toBe("pending");

      await new Promise((r) => setTimeout(r, 40));

      expect((await m.getState("t-4"))!.current).toBe("timedOut");
      m.cancelAllTimeouts();
    });

    it("chained timeouts — entering a state with timeout reschedules", async () => {
      const clock = FakeClock.create("2026-01-01T00:00:00Z");
      const clockStorage = new InMemoryStateMachineStorage({ clock });
      storage = clockStorage;

      type Chain = {
        a: { context: {}; transitions: {} };
        b: { context: {}; transitions: {} };
        c: { context: {}; transitions: {} };
      };

      const m = stateMachine<Chain>({
        name: "chain",
        storage,
        clock,
        autoScheduleTimeouts: false,
      })
        .state("a", { timeout: { ms: 100, target: "b" } })
        .state("b", { timeout: { ms: 100, target: "c" } })
        .state("c", { terminal: true })
        .initial("a")
        .build();

      await m.start({ id: "t-5", context: {} });

      clock.advance(150);
      expect(await m.checkTimeouts("t-5")).toBe(true);
      expect((await m.getState("t-5"))!.current).toBe("b");

      clock.advance(150);
      expect(await m.checkTimeouts("t-5")).toBe(true);
      expect((await m.getState("t-5"))!.current).toBe("c");
    });
  });
});
