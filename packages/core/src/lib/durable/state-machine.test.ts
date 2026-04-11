import { describe, it, expect, beforeEach } from "bun:test";
import { stateMachine, type StateMachineInstance } from "./state-machine.ts";
import { InMemoryStateMachineStorage } from "./state-machine-storage.ts";

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
});
