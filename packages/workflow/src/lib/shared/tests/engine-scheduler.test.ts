import { describe, expect, it } from "bun:test";
import { runExit } from "@spilne/perfect-core";
import { promiseOrDie } from "../eff.ts";
import { createEngineScheduler, type MacrotaskHop } from "../engine-scheduler.ts";

/** A macrotask source the test fires by hand. */
function manualHop(params: { cancellable: boolean }): {
  hop: MacrotaskHop;
  fire: () => void;
  pending: () => number;
} {
  const waiting = new Set<() => void>();
  return {
    hop: (fn) => {
      const entry = (): void => fn();
      waiting.add(entry);
      return params.cancellable ? () => void waiting.delete(entry) : undefined;
    },
    fire: () => {
      const due = [...waiting];
      waiting.clear();
      for (const fn of due) fn();
    },
    pending: () => waiting.size,
  };
}

const flushMicrotasks = async (): Promise<void> => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

describe("createEngineScheduler", () => {
  it("drains on microtasks within the budget, in order", async () => {
    const a = manualHop({ cancellable: true });
    const scheduler = createEngineScheduler({ microDrainBudget: 4, macrotaskHops: [a.hop] });
    const ran: number[] = [];
    scheduler.schedule(() => ran.push(1));
    scheduler.schedule(() => ran.push(2));
    expect(ran).toEqual([]);

    await flushMicrotasks();

    expect(ran).toEqual([1, 2]);
    expect(a.pending()).toBe(0);
  });

  it("yields a macrotask once the budget of consecutive drains is spent", async () => {
    const a = manualHop({ cancellable: true });
    const scheduler = createEngineScheduler({ microDrainBudget: 2, macrotaskHops: [a.hop] });
    const ran: number[] = [];
    // Each task schedules the next, so every drain makes a new request.
    const chain = (n: number): void => {
      ran.push(n);
      if (n < 5) scheduler.schedule(() => chain(n + 1));
    };
    scheduler.schedule(() => chain(0));

    await flushMicrotasks();
    expect(ran).toEqual([0, 1]);
    expect(a.pending()).toBe(1);

    // The yield drains one batch and resets the budget: two more microtask
    // drains follow before the next yield.
    a.fire();
    await flushMicrotasks();
    expect(ran).toEqual([0, 1, 2, 3, 4]);
    expect(a.pending()).toBe(1);

    a.fire();
    await flushMicrotasks();
    expect(ran).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it("serves a yield from whichever raced source fires first and cancels the rest", () => {
    const a = manualHop({ cancellable: true });
    const b = manualHop({ cancellable: true });
    const scheduler = createEngineScheduler({
      microDrainBudget: 0,
      macrotaskHops: [a.hop, b.hop],
    });
    let runs = 0;
    scheduler.schedule(() => runs++);
    expect([a.pending(), b.pending()]).toEqual([1, 1]);

    b.fire();

    expect(runs).toBe(1);
    expect(a.pending()).toBe(0);
  });

  it("ignores a late wakeup from a source that cannot be cancelled", () => {
    const a = manualHop({ cancellable: false });
    const b = manualHop({ cancellable: false });
    const scheduler = createEngineScheduler({
      microDrainBudget: 0,
      macrotaskHops: [a.hop, b.hop],
    });
    const ran: string[] = [];
    scheduler.schedule(() => ran.push("first"));
    a.fire(); // serves the first yield; b's wakeup for it is now stale
    scheduler.schedule(() => ran.push("second"));
    b.fire(); // the stale wakeup and the second yield's both fire here
    a.fire();

    expect(ran).toEqual(["first", "second"]);
  });

  it("keeps running when one raced source never fires", async () => {
    const blocked = manualHop({ cancellable: false });
    const scheduler = createEngineScheduler({
      microDrainBudget: 0,
      macrotaskHops: [blocked.hop, (fn) => void setTimeout(fn, 0)],
    });
    const step = (n: number) => promiseOrDie(async () => n).map((x) => x * 2);

    for (let i = 0; i < 10; i++) {
      expect(await runExit(step(i), scheduler)).toEqual({ _tag: "Success", value: i * 2 });
    }
    expect(blocked.pending()).toBeGreaterThan(0);
  });

  it("flush runs everything queued and cancels the yield in flight", () => {
    const a = manualHop({ cancellable: true });
    const scheduler = createEngineScheduler({ microDrainBudget: 0, macrotaskHops: [a.hop] });
    const ran: number[] = [];
    scheduler.schedule(() => {
      ran.push(1);
      scheduler.schedule(() => ran.push(2));
    });

    scheduler.flush();

    expect(ran).toEqual([1, 2]);
    expect(a.pending()).toBe(0);
  });

  it("shutdown drops queued tasks", () => {
    const a = manualHop({ cancellable: false });
    const scheduler = createEngineScheduler({ microDrainBudget: 0, macrotaskHops: [a.hop] });
    const ran: number[] = [];
    scheduler.schedule(() => ran.push(1));

    scheduler.shutdown();
    a.fire();

    expect(ran).toEqual([]);
  });
});
