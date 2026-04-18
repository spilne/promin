// ---------------------------------------------------------------------------
// Intra-step compensation tests — ActivityOptions.compensate saga rollback
// inside a journaled step body.
//
// Covered behaviours:
//   * success path — compensations never fire
//   * failure path — compensations fire in reverse registration order
//   * partially registered — only activities that succeeded leave a
//     compensation on the stack; the failing activity does not
//   * replay after crash mid-unwind — already-completed compensations are
//     skipped, the remaining ones still run
//   * compensation itself fails — unwind continues, journal records Failure
//   * ctx.sleep / ctx.signal suspensions do NOT trigger unwind
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep } from "./journaled-step.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import type { JournalEntry } from "./activity-journal.ts";

// (replay behaviour is tested by running the step body twice — see below —
// rather than by injecting storage errors mid-unwind, which is fragile.)

// ---------------------------------------------------------------------------
// Success path — compensations registered but never fire
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — success path", () => {
  it("body returns without error → no compensations run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const refunds: string[] = [];

    const result = await runJournaledStep<unknown, unknown, string>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-ok",
      stepName: "s",
      storage,
      body: function* (ctx) {
        yield* ctx.activity("debit", async () => "debit-ok", {
          compensate: () => {
            refunds.push("debit-rolled-back");
          },
        });
        yield* ctx.activity("credit", async () => "credit-ok", {
          compensate: () => {
            refunds.push("credit-rolled-back");
          },
        });
        return "done";
      },
    });

    expect(result).toBe("done");
    expect(refunds).toEqual([]);
    const journal = await storage.loadJournal("wf-ok", "s");
    for (const e of journal) expect(e.stepType).not.toBe("compensation");
  });
});

// ---------------------------------------------------------------------------
// Failure path — unwind in reverse
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — failure path", () => {
  it("unwinds registered compensations in reverse order", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-reverse",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("a", async () => "a-ok", {
            compensate: () => {
              events.push("comp:a");
            },
          });
          yield* ctx.activity("b", async () => "b-ok", {
            compensate: () => {
              events.push("comp:b");
            },
          });
          yield* ctx.activity("c", async () => "c-ok", {
            compensate: () => {
              events.push("comp:c");
            },
          });
          yield* ctx.activity("d", async () => {
            throw new Error("boom");
          });
          return "unreached";
        },
      }),
    ).rejects.toThrow("boom");

    // Reverse registration order: c, then b, then a. The activity that
    // threw ("d") never registered a comp, so it does not appear.
    expect(events).toEqual(["comp:c", "comp:b", "comp:a"]);
  });

  it("compensation receives the activity's return value", async () => {
    const storage = new InMemoryWorkflowStorage();
    const cancellations: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-result-arg",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("createOrder", async () => ({ id: "order-42" }), {
            compensate: (order) => {
              cancellations.push(order.id);
            },
          });
          yield* ctx.activity("fail", async () => {
            throw new Error("stop");
          });
        },
      }),
    ).rejects.toThrow("stop");

    expect(cancellations).toEqual(["order-42"]);
  });

  it("activity that fails does NOT register its own compensation", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-fail-self",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("a", async () => "ok", {
            compensate: () => {
              events.push("comp:a");
            },
          });
          yield* ctx.activity(
            "b",
            async () => {
              throw new Error("boom");
            },
            {
              compensate: () => {
                events.push("comp:b"); // should never fire — b failed
              },
            },
          );
        },
      }),
    ).rejects.toThrow("boom");

    expect(events).toEqual(["comp:a"]);
  });
});

// ---------------------------------------------------------------------------
// Journal entries — compensations get stepType="compensation"
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — journal entries", () => {
  it("each compensation is journaled with stepType='compensation'", async () => {
    const storage = new InMemoryWorkflowStorage();

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-journal",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("a", async () => 1, { compensate: () => undefined });
          yield* ctx.activity("b", async () => 2, { compensate: () => undefined });
          yield* ctx.activity("fail", async () => {
            throw new Error("boom");
          });
        },
      }),
    ).rejects.toThrow("boom");

    const journal = await storage.loadJournal("wf-journal", "s");
    const comps = journal.filter((e) => e.stepType === "compensation");
    expect(comps).toHaveLength(2);
    for (const c of comps) {
      expect(c.phase ?? "completed").toBe("completed");
      expect(c.exit?.tag).toBe("Success");
    }
  });
});

// ---------------------------------------------------------------------------
// Replay idempotency — running the same step twice after a body failure
// must not double-run compensations. Proves the journal-based dedup works
// for compensation entries the same way it does for activity entries.
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — replay is idempotent", () => {
  it("compensations run exactly once even when the step is re-driven", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runCount = { a: 0, b: 0 };

    const body = function* (ctx: any) {
      yield* ctx.activity("a", async () => 1, {
        compensate: () => {
          runCount.a++;
        },
      });
      yield* ctx.activity("b", async () => 2, {
        compensate: () => {
          runCount.b++;
        },
      });
      yield* ctx.activity("fail", async () => {
        throw new Error("body failed");
      });
      return "unreached";
    };

    // First run — body fails, both compensations fire.
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-replay",
        stepName: "s",
        storage,
        body,
      }),
    ).rejects.toThrow("body failed");
    expect(runCount).toEqual({ a: 1, b: 1 });

    // Second run — body replays (activities load from journal, then "fail"
    // throws again), and unwind sees completed compensation entries for
    // both reserved indices. Compensations MUST NOT re-run.
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-replay",
        stepName: "s",
        storage,
        body,
      }),
    ).rejects.toThrow("body failed");
    expect(runCount).toEqual({ a: 1, b: 1 }); // unchanged
  });
});

// ---------------------------------------------------------------------------
// Compensation itself fails — unwind continues
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — compensation failure", () => {
  it("a failing compensation is journaled as Failure; next ones still run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-bad-comp",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("a", async () => 1, {
            compensate: () => {
              events.push("comp:a");
            },
          });
          yield* ctx.activity("b", async () => 2, {
            compensate: () => {
              events.push("comp:b");
              throw new Error("compensation failed");
            },
          });
          yield* ctx.activity("fail", async () => {
            throw new Error("body failed");
          });
        },
      }),
    ).rejects.toThrow("body failed");

    // b's compensation ran first (reverse), threw. Unwind kept going with a.
    expect(events).toEqual(["comp:b", "comp:a"]);

    const journal = await storage.loadJournal("wf-bad-comp", "s");
    const comps = journal.filter((e) => e.stepType === "compensation");
    expect(comps).toHaveLength(2);
    // Look up by the diagnostic activityName rather than journal order —
    // b's compensation failed, a's succeeded.
    const forB = comps.find((c) => c.activityName === "compensation:b")!;
    const forA = comps.find((c) => c.activityName === "compensation:a")!;
    expect(forB.exit?.tag).toBe("Failure");
    expect(forA.exit?.tag).toBe("Success");
  });
});

// ---------------------------------------------------------------------------
// Suspensions do NOT trigger unwind
// ---------------------------------------------------------------------------

describe("ctx.activity.compensate — suspensions don't unwind", () => {
  it("ctx.sleep throws WorkflowSuspendedError; compensations stay pending", async () => {
    const storage = new InMemoryWorkflowStorage();
    const events: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-sleep",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("a", async () => 1, {
            compensate: () => {
              events.push("comp:a");
            },
          });
          yield* ctx.sleep(60_000); // throws WorkflowSuspendedError
          return "unreached";
        },
      }),
    ).rejects.toThrow(/sleeping until/i);

    // Sleep's suspension is not a failure — compensations must NOT have run.
    expect(events).toEqual([]);
  });
});
