// ---------------------------------------------------------------------------
// ctx.parallel failure tests — what happens when a branch throws.
//
// Covers:
//   * One branch fails → the parallel rejects with that error (Promise.all
//     semantics). Other branches may complete in the background — their
//     journal entries still land.
//   * Replay after a parallel failure: body re-runs deterministically;
//     successful branches hit the journal, the failing branch's recorded
//     failure rethrows.
//   * Compensations registered BEFORE the parallel unwind when the parallel
//     fails, confirming intra-step saga + parallel compose.
//   * `compensate` inside a parallel branch is rejected at call time with a
//     clear error (the index-reservation race is not solved yet).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep } from "../journaled-step.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";

describe("ctx.parallel — failure semantics", () => {
  it("first branch failure rejects the parallel; other branches still complete", async () => {
    const storage = new InMemoryWorkflowStorage();
    const ranOther = { value: false };

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-first-fail",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.parallel([
            ctx.activity("will-fail", async () => {
              throw new Error("boom");
            }),
            ctx.activity("slower", async () => {
              await new Promise((r) => setTimeout(r, 30));
              ranOther.value = true;
              return "ok";
            }),
          ]);
        },
      }),
    ).rejects.toThrow("boom");

    // Let the straggler settle so its journal write lands before we inspect.
    await new Promise((r) => setTimeout(r, 50));
    expect(ranOther.value).toBe(true);
    const journal = await storage.loadJournal("wf-first-fail", "s");
    const slower = journal.find((e) => e.activityName === "slower");
    expect(slower).toBeDefined();
    expect(slower!.exit?.tag).toBe("Success");
  });

  it("replay re-throws the recorded failure at the failing branch", async () => {
    const storage = new InMemoryWorkflowStorage();
    let ran = 0;

    const body = function* (ctx: any) {
      return yield* ctx.parallel([
        ctx.activity("ok", async () => {
          ran++;
          return 1;
        }),
        ctx.activity("bad", async () => {
          throw new Error("nope");
        }),
      ]);
    };

    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-replay-fail",
        stepName: "s",
        storage,
        body,
      }),
    ).rejects.toThrow("nope");
    expect(ran).toBe(1);

    // Replay: "ok" loads from journal; "bad" rethrows the recorded failure.
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "wf-replay-fail",
        stepName: "s",
        storage,
        body,
      }),
    ).rejects.toThrow("nope");
    expect(ran).toBe(1); // ok was NOT re-run — hit the journal
  });
});

describe("ctx.parallel — compensation across parallel", () => {
  it("activity BEFORE parallel compensates when parallel fails", async () => {
    const storage = new InMemoryWorkflowStorage();
    const rolledBack: string[] = [];

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-comp-parallel",
        stepName: "s",
        storage,
        body: function* (ctx) {
          yield* ctx.activity("create-order", async () => ({ id: "ord-1" }), {
            compensate: (order) => {
              rolledBack.push(`cancel:${order.id}`);
            },
          });
          return yield* ctx.parallel([
            ctx.activity("ship", async () => {
              throw new Error("shipping service down");
            }),
            ctx.activity("notify", async () => "notified"),
          ]);
        },
      }),
    ).rejects.toThrow("shipping service down");

    expect(rolledBack).toEqual(["cancel:ord-1"]);
  });

  it("compensate inside a parallel branch throws a clear error", async () => {
    const storage = new InMemoryWorkflowStorage();

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-comp-in-branch",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.parallel([
            ctx.activity("bad-branch", async () => 1, {
              compensate: () => undefined,
            }),
          ]);
        },
      }),
    ).rejects.toThrow(/not supported inside a ctx.parallel branch/);
  });
});

describe("ctx.parallel — failures compose with nested parallel", () => {
  it("inner parallel failure surfaces through the outer parallel", async () => {
    const storage = new InMemoryWorkflowStorage();

    await expect(
      runJournaledStep<unknown, unknown, unknown>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-nested-fail",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.parallel([
            ctx.activity("top", async () => "ok"),
            ctx.parallel([
              ctx.activity("inner-good", async () => "good"),
              ctx.activity("inner-bad", async () => {
                throw new Error("inner boom");
              }),
            ]),
          ]);
        },
      }),
    ).rejects.toThrow("inner boom");
  });
});
