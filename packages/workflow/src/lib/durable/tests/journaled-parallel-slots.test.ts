// ---------------------------------------------------------------------------
// ctx.parallel slot allocation — branch-path grammar, sleep/signal/child
// inside branches, replay of recorded journals and timing independence. The
// portable cases run through `journalReplayTestSuite` (also run by the
// Postgres, Redis and SQLite packages); the cases below are specific to the
// in-memory engine wiring.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { journalReplayTestSuite } from "../journal-replay-test-suite.ts";
import { runJournaledStep } from "../journaled-step.ts";
import type { Workflow } from "../workflow-types.ts";

journalReplayTestSuite(() => new InMemoryWorkflowStorage(), { timingRuns: 50 });

const CHILD = { name: "child-wf" } as unknown as Workflow<unknown, unknown>;

describe("ctx.parallel slots — in-memory specifics", () => {
  it("a child workflow driven from a branch allocates from its own top-level counter", async () => {
    const storage = new InMemoryWorkflowStorage();
    // runChild drives the child's own journaled body inline, still inside
    // the parent's branch async context.
    const runChild = (p: { workflowId: string }) =>
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: p.workflowId,
        stepName: "child-step",
        storage,
        body: function* (ctx: any) {
          const one = yield* ctx.activity("c1", async () => 1);
          const two = yield* ctx.activity("c2", async () => 2);
          return one + two;
        },
      });

    const result = await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "parent",
      stepName: "s",
      storage,
      runChild,
      body: function* (ctx: any) {
        return yield* ctx.parallel([ctx.activity("p", async () => 0), ctx.child(CHILD)]);
      },
    });
    expect(result).toEqual([0, 3]);

    const childJournal = await storage.loadJournal({
      workflowId: "parent.s.0~1.0",
      stepName: "child-step",
    });
    expect(childJournal.map((e) => [e.activityIndex, e.branchPath, e.activityName])).toEqual([
      [0, "", "c1"],
      [1, "", "c2"],
    ]);
    const parentJournal = await storage.loadJournal({ workflowId: "parent", stepName: "s" });
    expect(
      parentJournal.map((e) => [e.activityIndex, e.branchPath, e.activityName]).sort(),
    ).toEqual([
      [0, "/0.0", "p"],
      [0, "/1.0", "child-wf"],
    ]);
  });

  it("a parallel resumed after a recorded top-level entry takes the next slot", async () => {
    const storage = new InMemoryWorkflowStorage();
    const body = function* (ctx: any) {
      const pre = yield* ctx.activity("pre", async () => "pre");
      const [a] = yield* ctx.parallel([ctx.activity("a", async () => "a")]);
      return [pre, a];
    };
    // A previous worker recorded only "pre" before crashing.
    await storage.appendEntry({
      workflowId: "resumed",
      stepName: "s",
      activityIndex: 0,
      branchPath: "",
      activityName: "pre",
      exit: { tag: "Success", value: "pre" },
    });
    expect(
      await runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "resumed",
        stepName: "s",
        storage,
        body,
      }),
    ).toEqual(["pre", "a"]);
    const journal = await storage.loadJournal({ workflowId: "resumed", stepName: "s" });
    expect(journal.find((e) => e.activityName === "a")).toMatchObject({
      activityIndex: 1,
      branchPath: "/0.0",
    });
  });

  it("compensate inside a parallel branch is still rejected", async () => {
    const storage = new InMemoryWorkflowStorage();
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "comp",
        stepName: "s",
        storage,
        body: function* (ctx: any) {
          return yield* ctx.parallel([
            ctx.activity("a", async () => 1, { compensate: () => undefined }),
          ]);
        },
      }),
    ).rejects.toThrow(/not supported inside a ctx.parallel branch/);
  });
});
