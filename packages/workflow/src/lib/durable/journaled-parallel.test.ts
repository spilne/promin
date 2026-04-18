// ---------------------------------------------------------------------------
// ctx.parallel tests — concurrent branches inside a journaled step body.
//
// Covers:
//   * parallel runs branches concurrently and returns results in input order
//   * each branch's activities get the shared activityIndex + branch path
//   * nested parallel (parallel inside a branch) produces deterministic paths
//   * sub-generator branches with multiple yields extend branch paths
//     sequentially (".1", ".2")
//   * replay re-runs the body and loads every branch's journal entry once
//   * a single-branch parallel still behaves correctly
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep } from "./journaled-step.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

describe("ctx.parallel — basic concurrency + ordering", () => {
  it("returns branch results in INPUT order regardless of completion order", async () => {
    const storage = new InMemoryWorkflowStorage();
    const result = await runJournaledStep<unknown, unknown, [number, number, number]>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-order",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return (yield* ctx.parallel([
          // Make the first branch the slowest to prove ordering comes from
          // the input position, not completion order.
          ctx.activity("a", async () => {
            await new Promise((r) => setTimeout(r, 20));
            return 1;
          }),
          ctx.activity("b", async () => 2),
          ctx.activity("c", async () => {
            await new Promise((r) => setTimeout(r, 10));
            return 3;
          }),
        ])) as [number, number, number];
      },
    });
    expect(result).toEqual([1, 2, 3]);
  });

  it("empty branch list resolves to []", async () => {
    const storage = new InMemoryWorkflowStorage();
    const result = await runJournaledStep<unknown, unknown, number[]>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-empty",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.parallel<number>([]);
      },
    });
    expect(result).toEqual([]);
  });

  it("single-branch parallel still works (degenerate case)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const result = await runJournaledStep<unknown, unknown, number[]>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-single",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.parallel([ctx.activity("only", async () => 42)]);
      },
    });
    expect(result).toEqual([42]);
  });
});

describe("ctx.parallel — journal indexing", () => {
  it("branches share the parallel's activity_index with distinct branch paths", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number[]>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-idx",
      stepName: "s",
      storage,
      body: function* (ctx) {
        yield* ctx.activity("a", async () => 1); // top-level slot 0
        yield* ctx.parallel([
          ctx.activity("b", async () => 2),
          ctx.activity("c", async () => 3),
          ctx.activity("d", async () => 4),
        ]);
        return [];
      },
    });

    const journal = await storage.loadJournal("wf-idx", "s");
    // Top-level "a"
    const a = journal.find((e) => e.activityName === "a")!;
    expect(a.activityIndex).toBe(0);
    expect(a.branchPath).toBe("");

    // Parallel branches all share the parallel's slot (activity_index = 1)
    const branches = ["b", "c", "d"].map((n) => journal.find((e) => e.activityName === n)!);
    expect(branches.every((e) => e.activityIndex === 1)).toBe(true);
    expect(branches.map((e) => e.branchPath).sort()).toEqual(["0", "1", "2"]);
  });

  it("top-level yield AFTER a parallel resumes counter at the next index", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-resume",
      stepName: "s",
      storage,
      body: function* (ctx) {
        yield* ctx.parallel([ctx.activity("x", async () => 1), ctx.activity("y", async () => 2)]);
        return yield* ctx.activity("after", async () => 3);
      },
    });

    const journal = await storage.loadJournal("wf-resume", "s");
    const after = journal.find((e) => e.activityName === "after")!;
    // Parallel consumed slot 0; its two branches share 0 with paths "0"/"1";
    // the next top-level yield takes slot 1.
    expect(after.activityIndex).toBe(1);
    expect(after.branchPath).toBe("");
  });
});

describe("ctx.parallel — nested parallel", () => {
  it("inner parallel's branches get composite paths under the outer branch", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-nested",
      stepName: "s",
      storage,
      body: function* (ctx) {
        yield* ctx.parallel([
          ctx.activity("outer0", async () => "A"),
          ctx.parallel([
            ctx.activity("inner0", async () => "B"),
            ctx.activity("inner1", async () => "C"),
          ]),
        ]);
        return null;
      },
    });

    const journal = await storage.loadJournal("wf-nested", "s");
    const byName = new Map(journal.map((e) => [e.activityName, e]));

    // All four entries share the outer parallel's activity_index (0).
    for (const name of ["outer0", "inner0", "inner1"]) {
      expect(byName.get(name)!.activityIndex).toBe(0);
    }
    // outer0 lives at "0"; inner parallel holds slot "1"; its children get
    // "1.0" and "1.1".
    expect(byName.get("outer0")!.branchPath).toBe("0");
    expect(byName.get("inner0")!.branchPath).toBe("1.0");
    expect(byName.get("inner1")!.branchPath).toBe("1.1");
  });
});

describe("ctx.parallel — sub-generator branches with multiple yields", () => {
  it("sequential activities inside one branch get suffixed paths", async () => {
    const storage = new InMemoryWorkflowStorage();
    await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-sub",
      stepName: "s",
      storage,
      body: function* (ctx) {
        const multiStep = (function* () {
          yield* ctx.activity("m0", async () => "first");
          yield* ctx.activity("m1", async () => "second");
        })();
        yield* ctx.parallel([multiStep, ctx.activity("solo", async () => "c")]);
        return null;
      },
    });

    const journal = await storage.loadJournal("wf-sub", "s");
    const byName = new Map(journal.map((e) => [e.activityName, e]));

    // multiStep runs in branch 0: first yield → "0", second → "0.1".
    expect(byName.get("m0")!.branchPath).toBe("0");
    expect(byName.get("m1")!.branchPath).toBe("0.1");
    // solo runs in branch 1.
    expect(byName.get("solo")!.branchPath).toBe("1");
  });
});

describe("ctx.parallel — replay determinism", () => {
  it("each activity runs exactly once across fresh-run + replay", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runCounts = { a: 0, b: 0, c: 0 };

    const body = function* (ctx: any) {
      yield* ctx.parallel([
        ctx.activity("a", async () => {
          runCounts.a++;
          return "A";
        }),
        ctx.activity("b", async () => {
          runCounts.b++;
          return "B";
        }),
        ctx.activity("c", async () => {
          runCounts.c++;
          return "C";
        }),
      ]);
      return null;
    };

    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-replay",
      stepName: "s",
      storage,
      body,
    });
    // Drive the same step again — body replays, journal is loaded, no
    // activity fn should run a second time.
    await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-replay",
      stepName: "s",
      storage,
      body,
    });

    expect(runCounts).toEqual({ a: 1, b: 1, c: 1 });
  });

  it("replay returns the same value order the fresh run saw", async () => {
    const storage = new InMemoryWorkflowStorage();
    const body = function* (ctx: any) {
      return yield* ctx.parallel([
        ctx.activity("a", async () => ({ tag: "a-result", at: "fresh" })),
        ctx.activity("b", async () => ({ tag: "b-result", at: "fresh" })),
      ]);
    };

    const fresh = (await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-order-replay",
      stepName: "s",
      storage,
      body,
    })) as Array<{ tag: string; at: string }>;

    const replay = (await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-order-replay",
      stepName: "s",
      storage,
      body,
    })) as Array<{ tag: string; at: string }>;

    expect(fresh.map((r) => r.tag)).toEqual(["a-result", "b-result"]);
    expect(replay.map((r) => r.tag)).toEqual(["a-result", "b-result"]);
  });
});
