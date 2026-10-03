// ---------------------------------------------------------------------------
// Portable journal replay conformance suite
//
// Drives `runJournaledStep` against a real backend to prove the journal it
// persists replays deterministically: nested `ctx.parallel` branch paths,
// `ctx.sleep` / `ctx.signal` / `ctx.child` slots inside branches, journals
// written in the legacy branch-path format, and slot stability under
// randomized activity timing.
//
// Usage:
//   import { journalReplayTestSuite } from "@promin/workflow/testing";
//   journalReplayTestSuite(() => new MyStorage());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { JournaledSuspendStorage } from "./activity-journal.ts";
import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import type { Workflow } from "./durable-pipeline.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import { completeDueSleeps, completeSignal, runJournaledStep } from "./journaled-step.ts";
import { FakeWallClock } from "../shared/wall-clock.ts";

export interface JournalReplayTestSuiteOptions {
  /**
   * How many fresh runs (and as many replays) the randomized-timing case
   * performs. Default 20.
   */
  timingRuns?: number;
}

type Body = (ctx: any) => Generator<any, unknown, any>;

let idCounter = 0;
const uniqueId = (label: string): string =>
  `jr-${label}-${Date.now().toString(36)}-${(idCounter++).toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/** A journal-capable store; `createWorkflow` is called first when present. */
export type JournalReplayStorage = JournaledSuspendStorage &
  Partial<Pick<WorkflowStorage, "createWorkflow">>;

/** Fresh workflow id; creates the workflow row on stores whose journal references it. */
async function newWorkflow(params: {
  storage: JournalReplayStorage;
  label: string;
}): Promise<string> {
  const workflowId = uniqueId(params.label);
  await params.storage.createWorkflow?.({ workflowId, workflowName: "journal-replay", input: {} });
  return workflowId;
}

const CHILD_WORKFLOW = { name: "child-wf" } as unknown as Workflow<unknown, unknown>;

/**
 * Run the journal replay suite against any `JournaledSuspendStorage`. The
 * factory may return a shared store; every case uses unique workflow ids.
 */
export function journalReplayTestSuite(
  factory: () => JournalReplayStorage | Promise<JournalReplayStorage>,
  options: JournalReplayTestSuiteOptions = {},
): void {
  const timingRuns = options.timingRuns ?? 20;

  const runStep = (params: {
    storage: JournalReplayStorage;
    workflowId: string;
    body: Body;
    clock?: FakeWallClock;
    runChild?: (p: { workflowId: string; input: unknown }) => Promise<unknown>;
  }) =>
    runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: params.workflowId,
      stepName: "s",
      storage: params.storage,
      body: params.body,
      ...(params.clock && { clock: params.clock }),
      ...(params.runChild && { runChild: params.runChild }),
    });

  /** Sorted `index|branchPath|stepType|name` keys of the step's journal. */
  const keys = async (params: { storage: JournalReplayStorage; workflowId: string }) =>
    (await params.storage.loadJournal(params.workflowId, "s"))
      .map(
        (e) => `${e.activityIndex}|${e.branchPath}|${e.stepType ?? "activity"}|${e.activityName}`,
      )
      .sort();

  /**
   * Wait until `count` entries are completed. A suspending branch rejects
   * the parallel while sibling branches keep recording in the background.
   */
  const settle = async (params: {
    storage: JournalReplayStorage;
    workflowId: string;
    count: number;
  }) => {
    const { storage, workflowId, count } = params;
    for (let i = 0; i < 200; i++) {
      const journal = await storage.loadJournal(workflowId, "s");
      if (journal.filter((e) => (e.phase ?? "completed") === "completed").length >= count) return;
      await new Promise((r) => setTimeout(r, 5));
    }
    throw new Error(`journal for ${workflowId} never reached ${count} completed entries`);
  };

  describe("journal replay conformance", () => {
    it("nested parallel followed by a sequential yield in the same branch replays", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "nested" });
      const calls: string[] = [];
      const act = (ctx: any, name: string) =>
        ctx.activity(name, async () => {
          calls.push(name);
          return name;
        });
      const body: Body = function* (ctx) {
        return yield* ctx.parallel([
          (function* () {
            const inner = yield* ctx.parallel([act(ctx, "A"), act(ctx, "B")]);
            const c = yield* act(ctx, "C");
            return [...inner, c];
          })(),
          act(ctx, "D"),
        ]);
      };

      const fresh = await runStep({ storage, workflowId, body });
      expect(fresh).toEqual([["A", "B", "C"], "D"]);
      expect(await keys({ storage, workflowId })).toEqual([
        "0|/0.0/0.0|activity|A",
        "0|/0.0/1.0|activity|B",
        "0|/0.1|activity|C",
        "0|/1.0|activity|D",
      ]);

      const replay = await runStep({ storage, workflowId, body });
      expect(replay).toEqual(fresh);
      expect(calls.sort()).toEqual(["A", "B", "C", "D"]);
    });

    it("ctx.sleep inside a branch takes its slot from the branch and resumes", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "sleep" });
      const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
      let aCalls = 0;
      const body: Body = function* (ctx) {
        const before = yield* ctx.activity("before", async () => "b");
        const [woke, a] = yield* ctx.parallel([
          ctx.sleep(60_000),
          ctx.activity("a", async () => ++aCalls),
        ]);
        return [before, (woke as Date).toISOString(), a];
      };

      let suspended: unknown;
      try {
        await runStep({ storage, workflowId, body, clock });
      } catch (err) {
        suspended = err;
      }
      expect(suspended).toBeInstanceOf(WorkflowSuspendedError);
      // "before" and the sibling "a" branch; the sleep stays pending.
      await settle({ storage, workflowId, count: 2 });
      expect(await keys({ storage, workflowId })).toEqual([
        "0||activity|before",
        "1|/0.0|sleep|sleep",
        "1|/1.0|activity|a",
      ]);

      clock.advance(60_000);
      const due = await completeDueSleeps({ storage, now: clock.now(), limit: 1000 });
      expect(due.filter((d) => d.workflowId === workflowId)).toEqual([
        expect.objectContaining({ activityIndex: 1, branchPath: "/0.0" }),
      ]);

      const result = await runStep({ storage, workflowId, body, clock });
      expect(result).toEqual(["b", "2026-01-01T00:01:00.000Z", 1]);
      expect(aCalls).toBe(1);
    });

    it("ctx.signal inside a branch is delivered by completeSignal and replays", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "signal" });
      const body: Body = function* (ctx) {
        return yield* ctx.parallel([
          ctx.activity("x", async () => "x"),
          (function* () {
            const y = yield* ctx.activity("y", async () => "y");
            const go = yield* ctx.signal("go");
            return `${y}:${go}`;
          })(),
        ]);
      };

      let suspended: unknown;
      try {
        await runStep({ storage, workflowId, body });
      } catch (err) {
        suspended = err;
      }
      expect(suspended).toBeInstanceOf(WorkflowSuspendedError);
      await settle({ storage, workflowId, count: 2 });
      expect(await keys({ storage, workflowId })).toEqual([
        "0|/0.0|activity|x",
        "0|/1.0|activity|y",
        "0|/1.1|signal|go",
      ]);

      const delivered = await completeSignal({
        storage,
        workflowId,
        stepName: "s",
        signalName: "go",
        value: "approved",
      });
      expect(delivered).toBe(true);

      expect(await runStep({ storage, workflowId, body })).toEqual(["x", "y:approved"]);
    });

    it("ctx.child inside branches gets deterministic slots and default ids", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "child" });
      const childIds: string[] = [];
      const runChild = async (p: { workflowId: string; input: unknown }) => {
        childIds.push(p.workflowId);
        await new Promise((r) => setTimeout(r, Math.random() * 3));
        return `done:${String(p.input)}`;
      };
      const body: Body = function* (ctx) {
        return yield* ctx.parallel([
          ctx.child(CHILD_WORKFLOW, { input: 1 }),
          (function* () {
            yield* ctx.activity("prep", async () => null);
            return yield* ctx.child(CHILD_WORKFLOW, { input: 2 });
          })(),
        ]);
      };

      expect(await runStep({ storage, workflowId, body, runChild })).toEqual(["done:1", "done:2"]);
      expect(childIds.sort()).toEqual([`${workflowId}.s.0~0.0`, `${workflowId}.s.0~1.1`].sort());
      expect(await keys({ storage, workflowId })).toEqual([
        "0|/0.0|child|child-wf",
        "0|/1.0|activity|prep",
        "0|/1.1|child|child-wf",
      ]);

      expect(await runStep({ storage, workflowId, body, runChild })).toEqual(["done:1", "done:2"]);
      expect(childIds).toHaveLength(2);
    });

    it("a journal written in the legacy branch-path format replays unchanged", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "legacy" });
      const success = (value: unknown) => ({ tag: "Success" as const, value });
      // Legacy layout of: parallel([A, function*{ B; C }]) at slot 0, then a
      // sleep in branch 0 that took top-level slot 1, then "after" at 2.
      for (const [idx, path, name, value] of [
        [0, "0", "A", "a"],
        [0, "1", "B", "b"],
        [0, "1.1", "C", "c"],
        [2, "", "after", "z"],
      ] as const) {
        await storage.appendEntry({
          workflowId,
          stepName: "s",
          activityIndex: idx,
          branchPath: path,
          activityName: name,
          exit: success(value),
        });
      }
      await storage.appendPendingEntry({
        workflowId,
        stepName: "s",
        activityIndex: 1,
        activityName: "sleep",
        stepType: "sleep",
        wakeAt: new Date("2026-01-01T00:00:00Z"),
      });
      await storage.completePendingEntry({
        workflowId,
        stepName: "s",
        activityIndex: 1,
        exit: success("2026-01-01T00:00:00.000Z"),
      });

      const calls: string[] = [];
      const act = (ctx: any, name: string) =>
        ctx.activity(name, async () => {
          calls.push(name);
          return `${name}-fresh`;
        });
      const body: Body = function* (ctx) {
        const [a, bc] = yield* ctx.parallel([
          (function* () {
            const a = yield* act(ctx, "A");
            yield* ctx.sleep(1);
            return a;
          })(),
          (function* () {
            const b = yield* act(ctx, "B");
            const c = yield* act(ctx, "C");
            return `${b}${c}`;
          })(),
        ]);
        const after = yield* act(ctx, "after");
        const [d, e] = yield* ctx.parallel([act(ctx, "D"), act(ctx, "E")]);
        return [a, bc, after, d, e];
      };

      expect(await runStep({ storage, workflowId, body })).toEqual([
        "a",
        "bc",
        "z",
        "D-fresh",
        "E-fresh",
      ]);
      expect(calls.sort()).toEqual(["D", "E"]);
      // New work in a legacy journal keeps the legacy grammar.
      const journal = await keys({ storage, workflowId });
      expect(journal).toContain("3|0|activity|D");
      expect(journal).toContain("3|1|activity|E");
    });

    it("a legacy journal with only top-level slots (children in a parallel) replays", async () => {
      const storage = await factory();
      const workflowId = await newWorkflow({ storage, label: "legacy-child" });
      // Legacy: parallel at slot 0, its two children took top-level 1 and 2.
      for (const idx of [1, 2]) {
        await storage.appendPendingEntry({
          workflowId,
          stepName: "s",
          activityIndex: idx,
          activityName: "child-wf",
          stepType: "child",
        });
        await storage.completePendingEntry({
          workflowId,
          stepName: "s",
          activityIndex: idx,
          exit: { tag: "Success", value: `legacy-${idx}` },
        });
      }
      let childCalls = 0;
      const runChild = async () => {
        childCalls++;
        return "fresh";
      };
      const body: Body = function* (ctx) {
        return yield* ctx.parallel([ctx.child(CHILD_WORKFLOW), ctx.child(CHILD_WORKFLOW)]);
      };
      expect(await runStep({ storage, workflowId, body, runChild })).toEqual([
        "legacy-1",
        "legacy-2",
      ]);
      expect(childCalls).toBe(0);
    });

    it("slots are identical across runs with randomized activity timing", async () => {
      const storage = await factory();
      const jitter = () => new Promise((r) => setTimeout(r, Math.floor(Math.random() * 4)));
      const runChild = async (p: { workflowId: string }) => {
        await jitter();
        return p.workflowId.slice(p.workflowId.indexOf(".s."));
      };
      const executed = { count: 0 };
      const act = (ctx: any, name: string) =>
        ctx.activity(name, async () => {
          executed.count++;
          await jitter();
          return name;
        });
      const body: Body = function* (ctx) {
        const pre = yield* act(ctx, "pre");
        const branches = yield* ctx.parallel([
          (function* () {
            const a1 = yield* act(ctx, "a1");
            yield* ctx.sleep(0);
            const a2 = yield* act(ctx, "a2");
            return [a1, a2];
          })(),
          (function* () {
            const inner = yield* ctx.parallel([
              act(ctx, "x1"),
              (function* () {
                const y1 = yield* act(ctx, "y1");
                const kid = yield* ctx.child(CHILD_WORKFLOW);
                return [y1, kid];
              })(),
            ]);
            const tail = yield* act(ctx, "b1-tail");
            return [inner, tail];
          })(),
          (function* () {
            const kid = yield* ctx.child(CHILD_WORKFLOW);
            const a3 = yield* act(ctx, "a3");
            return [kid, a3];
          })(),
        ]);
        const post = yield* act(ctx, "post");
        return { pre, branches, post };
      };

      const clock = FakeWallClock.create(0);
      const ids: string[] = [];
      let expectedKeys: string[] | undefined;
      let expectedResult: unknown;
      for (let i = 0; i < timingRuns; i++) {
        const workflowId = await newWorkflow({ storage, label: `timing-${i}` });
        ids.push(workflowId);
        const result = await runStep({ storage, workflowId, body, clock, runChild });
        const journalKeys = await keys({ storage, workflowId });
        expectedKeys ??= journalKeys;
        expectedResult ??= result;
        expect(journalKeys).toEqual(expectedKeys);
        expect(result).toEqual(expectedResult);
      }
      expect(expectedKeys).toHaveLength(11);

      const freshExecutions = executed.count;
      for (const workflowId of ids) {
        expect(await runStep({ storage, workflowId, body, clock, runChild })).toEqual(
          expectedResult,
        );
      }
      expect(executed.count).toBe(freshExecutions);
    });
  });
}
