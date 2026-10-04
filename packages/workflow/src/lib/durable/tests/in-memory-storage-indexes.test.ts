// ---------------------------------------------------------------------------
// InMemoryWorkflowStorage keeps indexes beside its rows (task positions,
// journal slots, idempotency keys, children, pending sleeps) so its hot
// paths don't scan. These cases change the rows under each index the ways
// the indexes have to follow.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";

const success = (value: unknown) => ({ tag: "Success" as const, value });

describe("InMemoryWorkflowStorage indexes", () => {
  describe("map task rows", () => {
    it("a run's task writes after a load copy once, and the loaded state keeps its rows", async () => {
      const s = new InMemoryWorkflowStorage();
      await s.createWorkflow({ workflowId: "m", workflowName: "t", input: {} });
      for (let i = 0; i < 100; i++) {
        await s.saveTaskResult({ workflowId: "m", stepName: "map", taskIndex: i, result: i });
      }
      const first = (await s.loadWorkflow("m"))!.steps["map"]!.tasks!;
      for (let i = 100; i < 200; i++) {
        await s.saveTaskResult({ workflowId: "m", stepName: "map", taskIndex: i, result: i });
      }
      await s.saveTaskFailure({ workflowId: "m", stepName: "map", taskIndex: 5, error: "x" });

      expect(first).toHaveLength(100);
      expect(first[5]!.status).toBe("completed");
      const tasks = (await s.loadWorkflow("m"))!.steps["map"]!.tasks!;
      expect(tasks).toHaveLength(200);
      expect(tasks.map((t) => t.taskIndex)).toEqual(Array.from({ length: 200 }, (_, i) => i));
      expect(tasks[5]).toMatchObject({ status: "failed", error: "x", attempt: 2 });
    });

    it("rows replaced by a suspend, a reset or a fresh run start a new task array", async () => {
      const s = new InMemoryWorkflowStorage();
      await s.createWorkflow({ workflowId: "r", workflowName: "t", input: {} });
      await s.saveTaskResult({ workflowId: "r", stepName: "map", taskIndex: 0, result: 0 });
      await s.suspendWorkflow("r", "map", { status: "sleeping" });
      await s.saveTaskResult({ workflowId: "r", stepName: "map", taskIndex: 1, result: 1 });
      expect((await s.loadWorkflow("r"))!.steps["map"]!.tasks!.map((t) => t.taskIndex)).toEqual([
        1,
      ]);

      await s.resetSteps("r", ["map"]);
      await s.saveTaskResult({ workflowId: "r", stepName: "map", taskIndex: 2, result: 2 });
      expect((await s.loadWorkflow("r"))!.steps["map"]!.tasks!.map((t) => t.taskIndex)).toEqual([
        2,
      ]);

      await s.startFreshRun("r");
      await s.saveTaskResult({ workflowId: "r", stepName: "map", taskIndex: 3, result: 3 });
      const state = (await s.loadWorkflow("r"))!;
      expect(state.steps["map"]!.tasks!.map((t) => t.taskIndex)).toEqual([3]);
      const [archived] = await s.loadRunHistory("r", { offset: 1 });
      expect(archived!.steps["map"]!.tasks!.map((t) => t.taskIndex)).toEqual([2]);
    });
  });

  describe("journal slots", () => {
    it("slots stay addressable after entries are discarded", async () => {
      const s = new InMemoryWorkflowStorage();
      await s.createWorkflow({ workflowId: "j", workflowName: "t", input: {} });
      const slot = { workflowId: "j", stepName: "body", activityName: "a" };
      for (let i = 0; i < 4; i++) {
        await s.appendPendingEntry({ ...slot, activityIndex: i, stepType: "activity" });
      }
      await s.discardJournalEntries({
        workflowId: "j",
        stepName: "body",
        slots: [
          { activityIndex: 0, branchPath: "" },
          { activityIndex: 2, branchPath: "" },
        ],
      });

      const done = await s.completePendingEntry({ ...slot, activityIndex: 3, exit: success(3) });
      expect(done).toEqual({ completed: true, exit: success(3) });
      await s.appendEntry({ ...slot, activityIndex: 0, exit: success(0) });
      const again = await s.completePendingEntry({ ...slot, activityIndex: 3, exit: success(9) });
      expect(again).toEqual({ completed: false, exit: success(3) });

      const journal = await s.loadJournal("j", "body");
      expect(journal.map((e) => [e.activityIndex, e.phase])).toEqual([
        [0, "completed"],
        [1, "pending"],
        [3, "completed"],
      ]);
    });

    it("loadJournal is sorted by slot and reflects every write since the last load", async () => {
      const s = new InMemoryWorkflowStorage();
      await s.createWorkflow({ workflowId: "o", workflowName: "t", input: {} });
      const slot = { workflowId: "o", stepName: "body", activityName: "a" };
      await s.appendEntry({ ...slot, activityIndex: 1, branchPath: "b", exit: success(1) });
      await s.appendEntry({ ...slot, activityIndex: 1, branchPath: "a", exit: success(2) });
      const first = await s.loadJournal("o", "body");
      await s.appendEntry({ ...slot, activityIndex: 0, exit: success(0) });
      const second = await s.loadJournal("o", "body");

      expect(first.map((e) => `${e.activityIndex}${e.branchPath}`)).toEqual(["1a", "1b"]);
      expect(second.map((e) => `${e.activityIndex}${e.branchPath}`)).toEqual(["0", "1a", "1b"]);
      first.pop();
      expect(await s.loadJournal("o", "body")).toHaveLength(3);
    });

    it("findDueSleeps sees pending sleeps only, and not a fresh run's dropped journal", async () => {
      const clock = FakeWallClock.create(0);
      const s = new InMemoryWorkflowStorage({ clock });
      for (const id of ["s1", "s2"]) {
        await s.createWorkflow({ workflowId: id, workflowName: "t", input: {} });
        await s.appendPendingEntry({
          workflowId: id,
          stepName: "body",
          activityIndex: 0,
          activityName: "nap",
          stepType: "sleep",
          wakeAt: new Date(10),
        });
      }
      const now = new Date(20);
      expect((await s.findDueSleeps({ now, limit: 10 })).map((d) => d.workflowId)).toEqual([
        "s1",
        "s2",
      ]);

      await s.completePendingEntry({
        workflowId: "s1",
        stepName: "body",
        activityIndex: 0,
        exit: success(null),
      });
      await s.startFreshRun("s2");
      expect(await s.findDueSleeps({ now, limit: 10 })).toEqual([]);
    });
  });

  describe("idempotency keys", () => {
    it("an expired key is reclaimed, and purging the old run keeps the new claim", async () => {
      const clock = FakeWallClock.create(0);
      const s = new InMemoryWorkflowStorage({ clock });
      const key = { workflowName: "t", idempotencyKey: "k" };
      await s.createWorkflow({
        workflowId: "old",
        workflowName: "t",
        input: {},
        idempotencyKey: "k",
        idempotencyExpiresAt: new Date(100),
      });
      await s.completeWorkflow("old", 1);
      clock.advance(200);
      const created = await s.createWorkflow({
        workflowId: "new",
        workflowName: "t",
        input: {},
        idempotencyKey: "k",
        idempotencyExpiresAt: new Date(10_000),
      });
      expect(created.created).toBe(true);

      expect(await s.purgeCompleted({ olderThanMs: 0, limit: 10 })).toBe(1);
      expect(await s.findWorkflowByIdempotencyKey({ ...key, now: clock.now() })).toEqual({
        workflowId: "new",
      });
      const dup = await s.createWorkflow({
        workflowId: "dup",
        workflowName: "t",
        input: {},
        idempotencyKey: "k",
        idempotencyExpiresAt: new Date(10_000),
      });
      expect(dup.created).toBe(false);
    });

    it("keys are scoped by namespace", async () => {
      const s = new InMemoryWorkflowStorage();
      const create = (workflowId: string, namespace: string) =>
        s.createWorkflow({
          workflowId,
          workflowName: "t",
          namespace,
          input: {},
          idempotencyKey: "k",
          idempotencyExpiresAt: new Date(Date.now() + 60_000),
        });
      expect((await create("a", "one")).created).toBe(true);
      expect((await create("b", "two")).created).toBe(true);
      expect((await create("c", "one")).created).toBe(false);
    });
  });

  describe("children", () => {
    it("cascade cancel follows every descendant, and a purged child drops out", async () => {
      const s = new InMemoryWorkflowStorage();
      await s.createWorkflow({ workflowId: "p", workflowName: "t", input: {} });
      await s.createWorkflow({
        workflowId: "c1",
        workflowName: "t",
        input: {},
        parentWorkflowId: "p",
      });
      await s.createWorkflow({
        workflowId: "c2",
        workflowName: "t",
        input: {},
        parentWorkflowId: "p",
      });
      await s.createWorkflow({
        workflowId: "g1",
        workflowName: "t",
        input: {},
        parentWorkflowId: "c1",
      });
      await s.completeWorkflow("c2", 1);
      expect(await s.purgeCompleted({ olderThanMs: -1_000, limit: 10 })).toBe(1);

      await s.cancelWorkflow("p", { cascade: true });

      for (const id of ["p", "c1", "g1"]) {
        expect((await s.loadWorkflowStatus(id))!.errorTag).toBe("WorkflowCancelledError");
      }
      expect(await s.loadWorkflow("c2")).toBeNull();
    });
  });
});
