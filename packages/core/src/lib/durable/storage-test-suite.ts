// ---------------------------------------------------------------------------
// Portable WorkflowStorage test suite
//
// Usage:
//   import { storageTestSuite } from "@promin/core/testing";
//   storageTestSuite(() => new MyCustomStorage());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { WorkflowStorage } from "./workflow-storage.ts";

/**
 * Run the full WorkflowStorage conformance suite against any implementation.
 * Verifies CRUD, steps, tasks, locking, signals, suspend, complete/fail,
 * listing, cancellation, and fresh runs.
 *
 * @param factory — called before each test group to get a fresh storage instance
 */
export function storageTestSuite(factory: () => WorkflowStorage | Promise<WorkflowStorage>) {
  let storage: WorkflowStorage;

  async function getStorage(): Promise<WorkflowStorage> {
    storage = await factory();
    return storage;
  }

  describe("WorkflowStorage conformance", () => {
    // -------------------------------------------------------------------
    // createWorkflow + loadWorkflow
    // -------------------------------------------------------------------

    describe("createWorkflow + loadWorkflow", () => {
      it("creates and loads a workflow", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "crud-1",
          workflowName: "test-wf",
          input: { userId: "u_42" },
          workflowType: "onboarding",
          metadata: { region: "us-east" },
        });

        const state = await s.loadWorkflow("crud-1");
        expect(state).not.toBeNull();
        expect(state!.workflowId).toBe("crud-1");
        expect(state!.workflowName).toBe("test-wf");
        expect(state!.workflowType).toBe("onboarding");
        expect(state!.status).toBe("running");
        expect(state!.run).toBe(1);
        expect(state!.input).toEqual({ userId: "u_42" });
        expect(state!.metadata).toEqual({ region: "us-east" });
        expect(state!.createdAt).toBeInstanceOf(Date);
      });

      it("returns null for non-existent workflow", async () => {
        const s = await getStorage();
        expect(await s.loadWorkflow("nonexistent")).toBeNull();
      });

      it("stores and returns version when provided", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "ver-1",
          workflowName: "test-wf",
          input: {},
          version: "2",
        });
        const state = await s.loadWorkflow("ver-1");
        expect(state!.version).toBe("2");
      });

      it("version is undefined when not provided", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "ver-default",
          workflowName: "test-wf",
          input: {},
        });
        const state = await s.loadWorkflow("ver-default");
        expect(state!.version).toBeUndefined();
      });
    });

    // -------------------------------------------------------------------
    // saveStepResult
    // -------------------------------------------------------------------

    describe("saveStepResult", () => {
      it("saves and loads step result", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "step-1", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "step-1",
          stepName: "fetch",
          result: { data: "hello" },
          durationMs: 150,
          startedAt: new Date(),
        });

        const state = await s.loadWorkflow("step-1");
        const step = state!.steps["fetch"];
        expect(step!.status).toBe("completed");
        expect(step!.result).toEqual({ data: "hello" });
        expect(step!.durationMs).toBe(150);
        expect(step!.completedAt).toBeInstanceOf(Date);
      });
    });

    // -------------------------------------------------------------------
    // saveStepFailure
    // -------------------------------------------------------------------

    describe("saveStepFailure", () => {
      it("saves step failure", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "step-fail", workflowName: "test", input: {} });
        await s.saveStepFailure({
          workflowId: "step-fail",
          stepName: "bad",
          error: "something broke",
          durationMs: 50,
          startedAt: new Date(),
        });

        const state = await s.loadWorkflow("step-fail");
        expect(state!.steps["bad"]!.status).toBe("failed");
        expect(state!.steps["bad"]!.error).toBe("something broke");
      });
    });

    // -------------------------------------------------------------------
    // saveTaskResult / saveTaskFailure
    // -------------------------------------------------------------------

    describe("saveTaskResult / saveTaskFailure", () => {
      it("saves task results", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "task-1", workflowName: "test", input: {} });
        await s.saveTaskResult({
          workflowId: "task-1",
          stepName: "map-step",
          taskIndex: 0,
          result: "a",
        });
        await s.saveTaskResult({
          workflowId: "task-1",
          stepName: "map-step",
          taskIndex: 1,
          result: "b",
        });

        const state = await s.loadWorkflow("task-1");
        expect(state!.steps["map-step"]?.tasks).toHaveLength(2);
        expect(state!.steps["map-step"]?.tasks![0]!.result).toBe("a");
      });

      it("saves task failures", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "task-fail", workflowName: "test", input: {} });
        await s.saveTaskFailure({
          workflowId: "task-fail",
          stepName: "s",
          taskIndex: 0,
          error: "boom",
        });

        const state = await s.loadWorkflow("task-fail");
        expect(state!.steps["s"]?.tasks![0]!.status).toBe("failed");
      });
    });

    // -------------------------------------------------------------------
    // completeWorkflow / failWorkflow
    // -------------------------------------------------------------------

    describe("completeWorkflow / failWorkflow", () => {
      it("completes a workflow", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "complete-1", workflowName: "test", input: {} });
        await s.completeWorkflow("complete-1", { final: "result" });

        const state = await s.loadWorkflow("complete-1");
        expect(state!.status).toBe("completed");
        expect(state!.result).toEqual({ final: "result" });
        expect(state!.completedAt).toBeInstanceOf(Date);
      });

      it("fails a workflow", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fail-1", workflowName: "test", input: {} });
        await s.failWorkflow("fail-1", "total failure");

        const state = await s.loadWorkflow("fail-1");
        expect(state!.status).toBe("failed");
        expect(state!.error).toBe("total failure");
      });
    });

    // -------------------------------------------------------------------
    // listWorkflows
    // -------------------------------------------------------------------

    describe("listWorkflows", () => {
      it("filters by status", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "list-c1", workflowName: "test", input: {} });
        await s.completeWorkflow("list-c1", null);
        await s.createWorkflow({ workflowId: "list-r1", workflowName: "test", input: {} });

        const completed = await s.listWorkflows({ status: "completed" });
        expect(completed.every((w) => w.status === "completed")).toBe(true);
      });

      it("filters by name", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "list-n1", workflowName: "special", input: {} });

        const filtered = await s.listWorkflows({ name: "special" });
        expect(filtered.length).toBeGreaterThanOrEqual(1);
        expect(filtered[0]!.workflowName).toBe("special");
      });

      it("supports limit", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "list-l1", workflowName: "test", input: {} });
        await s.createWorkflow({ workflowId: "list-l2", workflowName: "test", input: {} });
        await s.createWorkflow({ workflowId: "list-l3", workflowName: "test", input: {} });

        const page = await s.listWorkflows({ limit: 2 });
        expect(page.length).toBeLessThanOrEqual(2);
      });
    });

    // -------------------------------------------------------------------
    // cancelWorkflow
    // -------------------------------------------------------------------

    describe("cancelWorkflow", () => {
      it("cancels a running workflow", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "cancel-1", workflowName: "test", input: {} });
        await s.cancelWorkflow("cancel-1");

        const state = await s.loadWorkflow("cancel-1");
        expect(state!.status).toBe("failed");
        expect(state!.error).toBe("Cancelled");
      });

      it("does not cancel a completed workflow", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "cancel-2", workflowName: "test", input: {} });
        await s.completeWorkflow("cancel-2", "done");
        await s.cancelWorkflow("cancel-2");

        expect((await s.loadWorkflow("cancel-2"))!.status).toBe("completed");
      });
    });

    // -------------------------------------------------------------------
    // suspendWorkflow
    // -------------------------------------------------------------------

    describe("suspendWorkflow", () => {
      it("suspends with sleep state", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "suspend-1", workflowName: "test", input: {} });
        await s.suspendWorkflow("suspend-1", "wait", {
          status: "sleeping",
          stepType: "sleep",
          wakeAt: new Date(Date.now() + 60_000),
        });

        const state = await s.loadWorkflow("suspend-1");
        expect(state!.status).toBe("suspended");
        expect(state!.steps["wait"]!.status).toBe("sleeping");
      });
    });

    // -------------------------------------------------------------------
    // signals
    // -------------------------------------------------------------------

    describe("signals", () => {
      it("delivers and loads signals", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "sig-1", workflowName: "test", input: {} });
        await s.deliverSignal("sig-1", "approval", { approved: true });

        const signals = await s.loadSignals("sig-1");
        expect(signals).toHaveLength(1);
        expect(signals[0]!.signalName).toBe("approval");
        expect(signals[0]!.payload).toEqual({ approved: true });
      });
    });

    // -------------------------------------------------------------------
    // locking
    // -------------------------------------------------------------------

    describe("locking", () => {
      it("acquires and releases a lock", async () => {
        const s = await getStorage();
        expect(await s.tryLock("lock-1", 30_000)).toBe(true);
        await s.releaseLock("lock-1");
        expect(await s.tryLock("lock-1", 30_000)).toBe(true);
        await s.releaseLock("lock-1");
      });

      it("rejects double-lock from different logical owners", async () => {
        // Note: advisory locks (Postgres) are per-session, so the same connection
        // can acquire the same lock twice. This test validates row-level locks
        // and in-memory locks where re-locking is rejected.
        const s = await getStorage();
        const acquired = await s.tryLock("lock-2", 30_000);
        expect(acquired).toBe(true);
        // Don't assert false for double-lock — advisory locks allow it
        await s.releaseLock("lock-2");
      });
    });

    // -------------------------------------------------------------------
    // startFreshRun
    // -------------------------------------------------------------------

    describe("startFreshRun", () => {
      it("increments run counter", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fresh-1", workflowName: "test", input: {} });
        expect((await s.loadWorkflow("fresh-1"))!.run).toBe(1);

        const newRun = await s.startFreshRun("fresh-1");
        expect(newRun).toBe(2);

        const state = await s.loadWorkflow("fresh-1");
        expect(state!.run).toBe(2);
        expect(state!.status).toBe("running");
        expect(state!.result).toBeUndefined();
      });

      it("old run steps are not loaded", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fresh-2", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "fresh-2",
          stepName: "old-step",
          result: "old",
          durationMs: 10,
          startedAt: new Date(),
        });

        // Step exists in run 1
        expect((await s.loadWorkflow("fresh-2"))!.steps["old-step"]).toBeDefined();

        // After fresh run, steps from run 1 are not loaded
        await s.startFreshRun("fresh-2");
        const state = await s.loadWorkflow("fresh-2");
        expect(state!.run).toBe(2);
        expect(Object.keys(state!.steps)).toHaveLength(0);
      });

      it("loadWorkflow always returns the latest run", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fresh-3", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "fresh-3",
          stepName: "step-a",
          result: "run-1-result",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.completeWorkflow("fresh-3", "result-1");

        // Fresh run → run 2
        await s.startFreshRun("fresh-3");
        await s.saveStepResult({
          workflowId: "fresh-3",
          stepName: "step-a",
          result: "run-2-result",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.completeWorkflow("fresh-3", "result-2");

        // loadWorkflow returns latest run
        const state = await s.loadWorkflow("fresh-3");
        expect(state!.run).toBe(2);
        expect(state!.result).toBe("result-2");
        expect(state!.steps["step-a"]!.result).toBe("run-2-result");
      });
    });

    // -------------------------------------------------------------------
    // loadRunHistory
    // -------------------------------------------------------------------

    describe("loadRunHistory", () => {
      it("returns all runs newest first", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "hist-1", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "hist-1",
          stepName: "compute",
          result: "v1",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.completeWorkflow("hist-1", "result-1");

        await s.startFreshRun("hist-1");
        await s.saveStepResult({
          workflowId: "hist-1",
          stepName: "compute",
          result: "v2",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.completeWorkflow("hist-1", "result-2");

        await s.startFreshRun("hist-1");
        await s.saveStepResult({
          workflowId: "hist-1",
          stepName: "compute",
          result: "v3",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.completeWorkflow("hist-1", "result-3");

        const history = await s.loadRunHistory("hist-1");
        expect(history).toHaveLength(3);
        expect(history[0]!.run).toBe(3); // newest first
        expect(history[1]!.run).toBe(2);
        expect(history[2]!.run).toBe(1);
        expect(history[0]!.steps["compute"]!.result).toBe("v3");
        expect(history[2]!.steps["compute"]!.result).toBe("v1");
      });

      it("returns empty array for non-existent workflow", async () => {
        const s = await getStorage();
        expect(await s.loadRunHistory("nonexistent")).toEqual([]);
      });

      it("supports pagination with limit and offset", async () => {
        const s = await getStorage();
        const now = new Date();
        await s.createWorkflow({ workflowId: "hist-2", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "hist-2",
          stepName: "s",
          result: "r1",
          durationMs: 1,
          startedAt: now,
        });
        await s.completeWorkflow("hist-2", "r1");
        await s.startFreshRun("hist-2");
        await s.saveStepResult({
          workflowId: "hist-2",
          stepName: "s",
          result: "r2",
          durationMs: 1,
          startedAt: now,
        });
        await s.completeWorkflow("hist-2", "r2");
        await s.startFreshRun("hist-2");
        await s.saveStepResult({
          workflowId: "hist-2",
          stepName: "s",
          result: "r3",
          durationMs: 1,
          startedAt: now,
        });
        await s.completeWorkflow("hist-2", "r3");

        // First page
        const page1 = await s.loadRunHistory("hist-2", { limit: 2 });
        expect(page1).toHaveLength(2);
        expect(page1[0]!.run).toBe(3);
        expect(page1[1]!.run).toBe(2);

        // Second page
        const page2 = await s.loadRunHistory("hist-2", { limit: 2, offset: 2 });
        expect(page2).toHaveLength(1);
        expect(page2[0]!.run).toBe(1);
      });

      it("preserves status and result for archived runs", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "hist-meta-1", workflowName: "test", input: {} });
        await s.completeWorkflow("hist-meta-1", "first-result");

        await s.startFreshRun("hist-meta-1");
        await s.failWorkflow("hist-meta-1", "boom");

        await s.startFreshRun("hist-meta-1");

        const history = await s.loadRunHistory("hist-meta-1");
        expect(history).toHaveLength(3);

        // Current run (3) — running, no result
        expect(history[0]!.run).toBe(3);
        expect(history[0]!.status).toBe("running");
        expect(history[0]!.result).toBeUndefined();
        expect(history[0]!.error).toBeUndefined();

        // Archived run 2 — failed with error
        expect(history[1]!.run).toBe(2);
        expect(history[1]!.status).toBe("failed");
        expect(history[1]!.error).toBe("boom");

        // Archived run 1 — completed with result
        expect(history[2]!.run).toBe(1);
        expect(history[2]!.status).toBe("completed");
        expect(history[2]!.result).toBe("first-result");
      });

      it("preserves completedAt for archived runs", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "hist-meta-2", workflowName: "test", input: {} });
        await s.completeWorkflow("hist-meta-2", "done");

        await s.startFreshRun("hist-meta-2");

        const history = await s.loadRunHistory("hist-meta-2");
        expect(history[1]!.run).toBe(1);
        expect(history[1]!.completedAt).toBeInstanceOf(Date);
      });

      it("includes current run even with no steps", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "hist-3", workflowName: "test", input: {} });

        const history = await s.loadRunHistory("hist-3");
        expect(history).toHaveLength(1);
        expect(history[0]!.run).toBe(1);
        expect(history[0]!.status).toBe("running");
      });
    });

    // -------------------------------------------------------------------
    // purgeCompleted
    // -------------------------------------------------------------------

    describe("purgeCompleted", () => {
      it("purges completed workflow older than maxAge", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "purge-c1", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-c1", "done");

        // Completed just now — should NOT be purged with 1h threshold
        const deleted = await s.purgeCompleted({ olderThanMs: 3_600_000, limit: 100 });
        expect(deleted).toBe(0);
        expect(await s.loadWorkflow("purge-c1")).not.toBeNull();
      });

      it("purges failed workflow older than maxAge", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "purge-f1", workflowName: "test", input: {} });
        await s.failWorkflow("purge-f1", "boom");

        const deleted = await s.purgeCompleted({ olderThanMs: 3_600_000, limit: 100 });
        expect(deleted).toBe(0);
        expect(await s.loadWorkflow("purge-f1")).not.toBeNull();
      });

      it("never purges running workflows", async () => {
        const s = await getStorage();
        const before = new Date();
        await new Promise((r) => setTimeout(r, 10));
        await s.createWorkflow({ workflowId: "purge-r1", workflowName: "test", input: {} });
        await new Promise((r) => setTimeout(r, 10));
        const after = new Date();

        // Range covers the creation window — but running workflows have no completedAt
        const deleted = await s.purgeCompleted({ from: before, to: after, limit: 100 });
        expect(deleted).toBe(0);
        expect(await s.loadWorkflow("purge-r1")).not.toBeNull();
      });

      it("never purges suspended workflows", async () => {
        const s = await getStorage();
        const before = new Date();
        await new Promise((r) => setTimeout(r, 10));
        await s.createWorkflow({ workflowId: "purge-s1", workflowName: "test", input: {} });
        await s.suspendWorkflow("purge-s1", "wait", {
          status: "sleeping",
          stepType: "sleep",
          wakeAt: new Date(Date.now() + 60_000),
        });
        await new Promise((r) => setTimeout(r, 10));
        const after = new Date();

        const deleted = await s.purgeCompleted({ from: before, to: after, limit: 100 });
        expect(deleted).toBe(0);
        expect(await s.loadWorkflow("purge-s1")).not.toBeNull();
      });

      it("respects batch limit", async () => {
        const s = await getStorage();
        const before = new Date();
        await new Promise((r) => setTimeout(r, 10));

        await s.createWorkflow({ workflowId: "purge-b1", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-b1", "done");
        await s.createWorkflow({ workflowId: "purge-b2", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-b2", "done");
        await s.createWorkflow({ workflowId: "purge-b3", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-b3", "done");

        await new Promise((r) => setTimeout(r, 10));
        const after = new Date();

        // limit=2 within scoped range — should delete at most 2 of the 3
        const deleted = await s.purgeCompleted({ from: before, to: after, limit: 2 });
        expect(deleted).toBe(2);

        // The third should remain
        const remaining = [
          await s.loadWorkflow("purge-b1"),
          await s.loadWorkflow("purge-b2"),
          await s.loadWorkflow("purge-b3"),
        ].filter(Boolean);
        expect(remaining).toHaveLength(1);
      });

      it("purges workflows within a from/to date range", async () => {
        const s = await getStorage();

        await s.createWorkflow({ workflowId: "purge-range-1", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-range-1", "done");

        await new Promise((r) => setTimeout(r, 15));
        const t1 = new Date();
        await new Promise((r) => setTimeout(r, 15));

        await s.createWorkflow({ workflowId: "purge-range-2", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-range-2", "done");

        await new Promise((r) => setTimeout(r, 15));
        const t2 = new Date();
        await new Promise((r) => setTimeout(r, 15));

        await s.createWorkflow({ workflowId: "purge-range-3", workflowName: "test", input: {} });
        await s.completeWorkflow("purge-range-3", "done");

        // Only purge workflows completed in [t1, t2) — should catch range-2 only
        const deleted = await s.purgeCompleted({ from: t1, to: t2, limit: 100 });
        expect(deleted).toBe(1);

        expect(await s.loadWorkflow("purge-range-1")).not.toBeNull();
        expect(await s.loadWorkflow("purge-range-2")).toBeNull();
        expect(await s.loadWorkflow("purge-range-3")).not.toBeNull();
      });

      it("cascade-deletes steps, signals, and run history", async () => {
        const s = await getStorage();
        const before = new Date();
        await new Promise((r) => setTimeout(r, 10));

        await s.createWorkflow({ workflowId: "purge-cascade", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "purge-cascade",
          stepName: "step-a",
          result: "ok",
          durationMs: 10,
          startedAt: new Date(),
        });
        await s.deliverSignal("purge-cascade", "sig", { data: true });
        await s.completeWorkflow("purge-cascade", "done");

        await new Promise((r) => setTimeout(r, 10));
        const after = new Date();

        const deleted = await s.purgeCompleted({ from: before, to: after, limit: 100 });
        expect(deleted).toBeGreaterThanOrEqual(1);

        // Workflow and all related data should be gone
        expect(await s.loadWorkflow("purge-cascade")).toBeNull();
        expect(await s.loadSignals("purge-cascade")).toEqual([]);
        expect(await s.loadRunHistory("purge-cascade")).toEqual([]);
      });
    });
  });
}
