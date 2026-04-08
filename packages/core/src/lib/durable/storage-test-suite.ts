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

      it("rejects double-lock", async () => {
        const s = await getStorage();
        expect(await s.tryLock("lock-2", 30_000)).toBe(true);
        expect(await s.tryLock("lock-2", 30_000)).toBe(false);
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
    });
  });
}
