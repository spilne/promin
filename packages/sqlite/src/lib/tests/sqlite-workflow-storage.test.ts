import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { storageTestSuite } from "@promin/workflow/testing";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

function makeStorage() {
  return SqliteWorkflowStorage.make({ db: new Database(":memory:") });
}

// ---- conformance suite (core + journal + suspend) ----

storageTestSuite(makeStorage, {
  hasJournal: true,
  hasJournaledSuspend: true,
  hasResetSteps: true,
});

// ---- SQLite-specific tests ----

describe("SqliteWorkflowStorage", () => {
  it("persists across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const s1 = SqliteWorkflowStorage.make({ db });
    await s1.createWorkflow({ workflowId: "persist-1", workflowName: "test", input: { x: 1 } });

    const s2 = SqliteWorkflowStorage.make({ db });
    const state = await s2.loadWorkflow("persist-1");
    expect(state).not.toBeNull();
    expect(state!.input).toEqual({ x: 1 });
  });

  it("custom table prefix avoids conflicts", async () => {
    const db = new Database(":memory:");
    const a = SqliteWorkflowStorage.make({ db, tablePrefix: "wf_a" });
    const b = SqliteWorkflowStorage.make({ db, tablePrefix: "wf_b" });

    await a.createWorkflow({ workflowId: "same-id", workflowName: "a", input: {} });
    await b.createWorkflow({ workflowId: "same-id", workflowName: "b", input: {} });

    expect((await a.loadWorkflow("same-id"))!.workflowName).toBe("a");
    expect((await b.loadWorkflow("same-id"))!.workflowName).toBe("b");
  });

  it("cancelWorkflow cascade cancels children", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "parent-1", workflowName: "parent", input: {} });
    await s.createWorkflow({
      workflowId: "child-1",
      workflowName: "child",
      input: {},
      parentWorkflowId: "parent-1",
    });

    await s.cancelWorkflow("parent-1", { cascade: true });

    expect((await s.loadWorkflow("parent-1"))!.status).toBe("failed");
    expect((await s.loadWorkflow("child-1"))!.status).toBe("failed");
  });

  it("loadRunHistory reflects archived runs in order", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "hist-sqlite", workflowName: "test", input: {} });
    await s.completeWorkflow("hist-sqlite", "run-1");
    await s.startFreshRun("hist-sqlite");
    await s.completeWorkflow("hist-sqlite", "run-2");

    const history = await s.loadRunHistory("hist-sqlite");
    expect(history).toHaveLength(2);
    expect(history[0]!.run).toBe(2);
    expect(history[0]!.result).toBe("run-2");
    expect(history[1]!.run).toBe(1);
    expect(history[1]!.result).toBe("run-1");
  });

  it("fence token counter survives across storage instances on the same db", async () => {
    const db = new Database(":memory:");
    const s1 = SqliteWorkflowStorage.make({ db });
    await s1.createWorkflow({ workflowId: "fence-persist", workflowName: "t", input: {} });
    const { token: t1 } = await s1.tryLock("fence-persist", 60_000);
    expect(t1).toBeDefined();

    // A fresh instance on the same db should not reuse the same token
    const s2 = SqliteWorkflowStorage.make({ db });
    await s1.releaseLock("fence-persist", { fenceToken: t1 });
    const { token: t2 } = await s2.tryLock("fence-persist", 60_000);
    expect(t2).toBeDefined();
    expect(t2).not.toBe(t1);
  });

  it("purgeCompleted removes signals and run history", async () => {
    const s = makeStorage();
    const before = new Date();
    await new Promise((r) => setTimeout(r, 10));
    await s.createWorkflow({ workflowId: "purge-sqlite", workflowName: "test", input: {} });
    await s.deliverSignal("purge-sqlite", "done", { ok: true });
    await s.completeWorkflow("purge-sqlite", "result");
    await new Promise((r) => setTimeout(r, 10));
    const after = new Date();

    await s.purgeCompleted({ from: before, to: after, limit: 100 });

    expect(await s.loadWorkflow("purge-sqlite")).toBeNull();
    expect(await s.loadSignals("purge-sqlite")).toEqual([]);
    expect(await s.loadRunHistory("purge-sqlite")).toEqual([]);
  });

  it("countWorkflows returns exact counts per status", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "cw-1", workflowName: "wf", input: {} });
    await s.createWorkflow({ workflowId: "cw-2", workflowName: "wf", input: {} });
    await s.createWorkflow({ workflowId: "cw-3", workflowName: "wf", input: {} });
    await s.completeWorkflow("cw-1", "done");

    expect(await s.countWorkflows({ status: "completed" })).toBe(1);
    expect(await s.countWorkflows({ status: "pending" })).toBe(2);
    expect(await s.countWorkflows()).toBe(3);
    expect(await s.countWorkflows({ name: "wf" })).toBe(3);
    expect(await s.countWorkflows({ name: "other" })).toBe(0);
  });

  it("listWorkflowSummaries omits steps/input/result blobs", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "ls-1", workflowName: "wf", input: { secret: true } });
    await s.saveStepResult({
      workflowId: "ls-1",
      stepName: "step1",
      result: { big: "payload" },
      durationMs: 10,
      startedAt: new Date(),
    });

    const summaries = await s.listWorkflowSummaries({ name: "wf" });
    expect(summaries).toHaveLength(1);
    expect(summaries[0]!.workflowId).toBe("ls-1");
    expect(summaries[0]!.status).toBe("running");
    // steps / input / result must not be present on WorkflowSummary
    expect("steps" in summaries[0]!).toBe(false);
    expect("input" in summaries[0]!).toBe(false);
    expect("result" in summaries[0]!).toBe(false);
  });

  it("cancelStaleWorkflows bulk-fails runs older than the cutoff", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "stale-1", workflowName: "wf", input: {} });
    await s.createWorkflow({ workflowId: "stale-2", workflowName: "wf", input: {} });
    await s.createWorkflow({ workflowId: "stale-3", workflowName: "wf", input: {} });
    await s.completeWorkflow("stale-3", "done");

    // Wait a tick so created_at is clearly in the past, then cancel with 0ms cutoff.
    await new Promise((r) => setTimeout(r, 5));
    const cancelled = s.cancelStaleWorkflows({ olderThanMs: 0, statuses: ["pending"] });

    expect(cancelled).toBe(2); // stale-1 and stale-2 (not stale-3, already completed)
    expect((await s.loadWorkflow("stale-1"))!.status).toBe("failed");
    expect((await s.loadWorkflow("stale-2"))!.status).toBe("failed");
    expect((await s.loadWorkflow("stale-3"))!.status).toBe("completed");
  });

  it("cancelStaleWorkflows respects the olderThanMs threshold", async () => {
    const s = makeStorage();
    await s.createWorkflow({ workflowId: "fresh-1", workflowName: "wf", input: {} });

    // 1-hour cutoff — the row was just created so it should NOT be cancelled.
    const cancelled = s.cancelStaleWorkflows({ olderThanMs: 60 * 60 * 1000 });
    expect(cancelled).toBe(0);
    expect((await s.loadWorkflow("fresh-1"))!.status).toBe("pending");
  });
});
