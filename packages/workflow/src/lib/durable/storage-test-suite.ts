// ---------------------------------------------------------------------------
// Portable WorkflowStorage test suite
//
// Usage:
//   import { storageTestSuite } from "@promin/workflow/testing";
//   storageTestSuite(() => new MyCustomStorage());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { WorkflowStorage } from "./workflow-storage.ts";
import {
  isActivityJournalStorage,
  isJournaledSuspendStorage,
  JOURNAL_STEP_TYPES,
  type ActivityJournalStorage,
  type JournaledSuspendStorage,
} from "./activity-journal.ts";

export interface StorageTestSuiteOptions {
  /**
   * Opt in to the `ActivityJournalStorage` conformance section.
   * Defaults to `false`. When `true`, the factory must return a storage that
   * also implements `ActivityJournalStorage` (`loadJournal` / `appendEntry`).
   */
  hasJournal?: boolean;
  /**
   * Opt in to the `JournaledSuspendStorage` conformance section
   * (`appendPendingEntry` / `completePendingEntry` / `findDueSleeps` /
   * `findPendingSignal`). Implies `hasJournal: true`.
   */
  hasJournaledSuspend?: boolean;
  /**
   * Run the `discardJournalEntries` cases. Defaults to `hasJournaledSuspend`;
   * pass `false` for a suspend-capable storage that leaves the optional
   * method out.
   */
  hasJournalDiscard?: boolean;
  /**
   * Opt in to the `resetSteps` conformance section. Defaults to `false`.
   * When `true`, the factory must return a storage that implements the
   * optional `resetSteps` method (backs `WorkflowRunner.resume`).
   */
  hasResetSteps?: boolean;
  /**
   * Opt in to the scanner-query conformance section. Defaults to `false`.
   * When `true`, the factory must return a storage that implements the
   * optional `listDueTimers`, `listSignalWakeups` and `listOrphanedRuns`
   * methods (back the sleep / signal scanners and coordinator recovery).
   */
  hasScannerQueries?: boolean;
  /**
   * Build a second storage instance over the same backend as `storage` —
   * what another process, pool client or worker would hold. Enables the
   * cross-instance lock-exclusion and fence-token cases. Omit for
   * single-instance backends (in-memory), where those cases fall back to
   * exercising one instance.
   */
  createPeer?: (storage: WorkflowStorage) => WorkflowStorage | Promise<WorkflowStorage>;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Run the full WorkflowStorage conformance suite against any implementation.
 * Verifies CRUD, steps, tasks, locking, signals, suspend, complete/fail,
 * listing, cancellation, fresh runs, and — when opted in — the optional
 * journal/suspend extensions.
 *
 * @param factory — called before each test group to get a fresh storage instance
 * @param options — opt-in flags for optional storage capabilities
 */
export function storageTestSuite(
  factory: () => WorkflowStorage | Promise<WorkflowStorage>,
  options: StorageTestSuiteOptions = {},
) {
  let storage: WorkflowStorage;

  async function getStorage(): Promise<WorkflowStorage> {
    storage = await factory();
    return storage;
  }

  async function getJournalStorage(): Promise<WorkflowStorage & ActivityJournalStorage> {
    const s = await getStorage();
    if (!isActivityJournalStorage(s)) {
      throw new Error(
        "storageTestSuite was invoked with hasJournal: true, but the factory returned " +
          "a storage that does not implement ActivityJournalStorage.",
      );
    }
    return s;
  }

  /** A second instance over the same backend (or the same instance when none is configured). */
  async function getPeer(s: WorkflowStorage): Promise<WorkflowStorage> {
    return options.createPeer ? options.createPeer(s) : s;
  }

  async function getSuspendStorage(): Promise<WorkflowStorage & JournaledSuspendStorage> {
    const s = await getJournalStorage();
    if (!isJournaledSuspendStorage(s)) {
      throw new Error(
        "storageTestSuite was invoked with hasJournaledSuspend: true, but the factory " +
          "returned a storage that does not implement JournaledSuspendStorage.",
      );
    }
    return s as WorkflowStorage & JournaledSuspendStorage;
  }

  describe("WorkflowStorage conformance", () => {
    // -------------------------------------------------------------------
    // createWorkflow + loadWorkflow
    // -------------------------------------------------------------------

    describe("createWorkflow + loadWorkflow", () => {
      it("creates and loads a workflow", async () => {
        const s = await getStorage();
        const result = await s.createWorkflow({
          workflowId: "crud-1",
          workflowName: "test-wf",
          input: { userId: "u_42" },
          workflowType: "onboarding",
          metadata: { region: "us-east" },
        });
        expect(result.created).toBe(true);

        const state = await s.loadWorkflow("crud-1");
        expect(state).not.toBeNull();
        expect(state!.workflowId).toBe("crud-1");
        expect(state!.workflowName).toBe("test-wf");
        expect(state!.workflowType).toBe("onboarding");
        expect(state!.status).toBe("pending");
        expect(state!.run).toBe(1);
        expect(state!.input).toEqual({ userId: "u_42" });
        expect(state!.metadata).toEqual({ region: "us-east" });
        expect(state!.createdAt).toBeInstanceOf(Date);
        expect(state!.startedAt).toBeUndefined();
      });

      it("returns existing workflow on duplicate create", async () => {
        const s = await getStorage();
        const result1 = await s.createWorkflow({
          workflowId: "dup-1",
          workflowName: "test",
          input: { a: 1 },
        });
        expect(result1.created).toBe(true);

        const result2 = await s.createWorkflow({
          workflowId: "dup-1",
          workflowName: "test",
          input: { a: 2 },
        });
        expect(result2.created).toBe(false);
        if (!result2.created) {
          expect(result2.existing.workflowId).toBe("dup-1");
          expect(result2.existing.input).toEqual({ a: 1 }); // original input preserved
        }
      });

      it("scopes idempotency-key creates by namespace", async () => {
        const s = await getStorage();
        const expires = new Date(Date.now() + 60_000);

        const first = await s.createWorkflow({
          workflowId: "idem-ns-a1",
          workflowName: "compute",
          namespace: "team-a",
          input: { n: 1 },
          idempotencyKey: "shared",
          idempotencyExpiresAt: expires,
        });
        const sameNamespace = await s.createWorkflow({
          workflowId: "idem-ns-a2",
          workflowName: "compute",
          namespace: "team-a",
          input: { n: 2 },
          idempotencyKey: "shared",
          idempotencyExpiresAt: expires,
        });
        const otherNamespace = await s.createWorkflow({
          workflowId: "idem-ns-b1",
          workflowName: "compute",
          namespace: "team-b",
          input: { n: 3 },
          idempotencyKey: "shared",
          idempotencyExpiresAt: expires,
        });

        expect(first.created).toBe(true);
        expect(sameNamespace.created).toBe(false);
        if (!sameNamespace.created) expect(sameNamespace.existing.workflowId).toBe("idem-ns-a1");
        expect(otherNamespace.created).toBe(true);

        await expect(
          s.findWorkflowByIdempotencyKey({
            workflowName: "compute",
            namespace: "team-a",
            idempotencyKey: "shared",
            now: new Date(),
          }),
        ).resolves.toEqual({ workflowId: "idem-ns-a1" });
        await expect(
          s.findWorkflowByIdempotencyKey({
            workflowName: "compute",
            namespace: "team-b",
            idempotencyKey: "shared",
            now: new Date(),
          }),
        ).resolves.toEqual({ workflowId: "idem-ns-b1" });
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

    describe("pending → running transition", () => {
      it("transitions to running when first step is saved", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "pending-1", workflowName: "test", input: {} });
        expect((await s.loadWorkflow("pending-1"))!.status).toBe("pending");

        await s.saveStepResult({
          workflowId: "pending-1",
          stepName: "step-a",
          result: "ok",
          durationMs: 10,
          startedAt: new Date(),
        });

        const state = await s.loadWorkflow("pending-1");
        expect(state!.status).toBe("running");
        expect(state!.startedAt).toBeInstanceOf(Date);
      });
    });

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
        // metadata stays undefined when the caller didn't provide any —
        // NOT an empty object, so queries like `metadata->>'matchCase'`
        // distinguish "step with no audit data" from "step with unknown case".
        expect(step!.metadata).toBeUndefined();
      });

      it("round-trips metadata on successful step", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "step-meta-ok", workflowName: "test", input: {} });
        await s.saveStepResult({
          workflowId: "step-meta-ok",
          stepName: "route",
          result: "done",
          durationMs: 5,
          startedAt: new Date(),
          metadata: { matchCase: "express", matchMode: "selector" },
        });

        const step = (await s.loadWorkflow("step-meta-ok"))!.steps["route"]!;
        expect(step.metadata).toEqual({ matchCase: "express", matchMode: "selector" });
      });
    });

    // -------------------------------------------------------------------
    // batchSaveStepResults
    // -------------------------------------------------------------------

    describe("batchSaveStepResults", () => {
      it("persists every record in the batch", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "batch-wf", workflowName: "t", input: {} });
        const startedAt = new Date();
        await s.batchSaveStepResults([
          { workflowId: "batch-wf", stepName: "a", result: 1, durationMs: 5, startedAt },
          { workflowId: "batch-wf", stepName: "b", result: 2, durationMs: 6, startedAt },
          { workflowId: "batch-wf", stepName: "c", result: 3, durationMs: 7, startedAt },
        ]);
        const state = (await s.loadWorkflow("batch-wf"))!;
        expect(state.steps["a"]!.result).toBe(1);
        expect(state.steps["b"]!.result).toBe(2);
        expect(state.steps["c"]!.result).toBe(3);
        expect(state.steps["a"]!.status).toBe("completed");
        expect(state.steps["b"]!.status).toBe("completed");
        expect(state.steps["c"]!.status).toBe("completed");
      });

      it("handles an empty batch as a no-op", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "batch-empty", workflowName: "t", input: {} });
        await s.batchSaveStepResults([]);
        const state = (await s.loadWorkflow("batch-empty"))!;
        expect(Object.keys(state.steps)).toHaveLength(0);
      });

      it("round-trips metadata per record", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "batch-meta", workflowName: "t", input: {} });
        const startedAt = new Date();
        await s.batchSaveStepResults([
          {
            workflowId: "batch-meta",
            stepName: "x",
            result: "ok",
            durationMs: 1,
            startedAt,
            metadata: { chose: "fast" },
          },
          {
            workflowId: "batch-meta",
            stepName: "y",
            result: "ok",
            durationMs: 1,
            startedAt,
            metadata: { chose: "slow" },
          },
        ]);
        const state = (await s.loadWorkflow("batch-meta"))!;
        expect(state.steps["x"]!.metadata).toEqual({ chose: "fast" });
        expect(state.steps["y"]!.metadata).toEqual({ chose: "slow" });
      });

      it("flips a pending workflow to running — same side effect as saveStepResult", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "batch-running", workflowName: "t", input: {} });
        const pre = (await s.loadWorkflow("batch-running"))!;
        expect(pre.status).toBe("pending");

        await s.batchSaveStepResults([
          {
            workflowId: "batch-running",
            stepName: "go",
            result: "ok",
            durationMs: 1,
            startedAt: new Date(),
          },
        ]);

        const post = (await s.loadWorkflow("batch-running"))!;
        expect(post.status).toBe("running");
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

      it("round-trips metadata on failed step — 'which case fired' survives a throw", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "step-meta-fail", workflowName: "test", input: {} });
        await s.saveStepFailure({
          workflowId: "step-meta-fail",
          stepName: "route",
          error: "branch threw",
          durationMs: 3,
          startedAt: new Date(),
          metadata: { matchCase: "express", matchMode: "selector" },
        });

        const step = (await s.loadWorkflow("step-meta-fail"))!.steps["route"]!;
        expect(step.status).toBe("failed");
        expect(step.metadata).toEqual({ matchCase: "express", matchMode: "selector" });
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

      it("tripwires a workflow with a structured reason", async () => {
        const s = await getStorage();
        if (typeof s.tripwireWorkflow !== "function") return;
        await s.createWorkflow({ workflowId: "trip-1", workflowName: "test", input: {} });
        await s.tripwireWorkflow("trip-1", { code: "fraud", score: 0.97 });

        const state = await s.loadWorkflow("trip-1");
        expect(state!.status).toBe("tripwire");
        expect(state!.tripwire).toEqual({ code: "fraud", score: 0.97 });
        expect(state!.completedAt).toBeInstanceOf(Date);
        // Tripwire is distinct from failed — error field stays unset.
        expect(state!.error).toBeUndefined();
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

      it("default order is startedAt desc, NULLS LAST — most-recently-started first, pending last", async () => {
        const s = await getStorage();
        // Three workflows created in order; first two get a step (which
        // marks them running and sets startedAt). Third stays pending.
        await s.createWorkflow({ workflowId: "ord-default-1", workflowName: "t", input: {} });
        await s.saveStepResult({
          workflowId: "ord-default-1",
          stepName: "go",
          result: "ok",
          durationMs: 1,
          startedAt: new Date(),
        });
        await new Promise((r) => setTimeout(r, 10));
        await s.createWorkflow({ workflowId: "ord-default-2", workflowName: "t", input: {} });
        await s.saveStepResult({
          workflowId: "ord-default-2",
          stepName: "go",
          result: "ok",
          durationMs: 1,
          startedAt: new Date(),
        });
        await s.createWorkflow({ workflowId: "ord-default-3", workflowName: "t", input: {} });

        const rows = await s.listWorkflows({ name: "t" });
        const ids = rows.map((r) => r.workflowId);
        // 2 started after 1, both before 3 (which never started).
        expect(ids[0]).toBe("ord-default-2");
        expect(ids[1]).toBe("ord-default-1");
        expect(ids[2]).toBe("ord-default-3");
      });

      it("orderBy=name asc returns alphabetical order", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "ord-name-1", workflowName: "gamma", input: {} });
        await s.createWorkflow({ workflowId: "ord-name-2", workflowName: "alpha", input: {} });
        await s.createWorkflow({ workflowId: "ord-name-3", workflowName: "beta", input: {} });

        const rows = await s.listWorkflows({ orderBy: "name", orderDir: "asc" });
        expect(rows.map((r) => r.workflowName)).toEqual(["alpha", "beta", "gamma"]);
      });

      it("orderBy=duration sorts NULL (still-running) last in both directions", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "ord-dur-running", workflowName: "dur", input: {} });
        await s.createWorkflow({ workflowId: "ord-dur-fast", workflowName: "dur", input: {} });
        await new Promise((r) => setTimeout(r, 30));
        await s.completeWorkflow("ord-dur-fast", "ok");
        await s.createWorkflow({ workflowId: "ord-dur-slow", workflowName: "dur", input: {} });
        await new Promise((r) => setTimeout(r, 60));
        await s.completeWorkflow("ord-dur-slow", "ok");

        const ascRows = await s.listWorkflows({
          name: "dur",
          orderBy: "duration",
          orderDir: "asc",
        });
        const ascIds = ascRows.map((r) => r.workflowId);
        // Running row sorts last regardless of direction.
        expect(ascIds[ascIds.length - 1]).toBe("ord-dur-running");
        // Among completed: shorter duration first when asc.
        const completedAsc = ascIds.filter((id) => id !== "ord-dur-running");
        expect(completedAsc).toEqual(["ord-dur-fast", "ord-dur-slow"]);

        const descRows = await s.listWorkflows({
          name: "dur",
          orderBy: "duration",
          orderDir: "desc",
        });
        const descIds = descRows.map((r) => r.workflowId);
        expect(descIds[descIds.length - 1]).toBe("ord-dur-running");
        const completedDesc = descIds.filter((id) => id !== "ord-dur-running");
        expect(completedDesc).toEqual(["ord-dur-slow", "ord-dur-fast"]);
      });

      it("orderBy=startedAt sorts NULL (pending) last in both directions", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "ord-st-pending", workflowName: "st", input: {} });
        await s.createWorkflow({ workflowId: "ord-st-started", workflowName: "st", input: {} });
        await s.saveStepResult({
          workflowId: "ord-st-started",
          stepName: "go",
          result: "ok",
          durationMs: 1,
          startedAt: new Date(),
        });

        const asc = await s.listWorkflows({ name: "st", orderBy: "startedAt", orderDir: "asc" });
        expect(asc.map((r) => r.workflowId)).toEqual(["ord-st-started", "ord-st-pending"]);

        const desc = await s.listWorkflows({ name: "st", orderBy: "startedAt", orderDir: "desc" });
        expect(desc.map((r) => r.workflowId)).toEqual(["ord-st-started", "ord-st-pending"]);
      });

      it("orderBy + limit + offset compose for paginated sorted results", async () => {
        const s = await getStorage();
        for (const n of ["delta", "alpha", "echo", "beta", "charlie"]) {
          await s.createWorkflow({ workflowId: `ord-pg-${n}`, workflowName: n, input: {} });
        }
        const page1 = await s.listWorkflows({
          orderBy: "name",
          orderDir: "asc",
          limit: 2,
          offset: 0,
        });
        const page2 = await s.listWorkflows({
          orderBy: "name",
          orderDir: "asc",
          limit: 2,
          offset: 2,
        });
        expect(page1.map((r) => r.workflowName)).toEqual(["alpha", "beta"]);
        expect(page2.map((r) => r.workflowName)).toEqual(["charlie", "delta"]);
      });

      it("filters by a single metadata key/value pair", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-1",
          workflowName: "meta",
          input: {},
          metadata: { userId: "u_42" },
        });
        await s.createWorkflow({
          workflowId: "md-2",
          workflowName: "meta",
          input: {},
          metadata: { userId: "u_99" },
        });
        await s.createWorkflow({ workflowId: "md-3", workflowName: "meta", input: {} });

        const hits = await s.listWorkflows({ metadata: { userId: "u_42" } });
        expect(hits.map((r) => r.workflowId)).toEqual(["md-1"]);
      });

      it("filters by multiple metadata pairs (AND semantics)", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-and-1",
          workflowName: "meta",
          input: {},
          metadata: { userId: "u_42", priority: "high" },
        });
        await s.createWorkflow({
          workflowId: "md-and-2",
          workflowName: "meta",
          input: {},
          metadata: { userId: "u_42", priority: "low" },
        });

        const hits = await s.listWorkflows({
          metadata: { userId: "u_42", priority: "high" },
        });
        expect(hits.map((r) => r.workflowId)).toEqual(["md-and-1"]);
      });

      it("returns empty when no workflow matches the metadata filter", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-none",
          workflowName: "meta",
          input: {},
          metadata: { userId: "u_42" },
        });
        const hits = await s.listWorkflows({ metadata: { userId: "u_999" } });
        expect(hits).toEqual([]);
      });

      it("workflows without metadata are excluded from metadata queries", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "md-empty", workflowName: "meta", input: {} });
        const hits = await s.listWorkflows({ metadata: { userId: "u_42" } });
        expect(hits).toEqual([]);
      });

      it("composes metadata filter with status filter", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-c-1",
          workflowName: "meta",
          input: {},
          metadata: { region: "us-east" },
        });
        await s.completeWorkflow("md-c-1", "ok");
        await s.createWorkflow({
          workflowId: "md-c-2",
          workflowName: "meta",
          input: {},
          metadata: { region: "us-east" },
        });

        const hits = await s.listWorkflows({
          metadata: { region: "us-east" },
          status: "completed",
        });
        expect(hits.map((r) => r.workflowId)).toEqual(["md-c-1"]);
      });

      it("filters by numeric and boolean metadata values", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-num-1",
          workflowName: "m",
          input: {},
          metadata: { retries: 3, dryRun: true },
        });
        await s.createWorkflow({
          workflowId: "md-num-2",
          workflowName: "m",
          input: {},
          metadata: { retries: 5, dryRun: false },
        });

        expect(
          (await s.listWorkflows({ metadata: { retries: 3 } })).map((r) => r.workflowId),
        ).toEqual(["md-num-1"]);
        expect(
          (await s.listWorkflows({ metadata: { dryRun: true } })).map((r) => r.workflowId),
        ).toEqual(["md-num-1"]);
      });

      it("filters by nested object metadata values (deep equality)", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "md-nest-1",
          workflowName: "m",
          input: {},
          metadata: { actor: { id: "u_1", role: "admin" } },
        });
        await s.createWorkflow({
          workflowId: "md-nest-2",
          workflowName: "m",
          input: {},
          metadata: { actor: { id: "u_1", role: "guest" } },
        });

        const hits = await s.listWorkflows({
          metadata: { actor: { id: "u_1", role: "admin" } },
        });
        expect(hits.map((r) => r.workflowId)).toEqual(["md-nest-1"]);
      });
    });

    // -------------------------------------------------------------------
    // distinctWorkflowNames / distinctWorkflowTypes / distinctNamespaces
    // -------------------------------------------------------------------

    describe("distinct values", () => {
      it("returns distinct workflow names sorted alphabetically", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "dn-1", workflowName: "alpha", input: {} });
        await s.createWorkflow({ workflowId: "dn-2", workflowName: "alpha", input: {} });
        await s.createWorkflow({ workflowId: "dn-3", workflowName: "beta", input: {} });
        await s.createWorkflow({ workflowId: "dn-4", workflowName: "gamma", input: {} });

        const names = await s.distinctWorkflowNames();
        expect(names).toEqual(["alpha", "beta", "gamma"]);
      });

      it("returns distinct workflow types, excluding undefined, sorted", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "dt-1",
          workflowName: "wf",
          input: {},
          workflowType: "ingest",
        });
        await s.createWorkflow({
          workflowId: "dt-2",
          workflowName: "wf",
          input: {},
          workflowType: "ingest",
        });
        await s.createWorkflow({
          workflowId: "dt-3",
          workflowName: "wf",
          input: {},
          workflowType: "report",
        });
        await s.createWorkflow({ workflowId: "dt-4", workflowName: "wf", input: {} });

        const types = await s.distinctWorkflowTypes();
        expect(types).toEqual(["ingest", "report"]);
      });

      it("returns distinct namespaces, excluding undefined, sorted", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "dns-1",
          workflowName: "wf",
          input: {},
          namespace: "team-a",
        });
        await s.createWorkflow({
          workflowId: "dns-2",
          workflowName: "wf",
          input: {},
          namespace: "team-a",
        });
        await s.createWorkflow({
          workflowId: "dns-3",
          workflowName: "wf",
          input: {},
          namespace: "team-b",
        });
        await s.createWorkflow({ workflowId: "dns-4", workflowName: "wf", input: {} });

        const namespaces = await s.distinctNamespaces();
        expect(namespaces).toEqual(["team-a", "team-b"]);
      });

      it("scopes distinct names by namespace when provided", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "dn-ns-1",
          workflowName: "alpha",
          input: {},
          namespace: "team-a",
        });
        await s.createWorkflow({
          workflowId: "dn-ns-2",
          workflowName: "beta",
          input: {},
          namespace: "team-a",
        });
        await s.createWorkflow({
          workflowId: "dn-ns-3",
          workflowName: "gamma",
          input: {},
          namespace: "team-b",
        });

        const teamA = await s.distinctWorkflowNames({ namespace: "team-a" });
        expect(teamA).toEqual(["alpha", "beta"]);

        const teamB = await s.distinctWorkflowNames({ namespace: "team-b" });
        expect(teamB).toEqual(["gamma"]);
      });

      it("scopes distinct types by namespace when provided", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "dt-ns-1",
          workflowName: "wf",
          input: {},
          namespace: "team-a",
          workflowType: "ingest",
        });
        await s.createWorkflow({
          workflowId: "dt-ns-2",
          workflowName: "wf",
          input: {},
          namespace: "team-b",
          workflowType: "report",
        });

        expect(await s.distinctWorkflowTypes({ namespace: "team-a" })).toEqual(["ingest"]);
        expect(await s.distinctWorkflowTypes({ namespace: "team-b" })).toEqual(["report"]);
      });

      it("returns empty arrays when storage has no workflows", async () => {
        const s = await getStorage();
        expect(await s.distinctWorkflowNames()).toEqual([]);
        expect(await s.distinctWorkflowTypes()).toEqual([]);
        expect(await s.distinctNamespaces()).toEqual([]);
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
        expect((await s.tryLock("lock-1", 30_000)).acquired).toBe(true);
        await s.releaseLock("lock-1");
        expect((await s.tryLock("lock-1", 30_000)).acquired).toBe(true);
        await s.releaseLock("lock-1");
      });

      it("rejects double-lock from the same instance and from a peer", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const first = await s.tryLock("lock-2", 30_000);
        expect(first.acquired).toBe(true);

        expect((await s.tryLock("lock-2", 30_000)).acquired).toBe(false);
        expect((await peer.tryLock("lock-2", 30_000)).acquired).toBe(false);
        const concurrent = await Promise.all(
          Array.from({ length: 5 }, () => peer.tryLock("lock-2", 30_000)),
        );
        expect(concurrent.every((r) => !r.acquired)).toBe(true);

        await s.releaseLock("lock-2", { fenceToken: first.token });
      });

      it("concurrent tryLock calls produce exactly one winner", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const results = await Promise.all(
          Array.from({ length: 8 }, (_, i) =>
            (i % 2 === 0 ? s : peer).tryLock("lock-race", 30_000),
          ),
        );
        expect(results.filter((r) => r.acquired)).toHaveLength(1);
        const winner = results.find((r) => r.acquired)!;
        await s.releaseLock("lock-race", { fenceToken: winner.token });
      });

      it("releaseLock frees the lock for a peer", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const a = await s.tryLock("lock-free", 30_000);
        expect(a.acquired).toBe(true);
        await s.releaseLock("lock-free", { fenceToken: a.token });

        const b = await peer.tryLock("lock-free", 30_000);
        expect(b.acquired).toBe(true);
        await peer.releaseLock("lock-free", { fenceToken: b.token });
      });

      it("releasing many locks concurrently frees every one of them", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const ids = Array.from({ length: 8 }, (_, i) => `lock-many-${i}`);
        const held = await Promise.all(ids.map((id) => s.tryLock(id, 30_000)));
        expect(held.every((r) => r.acquired)).toBe(true);
        await Promise.all(ids.map((id, i) => s.releaseLock(id, { fenceToken: held[i]!.token })));

        const again = await Promise.all(ids.map((id) => peer.tryLock(id, 30_000)));
        expect(again.every((r) => r.acquired)).toBe(true);
        await Promise.all(
          ids.map((id, i) => peer.releaseLock(id, { fenceToken: again[i]!.token })),
        );
      });

      it("an expired lock can be re-acquired with a newer token", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const a = await s.tryLock("lock-exp", 1);
        expect(a.acquired).toBe(true);
        await sleep(30);
        const b = await peer.tryLock("lock-exp", 30_000);
        expect(b.acquired).toBe(true);
        expect(b.token).toBeDefined();
        expect(b.token).not.toBe(a.token);
        await peer.releaseLock("lock-exp", { fenceToken: b.token });
      });

      it("heartbeat extends the lock past its original expiry", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const a = await s.tryLock("lock-hb", 600);
        expect(a.acquired).toBe(true);
        await s.heartbeat("lock-hb", 60_000, { fenceToken: a.token });
        await sleep(900);
        expect((await peer.tryLock("lock-hb", 30_000)).acquired).toBe(false);
        await s.releaseLock("lock-hb", { fenceToken: a.token });
      });

      it("a stale holder's release does not free the new holder's lock", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const stale = await s.tryLock("lock-stale-rel", 1);
        await sleep(30);
        const fresh = await peer.tryLock("lock-stale-rel", 30_000);
        expect(fresh.acquired).toBe(true);

        await s.releaseLock("lock-stale-rel", { fenceToken: stale.token });
        expect((await s.tryLock("lock-stale-rel", 30_000)).acquired).toBe(false);
        await peer.releaseLock("lock-stale-rel", { fenceToken: fresh.token });
      });

      it("tryLockAndLoad returns lock=true + the current state in one call", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "lal-ok",
          workflowName: "test",
          input: { amount: 42 },
        });
        const res = await s.tryLockAndLoad("lal-ok", 30_000);
        expect(res.locked).toBe(true);
        expect(res.state).not.toBeNull();
        expect(res.state!.workflowId).toBe("lal-ok");
        expect(res.state!.input).toEqual({ amount: 42 });
        await s.releaseLock("lal-ok");
      });

      it("tryLockAndLoad returns state=null when the workflow doesn't exist", async () => {
        const s = await getStorage();
        const res = await s.tryLockAndLoad("lal-missing", 30_000);
        expect(res.locked).toBe(true);
        expect(res.state).toBeNull();
        await s.releaseLock("lal-missing");
      });
    });

    // -------------------------------------------------------------------
    // fencing — optional; backends that don't return a token are exempt
    // -------------------------------------------------------------------

    describe("fencing", () => {
      it("tryLock returns a token that saveStepResult accepts", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fence-ok", workflowName: "t", input: {} });
        const { acquired, token } = await s.tryLock("fence-ok", 30_000);
        expect(acquired).toBe(true);
        expect(token).toBeDefined();
        await s.saveStepResult(
          {
            workflowId: "fence-ok",
            stepName: "s1",
            result: "ok",
            durationMs: 1,
            startedAt: new Date(),
          },
          { fenceToken: token },
        );
        const state = await s.loadWorkflow("fence-ok");
        expect(state?.steps["s1"]?.result).toBe("ok");
        await s.releaseLock("fence-ok", { fenceToken: token });
      });

      it("stale-token write is rejected after the lock moves on", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fence-stale", workflowName: "t", input: {} });
        // A acquires → stale.
        const { token: staleToken } = await s.tryLock("fence-stale", 1);
        expect(staleToken).toBeDefined();
        await sleep(30);
        // B acquires fresh token for the same workflow.
        const { acquired: bAcquired, token: freshToken } = await s.tryLock("fence-stale", 30_000);
        expect(bAcquired).toBe(true);
        expect(freshToken).not.toBe(staleToken);

        // A tries to write with its stale token — must be rejected.
        await expect(
          s.saveStepResult(
            {
              workflowId: "fence-stale",
              stepName: "stale-step",
              result: "should-not-persist",
              durationMs: 1,
              startedAt: new Date(),
            },
            { fenceToken: staleToken },
          ),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });

        // B's fresh write still works.
        await s.saveStepResult(
          {
            workflowId: "fence-stale",
            stepName: "fresh-step",
            result: "kept",
            durationMs: 1,
            startedAt: new Date(),
          },
          { fenceToken: freshToken },
        );
        const state = await s.loadWorkflow("fence-stale");
        expect(state?.steps["stale-step"]).toBeUndefined();
        expect(state?.steps["fresh-step"]?.result).toBe("kept");
        await s.releaseLock("fence-stale", { fenceToken: freshToken });
      });

      it("tryLockAndLoad round-trips the token to saveStepResult", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "fence-lal", workflowName: "t", input: {} });
        const res = await s.tryLockAndLoad("fence-lal", 30_000);
        expect(res.locked).toBe(true);
        expect(res.token).toBeDefined();
        await s.saveStepResult(
          {
            workflowId: "fence-lal",
            stepName: "s",
            result: 1,
            durationMs: 1,
            startedAt: new Date(),
          },
          { fenceToken: res.token },
        );
        const state = await s.loadWorkflow("fence-lal");
        expect(state?.steps["s"]?.result).toBe(1);
        await s.releaseLock("fence-lal", { fenceToken: res.token });
      });

      it("a stale token cannot complete or fail the workflow", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        await s.createWorkflow({ workflowId: "fence-term", workflowName: "t", input: {} });
        const stale = await s.tryLock("fence-term", 1);
        await sleep(30);
        const fresh = await peer.tryLock("fence-term", 30_000);
        expect(fresh.acquired).toBe(true);

        await expect(
          s.completeWorkflow("fence-term", "late", { fenceToken: stale.token }),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
        await expect(
          s.failWorkflow("fence-term", "late", { fenceToken: stale.token }),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
        expect((await s.loadWorkflow("fence-term"))!.status).toBe("pending");
        await peer.releaseLock("fence-term", { fenceToken: fresh.token });
      });

      it("tokens are never reused across instances", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        await s.createWorkflow({ workflowId: "fence-reuse", workflowName: "t", input: {} });
        const first = await s.tryLock("fence-reuse", 30_000);
        await s.releaseLock("fence-reuse", { fenceToken: first.token });
        const second = await peer.tryLock("fence-reuse", 30_000);
        expect(second.acquired).toBe(true);
        expect(second.token).not.toBe(first.token);

        await expect(
          s.saveStepResult(
            {
              workflowId: "fence-reuse",
              stepName: "late",
              result: 1,
              durationMs: 1,
              startedAt: new Date(),
            },
            { fenceToken: first.token },
          ),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
        await peer.releaseLock("fence-reuse", { fenceToken: second.token });
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
        expect(state!.status).toBe("pending");
        expect(state!.result).toBeUndefined();
        expect(state!.startedAt).toBeUndefined();
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

        // Current run (3) — pending, no result
        expect(history[0]!.run).toBe(3);
        expect(history[0]!.status).toBe("pending");
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
        expect(history[0]!.status).toBe("pending");
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

    // -------------------------------------------------------------------
    // terminal-state guards
    // -------------------------------------------------------------------

    describe("terminal-state guards", () => {
      it("a late completeWorkflow does not overwrite a cancel", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "term-cc", workflowName: "t", input: {} });
        await s.cancelWorkflow("term-cc");
        await s.completeWorkflow("term-cc", "late");
        const state = (await s.loadWorkflow("term-cc"))!;
        expect(state.status).toBe("failed");
        expect(state.error).toBe("Cancelled");
        expect(state.result).toBeUndefined();
      });

      it("a fenced completion from the lock holder does not resurrect a cancelled run", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "term-fenced", workflowName: "t", input: {} });
        const { token } = await s.tryLock("term-fenced", 30_000);
        await s.cancelWorkflow("term-fenced");
        await s.completeWorkflow("term-fenced", "late", { fenceToken: token });
        const state = (await s.loadWorkflow("term-fenced"))!;
        expect(state.status).toBe("failed");
        expect(state.error).toBe("Cancelled");
        await s.releaseLock("term-fenced", { fenceToken: token });
      });

      it("failWorkflow does not overwrite a cancel or a completion", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "term-cf", workflowName: "t", input: {} });
        await s.cancelWorkflow("term-cf");
        await s.failWorkflow("term-cf", "boom");
        expect((await s.loadWorkflow("term-cf"))!.error).toBe("Cancelled");

        await s.createWorkflow({ workflowId: "term-ok", workflowName: "t", input: {} });
        await s.completeWorkflow("term-ok", "done");
        await s.failWorkflow("term-ok", "boom");
        const ok = (await s.loadWorkflow("term-ok"))!;
        expect(ok.status).toBe("completed");
        expect(ok.result).toBe("done");
        expect(ok.error).toBeUndefined();
      });

      it("tripwireWorkflow does not overwrite a terminal run", async () => {
        const s = await getStorage();
        if (typeof s.tripwireWorkflow !== "function") return;
        await s.createWorkflow({ workflowId: "term-tw", workflowName: "t", input: {} });
        await s.completeWorkflow("term-tw", "done");
        await s.tripwireWorkflow("term-tw", { why: "late" });
        expect((await s.loadWorkflow("term-tw"))!.status).toBe("completed");

        await s.createWorkflow({ workflowId: "term-tw2", workflowName: "t", input: {} });
        await s.tripwireWorkflow("term-tw2", { why: "first" });
        await s.completeWorkflow("term-tw2", "late");
        const tw = (await s.loadWorkflow("term-tw2"))!;
        expect(tw.status).toBe("tripwire");
        expect(tw.tripwire).toEqual({ why: "first" });
      });

      it("completing a suspended run is allowed", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "term-susp", workflowName: "t", input: {} });
        await s.suspendWorkflow("term-susp", "wait", {
          status: "sleeping",
          stepType: "sleep",
          wakeAt: new Date(Date.now() + 60_000),
        });
        await s.completeWorkflow("term-susp", "done");
        expect((await s.loadWorkflow("term-susp"))!.status).toBe("completed");
      });
    });

    // -------------------------------------------------------------------
    // error tags, status reads, lock loss
    // -------------------------------------------------------------------

    describe("error tags and status", () => {
      it("saveStepFailure stores the step's errorTag; an untagged failure has none", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "etag-step", workflowName: "t", input: {} });
        await s.saveStepFailure({
          workflowId: "etag-step",
          stepName: "tagged",
          error: "card declined",
          errorTag: "PaymentDeclined",
          durationMs: 1,
          startedAt: new Date(),
        });
        await s.saveStepFailure({
          workflowId: "etag-step",
          stepName: "plain",
          error: "boom",
          durationMs: 1,
          startedAt: new Date(),
        });
        const steps = (await s.loadWorkflow("etag-step"))!.steps;
        expect(steps["tagged"]!.errorTag).toBe("PaymentDeclined");
        expect(steps["tagged"]!.error).toBe("card declined");
        expect(steps["plain"]!.errorTag).toBeUndefined();
      });

      it("failWorkflow stores the errorTag; fresh runs clear it", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "etag-wf", workflowName: "t", input: {} });
        await s.failWorkflow("etag-wf", "card declined", undefined, {
          errorTag: "PaymentDeclined",
        });
        const failed = (await s.loadWorkflow("etag-wf"))!;
        expect(failed.status).toBe("failed");
        expect(failed.errorTag).toBe("PaymentDeclined");
        expect(await s.loadWorkflowStatus("etag-wf")).toEqual({
          status: "failed",
          error: "card declined",
          errorTag: "PaymentDeclined",
        });

        await s.startFreshRun("etag-wf");
        const fresh = (await s.loadWorkflow("etag-wf"))!;
        expect(fresh.errorTag).toBeUndefined();
        expect(fresh.error).toBeUndefined();
      });

      it("cancelWorkflow stores the cancel tag", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "etag-cancel", workflowName: "t", input: {} });
        await s.cancelWorkflow("etag-cancel");
        expect((await s.loadWorkflow("etag-cancel"))!.errorTag).toBe("WorkflowCancelledError");
        expect(await s.loadWorkflowStatus("etag-cancel")).toEqual({
          status: "failed",
          error: "Cancelled",
          errorTag: "WorkflowCancelledError",
        });
      });

      it("loadWorkflowStatus reads a live run without error fields, and null when absent", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "status-live", workflowName: "t", input: {} });
        expect(await s.loadWorkflowStatus("status-live")).toEqual({ status: "pending" });
        await s.completeWorkflow("status-live", "done");
        expect(await s.loadWorkflowStatus("status-live")).toEqual({ status: "completed" });
        expect(await s.loadWorkflowStatus("status-missing")).toBeNull();
      });

      it("a fenced heartbeat rejects once the lock is held under another token", async () => {
        const s = await getStorage();
        const peer = await getPeer(s);
        const stale = await s.tryLock("hb-lost", 1);
        expect(stale.acquired).toBe(true);
        if (stale.token === undefined) return; // backend without fencing
        await sleep(30);
        const fresh = await peer.tryLock("hb-lost", 30_000);
        expect(fresh.acquired).toBe(true);

        await expect(
          s.heartbeat("hb-lost", 30_000, { fenceToken: stale.token }),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
        // The holder's own heartbeat still extends.
        await peer.heartbeat("hb-lost", 30_000, { fenceToken: fresh.token });
        await peer.releaseLock("hb-lost", { fenceToken: fresh.token });
      });

      it("a fenced heartbeat rejects once the lock was released", async () => {
        const s = await getStorage();
        const held = await s.tryLock("hb-gone", 30_000);
        if (held.token === undefined) return; // backend without fencing
        await s.releaseLock("hb-gone", { fenceToken: held.token });
        await expect(
          s.heartbeat("hb-gone", 30_000, { fenceToken: held.token }),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
      });
    });

    // -------------------------------------------------------------------
    // parent / run source persistence, filters and cascade cancel
    // -------------------------------------------------------------------

    describe("parent and run source", () => {
      async function seedFamily(s: WorkflowStorage): Promise<void> {
        await s.createWorkflow({
          workflowId: "fam-root",
          workflowName: "root",
          input: {},
          runSource: "schedule",
          runSourceId: "sched-1",
        });
        await s.createWorkflow({
          workflowId: "fam-child",
          workflowName: "child",
          input: {},
          parentWorkflowId: "fam-root",
          runSource: "parent",
          runSourceId: "fam-root",
        });
        await s.createWorkflow({
          workflowId: "fam-grandchild",
          workflowName: "child",
          input: {},
          parentWorkflowId: "fam-child",
          runSource: "parent",
          runSourceId: "fam-child",
        });
        await s.createWorkflow({
          workflowId: "fam-other",
          workflowName: "root",
          input: {},
          runSource: "schedule",
          runSourceId: "sched-2",
        });
        await s.createWorkflow({ workflowId: "fam-manual", workflowName: "root", input: {} });
      }

      it("round-trips parentWorkflowId, runSource and runSourceId", async () => {
        const s = await getStorage();
        await seedFamily(s);
        const child = (await s.loadWorkflow("fam-child"))!;
        expect(child.parentWorkflowId).toBe("fam-root");
        expect(child.runSource).toBe("parent");
        expect(child.runSourceId).toBe("fam-root");
        const manual = (await s.loadWorkflow("fam-manual"))!;
        expect(manual.parentWorkflowId).toBeUndefined();
        expect(manual.runSource).toBeUndefined();
        expect(manual.runSourceId).toBeUndefined();
      });

      it("listWorkflows filters by parentId, runSource and runSourceId", async () => {
        const s = await getStorage();
        await seedFamily(s);
        const ids = async (params: Parameters<WorkflowStorage["listWorkflows"]>[0]) =>
          (await s.listWorkflows(params)).map((w) => w.workflowId).sort();

        expect(await ids({ parentId: "fam-root" })).toEqual(["fam-child"]);
        expect(await ids({ runSource: "schedule" })).toEqual(["fam-other", "fam-root"]);
        expect(await ids({ runSource: "schedule", runSourceId: "sched-1" })).toEqual(["fam-root"]);
        expect(await ids({ runSource: "parent", name: "child" })).toEqual([
          "fam-child",
          "fam-grandchild",
        ]);
      });

      it("countWorkflows and listWorkflowSummaries apply the same filters", async () => {
        const s = await getStorage();
        await seedFamily(s);
        const cases: Array<Parameters<WorkflowStorage["listWorkflows"]>[0]> = [
          {},
          { parentId: "fam-root" },
          { runSource: "schedule" },
          { runSource: "schedule", runSourceId: "sched-2" },
          { name: "child" },
        ];
        for (const params of cases) {
          const expected = (await s.listWorkflows(params)).length;
          if (s.countWorkflows) expect(await s.countWorkflows(params)).toBe(expected);
          if (s.listWorkflowSummaries) {
            expect((await s.listWorkflowSummaries(params)).length).toBe(expected);
          }
        }
      });

      it("cancel without cascade leaves children alone", async () => {
        const s = await getStorage();
        await seedFamily(s);
        await s.cancelWorkflow("fam-root");
        expect((await s.loadWorkflow("fam-root"))!.status).toBe("failed");
        expect((await s.loadWorkflow("fam-child"))!.status).toBe("pending");
      });

      it("cancel with cascade cancels every descendant and nothing else", async () => {
        const s = await getStorage();
        await seedFamily(s);
        await s.cancelWorkflow("fam-root", { cascade: true });
        for (const id of ["fam-root", "fam-child", "fam-grandchild"]) {
          const state = (await s.loadWorkflow(id))!;
          expect(state.status).toBe("failed");
          expect(state.error).toBe("Cancelled");
        }
        expect((await s.loadWorkflow("fam-other"))!.status).toBe("pending");
        expect((await s.loadWorkflow("fam-manual"))!.status).toBe("pending");
      });
    });

    // -------------------------------------------------------------------
    // unfiltered listing
    // -------------------------------------------------------------------

    describe("unfiltered listWorkflows", () => {
      it("includes runs in every status", async () => {
        const s = await getStorage();
        const expected = ["st-pending", "st-running", "st-completed", "st-failed", "st-suspended"];
        for (const id of expected) {
          await s.createWorkflow({ workflowId: id, workflowName: "t", input: {} });
        }
        await s.saveStepResult({
          workflowId: "st-running",
          stepName: "a",
          result: 1,
          durationMs: 1,
          startedAt: new Date(),
        });
        await s.completeWorkflow("st-completed", "ok");
        await s.failWorkflow("st-failed", "boom");
        await s.suspendWorkflow("st-suspended", "wait", {
          status: "sleeping",
          stepType: "sleep",
          wakeAt: new Date(Date.now() + 60_000),
        });
        if (typeof s.tripwireWorkflow === "function") {
          await s.createWorkflow({ workflowId: "st-tripwire", workflowName: "t", input: {} });
          await s.tripwireWorkflow("st-tripwire", { why: "x" });
          expected.push("st-tripwire");
        }

        const ids = (await s.listWorkflows()).map((w) => w.workflowId).sort();
        expect(ids).toEqual([...expected].sort());
        if (s.countWorkflows) expect(await s.countWorkflows()).toBe(expected.length);
      });
    });

    // -------------------------------------------------------------------
    // idempotency keys
    // -------------------------------------------------------------------

    describe("idempotency keys", () => {
      it("an expired key is reclaimed by the next create", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "idem-old",
          workflowName: "w",
          input: 1,
          idempotencyKey: "k",
          idempotencyExpiresAt: new Date(Date.now() - 1_000),
        });
        expect(
          await s.findWorkflowByIdempotencyKey({
            workflowName: "w",
            idempotencyKey: "k",
            now: new Date(),
          }),
        ).toBeNull();

        const fresh = await s.createWorkflow({
          workflowId: "idem-new",
          workflowName: "w",
          input: 2,
          idempotencyKey: "k",
          idempotencyExpiresAt: new Date(Date.now() + 60_000),
        });
        expect(fresh.created).toBe(true);
        expect(
          await s.findWorkflowByIdempotencyKey({
            workflowName: "w",
            idempotencyKey: "k",
            now: new Date(),
          }),
        ).toEqual({ workflowId: "idem-new" });
        // The old run itself is untouched.
        expect((await s.loadWorkflow("idem-old"))!.input).toBe(1);
      });

      it("an unexpired key still redirects to the owning run", async () => {
        const s = await getStorage();
        const expiresAt = new Date(Date.now() + 60_000);
        await s.createWorkflow({
          workflowId: "idem-a",
          workflowName: "w",
          input: 1,
          idempotencyKey: "live",
          idempotencyExpiresAt: expiresAt,
        });
        const second = await s.createWorkflow({
          workflowId: "idem-b",
          workflowName: "w",
          input: 2,
          idempotencyKey: "live",
          idempotencyExpiresAt: expiresAt,
        });
        expect(second.created).toBe(false);
        if (!second.created) expect(second.existing.workflowId).toBe("idem-a");
        expect(await s.loadWorkflow("idem-b")).toBeNull();
      });
    });

    // -------------------------------------------------------------------
    // signal semantics
    // -------------------------------------------------------------------

    describe("signal semantics", () => {
      it("the last delivery under a name wins", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "sig-lw", workflowName: "t", input: {} });
        await s.deliverSignal("sig-lw", "a", 1);
        await s.deliverSignal("sig-lw", "a", 2);
        await s.deliverSignal("sig-lw", "b", "x");
        const signals = await s.loadSignals("sig-lw");
        const byName = Object.fromEntries(signals.map((x) => [x.signalName, x.payload]));
        expect(signals).toHaveLength(2);
        expect(byName).toEqual({ a: 2, b: "x" });
      });

      it("startFreshRun drops the previous run's signals", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "sig-fresh", workflowName: "t", input: {} });
        await s.deliverSignal("sig-fresh", "go", { approved: true });
        await s.completeWorkflow("sig-fresh", "done");
        await s.startFreshRun("sig-fresh");
        expect(await s.loadSignals("sig-fresh")).toEqual([]);

        await s.deliverSignal("sig-fresh", "go", { approved: false });
        expect((await s.loadSignals("sig-fresh")).map((x) => x.payload)).toEqual([
          { approved: false },
        ]);
      });
    });

    // -------------------------------------------------------------------
    // signal tokens
    // -------------------------------------------------------------------

    describe("signal tokens", () => {
      const future = () => new Date(Date.now() + 60_000);

      it("create is idempotent on (workflowId, idempotencyKey)", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "tok-idem", workflowName: "t", input: {} });
        const first = await s.createSignalToken({
          tokenId: "tk-1",
          workflowId: "tok-idem",
          signalName: "approve",
          bearer: "b1",
          tags: ["x"],
          idempotencyKey: "ik",
          expiresAt: future(),
        });
        const again = await s.createSignalToken({
          tokenId: "tk-2",
          workflowId: "tok-idem",
          signalName: "approve",
          bearer: "b2",
          tags: [],
          idempotencyKey: "ik",
          expiresAt: future(),
        });
        expect(first.isCached).toBe(false);
        expect(again.isCached).toBe(true);
        expect(again.record.tokenId).toBe("tk-1");
        expect(again.record.bearer).toBe("b1");
        expect(await s.findSignalTokenById("tk-2")).toBeNull();
      });

      it("concurrent completions produce exactly one delivery", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "tok-race", workflowName: "t", input: {} });
        await s.createSignalToken({
          tokenId: "tk-race",
          workflowId: "tok-race",
          signalName: "approve",
          bearer: "b",
          tags: [],
          expiresAt: future(),
        });
        const outcomes = await Promise.all(
          Array.from({ length: 6 }, (_, i) =>
            s.markSignalTokenCompleted({ tokenId: "tk-race", value: i, now: new Date() }),
          ),
        );
        const delivered = outcomes.filter((o) => o.outcome === "delivered");
        expect(delivered).toHaveLength(1);
        const stored = (await s.findSignalTokenById("tk-race"))!;
        expect(stored.completedAt).toBeInstanceOf(Date);
        expect(stored.completedValue).toEqual(delivered[0]!.record.completedValue);
        for (const o of outcomes.filter((x) => x.outcome === "already_completed")) {
          expect(o.record.completedValue).toEqual(stored.completedValue);
        }
      });

      it("lists a workflow's tokens newest first", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "tok-list", workflowName: "t", input: {} });
        for (const id of ["tk-a", "tk-b", "tk-c"]) {
          await s.createSignalToken({
            tokenId: id,
            workflowId: "tok-list",
            signalName: "approve",
            bearer: id,
            tags: [],
            expiresAt: future(),
          });
          await sleep(5);
        }
        const listed = await s.listSignalTokensForWorkflow("tok-list");
        expect(listed.map((t) => t.tokenId)).toEqual(["tk-c", "tk-b", "tk-a"]);
        expect(await s.listSignalTokensForWorkflow("tok-none")).toEqual([]);
      });
    });

    // -------------------------------------------------------------------
    // streams
    // -------------------------------------------------------------------

    describe("streams", () => {
      it("concurrent appends get distinct, gap-free indices", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "str-race", workflowName: "t", input: {} });
        const results = await Promise.all(
          Array.from({ length: 20 }, (_, i) =>
            s.appendStreamChunk({
              workflowId: "str-race",
              streamId: "out",
              payload: { i },
              appendedBy: "workflow",
            }),
          ),
        );
        const indices = results.map((r) => r.chunkIndex).sort((a, b) => a - b);
        expect(indices).toEqual(Array.from({ length: 20 }, (_, i) => i));
        const chunks = await s.readStreamChunks({ workflowId: "str-race", streamId: "out" });
        expect(chunks.map((c) => c.chunkIndex)).toEqual(indices);
      });

      it("reads with since (exclusive) and limit, per stream", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "str-page", workflowName: "t", input: {} });
        for (let i = 0; i < 5; i++) {
          await s.appendStreamChunk({
            workflowId: "str-page",
            streamId: "out",
            payload: i,
            appendedBy: i % 2 === 0 ? "workflow" : "external",
          });
        }
        await s.appendStreamChunk({
          workflowId: "str-page",
          streamId: "other",
          payload: "x",
          appendedBy: "external",
        });
        const page = await s.readStreamChunks({
          workflowId: "str-page",
          streamId: "out",
          since: 1,
          limit: 2,
        });
        expect(page.map((c) => [c.chunkIndex, c.payload, c.appendedBy])).toEqual([
          [2, 2, "workflow"],
          [3, 3, "external"],
        ]);
        expect(page[0]!.appendedAt).toBeInstanceOf(Date);
        const other = await s.readStreamChunks({ workflowId: "str-page", streamId: "other" });
        expect(other.map((c) => c.chunkIndex)).toEqual([0]);
      });
    });

    // -------------------------------------------------------------------
    // setWorkflowMetadata
    // -------------------------------------------------------------------

    describe("setWorkflowMetadata", () => {
      it("shallow-merges and deletes keys set to null", async () => {
        const s = await getStorage();
        await s.createWorkflow({
          workflowId: "meta-merge",
          workflowName: "t",
          input: {},
          metadata: { keep: 1, drop: 2 },
        });
        await s.setWorkflowMetadata("meta-merge", { drop: null, added: { deep: true } });
        expect((await s.loadWorkflow("meta-merge"))!.metadata).toEqual({
          keep: 1,
          added: { deep: true },
        });
      });

      it("concurrent patches on different keys all land", async () => {
        const s = await getStorage();
        await s.createWorkflow({ workflowId: "meta-race", workflowName: "t", input: {} });
        const keys = ["a", "b", "c", "d", "e", "f"];
        await Promise.all(keys.map((k, i) => s.setWorkflowMetadata("meta-race", { [k]: i })));
        expect((await s.loadWorkflow("meta-race"))!.metadata).toEqual({
          a: 0,
          b: 1,
          c: 2,
          d: 3,
          e: 4,
          f: 5,
        });
      });
    });

    // -------------------------------------------------------------------
    // purge removes dependent records
    // -------------------------------------------------------------------

    describe("purgeCompleted dependents", () => {
      it("purges tripwired runs too", async () => {
        const s = await getStorage();
        if (typeof s.tripwireWorkflow !== "function") return;
        const before = new Date(Date.now() - 1_000);
        await s.createWorkflow({ workflowId: "purge-tw", workflowName: "t", input: {} });
        await s.tripwireWorkflow("purge-tw", { why: "x" });
        await sleep(10);
        const deleted = await s.purgeCompleted({ from: before, to: new Date(), limit: 100 });
        expect(deleted).toBe(1);
        expect(await s.loadWorkflow("purge-tw")).toBeNull();
      });

      it("removes signal tokens and streams of purged runs", async () => {
        const s = await getStorage();
        const before = new Date(Date.now() - 1_000);
        await s.createWorkflow({ workflowId: "purge-deps", workflowName: "t", input: {} });
        await s.createSignalToken({
          tokenId: "tk-purge",
          workflowId: "purge-deps",
          signalName: "approve",
          bearer: "b",
          tags: [],
          expiresAt: new Date(Date.now() + 60_000),
        });
        await s.appendStreamChunk({
          workflowId: "purge-deps",
          streamId: "out",
          payload: 1,
          appendedBy: "workflow",
        });
        await s.completeWorkflow("purge-deps", "done");
        await sleep(10);

        await s.purgeCompleted({ from: before, to: new Date(), limit: 100 });

        expect(await s.loadWorkflow("purge-deps")).toBeNull();
        expect(await s.findSignalTokenById("tk-purge")).toBeNull();
        expect(await s.listSignalTokensForWorkflow("purge-deps")).toEqual([]);
        expect(await s.readStreamChunks({ workflowId: "purge-deps", streamId: "out" })).toEqual([]);
      });
    });

    // -------------------------------------------------------------------
    // scanner queries (opt-in) — listDueTimers / listSignalWakeups /
    // listOrphanedRuns
    // -------------------------------------------------------------------

    if (options.hasScannerQueries) {
      describe("scanner queries", () => {
        type ScannerStorage = WorkflowStorage &
          Required<
            Pick<WorkflowStorage, "listDueTimers" | "listSignalWakeups" | "listOrphanedRuns">
          >;

        async function getScannerStorage(): Promise<ScannerStorage> {
          const s = await getStorage();
          if (!s.listDueTimers || !s.listSignalWakeups || !s.listOrphanedRuns) {
            throw new Error(
              "storageTestSuite was invoked with hasScannerQueries: true, but the factory " +
                "returned a storage without listDueTimers / listSignalWakeups / listOrphanedRuns.",
            );
          }
          return s as ScannerStorage;
        }

        const past = (): Date => new Date(Date.now() - 60_000);
        const future = (): Date => new Date(Date.now() + 60 * 60_000);

        async function createSleeping(params: {
          s: WorkflowStorage;
          workflowId: string;
          wakeAt: Date;
          stepName?: string;
        }): Promise<void> {
          await params.s.createWorkflow({
            workflowId: params.workflowId,
            workflowName: "scan-wf",
            input: { id: params.workflowId },
            version: "v1",
          });
          await params.s.suspendWorkflow(params.workflowId, params.stepName ?? "nap", {
            status: "sleeping",
            stepType: "sleep",
            wakeAt: params.wakeAt,
          });
        }

        async function createWaiting(params: {
          s: WorkflowStorage;
          workflowId: string;
          signalName: string;
          stepName?: string;
          signalTimeoutAt?: Date;
        }): Promise<void> {
          await params.s.createWorkflow({
            workflowId: params.workflowId,
            workflowName: "scan-wf",
            input: { id: params.workflowId },
          });
          await params.s.suspendWorkflow(params.workflowId, params.stepName ?? "wait", {
            status: "waiting_for_signal",
            stepType: "signal",
            signalName: params.signalName,
            ...(params.signalTimeoutAt ? { signalTimeoutAt: params.signalTimeoutAt } : {}),
          });
        }

        describe("listDueTimers", () => {
          it("returns a suspended run whose sleep is due", async () => {
            const s = await getScannerStorage();
            await createSleeping({ s, workflowId: "due-1", wakeAt: past() });

            const rows = await s.listDueTimers({ now: new Date(), limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.workflowId).toBe("due-1");
            expect(rows[0]!.workflowName).toBe("scan-wf");
            expect(rows[0]!.version).toBe("v1");
            expect(rows[0]!.input).toEqual({ id: "due-1" });
            expect(rows[0]!.stepName).toBe("nap");
            expect(rows[0]!.reason).toBe("sleep");
          });

          it("excludes sleeps that are not yet due", async () => {
            const s = await getScannerStorage();
            await createSleeping({ s, workflowId: "due-later", wakeAt: future() });

            expect(await s.listDueTimers({ now: new Date(), limit: 10 })).toEqual([]);
          });

          it("excludes runs that are no longer suspended", async () => {
            const s = await getScannerStorage();
            await createSleeping({ s, workflowId: "due-done", wakeAt: past() });
            await s.completeWorkflow("due-done", "ok");
            await createSleeping({ s, workflowId: "due-failed", wakeAt: past() });
            await s.failWorkflow("due-failed", "boom");

            expect(await s.listDueTimers({ now: new Date(), limit: 10 })).toEqual([]);
          });

          it("reports a passed signal timeout as signal-timeout", async () => {
            const s = await getScannerStorage();
            await createWaiting({
              s,
              workflowId: "due-sigto",
              signalName: "approval",
              signalTimeoutAt: past(),
            });
            await createWaiting({
              s,
              workflowId: "due-sigto-later",
              signalName: "approval",
              signalTimeoutAt: future(),
            });
            await createWaiting({ s, workflowId: "due-sig-forever", signalName: "approval" });

            const rows = await s.listDueTimers({ now: new Date(), limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.workflowId).toBe("due-sigto");
            expect(rows[0]!.reason).toBe("signal-timeout");
            expect(rows[0]!.signalName).toBe("approval");
            expect(rows[0]!.stepName).toBe("wait");
          });

          it("returns one row per run when two of its steps are due", async () => {
            const s = await getScannerStorage();
            await createSleeping({ s, workflowId: "due-two", wakeAt: past(), stepName: "b-nap" });
            await s.suspendWorkflow("due-two", "a-nap", {
              status: "sleeping",
              stepType: "sleep",
              wakeAt: past(),
            });

            const rows = await s.listDueTimers({ now: new Date(), limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.workflowId).toBe("due-two");
            expect(rows[0]!.stepName).toBe("a-nap");
          });

          it("keyset-paginates every due run exactly once, in order, across resumes", async () => {
            const s = await getScannerStorage();
            const ids = Array.from({ length: 7 }, (_, i) => `due-page-${i}`);
            for (const id of ids) await createSleeping({ s, workflowId: id, wakeAt: past() });
            await createSleeping({ s, workflowId: "due-page-x-later", wakeAt: future() });

            const now = new Date();
            const seen: string[] = [];
            const page1 = await s.listDueTimers({ now, limit: 3 });
            expect(page1.map((r) => r.workflowId)).toEqual(ids.slice(0, 3));
            seen.push(...page1.map((r) => r.workflowId));

            // Resume rows of the first page before asking for the next one:
            // they leave the due set, which must not shift later pages.
            await s.saveStepResult({
              workflowId: "due-page-0",
              stepName: "nap",
              result: null,
              durationMs: 1,
              startedAt: new Date(),
            });
            await s.completeWorkflow("due-page-1", "ok");

            let after = page1.at(-1)!.workflowId;
            for (;;) {
              const page = await s.listDueTimers({ now, limit: 3, afterWorkflowId: after });
              if (page.length === 0) break;
              expect(page.length).toBeLessThanOrEqual(3);
              seen.push(...page.map((r) => r.workflowId));
              after = page.at(-1)!.workflowId;
            }
            expect(seen).toEqual(ids);

            const fromStart = await s.listDueTimers({ now, limit: 10 });
            expect(fromStart.map((r) => r.workflowId)).toEqual(ids.slice(2));
          });
        });

        describe("listSignalWakeups", () => {
          it("returns only runs whose awaited signal was delivered", async () => {
            const s = await getScannerStorage();
            await createWaiting({ s, workflowId: "sw-hit", signalName: "approval" });
            await createWaiting({ s, workflowId: "sw-other-name", signalName: "approval" });
            await s.deliverSignal("sw-other-name", "rejection", { no: true });
            await createWaiting({ s, workflowId: "sw-none", signalName: "approval" });
            await s.deliverSignal("sw-hit", "approval", { ok: 1 });

            const rows = await s.listSignalWakeups({ limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.workflowId).toBe("sw-hit");
            expect(rows[0]!.workflowName).toBe("scan-wf");
            expect(rows[0]!.input).toEqual({ id: "sw-hit" });
            expect(rows[0]!.stepName).toBe("wait");
            expect(rows[0]!.reason).toBe("signal");
            expect(rows[0]!.signalName).toBe("approval");
            expect(rows[0]!.signalPayload).toEqual({ ok: 1 });
          });

          it("carries the latest payload after a repeated delivery", async () => {
            const s = await getScannerStorage();
            await createWaiting({ s, workflowId: "sw-latest", signalName: "approval" });
            await s.deliverSignal("sw-latest", "approval", { n: 1 });
            await s.deliverSignal("sw-latest", "approval", { n: 2 });

            const rows = await s.listSignalWakeups({ limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.signalPayload).toEqual({ n: 2 });
          });

          it("excludes runs that are not suspended", async () => {
            const s = await getScannerStorage();
            await createWaiting({ s, workflowId: "sw-done", signalName: "approval" });
            await s.deliverSignal("sw-done", "approval", {});
            await s.completeWorkflow("sw-done", "ok");

            expect(await s.listSignalWakeups({ limit: 10 })).toEqual([]);
          });

          it("returns one row per run when two waiting steps have deliveries", async () => {
            const s = await getScannerStorage();
            await createWaiting({ s, workflowId: "sw-two", signalName: "b", stepName: "wait-b" });
            await s.suspendWorkflow("sw-two", "wait-a", {
              status: "waiting_for_signal",
              stepType: "signal",
              signalName: "a",
            });
            await s.deliverSignal("sw-two", "a", "A");
            await s.deliverSignal("sw-two", "b", "B");

            const rows = await s.listSignalWakeups({ limit: 10 });
            expect(rows).toHaveLength(1);
            expect(rows[0]!.stepName).toBe("wait-a");
            expect(rows[0]!.signalPayload).toBe("A");
          });

          it("does not return a run whose signals were cleared by startFreshRun", async () => {
            const s = await getScannerStorage();
            await createWaiting({ s, workflowId: "sw-fresh", signalName: "approval" });
            await s.deliverSignal("sw-fresh", "approval", { stale: true });
            await s.startFreshRun("sw-fresh");
            await s.suspendWorkflow("sw-fresh", "wait", {
              status: "waiting_for_signal",
              stepType: "signal",
              signalName: "approval",
            });

            expect(await s.listSignalWakeups({ limit: 10 })).toEqual([]);
          });

          it("keyset-paginates in workflowId order", async () => {
            const s = await getScannerStorage();
            const ids = Array.from({ length: 5 }, (_, i) => `sw-page-${i}`);
            for (const id of ids) {
              await createWaiting({ s, workflowId: id, signalName: "go" });
              await s.deliverSignal(id, "go", id);
            }

            const seen: string[] = [];
            let after: string | undefined;
            for (;;) {
              const page = await s.listSignalWakeups({ limit: 2, afterWorkflowId: after });
              if (page.length === 0) break;
              seen.push(...page.map((r) => r.workflowId));
              after = page.at(-1)!.workflowId;
            }
            expect(seen).toEqual(ids);
          });
        });

        describe("listOrphanedRuns", () => {
          // Lock expiry and `updatedAt` may come from the backend's own
          // clock, so the bounds sit well clear of "right now".
          const later = (): Date => new Date(Date.now() + 5_000);

          it("returns pending and running runs that hold no lock", async () => {
            const s = await getScannerStorage();
            await s.createWorkflow({
              workflowId: "orph-pending",
              workflowName: "orph-wf",
              input: { a: 1 },
              version: "v2",
              metadata: { team: "x" },
            });
            await s.createWorkflow({
              workflowId: "orph-running",
              workflowName: "orph-wf",
              input: {},
            });
            await s.saveStepResult({
              workflowId: "orph-running",
              stepName: "s1",
              result: 1,
              durationMs: 1,
              startedAt: new Date(),
            });

            const rows = await s.listOrphanedRuns({
              now: later(),
              updatedBefore: later(),
              limit: 10,
            });
            expect(rows.map((r) => r.workflowId)).toEqual(["orph-pending", "orph-running"]);
            expect(rows[0]!.status).toBe("pending");
            expect(rows[0]!.workflowName).toBe("orph-wf");
            expect(rows[0]!.version).toBe("v2");
            expect(rows[0]!.input).toEqual({ a: 1 });
            expect(rows[0]!.metadata).toEqual({ team: "x" });
            expect(rows[1]!.status).toBe("running");
          });

          it("excludes runs holding a live lock and includes runs whose lock expired", async () => {
            const s = await getScannerStorage();
            await s.createWorkflow({ workflowId: "orph-live", workflowName: "orph-wf", input: {} });
            await s.createWorkflow({
              workflowId: "orph-expired",
              workflowName: "orph-wf",
              input: {},
            });
            const live = await s.tryLock("orph-live", 60_000);
            expect(live.acquired).toBe(true);
            const expired = await s.tryLock("orph-expired", 1);
            expect(expired.acquired).toBe(true);
            await sleep(30);

            const rows = await s.listOrphanedRuns({
              now: later(),
              updatedBefore: later(),
              limit: 10,
            });
            expect(rows.map((r) => r.workflowId)).toEqual(["orph-expired"]);
            await s.releaseLock("orph-live", { fenceToken: live.token });
          });

          it("excludes suspended and terminal runs", async () => {
            const s = await getScannerStorage();
            await createSleeping({ s, workflowId: "orph-suspended", wakeAt: past() });
            await s.createWorkflow({
              workflowId: "orph-completed",
              workflowName: "orph-wf",
              input: {},
            });
            await s.completeWorkflow("orph-completed", "ok");
            await s.createWorkflow({
              workflowId: "orph-failed",
              workflowName: "orph-wf",
              input: {},
            });
            await s.failWorkflow("orph-failed", "boom");

            expect(
              await s.listOrphanedRuns({ now: later(), updatedBefore: later(), limit: 10 }),
            ).toEqual([]);
          });

          it("leaves alone runs updated after updatedBefore", async () => {
            const s = await getScannerStorage();
            await s.createWorkflow({
              workflowId: "orph-fresh",
              workflowName: "orph-wf",
              input: {},
            });

            expect(
              await s.listOrphanedRuns({
                now: later(),
                updatedBefore: new Date(Date.now() - 60_000),
                limit: 10,
              }),
            ).toEqual([]);
          });

          it("keyset-paginates in workflowId order", async () => {
            const s = await getScannerStorage();
            const ids = Array.from({ length: 5 }, (_, i) => `orph-page-${i}`);
            for (const id of ids) {
              await s.createWorkflow({ workflowId: id, workflowName: "orph-wf", input: {} });
            }

            const seen: string[] = [];
            let after: string | undefined;
            for (;;) {
              const page = await s.listOrphanedRuns({
                now: later(),
                updatedBefore: later(),
                limit: 2,
                afterWorkflowId: after,
              });
              if (page.length === 0) break;
              expect(page.length).toBeLessThanOrEqual(2);
              seen.push(...page.map((r) => r.workflowId));
              after = page.at(-1)!.workflowId;
            }
            expect(seen).toEqual(ids);
          });
        });
      });
    }

    // -------------------------------------------------------------------
    // resetSteps (opt-in) — backs WorkflowRunner.resume
    // -------------------------------------------------------------------

    if (options.hasResetSteps) {
      describe("resetSteps", () => {
        // A workflow with three completed steps, then completed overall.
        async function seedCompleted(s: WorkflowStorage, id: string): Promise<void> {
          await s.createWorkflow({ workflowId: id, workflowName: "reset-wf", input: {} });
          for (const name of ["s1", "s2", "s3"]) {
            await s.saveStepResult({
              workflowId: id,
              stepName: name,
              result: `${name}-result`,
              durationMs: 5,
              startedAt: new Date(),
            });
          }
          await s.completeWorkflow(id, "final");
        }

        it("clears the listed steps and flips a completed workflow to running", async () => {
          const s = await getStorage();
          if (!s.resetSteps) throw new Error("factory storage lacks resetSteps");
          await seedCompleted(s, "reset-1");
          expect((await s.loadWorkflow("reset-1"))!.status).toBe("completed");

          await s.resetSteps("reset-1", ["s2", "s3"]);

          const state = (await s.loadWorkflow("reset-1"))!;
          expect(state.status).toBe("running");
          // Reset steps read back as never-run; the unlisted step is kept.
          expect(state.steps["s1"]?.result).toBe("s1-result");
          expect(state.steps["s2"]).toBeUndefined();
          expect(state.steps["s3"]).toBeUndefined();
          // Terminal fields are cleared.
          expect(state.result).toBeUndefined();
          expect(state.completedAt).toBeUndefined();
        });

        it("an empty step list is a no-op", async () => {
          const s = await getStorage();
          if (!s.resetSteps) throw new Error("factory storage lacks resetSteps");
          await seedCompleted(s, "reset-empty");
          await s.resetSteps("reset-empty", []);
          const state = (await s.loadWorkflow("reset-empty"))!;
          expect(state.status).toBe("completed");
          expect(state.steps["s1"]?.result).toBe("s1-result");
        });

        it("leaves a non-terminal workflow's status untouched", async () => {
          const s = await getStorage();
          if (!s.resetSteps) throw new Error("factory storage lacks resetSteps");
          await s.createWorkflow({
            workflowId: "reset-running",
            workflowName: "reset-wf",
            input: {},
          });
          await s.saveStepResult({
            workflowId: "reset-running",
            stepName: "s1",
            result: "ok",
            durationMs: 5,
            startedAt: new Date(),
          });
          expect((await s.loadWorkflow("reset-running"))!.status).toBe("running");

          await s.resetSteps("reset-running", ["s1"]);

          const state = (await s.loadWorkflow("reset-running"))!;
          expect(state.status).toBe("running");
          expect(state.steps["s1"]).toBeUndefined();
        });

        it("is idempotent on step names that aren't present", async () => {
          const s = await getStorage();
          if (!s.resetSteps) throw new Error("factory storage lacks resetSteps");
          await seedCompleted(s, "reset-missing-step");
          await s.resetSteps("reset-missing-step", ["s2", "never-existed"]);
          const state = (await s.loadWorkflow("reset-missing-step"))!;
          expect(state.steps["s2"]).toBeUndefined();
          expect(state.steps["s1"]?.result).toBe("s1-result");
        });

        it("throws when the workflow doesn't exist", async () => {
          const s = await getStorage();
          if (!s.resetSteps) throw new Error("factory storage lacks resetSteps");
          await expect(s.resetSteps("ghost-workflow", ["s1"])).rejects.toThrow();
        });
      });
    }

    // -------------------------------------------------------------------
    // ActivityJournalStorage (opt-in)
    // -------------------------------------------------------------------

    if (options.hasJournal || options.hasJournaledSuspend) {
      describe("journal — ActivityJournalStorage", () => {
        it("loadJournal returns empty for a non-existent (workflow, step)", async () => {
          const s = await getJournalStorage();
          expect(await s.loadJournal("missing", "nope")).toEqual([]);
        });

        it("appendEntry + loadJournal round-trips success and failure exits", async () => {
          const s = await getJournalStorage();
          await s.createWorkflow({ workflowId: "j-rt", workflowName: "test", input: {} });
          await s.appendEntry({
            workflowId: "j-rt",
            stepName: "calc",
            activityIndex: 0,
            activityName: "fetch",
            exit: { tag: "Success", value: { n: 42 } },
          });
          await s.appendEntry({
            workflowId: "j-rt",
            stepName: "calc",
            activityIndex: 1,
            activityName: "failover",
            exit: { tag: "Failure", error: "boom" },
          });

          const entries = await s.loadJournal("j-rt", "calc");
          expect(entries).toHaveLength(2);
          expect(entries[0]!.activityIndex).toBe(0);
          expect(entries[0]!.activityName).toBe("fetch");
          expect(entries[0]!.stepType ?? "activity").toBe("activity");
          expect(entries[0]!.phase ?? "completed").toBe("completed");
          expect(entries[0]!.exit).toEqual({ tag: "Success", value: { n: 42 } });
          expect(entries[1]!.exit).toEqual({ tag: "Failure", error: "boom" });
        });

        it("loadJournal returns entries ordered by activityIndex ascending", async () => {
          const s = await getJournalStorage();
          await s.createWorkflow({ workflowId: "j-ord", workflowName: "test", input: {} });
          // Insert out of order — storage must still return sorted.
          for (const idx of [2, 0, 1]) {
            await s.appendEntry({
              workflowId: "j-ord",
              stepName: "calc",
              activityIndex: idx,
              activityName: `a${idx}`,
              exit: { tag: "Success", value: idx },
            });
          }

          const entries = await s.loadJournal("j-ord", "calc");
          expect(entries.map((e) => e.activityIndex)).toEqual([0, 1, 2]);
        });

        it("appendEntry is idempotent on (workflowId, stepName, activityIndex)", async () => {
          const s = await getJournalStorage();
          await s.createWorkflow({ workflowId: "j-idem", workflowName: "test", input: {} });
          await s.appendEntry({
            workflowId: "j-idem",
            stepName: "calc",
            activityIndex: 0,
            activityName: "first",
            exit: { tag: "Success", value: "v1" },
          });
          // Second call on same PK — must not duplicate.
          await s.appendEntry({
            workflowId: "j-idem",
            stepName: "calc",
            activityIndex: 0,
            activityName: "first",
            exit: { tag: "Success", value: "v1" },
          });

          const entries = await s.loadJournal("j-idem", "calc");
          expect(entries).toHaveLength(1);
        });

        it("journal is scoped by (workflowId, stepName)", async () => {
          const s = await getJournalStorage();
          await s.createWorkflow({ workflowId: "j-scope-a", workflowName: "test", input: {} });
          await s.createWorkflow({ workflowId: "j-scope-b", workflowName: "test", input: {} });
          await s.appendEntry({
            workflowId: "j-scope-a",
            stepName: "calc",
            activityIndex: 0,
            activityName: "a",
            exit: { tag: "Success", value: "A" },
          });
          await s.appendEntry({
            workflowId: "j-scope-b",
            stepName: "calc",
            activityIndex: 0,
            activityName: "b",
            exit: { tag: "Success", value: "B" },
          });
          await s.appendEntry({
            workflowId: "j-scope-a",
            stepName: "other",
            activityIndex: 0,
            activityName: "c",
            exit: { tag: "Success", value: "C" },
          });

          expect((await s.loadJournal("j-scope-a", "calc")).map((e) => e.exit)).toEqual([
            { tag: "Success", value: "A" },
          ]);
          expect((await s.loadJournal("j-scope-b", "calc")).map((e) => e.exit)).toEqual([
            { tag: "Success", value: "B" },
          ]);
          expect((await s.loadJournal("j-scope-a", "other")).map((e) => e.exit)).toEqual([
            { tag: "Success", value: "C" },
          ]);
        });

        it("startFreshRun clears the journal of every step", async () => {
          const s = await getJournalStorage();
          await s.createWorkflow({ workflowId: "j-fresh", workflowName: "test", input: {} });
          for (const stepName of ["calc", "other"]) {
            await s.appendEntry({
              workflowId: "j-fresh",
              stepName,
              activityIndex: 0,
              activityName: "fetch",
              exit: { tag: "Success", value: stepName },
            });
          }
          await s.completeWorkflow("j-fresh", "done");

          await s.startFreshRun("j-fresh");

          expect(await s.loadJournal("j-fresh", "calc")).toEqual([]);
          expect(await s.loadJournal("j-fresh", "other")).toEqual([]);
          // The new run journals from a clean slate.
          await s.appendEntry({
            workflowId: "j-fresh",
            stepName: "calc",
            activityIndex: 0,
            activityName: "fetch",
            exit: { tag: "Success", value: "run-2" },
          });
          expect((await s.loadJournal("j-fresh", "calc")).map((e) => e.exit)).toEqual([
            { tag: "Success", value: "run-2" },
          ]);
        });

        it("purgeCompleted removes the journal of purged runs", async () => {
          const s = await getJournalStorage();
          const before = new Date(Date.now() - 1_000);
          await s.createWorkflow({ workflowId: "j-purge", workflowName: "test", input: {} });
          await s.appendEntry({
            workflowId: "j-purge",
            stepName: "calc",
            activityIndex: 0,
            activityName: "fetch",
            exit: { tag: "Success", value: 1 },
          });
          await s.completeWorkflow("j-purge", "done");
          await sleep(10);
          await s.purgeCompleted({ from: before, to: new Date(), limit: 100 });
          expect(await s.loadJournal("j-purge", "calc")).toEqual([]);
        });

        it("startFreshRun leaves other workflows' journals alone", async () => {
          const s = await getJournalStorage();
          for (const id of ["j-keep-a", "j-keep-b"]) {
            await s.createWorkflow({ workflowId: id, workflowName: "test", input: {} });
            await s.appendEntry({
              workflowId: id,
              stepName: "calc",
              activityIndex: 0,
              activityName: "fetch",
              exit: { tag: "Success", value: id },
            });
          }
          await s.startFreshRun("j-keep-a");
          expect(await s.loadJournal("j-keep-b", "calc")).toHaveLength(1);
        });
      });
    }

    // -------------------------------------------------------------------
    // JournaledSuspendStorage (opt-in)
    // -------------------------------------------------------------------

    if (options.hasJournaledSuspend) {
      describe("journal — JournaledSuspendStorage", () => {
        it("appendPendingEntry writes a pending sleep with wakeAt", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-sleep", workflowName: "test", input: {} });
          const wakeAt = new Date(Date.now() + 60_000);
          await s.appendPendingEntry({
            workflowId: "j-sleep",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt,
          });

          const entries = await s.loadJournal("j-sleep", "wait");
          expect(entries).toHaveLength(1);
          const e = entries[0]!;
          expect(e.stepType).toBe("sleep");
          expect(e.phase).toBe("pending");
          expect(e.exit).toBeUndefined();
          expect(e.wakeAt?.getTime()).toBe(wakeAt.getTime());
        });

        it("completePendingEntry transitions a pending entry to completed", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-cp", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-cp",
            stepName: "sig",
            activityIndex: 0,
            activityName: "approval",
            stepType: "signal",
          });

          await s.completePendingEntry({
            workflowId: "j-cp",
            stepName: "sig",
            activityIndex: 0,
            exit: { tag: "Success", value: { approved: true } },
          });

          const entries = await s.loadJournal("j-cp", "sig");
          expect(entries).toHaveLength(1);
          expect(entries[0]!.phase).toBe("completed");
          expect(entries[0]!.exit).toEqual({ tag: "Success", value: { approved: true } });
        });

        it("completePendingEntry is a no-op on an already-completed entry", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-cp2", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-cp2",
            stepName: "sig",
            activityIndex: 0,
            activityName: "approval",
            stepType: "signal",
          });
          await s.completePendingEntry({
            workflowId: "j-cp2",
            stepName: "sig",
            activityIndex: 0,
            exit: { tag: "Success", value: "first" },
          });
          // Second delivery must not overwrite.
          await s.completePendingEntry({
            workflowId: "j-cp2",
            stepName: "sig",
            activityIndex: 0,
            exit: { tag: "Success", value: "second" },
          });

          const entries = await s.loadJournal("j-cp2", "sig");
          expect(entries[0]!.exit).toEqual({ tag: "Success", value: "first" });
        });

        it("appendPendingEntry is idempotent on (workflowId, stepName, activityIndex)", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-pidem", workflowName: "test", input: {} });
          const wakeAt = new Date(Date.now() + 60_000);
          await s.appendPendingEntry({
            workflowId: "j-pidem",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt,
          });
          await s.appendPendingEntry({
            workflowId: "j-pidem",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt,
          });

          expect(await s.loadJournal("j-pidem", "wait")).toHaveLength(1);
        });

        it("findDueSleeps returns pending sleep entries whose wakeAt <= now", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-due-1", workflowName: "test", input: {} });
          await s.createWorkflow({ workflowId: "j-due-2", workflowName: "test", input: {} });

          const past = new Date(Date.now() - 60_000);
          const future = new Date(Date.now() + 60_000);

          await s.appendPendingEntry({
            workflowId: "j-due-1",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt: past,
          });
          await s.appendPendingEntry({
            workflowId: "j-due-2",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt: future,
          });

          const due = await s.findDueSleeps({ now: new Date(), limit: 10 });
          const ids = due.map((d) => d.workflowId);
          expect(ids).toContain("j-due-1");
          expect(ids).not.toContain("j-due-2");
        });

        it("findDueSleeps ignores completed entries and respects limit", async () => {
          const s = await getSuspendStorage();
          const past = new Date(Date.now() - 60_000);

          for (const wid of ["j-lim-1", "j-lim-2", "j-lim-3"]) {
            await s.createWorkflow({ workflowId: wid, workflowName: "test", input: {} });
            await s.appendPendingEntry({
              workflowId: wid,
              stepName: "wait",
              activityIndex: 0,
              activityName: "nap",
              stepType: "sleep",
              wakeAt: past,
            });
          }
          // Mark one as completed — must not appear in due set.
          await s.completePendingEntry({
            workflowId: "j-lim-2",
            stepName: "wait",
            activityIndex: 0,
            exit: { tag: "Success", value: "woken" },
          });

          const due = await s.findDueSleeps({ now: new Date(), limit: 10 });
          const ids = due.map((d) => d.workflowId);
          expect(ids).toContain("j-lim-1");
          expect(ids).toContain("j-lim-3");
          expect(ids).not.toContain("j-lim-2");

          const capped = await s.findDueSleeps({ now: new Date(), limit: 1 });
          expect(capped).toHaveLength(1);
        });

        it("findPendingSignal returns pending signal by name, null otherwise", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-sig", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-sig",
            stepName: "wait",
            activityIndex: 0,
            activityName: "approval",
            stepType: "signal",
          });

          const hit = await s.findPendingSignal({
            workflowId: "j-sig",
            stepName: "wait",
            signalName: "approval",
          });
          expect(hit).not.toBeNull();
          expect(hit!.activityName).toBe("approval");
          expect(hit!.stepType).toBe("signal");
          expect(hit!.phase).toBe("pending");

          const miss = await s.findPendingSignal({
            workflowId: "j-sig",
            stepName: "wait",
            signalName: "cancel",
          });
          expect(miss).toBeNull();
        });

        it("findPendingSignal returns null once the signal is completed", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-sigc", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-sigc",
            stepName: "wait",
            activityIndex: 0,
            activityName: "approval",
            stepType: "signal",
          });
          await s.completePendingEntry({
            workflowId: "j-sigc",
            stepName: "wait",
            activityIndex: 0,
            exit: { tag: "Success", value: { approved: true } },
          });

          const hit = await s.findPendingSignal({
            workflowId: "j-sigc",
            stepName: "wait",
            signalName: "approval",
          });
          expect(hit).toBeNull();
        });

        it("completePendingEntry reports the winner, and the winner's exit to the loser", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-win", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-win",
            stepName: "sig",
            activityIndex: 0,
            branchPath: "/1.0",
            activityName: "approval",
            stepType: "signal",
          });
          const delivered = { tag: "Success", value: { $signal: "delivered", value: 1 } } as const;
          const timedOut = { tag: "Success", value: { $signal: "timeout" } } as const;

          const first = await s.completePendingEntry({
            workflowId: "j-win",
            stepName: "sig",
            activityIndex: 0,
            branchPath: "/1.0",
            exit: delivered,
          });
          expect(first).toEqual({ completed: true, exit: delivered });

          const second = await s.completePendingEntry({
            workflowId: "j-win",
            stepName: "sig",
            activityIndex: 0,
            branchPath: "/1.0",
            exit: timedOut,
          });
          expect(second).toEqual({ completed: false, exit: delivered });
          expect((await s.loadJournal("j-win", "sig"))[0]!.exit).toEqual(delivered);
        });

        it("a pending signal keeps its timeout deadline (wakeAt) and stays out of due sleeps", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-sig-wake", workflowName: "test", input: {} });
          const wakeAt = new Date(Date.now() - 60_000);
          await s.appendPendingEntry({
            workflowId: "j-sig-wake",
            stepName: "wait",
            activityIndex: 0,
            activityName: "approval",
            stepType: "signal",
            wakeAt,
          });
          const [entry] = await s.loadJournal("j-sig-wake", "wait");
          expect(entry!.wakeAt?.getTime()).toBe(wakeAt.getTime());
          const due = await s.findDueSleeps({ now: new Date(), limit: 1000 });
          expect(due.map((d) => d.workflowId)).not.toContain("j-sig-wake");
        });

        it("completePendingEntry on a missing entry completes nothing and reports no exit", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-miss", workflowName: "test", input: {} });
          const result = await s.completePendingEntry({
            workflowId: "j-miss",
            stepName: "sig",
            activityIndex: 3,
            exit: { tag: "Success", value: 1 },
          });
          expect(result).toEqual({ completed: false, exit: undefined });
          expect(await s.loadJournal("j-miss", "sig")).toEqual([]);
        });

        it("concurrent completePendingEntry calls have exactly one winner", async () => {
          const s = await getSuspendStorage();
          const peer = (await getPeer(s)) as WorkflowStorage & JournaledSuspendStorage;
          await s.createWorkflow({ workflowId: "j-race", workflowName: "test", input: {} });
          for (let round = 0; round < 5; round++) {
            await s.appendPendingEntry({
              workflowId: "j-race",
              stepName: "sig",
              activityIndex: round,
              activityName: `go-${round}`,
              stepType: "signal",
              wakeAt: new Date(Date.now() - 1_000),
            });
            const exits = Array.from({ length: 6 }, (_, i) => ({
              tag: "Success" as const,
              value: { $signal: "delivered", value: `writer-${i}` },
            }));
            const results = await Promise.all(
              exits.map((exit, i) =>
                (i % 2 === 0 ? s : peer).completePendingEntry({
                  workflowId: "j-race",
                  stepName: "sig",
                  activityIndex: round,
                  exit,
                }),
              ),
            );
            const winners = results.filter((r) => r.completed);
            expect(winners).toHaveLength(1);
            const stored = (await s.loadJournal("j-race", "sig")).find(
              (e) => e.activityIndex === round,
            )!;
            expect(stored.exit).toEqual(winners[0]!.exit!);
            for (const r of results) expect(r.exit).toEqual(stored.exit!);
          }
        });

        it("failure exits round-trip with tag, name and fields", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-fail", workflowName: "test", input: {} });
          const tagged = {
            tag: "Failure",
            error: "card declined",
            errorTag: "TerminalError",
            errorData: { code: "E42", attempts: 3, nested: { ok: false } },
          } as const;
          const named = { tag: "Failure", error: "bad input", errorName: "TypeError" } as const;
          for (const [i, exit] of [tagged, named].entries()) {
            await s.appendPendingEntry({
              workflowId: "j-fail",
              stepName: "body",
              activityIndex: i,
              activityName: `a${i}`,
              stepType: "activity",
            });
            await s.completePendingEntry({
              workflowId: "j-fail",
              stepName: "body",
              activityIndex: i,
              exit,
            });
          }
          await s.appendEntry({
            workflowId: "j-fail",
            stepName: "body",
            activityIndex: 2,
            activityName: "single-phase",
            exit: tagged,
          });
          const entries = await s.loadJournal("j-fail", "body");
          expect(entries.map((e) => e.exit)).toEqual([tagged, named, tagged]);
        });

        it("accepts a pending entry for every journal step type", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-types", workflowName: "test", input: {} });
          for (const [i, stepType] of JOURNAL_STEP_TYPES.entries()) {
            await s.appendPendingEntry({
              workflowId: "j-types",
              stepName: "body",
              activityIndex: i,
              activityName: `entry-${stepType}`,
              stepType,
              ...(stepType === "sleep" && { wakeAt: new Date(Date.now() + 60_000) }),
            });
          }
          const entries = await s.loadJournal("j-types", "body");
          expect(entries.map((e) => e.stepType)).toEqual([...JOURNAL_STEP_TYPES]);
          expect(entries.every((e) => e.phase === "pending")).toBe(true);
        });

        it("startFreshRun drops pending sleeps and signals of the old run", async () => {
          const s = await getSuspendStorage();
          await s.createWorkflow({ workflowId: "j-fresh-p", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-fresh-p",
            stepName: "wait",
            activityIndex: 0,
            activityName: "nap",
            stepType: "sleep",
            wakeAt: new Date(Date.now() - 60_000),
          });
          await s.appendPendingEntry({
            workflowId: "j-fresh-p",
            stepName: "wait",
            activityIndex: 1,
            activityName: "approval",
            stepType: "signal",
          });

          await s.startFreshRun("j-fresh-p");

          const due = await s.findDueSleeps({ now: new Date(), limit: 100 });
          expect(due.map((d) => d.workflowId)).not.toContain("j-fresh-p");
          expect(
            await s.findPendingSignal({
              workflowId: "j-fresh-p",
              stepName: "wait",
              signalName: "approval",
            }),
          ).toBeNull();
          expect(await s.loadJournal("j-fresh-p", "wait")).toEqual([]);
        });
      });
    }

    if (options.hasJournalDiscard ?? options.hasJournaledSuspend) {
      describe("journal — discardJournalEntries", () => {
        async function getDiscardStorage() {
          const s = await getSuspendStorage();
          const discard = s.discardJournalEntries;
          if (typeof discard !== "function") {
            throw new Error(
              "storageTestSuite runs the discardJournalEntries cases, but the storage does " +
                "not implement it. Pass hasJournalDiscard: false to skip them.",
            );
          }
          return {
            s,
            discard: (p: Parameters<typeof discard>[0]) => discard.call(s, p),
          };
        }

        it("deletes exactly the listed slots, branch paths included", async () => {
          const { s, discard } = await getDiscardStorage();
          await s.createWorkflow({ workflowId: "j-disc", workflowName: "test", input: {} });
          const slots = [
            { activityIndex: 0, branchPath: "" },
            { activityIndex: 1, branchPath: "/0.0" },
            { activityIndex: 1, branchPath: "/1.0" },
            { activityIndex: 2, branchPath: "" },
          ];
          for (const slot of slots) {
            await s.appendPendingEntry({
              workflowId: "j-disc",
              stepName: "body",
              ...slot,
              activityName: `a${slot.activityIndex}${slot.branchPath}`,
              stepType: "activity",
            });
            await s.completePendingEntry({
              workflowId: "j-disc",
              stepName: "body",
              ...slot,
              exit: { tag: "Failure", error: "boom" },
            });
          }
          await s.appendEntry({
            workflowId: "j-disc",
            stepName: "other",
            activityIndex: 0,
            activityName: "kept",
            exit: { tag: "Success", value: 1 },
          });

          await discard({
            workflowId: "j-disc",
            stepName: "body",
            slots: [
              { activityIndex: 1, branchPath: "/1.0" },
              { activityIndex: 2, branchPath: "" },
              { activityIndex: 9, branchPath: "" },
            ],
          });

          const left = (await s.loadJournal("j-disc", "body")).map(
            (e) => `${e.activityIndex}|${e.branchPath}`,
          );
          expect(left).toEqual(["0|", "1|/0.0"]);
          expect(await s.loadJournal("j-disc", "other")).toHaveLength(1);
        });

        it("a discarded slot can be recorded again", async () => {
          const { s, discard } = await getDiscardStorage();
          await s.createWorkflow({ workflowId: "j-disc2", workflowName: "test", input: {} });
          const slot = { activityIndex: 0, branchPath: "" };
          await s.appendPendingEntry({
            workflowId: "j-disc2",
            stepName: "body",
            ...slot,
            activityName: "pay",
            stepType: "activity",
          });
          await s.completePendingEntry({
            workflowId: "j-disc2",
            stepName: "body",
            ...slot,
            exit: { tag: "Failure", error: "declined" },
          });
          await discard({ workflowId: "j-disc2", stepName: "body", slots: [slot] });

          await s.appendPendingEntry({
            workflowId: "j-disc2",
            stepName: "body",
            ...slot,
            activityName: "pay",
            stepType: "activity",
          });
          const result = await s.completePendingEntry({
            workflowId: "j-disc2",
            stepName: "body",
            ...slot,
            exit: { tag: "Success", value: "paid" },
          });
          expect(result.completed).toBe(true);
          const entries = await s.loadJournal("j-disc2", "body");
          expect(entries).toHaveLength(1);
          expect(entries[0]!.exit).toEqual({ tag: "Success", value: "paid" });
        });

        it("drops discarded pending sleeps and signals from the lookups", async () => {
          const { s, discard } = await getDiscardStorage();
          await s.createWorkflow({ workflowId: "j-disc3", workflowName: "test", input: {} });
          await s.appendPendingEntry({
            workflowId: "j-disc3",
            stepName: "wait",
            activityIndex: 0,
            activityName: "sleep",
            stepType: "sleep",
            wakeAt: new Date(Date.now() - 60_000),
          });
          await s.appendPendingEntry({
            workflowId: "j-disc3",
            stepName: "wait",
            activityIndex: 1,
            activityName: "approval",
            stepType: "signal",
          });
          await discard({
            workflowId: "j-disc3",
            stepName: "wait",
            slots: [
              { activityIndex: 0, branchPath: "" },
              { activityIndex: 1, branchPath: "" },
            ],
          });

          const due = await s.findDueSleeps({ now: new Date(), limit: 1000 });
          expect(due.map((d) => d.workflowId)).not.toContain("j-disc3");
          expect(
            await s.findPendingSignal({
              workflowId: "j-disc3",
              stepName: "wait",
              signalName: "approval",
            }),
          ).toBeNull();
          expect(await s.loadJournal("j-disc3", "wait")).toEqual([]);
        });
      });
    }
  });
}
