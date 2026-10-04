import { describe, it, expect } from "bun:test";
import {
  applyMetadataPatch,
  batchSaveStepResultsDefault,
  sortWorkflowRows,
  tryLockAndLoadDefault,
  workflowSortKey,
  type WorkflowSortFields,
} from "../../../../storage-kit.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { FakeWallClock } from "../../../shared/wall-clock.ts";

describe("applyMetadataPatch", () => {
  it("merges shallowly and drops keys patched to null", () => {
    const current = { a: 1, b: { x: 1 }, c: "keep" };
    const merged = applyMetadataPatch({ current, patch: { a: 2, b: { y: 2 }, c: null, d: true } });
    expect(merged).toEqual({ a: 2, b: { y: 2 }, d: true });
    expect(current).toEqual({ a: 1, b: { x: 1 }, c: "keep" });
  });

  it("treats missing metadata as empty", () => {
    expect(applyMetadataPatch({ current: undefined, patch: { a: 1, gone: null } })).toEqual({
      a: 1,
    });
    expect(applyMetadataPatch({ current: null, patch: {} })).toEqual({});
  });
});

describe("listWorkflows ordering", () => {
  const row = (
    name: string,
    createdAtMs: number,
    extra: Partial<WorkflowSortFields> = {},
  ): WorkflowSortFields => ({ workflowName: name, status: "running", createdAtMs, ...extra });

  it("computes duration from createdAt to completedAt", () => {
    const fields = row("a", 100, { completedAtMs: 350 });
    expect(workflowSortKey({ fields, orderBy: "duration" })).toBe(250);
    expect(workflowSortKey({ fields: row("b", 100), orderBy: "duration" })).toBeUndefined();
  });

  it("sorts rows without a value last in both directions", () => {
    const rows = [
      row("pending", 1),
      row("early", 2, { startedAtMs: 10 }),
      row("late", 3, { startedAtMs: 30 }),
    ];
    const names = (orderDir: "asc" | "desc") =>
      sortWorkflowRows({ rows: [...rows], orderBy: "startedAt", orderDir, fields: (r) => r }).map(
        (r) => r.workflowName,
      );
    expect(names("desc")).toEqual(["late", "early", "pending"]);
    expect(names("asc")).toEqual(["early", "late", "pending"]);
  });

  it("keeps ties in input order", () => {
    const rows = [row("b", 5), row("a", 5), row("c", 5)];
    const sorted = sortWorkflowRows({
      rows,
      orderBy: "createdAt",
      orderDir: "desc",
      fields: (r) => r,
    });
    expect(sorted.map((r) => r.workflowName)).toEqual(["b", "a", "c"]);
  });
});

describe("storage method fallbacks", () => {
  it("tryLockAndLoadDefault locks, then loads", async () => {
    const storage = new InMemoryWorkflowStorage({ clock: FakeWallClock.create(0) });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "w", input: 1 });
    const first = await tryLockAndLoadDefault({ storage, workflowId: "wf", lockDurationMs: 1_000 });
    expect(first.locked).toBe(true);
    expect(first.token).toBeDefined();
    expect(first.state?.workflowId).toBe("wf");
    const second = await tryLockAndLoadDefault({
      storage,
      workflowId: "wf",
      lockDurationMs: 1_000,
    });
    expect(second.locked).toBe(false);
    expect(second.token).toBeUndefined();
    expect(second.state?.workflowId).toBe("wf");
  });

  it("batchSaveStepResultsDefault writes every record under the guard", async () => {
    const storage = new InMemoryWorkflowStorage({ clock: FakeWallClock.create(0) });
    await storage.createWorkflow({ workflowId: "wf", workflowName: "w", input: 1 });
    const { token } = await storage.tryLock({ workflowId: "wf", lockDurationMs: 1_000 });
    const startedAt = new Date(0);
    await batchSaveStepResultsDefault({
      storage,
      records: [
        { workflowId: "wf", stepName: "a", result: 1, durationMs: 1, startedAt },
        { workflowId: "wf", stepName: "b", result: 2, durationMs: 1, startedAt },
      ],
      guard: { fenceToken: token },
    });
    const state = await storage.loadWorkflow("wf");
    expect(state?.steps["a"]?.result).toBe(1);
    expect(state?.steps["b"]?.result).toBe(2);
    await expect(
      batchSaveStepResultsDefault({
        storage,
        records: [{ workflowId: "wf", stepName: "c", result: 3, durationMs: 1, startedAt }],
        guard: { fenceToken: "stale" },
      }),
    ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" });
  });
});
