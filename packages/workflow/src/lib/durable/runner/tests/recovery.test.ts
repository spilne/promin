// ---------------------------------------------------------------------------
// recover(): bounded in-flight resumes, keyset listing that runs changing
// status cannot shift, and a stale sweep that cannot loop forever.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import type { Workflow } from "../../durable-pipeline.ts";
import type { WorkflowState, WorkflowStatus } from "../../workflow-state.ts";
import type { OrphanedRun, WorkflowStorage } from "../../workflow-storage.ts";
import { recoverWorkflows, RecoveryStrategy } from "../recovery.ts";

const DEFINITION = { name: "wf", version: "1" } as unknown as Workflow<unknown, unknown>;
const registry = {
  resolve: async () => DEFINITION,
} as unknown as Parameters<typeof recoverWorkflows>[0]["registry"];

function row(id: string, status: WorkflowStatus, createdAt = 0): WorkflowState {
  return {
    workflowId: id,
    workflowName: "wf",
    version: "1",
    status,
    run: 1,
    input: { id },
    steps: {},
    createdAt: new Date(createdAt),
    updatedAt: new Date(createdAt),
  };
}

const ids = (n: number): string[] =>
  Array.from({ length: n }, (_, i) => `run-${String(i).padStart(6, "0")}`);

/**
 * A storage over `rows` whose `listOrphanedRuns` pages by `workflowId`
 * keyset; a resumed run leaves the orphan set as soon as it is resumed.
 */
function orphanStorage(rows: Map<string, WorkflowState>): WorkflowStorage {
  return {
    listOrphanedRuns: async (p: { limit: number; afterWorkflowId?: string }) => {
      const out: OrphanedRun[] = [];
      for (const id of [...rows.keys()].sort()) {
        if (p.afterWorkflowId !== undefined && id <= p.afterWorkflowId) continue;
        const r = rows.get(id)!;
        if (r.status !== "pending" && r.status !== "running" && r.status !== "compensating") {
          continue;
        }
        out.push({
          workflowId: id,
          workflowName: r.workflowName,
          status: r.status,
          input: r.input,
          version: "1",
        });
        if (out.length >= p.limit) break;
      }
      return out;
    },
  } as unknown as WorkflowStorage;
}

describe("recover()", () => {
  it("resumes 10k runs with at most `concurrent` in flight, each exactly once", async () => {
    const rows = new Map(ids(10_000).map((id) => [id, row(id, "pending")]));
    const storage = orphanStorage(rows);
    let inFlight = 0;
    let peak = 0;
    const resumed = new Map<string, number>();

    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder().resumeRecent({ concurrent: 25 }).build(),
      storage,
      registry,
      clock: FakeWallClock.create(0),
      resume: async (run) => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        resumed.set(run.workflowId, (resumed.get(run.workflowId) ?? 0) + 1);
        // The resumed run moves the row out of `pending` while it runs.
        rows.set(run.workflowId, row(run.workflowId, "running"));
        await new Promise<void>((r) => setImmediate(r));
        rows.set(run.workflowId, row(run.workflowId, "completed"));
        inFlight--;
      },
    });

    expect(result.resumed).toBe(10_000);
    expect(peak).toBeLessThanOrEqual(25);
    await result.settled;
    expect(peak).toBe(25);
    expect(inFlight).toBe(0);
    expect(resumed.size).toBe(10_000);
    expect([...resumed.values()].every((n) => n === 1)).toBe(true);
  });

  it("without listOrphanedRuns, lists every page before resuming any run", async () => {
    // Offset pages over a status whose rows leave it as they are resumed
    // would skip rows; the snapshot sees all of them.
    const rows = new Map(ids(1_000).map((id, i) => [id, row(id, "pending", i)]));
    const storage = {
      listWorkflows: async (p: { status: WorkflowStatus; limit: number; offset: number }) =>
        [...rows.values()]
          .filter((r) => r.status === p.status)
          .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
          .slice(p.offset, p.offset + p.limit),
    } as unknown as WorkflowStorage;
    const resumed = new Set<string>();

    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder().resumeRecent({ concurrent: 50 }).build(),
      storage,
      registry,
      clock: FakeWallClock.create(0),
      resume: async (run) => {
        resumed.add(run.workflowId);
        rows.set(run.workflowId, row(run.workflowId, "running"));
      },
    });
    await result.settled;

    expect(result.resumed).toBe(1_000);
    expect(resumed.size).toBe(1_000);
  });

  it("a resume that rejects frees its slot", async () => {
    const rows = new Map(ids(100).map((id) => [id, row(id, "running")]));
    let calls = 0;
    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder().resumeRecent({ concurrent: 2 }).build(),
      storage: orphanStorage(rows),
      registry,
      clock: FakeWallClock.create(0),
      resume: async () => {
        calls++;
        throw new Error("lock held elsewhere");
      },
    });
    await result.settled;
    expect(calls).toBe(100);
  });

  it("the stale sweep stops when the backend leaves terminated rows listed", async () => {
    const rows = ids(450).map((id) => row(id, "running", 0));
    let cancels = 0;
    const storage = {
      // `cancelWorkflow` is a no-op: every row stays `running`.
      listWorkflows: async (p: { limit: number; offset: number }) =>
        rows.slice(p.offset, p.offset + p.limit),
      cancelWorkflow: async () => {
        cancels++;
      },
    } as unknown as WorkflowStorage;

    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder()
        .cancelStale({ olderThanMs: 1_000, statuses: ["running"] })
        .build(),
      storage,
      registry,
      clock: FakeWallClock.create(10_000),
      resume: () => undefined,
    });

    expect(cancels).toBe(450);
    expect(result.terminated).toBe(450);
  });

  it("the stale sweep awaits an async cancelStaleWorkflows", async () => {
    const storage = {
      cancelStaleWorkflows: async () => {
        await new Promise<void>((r) => setImmediate(r));
        return 7;
      },
    } as unknown as WorkflowStorage;
    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder().failStale({ olderThanMs: 1 }).build(),
      storage,
      registry,
      clock: FakeWallClock.create(0),
      resume: () => undefined,
    });
    expect(result.terminated).toBe(7);
  });

  it("rejects a non-positive concurrency", () => {
    expect(() => RecoveryStrategy.builder().resumeRecent({ concurrent: 0 })).toThrow();
  });
});
