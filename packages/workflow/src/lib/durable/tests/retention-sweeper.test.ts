import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { InMemoryStepQueue } from "../../distributed/in-memory-step-queue.ts";
import { RetentionSweeper, type RetentionSweepReport } from "../retention-sweeper.ts";
import { workflow } from "../workflow-builder.ts";
import { CANCELLED_ERROR, CANCELLED_ERROR_TAG } from "../workflow-state.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import type { WorkflowTracer, WorkflowTraceRetentionSweep } from "../workflow-tracer.ts";

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

function setup(params: { storage?: (s: InMemoryWorkflowStorage) => WorkflowStorage } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const base = new InMemoryWorkflowStorage({ clock });
  const storage = params.storage?.(base) ?? base;
  const end = async (p: {
    id: string;
    name?: string;
    how: "complete" | "fail" | "cancel" | "tripwire";
    parent?: string;
  }) => {
    await storage.createWorkflow({
      workflowId: p.id,
      workflowName: p.name ?? "w",
      input: {},
      ...(p.parent !== undefined && { parentWorkflowId: p.parent }),
    });
    if (p.how === "complete") await storage.completeWorkflow({ workflowId: p.id, result: 1 });
    else if (p.how === "fail") await storage.failWorkflow({ workflowId: p.id, error: "boom" });
    else if (p.how === "tripwire") {
      await storage.tripwireWorkflow!({ workflowId: p.id, reason: "stop" });
    } else await storage.cancelWorkflow({ workflowId: p.id });
  };
  /** Finish a cancel the way a driver does: reopen for rollback, then settle. */
  const settleCancel = async (id: string) => {
    await base.beginCompensation({ workflowId: id, origin: "cancel", error: CANCELLED_ERROR });
    await storage.failWorkflow({
      workflowId: id,
      error: CANCELLED_ERROR,
      errorTag: CANCELLED_ERROR_TAG,
      compensationOrigin: "cancel-settled",
    });
  };
  const exists = async (id: string) => (await storage.loadWorkflow(id)) !== null;
  return { clock, base, storage, end, settleCancel, exists };
}

describe("RetentionSweeper", () => {
  it("deletes each class once it is older than its own age, with everything stored for it", async () => {
    const t = setup();
    await t.end({ id: "done", how: "complete" });
    await t.end({ id: "trip", how: "tripwire" });
    await t.end({ id: "bad", how: "fail" });
    await t.end({ id: "cxl", how: "cancel" });
    await t.settleCancel("cxl");
    await t.storage.deliverSignal({ workflowId: "done", signalName: "s", payload: 1 });

    const reports: RetentionSweepReport[] = [];
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR, failed: 3 * DAY, cancelled: { olderThan: DAY } },
      onSweep: (r) => reports.push(r),
    });

    expect((await sweeper.sweep()).deletedRuns).toBe(0);
    t.clock.advance(2 * HOUR);
    const first = await sweeper.sweep();
    expect(first.deletedRuns).toBe(2);
    expect(first.deletedByClass).toEqual({ completed: 2, failed: 0, cancelled: 0 });
    expect(await t.exists("done")).toBe(false);
    expect(await t.exists("trip")).toBe(false);
    expect(await t.storage.loadSignals("done")).toEqual([]);

    t.clock.advance(DAY);
    expect((await sweeper.sweep()).deletedByClass.cancelled).toBe(1);
    expect(await t.exists("bad")).toBe(true);

    t.clock.advance(2 * DAY);
    const last = await sweeper.sweep();
    expect(last.deletedByWorkflow).toEqual({ w: 1 });
    expect(await t.exists("bad")).toBe(false);
    expect(reports.map((r) => r.deletedRuns)).toEqual([0, 2, 1, 1]);
  });

  it("a definition's retention overrides the default per field, and null keeps a class forever", async () => {
    const t = setup();
    const keepFailures = workflow<unknown>({
      name: "audited",
      retention: { failed: null, completed: 2 * HOUR },
    }).build();
    await t.end({ id: "a-ok", name: "audited", how: "complete" });
    await t.end({ id: "a-bad", name: "audited", how: "fail" });
    await t.end({ id: "o-ok", name: "other", how: "complete" });
    await t.end({ id: "o-bad", name: "other", how: "fail" });
    t.clock.advance(3 * HOUR);

    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      workflows: [keepFailures],
      retention: { completed: 10 * DAY, failed: HOUR },
    });
    await sweeper.sweep();
    expect(await t.exists("a-ok")).toBe(false); // own completed: 2h
    expect(await t.exists("a-bad")).toBe(true); // own failed: forever
    expect(await t.exists("o-ok")).toBe(true); // default completed: 10d
    expect(await t.exists("o-bad")).toBe(false); // default failed: 1h
  });

  it("with only a definition's policy, sweeps just that workflow", async () => {
    const t = setup();
    const only = workflow<unknown>({ name: "only", retention: { completed: HOUR } }).build();
    await t.end({ id: "x", name: "only", how: "complete" });
    await t.end({ id: "y", name: "else", how: "complete" });
    t.clock.advance(DAY);
    await new RetentionSweeper({ storage: t.storage, clock: t.clock, workflows: [only] }).sweep();
    expect(await t.exists("x")).toBe(false);
    expect(await t.exists("y")).toBe(true);
  });

  it("keepAtLeast spares the newest runs of a workflow however old they are", async () => {
    const t = setup();
    for (const id of ["r1", "r2", "r3", "r4"]) {
      await t.end({ id, how: "complete" });
      t.clock.advance(1_000);
    }
    t.clock.advance(DAY);
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR, keepAtLeast: 3 },
    });
    const report = await sweeper.sweep();
    expect(report.deletedRuns).toBe(1);
    expect(report.kept.keepAtLeast).toBe(3);
    expect(await t.exists("r1")).toBe(false);
    for (const id of ["r2", "r3", "r4"]) expect(await t.exists(id)).toBe(true);
  });

  it("never deletes a cancelled run that owes its rollback, until it is settled", async () => {
    const t = setup();
    await t.end({ id: "owed", how: "cancel" });
    t.clock.advance(10 * DAY);
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { failed: HOUR, cancelled: HOUR, completed: HOUR },
    });
    const first = await sweeper.sweep();
    expect(first.deletedRuns).toBe(0);
    expect(first.kept.owedRollback).toBe(1);
    expect(await t.exists("owed")).toBe(true);

    await t.settleCancel("owed");
    t.clock.advance(2 * HOUR);
    expect((await sweeper.sweep()).deletedByClass.cancelled).toBe(1);
  });

  it("keeps a child while its parent still runs or owes a rollback, and a parent while a child runs", async () => {
    const t = setup();
    await t.storage.createWorkflow({ workflowId: "parent", workflowName: "p", input: {} });
    await t.end({ id: "child", name: "c", how: "complete", parent: "parent" });
    await t.end({ id: "owed-parent", name: "p", how: "cancel" });
    await t.end({ id: "child-of-owed", name: "c", how: "complete", parent: "owed-parent" });
    // A finished parent whose child is still running.
    await t.end({ id: "done-parent", name: "p", how: "complete" });
    await t.storage.createWorkflow({
      workflowId: "live-child",
      workflowName: "c",
      input: {},
      parentWorkflowId: "done-parent",
    });
    t.clock.advance(DAY);
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR },
    });

    const first = await sweeper.sweep();
    expect(first.deletedRuns).toBe(0);
    expect(first.kept).toMatchObject({ parentActive: 2, childActive: 1 });

    // The parent finishes; now the child may go, the parent still owes nothing.
    await t.storage.completeWorkflow({ workflowId: "parent", result: 1 });
    t.clock.advance(2 * HOUR);
    const second = await sweeper.sweep();
    expect(await t.exists("child")).toBe(false);
    expect(await t.exists("parent")).toBe(false);
    expect(second.deletedRuns).toBe(2);
    expect(await t.exists("child-of-owed")).toBe(true);
    expect(await t.exists("done-parent")).toBe(true);
  });

  it("never deletes a run that was resumed after it was listed", async () => {
    let revive: (() => Promise<void>) | undefined;
    const t = setup({
      storage: (base) =>
        new Proxy(base, {
          get(target, prop, receiver) {
            if (prop === "listRetentionCandidates") {
              return async (p: Parameters<typeof base.listRetentionCandidates>[0]) => {
                const rows = await target.listRetentionCandidates(p);
                await revive?.();
                return rows;
              };
            }
            const v = Reflect.get(target, prop, receiver);
            return typeof v === "function" ? v.bind(target) : v;
          },
        }),
    });
    await t.end({ id: "raced", how: "complete" });
    await t.end({ id: "calm", how: "complete" });
    t.clock.advance(DAY);
    revive = async () => {
      // Another driver reopens the run between the listing and the delete.
      await t.base.startFreshRun({ workflowId: "raced" });
    };
    const report = await new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR },
    }).sweep();
    expect(report.deletedRuns).toBe(1);
    expect(report.kept.changed).toBe(1);
    expect((await t.storage.loadWorkflow("raced"))?.status).toBe("pending");
    expect(await t.exists("calm")).toBe(false);
  });

  it("several sweepers over one storage delete every run exactly once", async () => {
    const t = setup();
    for (let i = 0; i < 40; i++) await t.end({ id: `run-${i}`, how: "complete" });
    t.clock.advance(DAY);
    const make = () =>
      new RetentionSweeper({
        storage: t.storage,
        clock: t.clock,
        retention: { completed: HOUR },
        batchSize: 7,
      });
    const reports = await Promise.all([make().sweep(), make().sweep(), make().sweep()]);
    expect(reports.reduce((n, r) => n + r.deletedRuns, 0)).toBe(40);
    expect(await t.storage.listWorkflows()).toEqual([]);
  });

  it("deletes in bounded batches and resumes after the budget ends a sweep", async () => {
    const t = setup();
    const batches: number[] = [];
    const slow = new Proxy(t.storage, {
      get(target, prop, receiver) {
        if (prop === "purgeRuns") {
          return async (p: { runs: readonly unknown[] }) => {
            batches.push(p.runs.length);
            t.clock.advance(1_000); // each batch costs a second
            return (target as any).purgeRuns(p);
          };
        }
        const v = Reflect.get(target, prop, receiver);
        return typeof v === "function" ? v.bind(target) : v;
      },
    }) as WorkflowStorage;
    // `b-00` stays: its child still runs. Ten more runs follow it in keyset order.
    await t.end({ id: "b-00", how: "complete" });
    await t.storage.createWorkflow({
      workflowId: "b-00-child",
      workflowName: "c",
      input: {},
      parentWorkflowId: "b-00",
    });
    for (let i = 1; i <= 10; i++) {
      await t.end({ id: `b-${String(i).padStart(2, "0")}`, how: "complete" });
    }
    t.clock.advance(DAY);
    const sweeper = new RetentionSweeper({
      storage: slow,
      clock: t.clock,
      retention: { completed: HOUR },
      batchSize: 3,
      perSweepBudgetMs: 2_500,
    });

    const first = await sweeper.sweep();
    expect(first.complete).toBe(false);
    expect(batches).toEqual([2, 3, 3]);
    expect(first.deletedRuns).toBe(8);
    expect(first.kept.childActive).toBe(1);

    // The second sweep resumes after the first one's last run: it does not look at `b-00` again.
    const second = await sweeper.sweep();
    expect(second.complete).toBe(true);
    expect(second.deletedRuns).toBe(2);
    expect(second.kept.childActive).toBe(0);
    expect((await t.storage.listWorkflows()).map((w) => w.workflowId).sort()).toEqual([
      "b-00",
      "b-00-child",
    ]);
  });

  it("a dry run counts what would go and deletes nothing", async () => {
    const t = setup();
    await t.end({ id: "d1", how: "complete" });
    await t.end({ id: "d2", how: "fail" });
    t.clock.advance(DAY);
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR, failed: HOUR },
    });
    const dry = await sweeper.sweep({ dryRun: true });
    expect(dry).toMatchObject({ dryRun: true, deletedRuns: 2, deletedStepTasks: undefined });
    expect((await t.storage.listWorkflows()).length).toBe(2);
    expect((await sweeper.sweep()).deletedRuns).toBe(2);
  });

  it("purges old terminal step-queue tasks and reports them to the tracer", async () => {
    const t = setup();
    const queue = new InMemoryStepQueue({ clock: t.clock });
    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      ids.push(await queue.enqueue({ workflowId: `q-${i}`, stepName: "s", input: {} }));
    }
    for (const task of await queue.claim({ workerId: "w", limit: 5 })) {
      await queue.complete({
        taskId: task.id,
        claimToken: task.claimToken,
        result: 1,
        durationMs: 1,
      });
    }
    t.clock.advance(2 * HOUR);
    const events: WorkflowTraceRetentionSweep[] = [];
    const tracer: WorkflowTracer = { retentionSweep: (e) => void events.push(e) };
    const report = await new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      stepQueue: queue,
      retention: { stepQueue: HOUR },
      batchSize: 2,
      tracer,
    }).sweep();
    expect(report.deletedStepTasks).toBe(5);
    for (const id of ids) expect(await queue.get(id)).toBeUndefined();
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ deletedStepTasks: 5, deletedRuns: 0, dryRun: false });
  });

  it("start() sweeps on the clock until stop()", async () => {
    const t = setup();
    await t.end({ id: "s1", how: "complete" });
    const sweeper = new RetentionSweeper({
      storage: t.storage,
      clock: t.clock,
      retention: { completed: HOUR },
      intervalMs: 10 * 60_000,
    });
    const running = sweeper.start();
    await new Promise((r) => setTimeout(r, 20));
    // The first tick runs at start; the run is too young for it.
    t.clock.advance(2 * HOUR);
    await new Promise((r) => setTimeout(r, 20));
    expect(sweeper.lastReport?.deletedRuns).toBe(1);
    expect(await t.exists("s1")).toBe(false);
    await sweeper.stop();
    await running;
  });

  it("rejects a storage without the retention capability and an unusable policy", () => {
    const t = setup();
    const bare = Object.create(t.storage) as WorkflowStorage;
    Object.defineProperty(bare, "purgeRuns", { value: undefined });
    expect(() => new RetentionSweeper({ storage: bare })).toThrow("retention");
    expect(
      () => new RetentionSweeper({ storage: t.storage, retention: { completed: -1 } }),
    ).toThrow("retention.completed");
    expect(() => workflow<unknown>({ name: "bad", retention: { keepAtLeast: 1.5 } })).toThrow(
      "keepAtLeast",
    );
  });
});
