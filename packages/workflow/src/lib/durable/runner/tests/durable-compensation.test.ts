// ---------------------------------------------------------------------------
// Durable compensation — the `compensating` phase and the per-step ledger:
// a rollback interrupted by a crash resumes where it stopped instead of
// re-running the workflow, compensations already recorded never run again,
// the remaining ones do, and `compensate` sees decoded step values.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { workflow, type Workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { createWorkflowRunner, RecoveryStrategy } from "../../workflow-runner.ts";
import { InMemoryWorkflowVersionRegistry } from "../../workflow-version-registry.ts";
import type { WorkflowState } from "../../workflow-state.ts";
import type { WorkflowStorage } from "../../workflow-storage.ts";
import { compensationOrder } from "../compensation.ts";

class ChargeDeclined extends TaggedError("ChargeDeclined")<{ readonly message: string }>() {}

/** Writes the run makes; a crashed storage rejects every one of them. */
const RUN_WRITES = new Set([
  "saveStepResult",
  "batchSaveStepResults",
  "saveStepFailure",
  "saveStepAttempt",
  "beginCompensation",
  "saveStepCompensation",
  "failWorkflow",
  "completeWorkflow",
]);

/**
 * `inner` behind a switch: while `crashed()` is true every run write
 * rejects, as if the process driving the run had died (lock calls still go
 * through, so the next driver can take the lock).
 */
function crashable(inner: InMemoryWorkflowStorage, crashed: () => boolean): WorkflowStorage {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      if (typeof prop === "string" && RUN_WRITES.has(prop)) {
        return (...args: unknown[]) =>
          crashed()
            ? Promise.reject(new Error(`storage unavailable (${prop})`))
            : value.apply(target, args);
      }
      return value.bind(target);
    },
  });
}

/** Settle `promise`, advancing `clock` so checkpoint retry backoffs elapse. */
async function drive<T>(promise: Promise<T>, clock: FakeWallClock): Promise<T> {
  let settled = false;
  const tracked = promise.finally(() => {
    settled = true;
  });
  while (!settled) {
    await new Promise<void>((r) => setImmediate(r));
    clock.advance(250);
  }
  return tracked;
}

/** a → b → c → d → e; e fails; a–d record their compensations in `calls`. */
function sagaWorkflow(params: {
  calls: string[];
  compensate?: Partial<Record<"a" | "b" | "c" | "d", () => void>>;
  onComplete?: (report: { compensated: string[]; failed: string[] }) => void;
}): Workflow<{ order: string }, unknown> {
  const { calls } = params;
  const undo = (name: "a" | "b" | "c" | "d") => ({
    compensate: async () => {
      calls.push(`undo:${name}`);
      params.compensate?.[name]?.();
    },
  });
  return workflow<{ order: string }>({
    name: "durable-saga",
    version: "1",
    compensate: {
      onComplete: async ({ compensatedSteps, failedCompensations }) => {
        params.onComplete?.({
          compensated: compensatedSteps,
          failed: failedCompensations.map((f) => f.stepName),
        });
      },
    },
  })
    .step("a", () => succeed("a"), undo("a"))
    .step("b", () => succeed("b"), undo("b"))
    .step("c", () => succeed("c"), undo("c"))
    .step("d", () => succeed("d"), undo("d"))
    .step("e", () => fail(new ChargeDeclined({ message: "card declined" })))
    .build() as unknown as Workflow<{ order: string }, unknown>;
}

describe("durable compensation", () => {
  it("a crash in the second compensation resumes the rollback without repeating the first", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage();
    let crashed = false;
    let crashedOnce = false;
    const calls: string[] = [];
    const reports: Array<{ compensated: string[]; failed: string[] }> = [];
    const wf = sagaWorkflow({
      calls,
      compensate: {
        c: () => {
          if (crashedOnce) return;
          crashedOnce = true;
          crashed = true;
          throw new Error("process died mid-compensation");
        },
      },
      onComplete: (r) => reports.push(r),
    });

    // First driver: dies while compensating c.
    const first = createWorkflowRunner({ storage: crashable(inner, () => crashed), clock });
    const r1 = await drive(
      first.runSafe({ workflow: wf, workflowId: "saga-1", input: { order: "o-1" } }),
      clock,
    );
    expect((r1.error as { _tag?: string })._tag).toBe("CheckpointError");
    expect(calls).toEqual(["undo:d", "undo:c"]);

    const midway = (await inner.loadWorkflow("saga-1"))!;
    expect(midway.status).toBe("compensating");
    expect(midway.error).toBe("card declined");
    expect(midway.errorTag).toBe("ChargeDeclined");
    expect(midway.steps.d!.compensationStatus).toBe("compensated");
    expect(midway.steps.c!.compensationStatus).toBeUndefined();
    expect(reports).toEqual([]);

    // Next driver: finishes the rollback, never re-runs the workflow.
    crashed = false;
    const second = createWorkflowRunner({ storage: inner, clock });
    const r2 = await drive(
      second.runSafe({ workflow: wf, workflowId: "saga-1", input: { order: "o-1" } }),
      clock,
    );
    expect(r2.error).toMatchObject({
      _tag: "WorkflowFailedError",
      errorTag: "ChargeDeclined",
      message: "card declined",
    });
    // d is not repeated; c (interrupted before its ledger entry), b and a run.
    expect(calls).toEqual(["undo:d", "undo:c", "undo:c", "undo:b", "undo:a"]);
    expect(reports).toEqual([{ compensated: ["d", "c", "b", "a"], failed: [] }]);

    const final = (await inner.loadWorkflow("saga-1"))!;
    expect(final.status).toBe("failed");
    expect(final.error).toBe("card declined");
    expect(final.errorTag).toBe("ChargeDeclined");
    for (const name of ["a", "b", "c", "d"]) {
      expect(final.steps[name]!.compensationStatus).toBe("compensated");
      expect(final.steps[name]!.compensatedAt).toBeInstanceOf(Date);
    }
    // e never ran a second time.
    expect(final.steps.e!.attempt).toBe(1);

    // A further run answers with the stored failure and rolls back nothing.
    const r3 = await second.runSafe({
      workflow: wf,
      workflowId: "saga-1",
      input: { order: "o-1" },
    });
    expect(r3.error).toMatchObject({ _tag: "WorkflowFailedError", errorTag: "ChargeDeclined" });
    expect(calls).toHaveLength(5);
  });

  it("a compensation that failed before the crash stays failed and is not retried", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage();
    let crashed = false;
    let crashedOnce = false;
    const calls: string[] = [];
    const reports: Array<{ compensated: string[]; failed: string[] }> = [];
    const wf = sagaWorkflow({
      calls,
      compensate: {
        c: () => {
          throw new Error("refund API down");
        },
        b: () => {
          if (crashedOnce) return;
          crashedOnce = true;
          crashed = true;
          throw new Error("process died");
        },
      },
      onComplete: (r) => reports.push(r),
    });

    const first = createWorkflowRunner({ storage: crashable(inner, () => crashed), clock });
    await drive(
      first.runSafe({ workflow: wf, workflowId: "saga-2", input: { order: "o" } }),
      clock,
    );
    const midway = (await inner.loadWorkflow("saga-2"))!;
    expect(midway.status).toBe("compensating");
    expect(midway.steps.c!.compensationStatus).toBe("compensation_failed");
    expect(midway.steps.c!.compensationError).toBe("refund API down");

    crashed = false;
    const second = createWorkflowRunner({ storage: inner, clock });
    await drive(
      second.runSafe({ workflow: wf, workflowId: "saga-2", input: { order: "o" } }),
      clock,
    );

    expect(calls).toEqual(["undo:d", "undo:c", "undo:b", "undo:b", "undo:a"]);
    expect(reports).toEqual([{ compensated: ["d", "b", "a"], failed: ["c"] }]);
    expect((await inner.loadWorkflow("saga-2"))!.status).toBe("failed");

    // Compensation attempt rows: one per try, numbered across both drivers.
    const attempts = await inner.loadStepAttempts({ workflowId: "saga-2" });
    const comp = attempts
      .filter((a) => a.type === "compensation")
      .map((a) => `${a.stepName}#${a.attempt}:${a.status}`);
    expect(comp).toEqual(["d#1:completed", "c#1:failed", "b#1:completed", "a#1:completed"]);
  });

  it("recover() finishes the rollback of a run left compensating", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage({ clock });
    let crashed = false;
    let crashedOnce = false;
    const calls: string[] = [];
    const wf = sagaWorkflow({
      calls,
      compensate: {
        b: () => {
          if (crashedOnce) return;
          crashedOnce = true;
          crashed = true;
          throw new Error("process died");
        },
      },
    });
    const first = createWorkflowRunner({ storage: crashable(inner, () => crashed), clock });
    await drive(
      first.runSafe({ workflow: wf, workflowId: "saga-3", input: { order: "o" } }),
      clock,
    );
    expect((await inner.loadWorkflow("saga-3"))!.status).toBe("compensating");
    crashed = false;

    const registry = new InMemoryWorkflowVersionRegistry();
    registry.register(wf as unknown as Workflow<unknown, unknown>);
    const restarted = createWorkflowRunner({ storage: inner, clock, registry });
    const result = await restarted.recover(RecoveryStrategy.builder().resumeRecent().build());
    expect(result.resumed).toBe(1);
    await drive(result.settled, clock);

    expect(calls).toEqual(["undo:d", "undo:c", "undo:b", "undo:b", "undo:a"]);
    const final = (await inner.loadWorkflow("saga-3"))!;
    expect(final.status).toBe("failed");
    expect(final.errorTag).toBe("ChargeDeclined");
  });

  it("compensate receives the step's decoded value, not its stored encoding", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const seen: unknown[] = [];
    const wf = workflow({ name: "decoded-compensate" })
      .step("reserve", () => succeed({ at: new Date(42), ids: new Set([1, 2]) }), {
        compensate: async ({ result }) => {
          seen.push(result);
        },
      })
      .step("charge", () => fail(new ChargeDeclined({ message: "no" })))
      .build();

    await runner.runSafe({ workflow: wf, workflowId: "dec-1", input: undefined });

    expect(seen).toHaveLength(1);
    const value = seen[0] as { at: unknown; ids: unknown };
    expect(value.at).toBeInstanceOf(Date);
    expect((value.at as Date).getTime()).toBe(42);
    expect(value.ids).toBeInstanceOf(Set);
    expect([...(value.ids as Set<number>)]).toEqual([1, 2]);
  });

  it("parallel branches roll back in reverse completion order", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const calls: string[] = [];
    const undo = (name: string) => ({
      compensate: async () => {
        calls.push(name);
      },
    });
    let slowArmed = false;
    const wf = workflow({ name: "parallel-rollback" })
      .stepAsync("root", async () => "root", undo("root"))
      .stepAsync(
        "slow",
        { dependsOn: ["root"] },
        async () => {
          // 30ms of clock time: "slow" completes after "fast" on the clock,
          // although it is defined first.
          const waited = new Promise<void>((r) => clock.setTimeout(r, 30));
          slowArmed = true;
          await waited;
          return "slow";
        },
        undo("slow"),
      )
      .stepAsync("fast", { dependsOn: ["root"] }, async () => "fast", undo("fast"))
      .stepAsync("join", { dependsOn: ["slow", "fast"] }, async () => {
        throw new Error("join failed");
      })
      .build();

    const running = runner.runSafe({ workflow: wf, workflowId: "par-1", input: undefined });
    // Advance only once `slow` waits on the clock and `fast` is checkpointed
    // at the current time; advancing earlier would stamp both at +30ms.
    const fastSaved = async () =>
      (await storage.loadWorkflow("par-1"))?.steps["fast"]?.status === "completed";
    for (let i = 0; i < 2_000 && !(slowArmed && (await fastSaved())); i++) {
      await new Promise<void>((r) => setImmediate(r));
    }
    expect(slowArmed && (await fastSaved())).toBe(true);
    clock.advance(30);
    await running;
    // `slow` finished last, so it is undone first, then `fast`, then `root`.
    expect(calls).toEqual(["slow", "fast", "root"]);
  });
});

describe("compensationOrder", () => {
  const step = (name: string) => ({ name, compensate: async () => undefined });
  const stateWith = (
    rows: Record<string, { completedAt?: number; status?: "completed" | "failed" }>,
  ): WorkflowState =>
    ({
      workflowId: "w",
      workflowName: "w",
      status: "running",
      run: 1,
      input: null,
      createdAt: new Date(0),
      updatedAt: new Date(0),
      steps: Object.fromEntries(
        Object.entries(rows).map(([name, r]) => [
          name,
          {
            stepName: name,
            run: 1,
            status: r.status ?? "completed",
            dependsOn: [],
            stepType: "single",
            attempt: 1,
            ...(r.completedAt !== undefined && { completedAt: new Date(r.completedAt) }),
          },
        ]),
      ),
    }) as WorkflowState;

  it("latest completion first; ties go to the later-defined step", () => {
    const steps = ["root", "x", "y", "z"].map(step);
    const order = compensationOrder({
      steps,
      state: stateWith({
        root: { completedAt: 1 },
        x: { completedAt: 5 },
        y: { completedAt: 9 },
        z: { completedAt: 5 },
      }),
      dagNodes: [
        { name: "root", dependsOn: [] },
        { name: "x", dependsOn: ["root"] },
        { name: "y", dependsOn: ["root"] },
        { name: "z", dependsOn: ["root"] },
      ],
    });
    expect(order.map((s) => s.name)).toEqual(["y", "z", "x", "root"]);
  });

  it("a dependent is rolled back before its dependency even when clocks disagree", () => {
    const steps = ["a", "b", "c"].map(step);
    const order = compensationOrder({
      steps,
      // b depends on a but was stamped earlier (clock skew between executors).
      state: stateWith({ a: { completedAt: 10 }, b: { completedAt: 3 }, c: { completedAt: 7 } }),
      dagNodes: [
        { name: "a", dependsOn: [] },
        { name: "b", dependsOn: ["a"] },
        { name: "c", dependsOn: [] },
      ],
    });
    expect(order.map((s) => s.name)).toEqual(["c", "b", "a"]);
  });

  it("skips steps that did not complete or have no compensate", () => {
    const steps = [step("a"), { name: "plain" }, step("failed")];
    const order = compensationOrder({
      steps,
      state: stateWith({
        a: { completedAt: 1 },
        plain: { completedAt: 2 },
        failed: { status: "failed" },
      }),
      dagNodes: [
        { name: "a", dependsOn: [] },
        { name: "plain", dependsOn: ["a"] },
        { name: "failed", dependsOn: ["plain"] },
      ],
    });
    expect(order.map((s) => s.name)).toEqual(["a"]);
  });
});
