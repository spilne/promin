// ---------------------------------------------------------------------------
// A run cancelled while no driver holds it (suspended, or its worker died)
// still gets its `compensate.onCancel: "rollback"`: recovery finds the
// cancelled run whose rollback is owed and finishes it, and a crash at any
// point of that rollback resumes the remaining compensations once.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { WorkflowCancelledError, WorkflowLockLostError } from "../../durable-pipeline-error.ts";
import type { Workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { isCancelledRun } from "../../workflow-state.ts";
import { workflow } from "../../workflow-builder.ts";
import { createWorkflowRunner, RecoveryStrategy } from "../../workflow-runner.ts";
import { InMemoryWorkflowVersionRegistry } from "../../workflow-version-registry.ts";

const LOCK = { heartbeatIntervalMs: 10, durationMs: 10_000 };

/** Loses the lock when the ledger entry of `crashOn` is written, like a worker dying then. */
class CrashingStorage extends InMemoryWorkflowStorage {
  crashOn: string | undefined;
  override async saveStepCompensation(
    params: Parameters<InMemoryWorkflowStorage["saveStepCompensation"]>[0],
  ): Promise<void> {
    if (params.stepName === this.crashOn) {
      throw new WorkflowLockLostError({ workflowId: params.workflowId, message: "worker died" });
    }
    return super.saveStepCompensation(params);
  }
}

function setup(params: { onCancel?: "rollback" | "none"; failB?: { times: number } }) {
  const clock = FakeWallClock.create(0);
  const storage = new CrashingStorage({ clock });
  const undone: string[] = [];
  const failB = { times: params.failB?.times ?? 0 };
  const wf = workflow<number>({
    name: "owed-rollback",
    version: "1",
    ...(params.onCancel !== undefined && { compensate: { onCancel: params.onCancel } }),
  })
    .step("a", () => succeed(1), {
      compensate: async () => {
        undone.push("a");
      },
    })
    .step("b", () => succeed(2), {
      compensate: async () => {
        if (failB.times > 0) {
          failB.times--;
          throw new Error("crash in b");
        }
        undone.push("b");
      },
    })
    .sleep("nap", 60_000)
    .step("c", () => succeed(3))
    .build();
  const registry = new InMemoryWorkflowVersionRegistry();
  registry.register(wf as unknown as Workflow<unknown, unknown>);
  const runner = () => createWorkflowRunner({ storage, clock, lock: LOCK, registry });
  const recover = async () => {
    clock.advance(60_000);
    const result = await runner().recover(RecoveryStrategy.builder().resumeRecent().build());
    await result.settled;
    return result;
  };
  return { clock, storage, wf, undone, runner, recover };
}

describe("a cancel while no driver holds the run", () => {
  it("rolls a run cancelled while suspended back through recovery, without a caller", async () => {
    const { storage, wf, undone, runner, recover } = setup({ onCancel: "rollback" });
    const first = await runner().runSafe({ workflow: wf, workflowId: "s-1", input: 0 });
    expect(first.error).not.toBeNull();
    await runner().handle("s-1").cancel();
    expect(undone).toEqual([]);

    const result = await recover();
    expect(result.resumed).toBe(1);
    expect(undone).toEqual(["b", "a"]);
    const final = (await storage.loadWorkflow("s-1"))!;
    expect(isCancelledRun(final)).toBe(true);
    expect(final.steps["a"]?.compensationStatus).toBe("compensated");
    expect(final.steps["b"]?.compensationStatus).toBe("compensated");

    // Settled: a second sweep leaves it alone and nothing runs twice.
    const again = await recover();
    expect(again.resumed).toBe(0);
    expect(undone).toEqual(["b", "a"]);
  });

  it("rolls back a run whose worker died, cancelled through storage", async () => {
    const { storage, wf, undone, runner, recover } = setup({ onCancel: "rollback" });
    await runner().runSafe({ workflow: wf, workflowId: "o-1", input: 0 });
    // The driver is gone; an operator cancels through storage directly.
    await storage.cancelWorkflow({ workflowId: "o-1" });

    await recover();
    expect(undone).toEqual(["b", "a"]);
    expect(isCancelledRun((await storage.loadWorkflowStatus("o-1"))!)).toBe(true);
  });

  it("resumes the remaining compensations once after a crash mid-rollback", async () => {
    const { storage, wf, undone, runner, recover } = setup({ onCancel: "rollback" });
    await runner().runSafe({ workflowId: "x-1", workflow: wf, input: 0 });
    await storage.cancelWorkflow({ workflowId: "x-1" });

    // The first sweep rolls `b` back, then its worker dies writing `a`'s ledger entry.
    storage.crashOn = "a";
    await recover();
    const midway = (await storage.loadWorkflow("x-1"))!;
    expect(midway.status).toBe("compensating");
    expect(midway.steps["b"]?.compensationStatus).toBe("compensated");

    storage.crashOn = undefined;
    await recover();
    // `b` is in the ledger: never undone again. `a` had no entry yet, so it ran again.
    expect(undone.filter((n) => n === "b")).toEqual(["b"]);
    const final = (await storage.loadWorkflow("x-1"))!;
    expect(isCancelledRun(final)).toBe(true);
    expect(final.steps["a"]?.compensationStatus).toBe("compensated");
    expect((await recover()).resumed).toBe(0);
  });

  it("ends cancelled with the steps whose rollback failed recorded", async () => {
    const { storage, wf, runner, recover } = setup({ onCancel: "rollback", failB: { times: 1 } });
    await runner().runSafe({ workflowId: "p-1", workflow: wf, input: 0 });
    await storage.cancelWorkflow({ workflowId: "p-1" });

    await recover();
    const final = (await storage.loadWorkflow("p-1"))!;
    expect(isCancelledRun(final)).toBe(true);
    expect(final.steps["a"]?.compensationStatus).toBe("compensated");
    expect(final.steps["b"]?.compensationStatus).toBe("compensation_failed");
    expect((await recover()).resumed).toBe(0);
  });

  it("settles a cancelled run without compensating when onCancel is not rollback", async () => {
    const { storage, wf, undone, runner, recover } = setup({});
    await runner().runSafe({ workflow: wf, workflowId: "n-1", input: 0 });
    await runner().handle("n-1").cancel();

    expect((await recover()).resumed).toBe(1);
    expect(undone).toEqual([]);
    expect(isCancelledRun((await storage.loadWorkflowStatus("n-1"))!)).toBe(true);
    expect((await recover()).resumed).toBe(0);
  });

  it("a driver resuming the cancelled run finishes its rollback and rejects cancelled", async () => {
    const { wf, undone, runner } = setup({ onCancel: "rollback" });
    await runner().runSafe({ workflow: wf, workflowId: "d-1", input: 0 });
    await runner().handle("d-1").cancel();

    await expect(
      runner().run({ workflow: wf, workflowId: "d-1", input: 0 }),
    ).rejects.toBeInstanceOf(WorkflowCancelledError);
    expect(undone).toEqual(["b", "a"]);
    await expect(
      runner().run({ workflow: wf, workflowId: "d-1", input: 0 }),
    ).rejects.toBeInstanceOf(WorkflowCancelledError);
    expect(undone).toEqual(["b", "a"]);
  });
});
