// ---------------------------------------------------------------------------
// Cancelling a run through storage while a step is in flight: the heartbeat
// sees it and aborts the step's signal, and `compensate.onCancel: "rollback"`
// rolls the completed steps back before the run ends cancelled.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import { WorkflowCancelledError, WorkflowLockLostError } from "../../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { isCancelledRun } from "../../workflow-state.ts";
import { workflow } from "../../workflow-builder.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";

const BEAT_MS = 10;

/** Resolves with the signal's reason once it aborts. */
function abortReason(signal: AbortSignal): Promise<unknown> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve(signal.reason);
    else signal.addEventListener("abort", () => resolve(signal.reason), { once: true });
  });
}

function setup(onCancel?: "rollback" | "none") {
  const storage = new InMemoryWorkflowStorage();
  const clock = FakeWallClock.create(0);
  const runner = createWorkflowRunner({
    storage,
    clock,
    lock: { heartbeatIntervalMs: BEAT_MS, durationMs: 10_000 },
  });
  const undone: string[] = [];
  const hold = {
    started: undefined as unknown as () => void,
    finish: undefined as unknown as () => void,
    reason: undefined as unknown,
  };
  const finished = new Promise<void>((resolve) => (hold.finish = resolve));
  const started = new Promise<void>((resolve) => (hold.started = resolve));
  const undo = (name: string) => async () => {
    undone.push(name);
  };
  const wf = workflow<number>({
    name: "cancel-mid-step",
    ...(onCancel !== undefined && { compensate: { onCancel } }),
  })
    .step("a", async () => 1, { compensate: undo("a") })
    .step("b", async () => 2, { compensate: undo("b") })
    .step(
      "slow",
      async ({ signal }) => {
        hold.started();
        const aborted = await Promise.race([abortReason(signal), finished.then(() => undefined)]);
        if (aborted !== undefined) {
          hold.reason = aborted;
          throw aborted;
        }
        return 3;
      },
      { compensate: undo("slow") },
    )
    .build();
  return { storage, clock, runner, wf, undone, hold, started };
}

describe("cancel observed mid-step", () => {
  it("aborts the in-flight step's signal with a cancelled reason and ends the run cancelled", async () => {
    const { storage, clock, runner, wf, hold, started } = setup();
    const run = runner.run({ workflow: wf, workflowId: "c-1", input: 0 });
    const outcome = run.then(
      () => undefined,
      (e: unknown) => e,
    );
    await started;
    await storage.cancelWorkflow({ workflowId: "c-1" });
    clock.advance(BEAT_MS);

    expect(await outcome).toBeInstanceOf(WorkflowCancelledError);
    expect(hold.reason).toBeInstanceOf(WorkflowCancelledError);
    expect(isCancelledRun((await storage.loadWorkflowStatus("c-1"))!)).toBe(true);
  });

  it("leaves a run nobody cancels alone across heartbeats", async () => {
    const { clock, runner, wf, hold, started } = setup();
    const run = runner.run({ workflow: wf, workflowId: "c-2", input: 0 });
    await started;
    clock.advance(BEAT_MS * 5);
    hold.finish();

    expect(await run).toBe(3);
    expect(hold.reason).toBeUndefined();
  });

  it("rolls completed steps back in reverse order, then ends cancelled, with onCancel: rollback", async () => {
    const { storage, clock, runner, wf, undone, started } = setup("rollback");
    const outcome = runner.run({ workflow: wf, workflowId: "c-3", input: 0 }).then(
      () => undefined,
      (e: unknown) => e,
    );
    await started;
    await storage.cancelWorkflow({ workflowId: "c-3" });
    clock.advance(BEAT_MS);

    expect(await outcome).toBeInstanceOf(WorkflowCancelledError);
    // `slow` never completed, so only `a` and `b` are undone, latest first.
    expect(undone).toEqual(["b", "a"]);
    const state = (await storage.loadWorkflow("c-3"))!;
    expect(isCancelledRun(state)).toBe(true);
    expect(state.steps["a"]?.compensationStatus).toBe("compensated");
    expect(state.steps["b"]?.compensationStatus).toBe("compensated");
  });

  it("compensates nothing on cancel without the option", async () => {
    const { storage, clock, runner, wf, undone, started } = setup();
    const outcome = runner.run({ workflow: wf, workflowId: "c-4", input: 0 }).then(
      () => undefined,
      (e: unknown) => e,
    );
    await started;
    await storage.cancelWorkflow({ workflowId: "c-4" });
    clock.advance(BEAT_MS);

    expect(await outcome).toBeInstanceOf(WorkflowCancelledError);
    expect(undone).toEqual([]);
  });

  it("does not compensate again once the lock is lost mid-rollback", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create(0);
    const runner = createWorkflowRunner({
      storage,
      clock,
      lock: { heartbeatIntervalMs: BEAT_MS, durationMs: 10_000 },
    });
    const caller = new AbortController();
    const undone: string[] = [];
    let release!: () => void;
    const started = new Promise<void>((resolve) => (release = resolve));
    const wf = workflow<number>({ name: "cancel-lock-race", compensate: { onCancel: "rollback" } })
      .step("a", async () => 1, {
        compensate: async () => {
          undone.push("a");
        },
      })
      .step("b", async () => 2, {
        compensate: async () => {
          undone.push("b");
          // The lock goes while the rollback is under way.
          caller.abort(new Error("lock gone"));
        },
      })
      .step("slow", async ({ signal }) => {
        release();
        await abortReason(signal);
        return 3;
      })
      .build();

    const outcome = runner
      .run({ workflow: wf, workflowId: "c-5", input: 0, signal: caller.signal })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    await started;
    await storage.cancelWorkflow({ workflowId: "c-5" });
    clock.advance(BEAT_MS);

    expect(await outcome).toBeInstanceOf(WorkflowLockLostError);
    expect(undone).toEqual(["b"]);
  });

  it("does not roll back when the lock was already lost when the cancel landed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create(0);
    const runner = createWorkflowRunner({
      storage,
      clock,
      lock: { heartbeatIntervalMs: BEAT_MS, durationMs: 10_000 },
    });
    const caller = new AbortController();
    const undone: string[] = [];
    let release!: () => void;
    const started = new Promise<void>((resolve) => (release = resolve));
    const wf = workflow<number>({ name: "lock-then-cancel", compensate: { onCancel: "rollback" } })
      .step("a", async () => 1, {
        compensate: async () => {
          undone.push("a");
        },
      })
      .step("slow", async ({ signal }) => {
        release();
        await abortReason(signal);
        return 3;
      })
      .build();

    const outcome = runner
      .run({ workflow: wf, workflowId: "c-6", input: 0, signal: caller.signal })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    await started;
    caller.abort(new Error("lock gone"));
    await storage.cancelWorkflow({ workflowId: "c-6" });
    clock.advance(BEAT_MS);

    expect(await outcome).toBeInstanceOf(WorkflowLockLostError);
    expect(undone).toEqual([]);
  });
});
