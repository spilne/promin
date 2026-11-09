// ---------------------------------------------------------------------------
// waitingOn `activity` / `retry-backoff` — what a live run's step is doing
// between its settled writes, recorded through `saveStepProgress` and read
// back by `runner.getStatus`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { fail, succeed, TaggedError } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { InProcessStepExecutor } from "../runner/step-executor.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { deriveWaitingOn, mergeWaits } from "../workflow-waits.ts";
import type { StoredStepProgress, WorkflowState } from "../workflow-state.ts";

class Flaky extends TaggedError("Flaky")<{ readonly message: string }>() {}

/** A promise settled from outside, to hold a step body open. */
function gate() {
  let open!: () => void;
  const opened = new Promise<void>((r) => (open = r));
  return { opened, open };
}

/** Let queued microtasks and the in-memory storage's async writes run. */
const settle = () => new Promise<void>((r) => setTimeout(r, 0));

function setup() {
  const storage = new InMemoryWorkflowStorage();
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const runner = createWorkflowRunner({ storage, clock, executorId: "exec-1" });
  return { storage, clock, runner };
}

describe("waitingOn: activity and retry-backoff", () => {
  it("lists a running step body with its attempt, start and holder, and clears when it settles", async () => {
    const { runner, clock } = setup();
    const hold = gate();
    const wf = workflow<unknown>({ name: "busy" })
      .step("work", async () => {
        await hold.opened;
        return 1;
      })
      .build();
    const run = runner.run({ workflow: wf, workflowId: "busy-1", input: {} });
    await settle();
    // A step that has run for a moment shows up; before that it costs no write.
    clock.advance(5_000);
    await settle();

    const status = await runner.getStatus({ workflowId: "busy-1" });
    expect(status?.waitingOn).toHaveLength(1);
    expect(status?.waitingOn[0]).toMatchObject({
      kind: "activity",
      stepName: "work",
      attempt: 1,
      holder: "exec-1",
    });
    expect(status?.waitingOn[0]?.since.toISOString()).toBe("2026-01-01T00:00:00.000Z");
    expect(status?.waitingOnSummary).toBe("running step work (attempt 1, for 5s, on exec-1)");

    hold.open();
    await run;
    const done = await runner.getStatus({ workflowId: "busy-1" });
    expect(done?.state).toBe("completed");
    expect(done?.waitingOn).toEqual([]);
  });

  it("lists a failed step as retry-backoff until its next attempt, then as activity again", async () => {
    const { runner, clock } = setup();
    const hold = gate();
    let calls = 0;
    const wf = workflow<unknown>({ name: "flaky" })
      .step(
        "charge",
        () => {
          calls++;
          return calls === 1 ? fail(new Flaky({ message: "once" })) : succeed("ok");
        },
        { retry: { maxRetries: 2, baseDelayMs: 4_000 } },
      )
      .step("after", async () => {
        await hold.opened;
        return 1;
      })
      .build();
    const run = runner.run({ workflow: wf, workflowId: "flaky-1", input: {} });
    await settle();

    const backoff = await runner.getStatus({ workflowId: "flaky-1" });
    expect(backoff?.waitingOn).toHaveLength(1);
    expect(backoff?.waitingOn[0]).toMatchObject({
      kind: "retry-backoff",
      stepName: "charge",
      attempt: 1,
    });
    expect(backoff?.waitingOn[0]?.until?.toISOString()).toBe("2026-01-01T00:00:04.000Z");
    expect(backoff?.waitingOnSummary).toBe(
      "retrying step charge after failed attempt 1 (next attempt in 4s)",
    );

    clock.advance(4_000);
    await settle();
    clock.advance(1_000);
    await settle();
    // The retry succeeded and the next step is running: nothing stale about `charge`.
    const next = await runner.getStatus({ workflowId: "flaky-1" });
    expect(next?.waitingOn.map((w) => [w.kind, w.stepName])).toEqual([["activity", "after"]]);

    hold.open();
    await run;
    expect(calls).toBe(2);
    expect((await runner.getStatus({ workflowId: "flaky-1" }))?.waitingOn).toEqual([]);
  });

  it("an executor reports the same through the request's progress callback", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const hold = gate();
    let calls = 0;
    const wf = workflow<unknown>({ name: "executed" })
      .step(
        "charge",
        () => {
          calls++;
          return calls === 1 ? fail(new Flaky({ message: "once" })) : succeed("ok");
        },
        { retry: { maxRetries: 2, baseDelayMs: 4_000 } },
      )
      .step("after", async () => {
        await hold.opened;
        return 1;
      })
      .build();
    const runner = createWorkflowRunner({
      storage,
      clock,
      executorId: "exec-1",
      stepExecutor: new InProcessStepExecutor({ workflow: wf, storage, clock }),
    });
    const run = runner.run({ workflow: wf, workflowId: "exe-1", input: {} });
    await settle();
    expect((await runner.getStatus({ workflowId: "exe-1" }))?.waitingOn).toEqual([
      expect.objectContaining({ kind: "retry-backoff", stepName: "charge", attempt: 1 }),
    ]);
    clock.advance(4_000);
    await settle();
    clock.advance(1_000);
    await settle();
    expect((await runner.getStatus({ workflowId: "exe-1" }))?.waitingOn).toEqual([
      expect.objectContaining({ kind: "activity", stepName: "after", holder: "exec-1" }),
    ]);
    hold.open();
    await run;
  });

  it("a step that settles before the delay writes no progress at all", async () => {
    const { storage, runner } = setup();
    let writes = 0;
    const save = storage.saveStepProgress.bind(storage);
    storage.saveStepProgress = (params) => {
      writes++;
      return save(params);
    };
    const wf = workflow<unknown>({ name: "quick" })
      .step("a", () => succeed(1))
      .step("b", () => succeed(2))
      .build();
    await runner.run({ workflow: wf, workflowId: "quick-1", input: {} });
    expect(writes).toBe(0);
  });

  it("derive: only progress of this run that still describes the step counts", () => {
    const at = (s: number) => new Date(s * 1000);
    const step = (
      status: "completed" | "failed" | "sleeping" | "pending",
      extra: { completedAt?: Date; startedAt?: Date; wakeAt?: Date } = {},
    ) => ({
      stepName: "s",
      run: 1,
      status,
      dependsOn: [],
      stepType: "single" as const,
      attempt: 1,
      ...extra,
    });
    const progress = (extra: Partial<StoredStepProgress> = {}): StoredStepProgress => ({
      phase: "running",
      attempt: 1,
      at: at(10),
      run: 1,
      ...extra,
    });
    const state = (
      overrides: Partial<WorkflowState> & { row?: ReturnType<typeof step> },
    ): WorkflowState => ({
      workflowId: "w",
      workflowName: "w",
      status: "running",
      run: 1,
      input: {},
      steps: overrides.row !== undefined ? { s: overrides.row } : {},
      stepProgress: { s: progress() },
      createdAt: at(0),
      updatedAt: at(0),
      ...overrides,
    });
    const kinds = (s: WorkflowState) => deriveWaitingOn(s).map((w) => w.kind);

    expect(kinds(state({}))).toEqual(["activity"]);
    expect(kinds(state({ row: step("pending") }))).toEqual(["activity"]);
    // Settled step, earlier run, parked run, ended run: stale.
    expect(kinds(state({ row: step("completed", { completedAt: at(11) }) }))).toEqual([]);
    expect(kinds(state({ stepProgress: { s: progress({ run: 0 }) } }))).toEqual([]);
    expect(kinds(state({ status: "suspended" }))).toEqual([]);
    expect(kinds(state({ status: "failed" }))).toEqual([]);
    // A failed step runs again only when the record is newer than the failure.
    expect(kinds(state({ row: step("failed", { completedAt: at(11) }) }))).toEqual([]);
    expect(kinds(state({ row: step("failed", { completedAt: at(9) }) }))).toEqual(["activity"]);
    // A parked step re-executing after its wake shows as activity; the park wins otherwise.
    const sleeping = (startedAt: number) =>
      step("sleeping", { startedAt: at(startedAt), wakeAt: at(20) });
    expect(kinds(state({ row: sleeping(9) }))).toEqual(["activity"]);
    expect(kinds(state({ row: sleeping(10) }))).toEqual(["sleep"]);
    expect(
      kinds(
        state({
          row: step("pending"),
          stepProgress: { s: progress({ phase: "backoff", nextAttemptAt: at(15) }) },
        }),
      ),
    ).toEqual(["retry-backoff"]);
  });

  it("a queued step replaces its older activity / retry-backoff entry, a parked one keeps it out", () => {
    const base = { stepName: "s", since: new Date(0) };
    const backoff = { kind: "retry-backoff" as const, ...base };
    const queued = {
      kind: "step-queue" as const,
      ...base,
      groupName: "g",
      reason: "rate" as const,
    };
    expect(mergeWaits({ stored: [backoff], extra: [queued] })).toEqual([queued]);
    const sleep = { kind: "sleep" as const, ...base };
    expect(mergeWaits({ stored: [sleep], extra: [queued] })).toEqual([sleep]);
  });
});
