// ---------------------------------------------------------------------------
// waitingOn — what a live run is parked on, derived from its stored step
// rows and reported by `runner.getStatus` / `handle.status()`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { deriveWaitingOn } from "../workflow-waits.ts";
import type { WorkflowState } from "../workflow-state.ts";

function setup() {
  const storage = new InMemoryWorkflowStorage();
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const runner = createWorkflowRunner({ storage, clock });
  return { storage, clock, runner };
}

describe("waitingOn", () => {
  it("reports a sleep with its wake time", async () => {
    const { runner } = setup();
    const wf = workflow<unknown>({ name: "sleeper" })
      .journaled("nap", function* (ctx) {
        yield* ctx.sleep(2 * 3_600_000);
        return 1;
      })
      .build();
    await runner.run({ workflow: wf, workflowId: "s1", input: {} }).catch(() => undefined);
    const status = await runner.getStatus({ workflowId: "s1" });
    expect(status?.state).toBe("suspended");
    expect(status?.waitingOn).toHaveLength(1);
    expect(status?.waitingOn[0]).toMatchObject({ kind: "sleep", stepName: "nap" });
    expect(status?.waitingOn[0]?.until?.toISOString()).toBe("2026-01-01T02:00:00.000Z");
    expect(status?.waitingOnSummary).toBe("sleeping (step nap, wakes in 2h)");
  });

  it("reports a signal wait with its name and timeout, and an approval separately", async () => {
    const { runner } = setup();
    const wf = workflow<unknown>({ name: "signaller" })
      .journaled("gate", function* (ctx) {
        yield* ctx.signal("manager-approved", { timeout: 2 * 3_600_000 });
        return 1;
      })
      .build();
    await runner.run({ workflow: wf, workflowId: "g1", input: {} }).catch(() => undefined);
    const status = await runner.getStatus({ workflowId: "g1" });
    expect(status?.waitingOn).toHaveLength(1);
    expect(status?.waitingOn[0]).toMatchObject({
      kind: "signal",
      stepName: "gate",
      signalName: "manager-approved",
    });
    expect(status?.waitingOn[0]?.until?.toISOString()).toBe("2026-01-01T02:00:00.000Z");
    expect(status?.waitingOnSummary).toBe(
      "waiting for signal manager-approved (step gate, times out in 2h)",
    );

    const approving = workflow<unknown>({ name: "approver" })
      .journaled("review", function* (ctx) {
        yield* ctx.approval("deploy");
        return 1;
      })
      .build();
    await runner.run({ workflow: approving, workflowId: "a1", input: {} }).catch(() => undefined);
    const approval = await runner.getStatus({ workflowId: "a1" });
    expect(approval?.waitingOn).toEqual([
      expect.objectContaining({
        kind: "approval",
        stepName: "review",
        signalName: "approve:deploy",
      }),
    ]);
    // No timeout: no `until`.
    expect(approval?.waitingOn[0]?.until).toBeUndefined();
  });

  it("reports a parent parked on a suspended child, and clears once the run ends", async () => {
    const { runner, clock } = setup();
    const napper = workflow<unknown>({ name: "napper" })
      .journaled("nap", function* (ctx) {
        yield* ctx.sleep(60_000);
        return 1;
      })
      .build();
    const parent = workflow<unknown>({ name: "napper-parent" })
      .journaled("run", function* (ctx) {
        return yield* ctx.child(napper, { workflowId: "kid-1" });
      })
      .build();
    await runner.run({ workflow: parent, workflowId: "p1", input: {} }).catch(() => undefined);
    const status = await runner.getStatus({ workflowId: "p1" });
    expect(status?.waitingOn).toEqual([
      expect.objectContaining({ kind: "child", stepName: "run", childWorkflowId: "kid-1" }),
    ]);
    expect(status?.waitingOn[0]?.until?.toISOString()).toBe("2026-01-01T00:01:00.000Z");
    // The child itself reports its sleep.
    expect((await runner.getStatus({ workflowId: "kid-1" }))?.waitingOn[0]?.kind).toBe("sleep");

    clock.advance(60_000);
    await runner.run({ workflow: parent, workflowId: "p1", input: {} });
    const done = await runner.getStatus({ workflowId: "p1" });
    expect(done?.state).toBe("completed");
    expect(done?.waitingOn).toEqual([]);
    expect(done?.waitingOnSummary).toBeUndefined();
  });

  it("handle.status() carries waitingOn", async () => {
    const { runner } = setup();
    const wf = workflow<unknown>({ name: "handled" })
      .journaled("gate", function* (ctx) {
        yield* ctx.signal("go");
        return 1;
      })
      .build();
    await runner.run({ workflow: wf, workflowId: "h1", input: {} }).catch(() => undefined);
    const status = await runner.handle("h1").status();
    expect(status?.waitingOn.map((w) => w.signalName)).toEqual(["go"]);
  });

  it("is empty for a running run with nothing parked and for ended runs", () => {
    const base: WorkflowState = {
      workflowId: "w",
      workflowName: "w",
      status: "running",
      run: 1,
      input: {},
      steps: {
        busy: {
          stepName: "busy",
          run: 1,
          status: "running",
          dependsOn: [],
          stepType: "single",
          attempt: 1,
        },
      },
      createdAt: new Date(0),
      updatedAt: new Date(0),
    };
    expect(deriveWaitingOn(base)).toEqual([]);

    const sleepingStep = {
      ...base.steps.busy!,
      stepName: "nap",
      status: "sleeping" as const,
      wakeAt: new Date(1000),
    };
    const parked = { ...base, steps: { nap: sleepingStep } };
    expect(deriveWaitingOn({ ...parked, status: "suspended" })).toHaveLength(1);
    // Negative control: stale parked rows of an ended run are not waits.
    for (const status of ["completed", "failed", "tripwire", "compensating"] as const) {
      expect(deriveWaitingOn({ ...parked, status })).toEqual([]);
    }
  });
});
