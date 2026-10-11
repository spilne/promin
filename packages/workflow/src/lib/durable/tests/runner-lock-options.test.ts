// ---------------------------------------------------------------------------
// Runner lock lease (`WorkflowRunnerConfig.lock`) and abandoning a run
// (`run({ signal })`): an abandoned run stops at its next safe point like a
// lost lock, releases the lock and stays in flight for another driver.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, InMemoryWorkflowStorage, RecoveryStrategy } from "../../../index.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

describe("WorkflowRunnerConfig.lock", () => {
  it("takes the run lock for `durationMs`, so a dead driver's run is free that much sooner", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({
      storage,
      clock,
      lock: { durationMs: 5_000, heartbeatIntervalMs: 1_000 },
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const wf = workflow<string>({ name: "held" })
      .step("wait", () => gate.then(() => "ok"))
      .build();
    const done = runner.run({ workflow: wf, workflowId: "h-1", input: "x" });
    for (let i = 0; i < 50 && clock.pendingCount() === 0; i++) await flush();

    // While the driver heartbeats, the lock stays held past the lease.
    for (let i = 0; i < 8; i++) {
      clock.advance(1_000);
      await flush();
    }
    expect((await storage.tryLock({ workflowId: "h-1", lockDurationMs: 1_000 })).acquired).toBe(
      false,
    );
    release();
    expect(await done).toBe("ok");
  });
});

describe("run({ signal }) abandons the run", () => {
  it("stops at the next wave with WorkflowLockLostError, releases the lock, leaves the run running", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const abort = new AbortController();
    const ran: string[] = [];
    const wf = workflow<string>({ name: "abandoned" })
      .step("first", () => {
        ran.push("first");
        abort.abort(new Error("claim lost"));
        return succeed(1);
      })
      .step("second", () => {
        ran.push("second");
        return succeed(2);
      })
      .build();

    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "a-1",
      input: "x",
      signal: abort.signal,
    });
    expect((error as { _tag?: string })._tag).toBe("WorkflowLockLostError");
    expect(ran).toEqual(["first"]);
    const state = await storage.loadWorkflow("a-1");
    expect(state?.status).toBe("running");
    expect(state?.steps.first?.status).toBe("completed");
    // The lock was released: another driver picks the run up at once.
    expect(await runner.run({ workflow: wf, workflowId: "a-1", input: "x" })).toBe(2);
    expect(ran).toEqual(["first", "second"]);
  });

  it("an already-aborted signal never runs a step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let ran = 0;
    const wf = workflow<string>({ name: "never" })
      .step("only", () => {
        ran += 1;
        return succeed(1);
      })
      .build();
    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "a-2",
      input: "x",
      signal: AbortSignal.abort(new Error("stopping")),
    });
    expect((error as { _tag?: string })._tag).toBe("WorkflowLockLostError");
    expect(ran).toBe(0);
  });
});

describe("stale termination respects live locks", () => {
  it("failStale skips a run another driver holds the lock of", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    await storage.createWorkflow({ workflowId: "live", workflowName: "w", input: null });
    await storage.createWorkflow({ workflowId: "dead", workflowName: "w", input: null });
    clock.advance(10_000);
    const held = await storage.tryLock({ workflowId: "live", lockDurationMs: 60_000 });
    expect(held.acquired).toBe(true);

    const result = await runner.recover(
      RecoveryStrategy.builder().failStale({ olderThanMs: 5_000, error: "stale" }).build(),
    );
    expect(result.terminated).toBe(1);
    expect((await storage.loadWorkflowStatus("dead"))?.status).toBe("failed");
    expect((await storage.loadWorkflowStatus("live"))?.status).toBe("pending");
  });
});
