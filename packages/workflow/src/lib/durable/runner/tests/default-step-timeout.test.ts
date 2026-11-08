import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../../../shared/wall-clock.ts";
import {
  StepTimeoutError,
  WorkflowLockLostError,
  WorkflowMaxHoldExceededError,
} from "../../durable-pipeline-error.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { withLock } from "../../with-lock.ts";
import { workflow } from "../../workflow-builder.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";

const never = () => new Promise<number>(() => {});

const nextMacrotask = () => new Promise<void>((r) => setImmediate(r));
async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !cond(); i++) await nextMacrotask();
  if (!cond()) throw new Error("waitFor: condition never held");
}

describe("defaultStepTimeoutMs", () => {
  it("fails a never-settling step with StepTimeoutError under the runner default", async () => {
    const runner = createWorkflowRunner({
      storage: new InMemoryWorkflowStorage(),
      defaultStepTimeoutMs: 20,
    });
    const wf = workflow<number>({ name: "dt-runner" }).step("hung", never).build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "r-1", input: 0 });
    expect(r.error).toBeInstanceOf(StepTimeoutError);
    expect((r.error as StepTimeoutError).timeoutMs).toBe(20);
    expect((r.error as Error).message).toMatch(/hung.*timed out after 20ms/);
  });

  it("applies the workflow default, which beats the runner's", async () => {
    const runner = createWorkflowRunner({
      storage: new InMemoryWorkflowStorage(),
      defaultStepTimeoutMs: 60_000,
    });
    const wf = workflow<number>({ name: "dt-wf", defaultStepTimeoutMs: 15 })
      .step("hung", never)
      .build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "w-1", input: 0 });
    expect((r.error as StepTimeoutError).timeoutMs).toBe(15);
  });

  it("lets a step's own timeoutMs override the default", async () => {
    const runner = createWorkflowRunner({
      storage: new InMemoryWorkflowStorage(),
      defaultStepTimeoutMs: 60_000,
    });
    const wf = workflow<number>({ name: "dt-step" }).step("hung", never, { timeoutMs: 10 }).build();
    const r = await runner.runSafe({ workflow: wf, workflowId: "s-1", input: 0 });
    expect((r.error as StepTimeoutError).timeoutMs).toBe(10);
  });

  it("lets a step opt out with timeoutMs: null, and a workflow opt out with null", async () => {
    const runner = createWorkflowRunner({
      storage: new InMemoryWorkflowStorage(),
      defaultStepTimeoutMs: 10,
    });
    const slow = async () => {
      await new Promise((r) => setTimeout(r, 60));
      return 7;
    };
    const stepOptOut = workflow<number>({ name: "dt-optout-step" })
      .step("slow", slow, { timeoutMs: null })
      .build();
    expect(await runner.run({ workflow: stepOptOut, workflowId: "o-1", input: 0 })).toBe(7);

    const wfOptOut = workflow<number>({ name: "dt-optout-wf", defaultStepTimeoutMs: null })
      .step("slow", slow)
      .build();
    expect(await runner.run({ workflow: wfOptOut, workflowId: "o-2", input: 0 })).toBe(7);
  });

  it("is off unless configured", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<number>({ name: "dt-off" })
      .step("slow", async () => {
        await new Promise((r) => setTimeout(r, 40));
        return 3;
      })
      .build();
    expect(await runner.run({ workflow: wf, workflowId: "off-1", input: 0 })).toBe(3);
  });
});

describe("lock.maxHoldMs", () => {
  it("stops extending the lease and aborts the signal with a typed cause", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    let heartbeats = 0;
    const original = storage.heartbeat.bind(storage);
    storage.heartbeat = async (p) => {
      heartbeats++;
      return original(p);
    };
    let signal: AbortSignal | undefined;
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const run = withLock({
      storage,
      workflowId: "mh-1",
      options: { clock, heartbeatIntervalMs: 10, lockDurationMs: 30, maxHoldMs: 25 },
      fn: async (ctx) => {
        signal = ctx.signal;
        await gate;
        return "done";
      },
    });
    await waitFor(() => signal !== undefined);
    clock.advance(10);
    clock.advance(10);
    await waitFor(() => heartbeats === 2);
    expect(signal!.aborted).toBe(false);

    clock.advance(10); // 30ms held >= 25
    await waitFor(() => signal!.aborted);
    expect(signal!.reason).toBeInstanceOf(WorkflowLockLostError);
    expect((signal!.reason as WorkflowLockLostError).cause).toBeInstanceOf(
      WorkflowMaxHoldExceededError,
    );
    expect((signal!.reason as Error).message).toMatch(/maxHoldMs/);
    expect(heartbeats).toBe(2);

    // The lease runs out unextended, so another driver can take the run.
    clock.advance(100);
    const taken = await storage.tryLock({ workflowId: "mh-1", lockDurationMs: 1_000 });
    expect(taken.acquired).toBe(true);
    release();
    await run;
  });

  it("surfaces the max-hold error as the run's error through the runner", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({
      storage,
      clock,
      lock: { durationMs: 30, heartbeatIntervalMs: 10, maxHoldMs: 25 },
    });
    let started = false;
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const wf = workflow<number>({ name: "mh-run" })
      .step("hung", async ({ signal }) => {
        started = true;
        await gate;
        return signal.aborted ? -1 : 1;
      })
      .step("after", async ({ prev }) => prev)
      .build();
    const run = runner.runSafe({ workflow: wf, workflowId: "mh-2", input: 0 });
    await waitFor(() => started);
    for (let i = 0; i < 3; i++) clock.advance(10);
    release();
    const r = await run;
    expect(r.error).toBeInstanceOf(WorkflowLockLostError);
    expect((r.error as WorkflowLockLostError).cause).toBeInstanceOf(WorkflowMaxHoldExceededError);
    expect((await storage.loadWorkflow("mh-2"))!.status).toBe("running");
  });
});
