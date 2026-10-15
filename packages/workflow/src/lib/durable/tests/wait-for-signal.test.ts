// ---------------------------------------------------------------------------
// `.waitForSignal()` — the timeout deadline is computed once and survives
// resumes, and signals behave as named last-wins values on the run (not a
// queue): one delivery satisfies every wait on that name, a re-delivery
// replaces the payload, and a fresh run starts with no signals.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00.000Z";

const tagOf = (e: unknown) => (e as { _tag?: string } | null | undefined)?._tag;

function setup() {
  const clock = FakeWallClock.create(T0);
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

describe(".waitForSignal() — timeout deadline across resumes", () => {
  it("keeps the first deadline when resumed before it, then times out at it", async () => {
    const { clock, storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-deadline" })
        .waitForSignal<string>("approval", { signalName: "approve", timeoutMs: 30 })
        .build();

    const first = await runner.runSafe({ workflow: build(), workflowId: "sd-1", input: 1 });
    expect(tagOf(first.error)).toBe("WorkflowSuspendedError");
    const deadline = "2026-01-01T00:00:00.030Z";
    const stored = async () =>
      (await storage.loadWorkflow("sd-1"))?.steps["approval"]?.signalTimeoutAt?.toISOString();
    expect(await stored()).toBe(deadline);

    // Resume every 15ms (like result() polling). Before the fix each resume
    // re-armed the deadline from "now", so the step never timed out.
    clock.advance(15);
    const early = await runner.runSafe({ workflow: build(), workflowId: "sd-1", input: 1 });
    expect(tagOf(early.error)).toBe("WorkflowSuspendedError");
    expect(await stored()).toBe(deadline);

    clock.advance(14);
    const almost = await runner.runSafe({ workflow: build(), workflowId: "sd-1", input: 1 });
    expect(tagOf(almost.error)).toBe("WorkflowSuspendedError");
    expect(await stored()).toBe(deadline);

    clock.advance(1);
    const late = await runner.runSafe({ workflow: build(), workflowId: "sd-1", input: 1 });
    expect(tagOf(late.error)).toBe("WorkflowTimeoutError");
  });

  it("a signal delivered before the deadline completes the step", async () => {
    const { clock, storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-in-time" })
        .waitForSignal<string>("approval", { signalName: "approve", timeoutMs: 30 })
        .step("after", ({ prev }) => succeed(`approved by ${prev}`))
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "sit-1", input: 1 });
    clock.advance(15);
    await runner.runSafe({ workflow: build(), workflowId: "sit-1", input: 1 });
    clock.advance(10);
    await storage.deliverSignal("sit-1", "approve", "alice");
    const r = await runner.runSafe({ workflow: build(), workflowId: "sit-1", input: 1 });
    expect(r.error).toBeNull();
    expect(r.data).toBe("approved by alice");
  });

  it("a wait without timeoutMs stays suspended across resumes", async () => {
    const { clock, storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-no-timeout" })
        .waitForSignal<string>("approval", { signalName: "approve" })
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "snt-1", input: 1 });
    clock.advance(86_400_000);
    const r = await runner.runSafe({ workflow: build(), workflowId: "snt-1", input: 1 });
    expect(tagOf(r.error)).toBe("WorkflowSuspendedError");
    const state = await storage.loadWorkflow("snt-1");
    expect(state?.steps["approval"]?.status).toBe("waiting_for_signal");
    expect(state?.steps["approval"]?.signalTimeoutAt).toBeUndefined();
  });
});

describe(".waitForSignal() — repeated signal names", () => {
  it("one delivery satisfies every wait on the same signal name", async () => {
    const { storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-shared" })
        .waitForSignal<string>("first", { signalName: "approve" })
        .waitForSignal<string>("second", { signalName: "approve" })
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "ss-1", input: 1 });
    await storage.deliverSignal("ss-1", "approve", "manager");
    const r = await runner.runSafe({ workflow: build(), workflowId: "ss-1", input: 1 });

    expect(r.error).toBeNull();
    expect(r.data).toBe("manager");
    const state = await storage.loadWorkflow("ss-1");
    expect(state?.steps["first"]?.result).toBe("manager");
    expect(state?.steps["second"]?.result).toBe("manager");
  });

  it("a re-delivery replaces the payload seen by later waits (last wins)", async () => {
    const { storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-last-wins" })
        .waitForSignal<string>("first", { signalName: "approve" })
        .waitForSignal<string>("gate", { signalName: "continue" })
        .waitForSignal<string>("second", { signalName: "approve" })
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "slw-1", input: 1 });
    await storage.deliverSignal("slw-1", "approve", "v1");
    const mid = await runner.runSafe({ workflow: build(), workflowId: "slw-1", input: 1 });
    expect(tagOf(mid.error)).toBe("WorkflowSuspendedError"); // parked on "gate"

    await storage.deliverSignal("slw-1", "approve", "v2");
    await storage.deliverSignal("slw-1", "continue", "go");
    const r = await runner.runSafe({ workflow: build(), workflowId: "slw-1", input: 1 });

    expect(r.error).toBeNull();
    const state = await storage.loadWorkflow("slw-1");
    expect(state?.steps["first"]?.result).toBe("v1"); // checkpointed before re-delivery
    expect(state?.steps["second"]?.result).toBe("v2");
    const signals = await storage.loadSignals("slw-1");
    expect(signals.filter((s) => s.signalName === "approve")).toHaveLength(1);
  });

  it("distinct signal names wait for distinct deliveries", async () => {
    const { storage, runner } = setup();
    const build = () =>
      workflow<number>({ name: "sig-distinct" })
        .waitForSignal<string>("first", { signalName: "approve-1" })
        .waitForSignal<string>("second", { signalName: "approve-2" })
        .build();

    await runner.runSafe({ workflow: build(), workflowId: "sdn-1", input: 1 });
    await storage.deliverSignal("sdn-1", "approve-1", "manager");
    const mid = await runner.runSafe({ workflow: build(), workflowId: "sdn-1", input: 1 });
    expect(tagOf(mid.error)).toBe("WorkflowSuspendedError");

    await storage.deliverSignal("sdn-1", "approve-2", "director");
    const r = await runner.runSafe({ workflow: build(), workflowId: "sdn-1", input: 1 });
    expect(r.data).toBe("director");
  });
});
