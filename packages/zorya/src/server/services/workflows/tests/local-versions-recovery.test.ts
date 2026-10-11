// ---------------------------------------------------------------------------
// LocalWorkflows: one (name, version) resolver for trigger, scanners and
// recovery, and periodic orphan recovery that never touches a live run.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { LocalWorkflows, RunCancelledError, UnknownWorkflowVersionError } from "../index.ts";

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxSteps?: number;
}): Promise<void> {
  const { clock, done, stepMs = 100, maxSteps = 500 } = params;
  for (let i = 0; i < maxSteps; i++) {
    await flush();
    if (await done()) return;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

const greet = (version: string) =>
  workflow<string>({ name: "greet", version })
    .sleep("nap", 1_000)
    .step("say", () => succeed(`v${version}`))
    .build();

describe("LocalWorkflows version resolution", () => {
  it("a promoted version's run is resumed after its sleep with that version's code", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const versionRegistry = new InMemoryWorkflowVersionRegistry({ clock });
    versionRegistry.register(greet("1"));
    versionRegistry.register(greet("2"));
    await versionRegistry.promote({ name: "greet", version: "2" });
    const errors: unknown[] = [];
    const local = new LocalWorkflows({
      storage,
      runner,
      clock,
      definitions: { greet: greet("1") },
      versionRegistry,
      sleepScanIntervalMs: 100,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
      onError: (e) => errors.push(e.error),
    });
    await local.start();
    const { workflowId } = await local.trigger("greet", "x");
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflowStatus(workflowId))?.status === "completed",
    });
    await local.stop();
    expect((await storage.loadWorkflow(workflowId))?.version).toBe("2");
    expect((await storage.loadWorkflow(workflowId))?.result).toBe("v2");
    expect(errors).toEqual([]);
  });

  it("an explicit version the layer can't run is rejected", async () => {
    const storage = new InMemoryWorkflowStorage();
    const local = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: { greet: greet("1") },
      orphanRecovery: false,
    });
    await expect(local.trigger("greet", "x", { version: "7" })).rejects.toBeInstanceOf(
      UnknownWorkflowVersionError,
    );
  });
});

describe("LocalWorkflows.resume", () => {
  it("re-drives a run from a step under its own run number; refuses while it is driven", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const ran: string[] = [];
    const abc = workflow<string>({ name: "abc" })
      .step("a", () => succeed(ran.push("a")))
      .step("b", () => succeed(ran.push("b")))
      .step("c", () => succeed(ran.push("c")))
      .build();
    const local = new LocalWorkflows({
      storage,
      runner,
      clock,
      definitions: { abc },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: false,
    });
    const { workflowId } = await local.trigger("abc", "x", { workflowId: "abc-1" });
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflowStatus(workflowId))?.status === "completed",
    });
    expect(ran).toEqual(["a", "b", "c"]);

    const held = await storage.tryLock({ workflowId, lockDurationMs: 1_000 });
    await expect(local.resume({ workflowId, fromStep: "b" })).rejects.toMatchObject({
      _tag: "WorkflowLockError",
    });
    await storage.releaseLock({ workflowId, guard: { fenceToken: held.token! } });
    await expect(local.resume({ workflowId, fromStep: "nope" })).rejects.toThrow(/no step "nope"/);

    await local.resume({ workflowId, fromStep: "b" });
    await driveUntil({
      clock,
      done: async () =>
        ran.length === 5 && (await storage.loadWorkflowStatus(workflowId))?.status === "completed",
    });
    expect(ran).toEqual(["a", "b", "c", "b", "c"]);
    expect((await storage.loadWorkflow(workflowId))?.run).toBe(1);
  });
});

describe("LocalWorkflows.resume of a cancelled run", () => {
  it("is refused: a cancelled run stays cancelled", async () => {
    const storage = new InMemoryWorkflowStorage();
    const quick = workflow<string>({ name: "quick" })
      .step("only", () => succeed("ok"))
      .build();
    const local = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: { quick },
      orphanRecovery: false,
    });
    await storage.createWorkflow({ workflowId: "c-1", workflowName: "quick", input: "x" });
    await storage.cancelWorkflow({ workflowId: "c-1" });
    await expect(local.resume({ workflowId: "c-1", fromStep: "only" })).rejects.toBeInstanceOf(
      RunCancelledError,
    );
    expect((await storage.loadWorkflow("c-1"))?.errorTag).toBe("WorkflowCancelledError");
  });
});

describe("LocalWorkflows orphan recovery", () => {
  it("periodically resumes a run whose driver died, once its lock expires", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const quick = workflow<string>({ name: "quick" })
      .step("only", () => succeed("ok"))
      .build();
    // The previous process created the run, took its lock and died.
    await storage.createWorkflow({ workflowId: "q-1", workflowName: "quick", input: "x" });
    await storage.tryLock({ workflowId: "q-1", lockDurationMs: 5_000 });

    const local = new LocalWorkflows({
      storage,
      runner,
      clock,
      definitions: { quick },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      orphanRecovery: { intervalMs: 500, graceMs: 1_000 },
    });
    await local.start();
    // Locked: left alone.
    for (let i = 0; i < 40; i++) {
      clock.advance(100);
      await flush();
    }
    expect((await storage.loadWorkflowStatus("q-1"))?.status).toBe("pending");
    // The lock expires: the next sweep resumes it.
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflowStatus("q-1"))?.status === "completed",
    });
    await local.stop();
  });
});
