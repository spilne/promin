// ---------------------------------------------------------------------------
// LocalWorkflows tests — trigger, rerun, recovery wiring.
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import { describe, it, expect } from "bun:test";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  workflow,
  createWorkflowRunner,
  RecoveryStrategy,
  type WorkflowStorage,
} from "@promin/workflow";
import { LocalWorkflows } from "../index.ts";

const makeWorkflow = () =>
  workflow<{ n: number }>({ name: "double" })
    .step("doIt", ({ input }) => succeed(input.n * 2))
    .build();

describe("LocalWorkflows.trigger", () => {
  it("dispatches an in-process workflow and returns workflowId", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = makeWorkflow();

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: wf },
      sleepScanIntervalMs: 0, // disable scanner for test
    });

    const r = await workflows.trigger("double", { n: 5 });
    expect(r.workflowId).toBeDefined();

    // Wait for completion (fire-and-forget runner)
    await waitFor(async () => (await storage.loadWorkflow(r.workflowId))?.status === "completed");
    const state = await storage.loadWorkflow(r.workflowId);
    expect(state?.status).toBe("completed");
    expect(state?.result).toBe(10);
  });

  it("uses provided workflowId when given", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
    });

    const r = await workflows.trigger("double", { n: 3 }, { workflowId: "fixed-id" });
    expect(r.workflowId).toBe("fixed-id");
  });

  it("pre-creates row when namespace/metadata/runSource present", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
    });

    await workflows.trigger(
      "double",
      { n: 7 },
      {
        workflowId: "with-meta",
        namespace: "tenant-a",
        metadata: { source: "test" },
        runSource: "manual",
      },
    );

    const state = await storage.loadWorkflow("with-meta");
    expect(state?.namespace).toBe("tenant-a");
    expect(state?.metadata?.source).toBe("test");
    expect(state?.runSource).toBe("manual");
  });

  it("throws UnknownWorkflowError for missing definition (no fallback)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
    });

    await expect(workflows.trigger("nope", {})).rejects.toThrow(/Unknown workflow/);
  });
});

describe("LocalWorkflows.define admission exemptions", () => {
  it("accepts only internal exemptions and an invalid redefine cannot make one public", async () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const internal = workflow<void>({ name: "owned-internal" })
      .step("body", () => succeed(undefined))
      .build();
    const replacement = workflow<void>({ name: "owned-internal" })
      .step("replacement", () => succeed(undefined))
      .build();
    const publicWorkflow = workflow<void>({ name: "public-exempt" })
      .step("body", () => succeed(undefined))
      .build();

    workflows.define(internal, { internal: true, admissionExempt: true });
    expect(workflows.isInternal(internal.name)).toBe(true);
    expect(() =>
      (
        workflows.define as (
          workflow: typeof internal,
          options: { admissionExempt: boolean },
        ) => void
      )(replacement, { admissionExempt: true }),
    ).toThrow("admissionExempt requires internal: true");
    expect(workflows.definitions?.[internal.name]).toBe(internal);
    expect(workflows.isInternal(internal.name)).toBe(true);
    await expect(workflows.trigger(internal.name, undefined)).rejects.toThrow("Unknown workflow");

    expect(() =>
      (
        workflows.define as (
          workflow: typeof publicWorkflow,
          options: { admissionExempt: boolean },
        ) => void
      )(publicWorkflow, { admissionExempt: true }),
    ).toThrow("admissionExempt requires internal: true");
    expect(workflows.definitions?.[publicWorkflow.name]).toBeUndefined();
  });

  it("snapshots stateful option getters exactly once", () => {
    const storage = new InMemoryWorkflowStorage();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const internal = workflow<void>({ name: "stateful-options" })
      .step("body", () => succeed(undefined))
      .build();
    let internalReads = 0;
    let exemptionReads = 0;
    const stateful = Object.defineProperties(
      {},
      {
        internal: {
          get: () => ++internalReads === 1,
        },
        admissionExempt: {
          get: () => ++exemptionReads === 1,
        },
      },
    );

    (
      workflows.define as (
        workflow: typeof internal,
        options: { internal: boolean; admissionExempt: boolean },
      ) => void
    )(internal, stateful as { internal: boolean; admissionExempt: boolean });

    expect({ internalReads, exemptionReads }).toEqual({ internalReads: 1, exemptionReads: 1 });
    expect(workflows.definitions?.[internal.name]).toBe(internal);
    expect(workflows.isInternal(internal.name)).toBe(true);
  });

  it("leaves the prior definition and visibility unchanged when a getter or hook throws", () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const prior = workflow<void>({ name: "atomic-internal" })
      .step("prior", () => succeed(undefined))
      .build();
    const replacement = workflow<void>({ name: "atomic-internal" })
      .step("replacement", () => succeed(undefined))
      .build();
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    workflows.define(prior, { internal: true });

    const throwingOptions = Object.defineProperties(
      {},
      {
        internal: { get: () => true },
        admissionExempt: {
          get: () => {
            throw new Error("getter failed");
          },
        },
      },
    );
    expect(() =>
      (
        workflows.define as (
          workflow: typeof replacement,
          options: { internal: boolean; admissionExempt: boolean },
        ) => void
      )(replacement, throwingOptions as { internal: boolean; admissionExempt: boolean }),
    ).toThrow("getter failed");
    expect(workflows.definitions?.[prior.name]).toBe(prior);
    expect(workflows.isInternal(prior.name)).toBe(true);

    Object.defineProperty(runner, "exemptInternalWorkflowFromRunAdmission", {
      configurable: true,
      value: () => {
        throw new Error("hook failed");
      },
    });
    expect(() => workflows.define(replacement, { internal: true, admissionExempt: true })).toThrow(
      "hook failed",
    );
    expect(workflows.definitions?.[prior.name]).toBe(prior);
    expect(workflows.isInternal(prior.name)).toBe(true);
  });
});

describe("LocalWorkflows.trigger — existing workflowId", () => {
  const settle = async () => {
    for (let i = 0; i < 50; i++) await new Promise((r) => setTimeout(r, 0));
  };

  for (const runSource of ["schedule", "manual", undefined] as const) {
    it(`a second trigger of a completed id doesn't run the body again (runSource ${runSource})`, async () => {
      let executions = 0;
      const wf = workflow<unknown>({ name: "w" })
        .step("a", () => succeed(++executions))
        .build();
      const storage = new InMemoryWorkflowStorage();
      const workflows = new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: { w: wf as never },
        sleepScanIntervalMs: 0,
        signalScanIntervalMs: 0,
      });
      const opts = {
        workflowId: "sched-1.7",
        ...(runSource !== undefined && { runSource, runSourceId: "sched-1" }),
      };

      expect(await workflows.trigger("w", {}, opts)).toEqual({ workflowId: "sched-1.7" });
      await settle();
      expect(await workflows.trigger("w", {}, opts)).toEqual({
        workflowId: "sched-1.7",
        existing: true,
      });
      await settle();

      const run = await storage.loadWorkflow("sched-1.7");
      expect([executions, run?.run, run?.status]).toEqual([1, 1, "completed"]);
    });
  }

  it("returns an existing secured run without replacing its starter identity", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({
      workflowId: "secured-existing",
      workflowName: "w",
      input: {},
      securityContext: { actor: { kind: "user", id: "alice" } },
    });
    const wf = workflow<unknown>({ name: "w" })
      .step("a", () => succeed("ok"))
      .build();
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: { w: wf as never },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });

    await expect(
      workflows.trigger(
        "w",
        {},
        {
          workflowId: "secured-existing",
          securityContext: { actor: { kind: "user", id: "mallory" } },
        },
      ),
    ).resolves.toEqual({ workflowId: "secured-existing", existing: true });
    expect((await storage.loadWorkflow("secured-existing"))?.securityContext).toEqual({
      actor: { kind: "user", id: "alice" },
    });
  });
});

describe("LocalWorkflows.rerun", () => {
  it("resets the row and re-executes", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = makeWorkflow();
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: wf },
      sleepScanIntervalMs: 0,
    });

    const { workflowId } = await workflows.trigger("double", { n: 4 });
    await waitFor(async () => (await storage.loadWorkflow(workflowId))?.status === "completed");

    const before = await storage.loadWorkflow(workflowId);
    expect(before?.status).toBe("completed");

    // A rerun starts a fresh run (run counter + 1) and executes it again.
    await workflows.rerun(workflowId);
    await waitFor(async () => {
      const s = await storage.loadWorkflow(workflowId);
      return s?.run === before!.run + 1 && s.status === "completed";
    });

    const after = await storage.loadWorkflow(workflowId);
    expect(after?.status).toBe("completed");
    expect(after?.result).toBe(8);
  });

  it("throws when workflow id not found and no fallback", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });

    await expect(workflows.rerun("nonexistent")).rejects.toThrow(/not found/);
  });
});

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** `inner` without `listOrphanedRuns`, so recovery takes the listing fallback. */
function withoutOrphanQuery(inner: InMemoryWorkflowStorage): WorkflowStorage {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop === "listOrphanedRuns") return undefined;
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
    has(target, prop) {
      return prop === "listOrphanedRuns" ? false : Reflect.has(target, prop);
    },
  });
}

/** A one-step saga; its rollback records the run id in `undone`. */
function sagaFor(undone: string[]) {
  return workflow<{ n: number }>({ name: "saga" })
    .step("charge", ({ input }) => succeed(input.n), {
      compensate: async ({ workflowId }) => {
        undone.push(workflowId);
      },
    })
    .build();
}

/** A run a crashed process left `compensating` after `charge` completed. */
async function leaveCompensating(storage: InMemoryWorkflowStorage, workflowId: string) {
  await storage.createWorkflow({ workflowId, workflowName: "saga", input: { n: 1 } });
  await storage.saveStepResult({
    workflowId,
    stepName: "charge",
    result: 1,
    durationMs: 1,
    startedAt: new Date(0),
  });
  expect(await storage.beginCompensation({ workflowId, error: "declined" })).toBe(true);
}

/** `count` pending `double` runs, ids `p-000`, `p-001`, … */
async function createPending(storage: InMemoryWorkflowStorage, count: number): Promise<string[]> {
  const ids = Array.from({ length: count }, (_, i) => `p-${String(i).padStart(3, "0")}`);
  for (const [i, workflowId] of ids.entries()) {
    await storage.createWorkflow({ workflowId, workflowName: "double", input: { n: i } });
  }
  return ids;
}

async function statusCount(storage: InMemoryWorkflowStorage, status: "completed" | "failed") {
  return (await storage.listWorkflows({ status, limit: 10_000 })).length;
}

describe("LocalWorkflows.start — recovery", () => {
  it("terminates stale runs with the configured action", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    await storage.createWorkflow({ workflowId: "old", workflowName: "double", input: { n: 1 } });
    clock.advance(2 * 60 * 60 * 1000);
    await storage.createWorkflow({ workflowId: "new", workflowName: "double", input: { n: 1 } });

    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage, clock }),
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      clock,
      recovery: RecoveryStrategy.builder()
        .failStale({ olderThanMs: 60 * 60 * 1000, error: "stale" })
        .build(),
    });
    await workflows.start();
    await workflows.stop();

    const old = await storage.loadWorkflow("old");
    expect(old?.status).toBe("failed");
    expect(old?.error).toBe("stale");
    // Not stale, and no resumeRecent(): left as it was.
    expect((await storage.loadWorkflow("new"))?.status).toBe("pending");
  });

  it("resumes pending and compensating runs nobody is driving; skips locked and unknown ones", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const undone: string[] = [];
    // More than one 200-row page of pending runs.
    const pending = await createPending(storage, 450);
    await leaveCompensating(storage, "comp");
    await storage.createWorkflow({ workflowId: "owned", workflowName: "double", input: { n: 1 } });
    await storage.tryLock({ workflowId: "owned", lockDurationMs: 600_000 });
    await storage.createWorkflow({ workflowId: "alien", workflowName: "other", input: {} });
    clock.advance(1);

    const runner = createWorkflowRunner({ storage, clock });
    let inFlight = 0;
    let maxInFlight = 0;
    const runSafe = runner.runSafe.bind(runner);
    runner.runSafe = (async (params: Parameters<typeof runner.runSafe>[0]) => {
      maxInFlight = Math.max(maxInFlight, ++inFlight);
      try {
        return await runSafe(params);
      } finally {
        inFlight--;
      }
    }) as typeof runner.runSafe;

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow(), saga: sagaFor(undone) },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      clock,
      recovery: RecoveryStrategy.builder().resumeRecent({ concurrent: 5 }).build(),
    });
    await workflows.start();
    await waitFor(async () => (await statusCount(storage, "completed")) === pending.length);
    await waitFor(async () => (await storage.loadWorkflow("comp"))?.status === "failed");
    await workflows.stop();

    expect((await storage.loadWorkflow("p-449"))?.result).toBe(898);
    expect(undone).toEqual(["comp"]);
    expect((await storage.loadWorkflow("owned"))?.status).toBe("pending");
    expect((await storage.loadWorkflow("alien"))?.status).toBe("pending");
    expect(maxInFlight).toBeLessThanOrEqual(5);
  });

  it("resumes every run across listing pages when the storage has no listOrphanedRuns", async () => {
    const clock = FakeWallClock.create(0);
    const inner = new InMemoryWorkflowStorage({ clock });
    const undone: string[] = [];
    const pending = await createPending(inner, 450);
    const compensating = Array.from({ length: 250 }, (_, i) => `c-${String(i).padStart(3, "0")}`);
    for (const id of compensating) await leaveCompensating(inner, id);
    clock.advance(1);
    const storage = withoutOrphanQuery(inner);

    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage, clock }),
      definitions: { double: makeWorkflow(), saga: sagaFor(undone) },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
      clock,
      recovery: RecoveryStrategy.builder().resumeRecent({ concurrent: 20 }).build(),
    });
    await workflows.start();
    await waitFor(
      async () =>
        (await statusCount(inner, "completed")) === pending.length &&
        (await statusCount(inner, "failed")) === compensating.length,
    );
    await workflows.stop();

    expect([...undone].sort()).toEqual(compensating);
  });

  it("does nothing on start without a recovery strategy", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "p", workflowName: "double", input: { n: 1 } });
    const runner = createWorkflowRunner({ storage });
    let recoverCalled = false;
    runner.recover = async () => {
      recoverCalled = true;
      return { terminated: 0, resumed: 0, skipped: [], settled: Promise.resolve() };
    };

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });

    await workflows.start();
    await workflows.stop();

    expect(recoverCalled).toBe(false);
    expect((await storage.loadWorkflow("p"))?.status).toBe("pending");
  });
});
