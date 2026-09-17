// ---------------------------------------------------------------------------
// LocalWorkflows tests — trigger, rerun, recovery wiring.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import {
  InMemoryWorkflowStorage,
  workflow,
  createWorkflowRunner,
  RecoveryStrategy,
} from "@promin/workflow";
import { Pipeline } from "@promin/core";
import { LocalWorkflows } from "../index.ts";

const makeWorkflow = () =>
  workflow<{ n: number }>({ name: "double" })
    .step("doIt", ({ input }) => Pipeline.succeed(input.n * 2))
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
    await new Promise((res) => setTimeout(res, 100));
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
    await new Promise((r) => setTimeout(r, 100));

    const before = await storage.loadWorkflow(workflowId);
    expect(before?.status).toBe("completed");

    await workflows.rerun(workflowId);
    await new Promise((r) => setTimeout(r, 100));

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

describe("LocalWorkflows.start — recovery wiring", () => {
  it("invokes runner.recover when recovery strategy is configured", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let recoverCalled = false;
    const origRecover = runner.recover.bind(runner);
    runner.recover = async (strategy) => {
      recoverCalled = true;
      return origRecover(strategy);
    };

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
      recovery: RecoveryStrategy.builder()
        .failStale({ olderThanMs: 60 * 60 * 1000, error: "stale" })
        .build(),
    });

    await workflows.start();
    await workflows.stop();

    expect(recoverCalled).toBe(true);
  });

  it("does not invoke runner.recover when no strategy is configured", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let recoverCalled = false;
    runner.recover = async () => {
      recoverCalled = true;
      return { terminated: 0, resumed: 0, skipped: [] };
    };

    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: { double: makeWorkflow() },
      sleepScanIntervalMs: 0,
    });

    await workflows.start();
    await workflows.stop();

    expect(recoverCalled).toBe(false);
  });
});
