// ---------------------------------------------------------------------------
// Coverage for the WorkflowRunner facade — `run`, `runSafe`, `start`,
// `getStatus`, plus registry-backed name resolution and version-drain-resume.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { InProcessStepExecutor, createWorkflowRunner } from "../workflow-runner.ts";
import { WorkflowVersionRegistry } from "../workflow-version-registry.ts";

class TestError extends Data.TaggedError("TestError")<{ readonly message: string }> {}

describe("WorkflowRunner", () => {
  it("run({ workflow }) drives a pure Workflow through the runner's storage", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "runner-run-direct" })
      .step("triple", ({ input }) => Pipeline.succeed(input.n * 3))
      .build();

    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: wf,
      workflowId: "run-direct-1",
      input: { n: 4 },
    });
    expect(result).toBe(12);
    const state = await storage.loadWorkflow("run-direct-1");
    expect(state?.status).toBe("completed");
  });

  it("runSafe({ workflow }) returns { data, error } for failures", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<void>({ name: "runner-run-fail" })
      .step("boom", () => Pipeline.fail(new TestError({ message: "nope" })))
      .build();

    const runner = createWorkflowRunner({ storage });
    const { data, error } = await runner.runSafe({
      workflow: wf,
      workflowId: "run-fail-2",
      input: undefined,
    });
    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

  it("run({ name }) resolves the latest version via the registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1 = workflow<{ n: number }>({ name: "by-name", version: "1" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 1))
      .build();
    const v2 = workflow<{ n: number }>({ name: "by-name", version: "2" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 100))
      .build();

    const registry = new WorkflowVersionRegistry();
    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({
      name: "by-name",
      workflowId: "by-name-1",
      input: { n: 5 },
    });
    expect(result).toBe(105); // v2 is latest
  });

  it("run({ name }) drains an in-flight workflow on the version it was created under", async () => {
    const storage = new InMemoryWorkflowStorage();
    const v1 = workflow<{ n: number }>({ name: "drain", version: "1" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 1))
      .build();
    const v2 = workflow<{ n: number }>({ name: "drain", version: "2" })
      .step("bump", ({ input }) => Pipeline.succeed(input.n + 100))
      .build();

    // Seed storage with a row under v1 so the name-based resume must drain.
    await storage.createWorkflow({
      workflowId: "drain-1",
      workflowName: "drain",
      input: { n: 5 },
      version: "1",
    });

    const registry = new WorkflowVersionRegistry();
    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({
      name: "drain",
      workflowId: "drain-1",
      input: { n: 5 },
    });
    expect(result).toBe(6); // v1 still drives the resume
  });

  it("run({ name }) without registry throws a pointed error", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    await expect(
      runner.run({ name: "missing", workflowId: "x", input: undefined }),
    ).rejects.toThrow(/registry/);
  });

  it("InProcessStepExecutor is constructable but throws until phase 2 — guards against premature adoption", async () => {
    const wf = workflow<void>({ name: "placeholder" })
      .step("noop", () => Pipeline.succeed(undefined))
      .build();

    const executor = new InProcessStepExecutor(wf);
    await expect(
      executor.executeStep({
        workflowId: "x",
        stepName: "noop",
        input: undefined,
        prevResults: {},
        attempt: 1,
      }),
    ).rejects.toThrow(/phase 2/);
  });
});
