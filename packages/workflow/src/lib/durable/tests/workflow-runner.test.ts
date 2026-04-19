// ---------------------------------------------------------------------------
// Coverage for the WorkflowRunner facade. Exercises the new
// `run({ workflow | name, ... })` API plus the legacy `execute` shape that
// still wraps pre-bound RunnableWorkflows during the migration.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import {
  DefaultWorkflowRunner,
  InProcessStepExecutor,
  createWorkflowRunner,
} from "../workflow-runner.ts";
import { WorkflowVersionRegistry } from "../workflow-version-registry.ts";

class TestError extends Data.TaggedError("TestError")<{ readonly message: string }> {}

describe("WorkflowRunner (phase 1 facade)", () => {
  it("execute runs a workflow and returns the final step result", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "runner-ok" })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .build()
      .bind(storage);

    const runner = createWorkflowRunner();
    const result = await runner.execute<{ n: number }, number>({
      workflow: wf,
      workflowId: "run-1",
      input: { n: 5 },
    });
    expect(result).toBe(10);
  });

  it("executeSafe surfaces step failures as { data: null, error }", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<void>({ name: "runner-fail" })
      .step("boom", () => Pipeline.fail(new TestError({ message: "nope" })))
      .build()
      .bind(storage);

    const runner = new DefaultWorkflowRunner();
    const { data, error } = await runner.executeSafe<void, unknown>({
      workflow: wf,
      workflowId: "run-fail-1",
      input: undefined,
    });
    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

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

  it("run() without storage throws a pointed error", async () => {
    const runner = createWorkflowRunner();
    const wf = workflow<void>({ name: "no-storage" })
      .step("noop", () => Pipeline.succeed(undefined))
      .build();
    await expect(runner.run({ workflow: wf, workflowId: "x", input: undefined })).rejects.toThrow(
      /storage/,
    );
  });

  it("run({ name }) without registry throws a pointed error", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    await expect(
      runner.run({ name: "missing", workflowId: "x", input: undefined }),
    ).rejects.toThrow(/registry/);
  });

  it("InProcessStepExecutor is constructable but throws until phase 2 — guards against premature adoption", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<void>({ name: "placeholder" })
      .step("noop", () => Pipeline.succeed(undefined))
      .build()
      .bind(storage);

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
