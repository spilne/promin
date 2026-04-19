// ---------------------------------------------------------------------------
// Phase-1 coverage for the WorkflowRunner facade. Doesn't test the final
// orchestration engine (phase 2 does that) — just verifies the public
// surface works end-to-end today against a real workflow.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import {
  DefaultWorkflowRunner,
  InProcessStepExecutor,
  createWorkflowRunner,
} from "./workflow-runner.ts";

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
