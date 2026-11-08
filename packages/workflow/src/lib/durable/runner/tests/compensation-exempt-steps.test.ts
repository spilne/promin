// ---------------------------------------------------------------------------
// A saga rollback undoes only what ran: a step skipped by `skipWhen` and a
// step whose value came from `onFailure` have no effect of their own, so their
// `compensate` never runs. The row says why (`metadata.stepOutcome`).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { workflow } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";

class Boom extends TaggedError("Boom")<{ readonly message: string }>() {}

describe("saga rollback and steps that did not run", () => {
  it("compensates the step that ran, not the skipped or fallback ones", async () => {
    const calls: string[] = [];
    const undo = (name: string) => ({
      compensate: async () => {
        calls.push(`undo:${name}`);
      },
    });
    const wf = workflow<{ n: number }>({ name: "exempt-steps", version: "1" })
      .step("ran", () => succeed("ran"), undo("ran"))
      .step("skipped", () => succeed("body"), { skipWhen: () => true, ...undo("skipped") })
      .step("fallback", () => fail(new Boom({ message: "flaky" })), {
        onFailure: { fallback: () => "fallback-value" },
        ...undo("fallback"),
      })
      .step("dropped", () => fail(new Boom({ message: "flaky" })), {
        onFailure: "skip",
        ...undo("dropped"),
      })
      .step("last", () => fail(new Boom({ message: "real failure" })))
      .build();
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const { error } = await runner.runSafe({
      workflow: wf as never,
      workflowId: "exempt-1",
      input: { n: 1 },
    });
    expect((error as { _tag?: string })._tag).toBe("Boom");

    expect(calls).toEqual(["undo:ran"]);
    const state = (await storage.loadWorkflow("exempt-1"))!;
    expect(state.steps["ran"]!.metadata?.["stepOutcome"]).toBeUndefined();
    expect(state.steps["skipped"]!.metadata?.["stepOutcome"]).toBe("skipped");
    expect(state.steps["fallback"]!.metadata?.["stepOutcome"]).toBe("fallback");
    expect(state.steps["dropped"]!.metadata?.["stepOutcome"]).toBe("fallback");
    expect(state.steps["ran"]!.compensationStatus).toBe("compensated");
    expect(state.steps["skipped"]!.compensationStatus).toBeUndefined();
  });
});
