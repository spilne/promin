// ---------------------------------------------------------------------------
// run({ useStoredInput }): a driver of runs created elsewhere drives the
// stored run with the input stored on it, without carrying the input; a run
// the call creates is seeded with `input` as usual.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../../../index.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

function echo(seen: unknown[]) {
  return workflow<{ n: number }>({ name: "echo" })
    .step("read", ({ input }) => {
      seen.push(input);
      return succeed(input.n);
    })
    .build();
}

describe("run({ useStoredInput })", () => {
  it("drives a stored run with its stored input", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    await storage.createWorkflow({ workflowId: "s-1", workflowName: "echo", input: { n: 7 } });
    const seen: unknown[] = [];

    const result = await runner.run({
      workflow: echo(seen) as never,
      workflowId: "s-1",
      input: undefined,
      useStoredInput: true,
    });

    expect(result).toBe(7);
    expect(seen).toEqual([{ n: 7 }]);
  });

  it("never creates a run: with none stored it rejects with WorkflowRunNotFoundError", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const seen: unknown[] = [];

    await expect(
      runner.run({
        workflow: echo(seen) as never,
        workflowId: "s-2",
        input: { n: 3 },
        useStoredInput: true,
      }),
    ).rejects.toMatchObject({ _tag: "WorkflowRunNotFoundError", workflowId: "s-2" });
    expect(seen).toEqual([]);
    expect(await storage.loadWorkflow("s-2")).toBeNull();
  });

  it("continue-as-new stores the next run's input, so a later resume reads it", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ count: number }>({ name: "chain" })
      .journaled("loop", function* (ctx) {
        const count = yield* ctx.activity("read", () => ctx.input.count);
        if (count < 2) ctx.continueAsNew({ count: count + 1 });
        return count;
      })
      .build();

    expect(await runner.run({ workflow: wf, workflowId: "c-1", input: { count: 0 } })).toBe(2);
    expect((await storage.loadWorkflow("c-1"))?.input).toEqual({ count: 2 });
  });

  it("without it, the passed input drives the run (the default)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    await storage.createWorkflow({ workflowId: "s-3", workflowName: "echo", input: { n: 1 } });
    const seen: unknown[] = [];

    expect(
      await runner.run({ workflow: echo(seen) as never, workflowId: "s-3", input: { n: 2 } }),
    ).toBe(2);
  });
});
