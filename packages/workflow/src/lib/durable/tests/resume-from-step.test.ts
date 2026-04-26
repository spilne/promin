// ---------------------------------------------------------------------------
// runner.resume({ workflow, workflowId, fromStep }) — debugging primitive
// for incident response. Resets fromStep + everything downstream of it
// (transitively in the DAG) back to pending; preserves upstream completed
// step results so they don't re-execute.
//
// Pins the contract:
//   - 5-step linear chain: reset to step 3 → steps 1+2 not re-executed,
//     steps 3-5 fresh execute.
//   - Diamond: reset to a node mid-DAG → only that node + its descendants
//     reset, parallel branches that don't depend on it stay completed.
//   - StepNotFoundError on unknown fromStep.
//   - Storage missing resetSteps throws a clear error.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("runner.resume — replay from a specific step", () => {
  it("5-step linear chain — reset to step 3 only re-executes 3, 4, 5", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const calls: Record<string, number> = { s1: 0, s2: 0, s3: 0, s4: 0, s5: 0 };

    const wf = workflow<{ x: number }>({ name: "linear" })
      .step("s1", ({ input }) => {
        calls.s1++;
        return Pipeline.succeed(input.x + 1);
      })
      .step("s2", { dependsOn: ["s1"] }, ({ deps }) => {
        calls.s2++;
        return Pipeline.succeed((deps.s1 as number) + 1);
      })
      .step("s3", { dependsOn: ["s2"] }, ({ deps }) => {
        calls.s3++;
        return Pipeline.succeed((deps.s2 as number) + 1);
      })
      .step("s4", { dependsOn: ["s3"] }, ({ deps }) => {
        calls.s4++;
        return Pipeline.succeed((deps.s3 as number) + 1);
      })
      .step("s5", { dependsOn: ["s4"] }, ({ deps }) => {
        calls.s5++;
        return Pipeline.succeed((deps.s4 as number) + 1);
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "lin-1", input: { x: 0 } });
    expect(calls).toEqual({ s1: 1, s2: 1, s3: 1, s4: 1, s5: 1 });

    await runner.resume({ workflow: wf, workflowId: "lin-1", fromStep: "s3" });
    // s1, s2 NOT re-executed (call count unchanged); s3-s5 re-executed (+1).
    expect(calls).toEqual({ s1: 1, s2: 1, s3: 2, s4: 2, s5: 2 });

    const state = await storage.loadWorkflow("lin-1");
    expect(state?.status).toBe("completed");
  });

  it("diamond DAG — reset a left-branch node leaves the right branch completed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const calls: Record<string, number> = { root: 0, left: 0, right: 0, sink: 0 };

    //         root
    //        /    \
    //     left    right
    //        \    /
    //         sink
    const wf = workflow<number>({ name: "diamond" })
      .step("root", ({ input }) => {
        calls.root++;
        return Pipeline.succeed(input + 1);
      })
      .step("left", { dependsOn: ["root"] }, ({ deps }) => {
        calls.left++;
        return Pipeline.succeed((deps.root as number) * 10);
      })
      .step("right", { dependsOn: ["root"] }, ({ deps }) => {
        calls.right++;
        return Pipeline.succeed((deps.root as number) * 100);
      })
      .step("sink", { dependsOn: ["left", "right"] }, ({ deps }) => {
        calls.sink++;
        return Pipeline.succeed((deps.left as number) + (deps.right as number));
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "dia-1", input: 1 });
    expect(calls).toEqual({ root: 1, left: 1, right: 1, sink: 1 });

    // Reset left → left + sink re-execute; root + right preserved.
    await runner.resume({ workflow: wf, workflowId: "dia-1", fromStep: "left" });
    expect(calls).toEqual({ root: 1, left: 2, right: 1, sink: 2 });
  });

  it("throws when fromStep doesn't exist on the workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "tiny" })
      .step("only", ({ input }) => Pipeline.succeed(input))
      .build();

    await runner.run({ workflow: wf, workflowId: "tiny-1", input: 1 });

    await expect(
      runner.resume({ workflow: wf, workflowId: "tiny-1", fromStep: "does-not-exist" }),
    ).rejects.toThrow(/not found on workflow/);
  });

  it("throws when storage doesn't implement resetSteps", async () => {
    const storage = new InMemoryWorkflowStorage();
    // Strip resetSteps to simulate a backend that hasn't implemented it.
    (storage as unknown as { resetSteps?: unknown }).resetSteps = undefined;
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "no-reset" })
      .step("a", ({ input }) => Pipeline.succeed(input))
      .build();

    await expect(
      runner.resume({ workflow: wf, workflowId: "nr-1", fromStep: "a" }),
    ).rejects.toThrow(/resetSteps/);
  });

  it("throws when the workflow doesn't exist in storage", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "ghost" })
      .step("x", ({ input }) => Pipeline.succeed(input))
      .build();

    await expect(
      runner.resume({ workflow: wf, workflowId: "missing", fromStep: "x" }),
    ).rejects.toThrow(/not found in storage/);
  });
});
