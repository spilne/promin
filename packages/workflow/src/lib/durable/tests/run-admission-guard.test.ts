import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { workflow } from "../workflow-builder.ts";

describe("WorkflowRunAdmissionGuard", () => {
  it("runs under the fence before user code and is inherited by children", async () => {
    const storage = new InMemoryWorkflowStorage();
    const admitted: string[] = [];
    let childBodies = 0;
    const runner = createWorkflowRunner({
      storage,
      runAdmissionGuard: async ({ state, guard }) => {
        admitted.push(state.workflowId);
        expect(guard?.fenceToken).toBeDefined();
        if (state.workflowId === "guard-child") {
          await storage.failWorkflow({
            workflowId: state.workflowId,
            error: "blocked",
            errorTag: "HostBlocked",
            ...(guard !== undefined && { guard }),
          });
          throw new Error("blocked");
        }
      },
    });
    const child = workflow<number>({ name: "guard-child" })
      .step("body", ({ input }) => {
        childBodies++;
        return succeed(input + 1);
      })
      .build();
    const parent = workflow<number>({ name: "guard-parent" })
      .subworkflow("child", child, {
        input: (input) => input,
        workflowId: () => "guard-child",
      })
      .build();

    await expect(
      runner.run({ workflow: parent, workflowId: "guard-parent", input: 1 }),
    ).rejects.toThrow();

    expect(admitted).toEqual(["guard-parent", "guard-child"]);
    expect(childBodies).toBe(0);
    expect(await storage.loadWorkflow("guard-child")).toMatchObject({
      status: "failed",
      errorTag: "HostBlocked",
    });
  });

  it("a guard installed after runner construction protects recovery-style runs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let bodies = 0;
    const secured = workflow<void>({ name: "late-guard" })
      .step("body", () => {
        bodies++;
        return succeed(undefined);
      })
      .build();
    await storage.createWorkflow({
      workflowId: "late-guard",
      workflowName: secured.name,
      input: undefined,
    });
    runner.setRunAdmissionGuard?.(async ({ state, guard }) => {
      await storage.failWorkflow({
        workflowId: state.workflowId,
        error: "blocked",
        errorTag: "HostBlocked",
        ...(guard !== undefined && { guard }),
      });
      throw new Error("blocked");
    });

    await runner.runSafe({ workflow: secured, workflowId: "late-guard", input: undefined });

    expect(bodies).toBe(0);
    expect(await storage.loadWorkflow("late-guard")).toMatchObject({
      status: "failed",
      errorTag: "HostBlocked",
    });
  });
});
