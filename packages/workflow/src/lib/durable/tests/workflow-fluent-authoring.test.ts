import { describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import { bindWorkflow, createWorkflowApp, createWorkflowRunner } from "../workflow-runner.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { step, stepOptions, workflow } from "../durable-pipeline.ts";

describe("workflow fluent authoring", () => {
  it("accepts Promise-returning functions in step()", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ n: number }>({ name: "unified-step" })
      .step("double", async ({ input }) => input.n * 2)
      .step("plus-one", async ({ prev }) => prev + 1)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "unified-step-1", input: { n: 20 } }),
    ).resolves.toBe(41);
  });

  it("builds step options fluently", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ n: number }>({ name: "fluent-options" })
      .step(
        "slow",
        async ({ input }) => input.n,
        stepOptions<number>().timeout(1_000).priority(8).needs(["cpu"]).build(),
      )
      .build();

    await runner.run({ workflow: wf, workflowId: "fluent-options-1", input: { n: 7 } });
    expect(wf.dag.steps[0]).toMatchObject({ name: "slow", priority: 8, needs: ["cpu"] });
  });

  it("applies reusable task fragments", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const normalize = step<{ name: string }, string>("trim", async ({ input }) =>
      input.name.trim(),
    ).andThen("upper", async ({ prev }) => prev.toUpperCase());

    const wf = workflow<{ name: string }>({ name: "fragment-use" }).use(normalize).build();

    await expect(
      runner.run({ workflow: wf, workflowId: "fragment-use-1", input: { name: " ada " } }),
    ).resolves.toBe("ADA");
  });

  it("runs workflows through bound app helpers", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "bound-app" })
      .step("triple", ({ input }) => Pipeline.succeed(input.n * 3))
      .build();

    const app = createWorkflowApp({ storage });
    const bound = app.workflow(wf);

    await expect(bound.run({ workflowId: "bound-app-1", input: { n: 5 } })).resolves.toBe(15);
    await expect(
      bindWorkflow(app.runner, wf).run({ workflowId: "bound-app-2", input: { n: 6 } }),
    ).resolves.toBe(18);
  });

  it("provides approval as signal sugar", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ requestId: string }>({ name: "approval-sugar" })
      .step("load", async ({ input }) => input.requestId)
      .approval<{ approved: boolean }>("manager", { signalName: "manager-approved" })
      .step("finish", async ({ prev }) => prev.approved)
      .build();

    await runner.runSafe({
      workflow: wf,
      workflowId: "approval-sugar-1",
      input: { requestId: "r1" },
    });
    await storage.deliverSignal("approval-sugar-1", "manager-approved", { approved: true });

    await expect(
      runner.run({ workflow: wf, workflowId: "approval-sugar-1", input: { requestId: "r1" } }),
    ).resolves.toBe(true);
  });

  it("supports parallel as a fork/join alias", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ n: number }>({ name: "parallel-alias" })
      .step("start", async ({ input }) => input.n)
      .parallel("fanout", {
        left: async ({ prev }) => prev + 1,
        right: async ({ prev }) => prev + 2,
      })
      .step("join", async ({ prev }) => prev.left + prev.right)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "parallel-alias-1", input: { n: 10 } }),
    ).resolves.toBe(23);
  });
});
