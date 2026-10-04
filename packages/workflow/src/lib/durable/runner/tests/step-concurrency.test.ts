import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow, type StepQueueContext } from "../../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import {
  InProcessStepExecutor,
  createWorkflowRunner,
  type StepExecutionRequest,
  type StepExecutor,
} from "../../workflow-runner.ts";
import { resolveStepConcurrency } from "../step-concurrency.ts";

describe("step queue key context", () => {
  it("a step-level key function sees the workflow input, prev, deps, workflowId and attempt", () => {
    const seen: StepQueueContext[] = [];
    const wf = workflow<{ tenant: string }>({ name: "keyed" })
      .step("load", ({ input }) => succeed(input.tenant.length))
      .step("send", ({ prev }) => succeed(prev), {
        queue: {
          concurrencyLimit: 3,
          concurrencyKey: (ctx) => (seen.push(ctx), "k"),
        },
      })
      .build();
    const stepDef = wf._definition.steps.find((s) => s.name === "send")!;

    const resolved = resolveStepConcurrency({
      workflowName: "keyed",
      stepDef,
      workflowInput: { tenant: "acme" },
      stepInput: 4,
      workflowId: "wf-1",
      attempt: 2,
      results: { load: 4 },
    });

    expect(resolved).toEqual({ scope: "keyed::send", key: "k", limit: 3 });
    expect(seen).toEqual([
      { input: { tenant: "acme" }, prev: 4, deps: { load: 4 }, workflowId: "wf-1", attempt: 2 },
    ]);
  });

  it("the runner stamps a key computed from the real workflow input on the dispatched step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ tenant: string }>({ name: "keyed-run" })
      .step("load", ({ input }) => succeed(input.tenant.length))
      .step("send", ({ prev }) => succeed(prev), {
        queue: {
          concurrencyLimit: 1,
          concurrencyKey: (ctx) =>
            `${(ctx.input as { tenant: string }).tenant}:${String(ctx.prev)}`,
        },
      })
      .build();
    const inner = new InProcessStepExecutor(wf, { storage });
    const requests: StepExecutionRequest[] = [];
    const executor: StepExecutor = {
      executeStep: (req) => (requests.push(req), inner.executeStep(req)),
    };
    const runner = createWorkflowRunner({ storage, stepExecutor: executor });

    await runner.run({ workflow: wf, workflowId: "k-1", input: { tenant: "acme" } });

    const send = requests.find((r) => r.stepName === "send");
    expect(send?.concurrencyKey).toBe("acme:4");
    expect(send?.concurrencyScope).toBe("keyed-run::send");
    expect(send?.concurrencyLimit).toBe(1);
  });
});
