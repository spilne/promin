import { describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import { createWorkflowStepCatalog } from "../step-catalog.ts";
import { compileWorkflow } from "../workflow-compiler.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("WorkflowStepCatalog", () => {
  it("lists UI metadata while resolving activities for compiled schemas", async () => {
    const catalog = createWorkflowStepCatalog([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        category: "Transform",
        inputSchema: { type: "string" },
        outputSchema: { type: "string" },
        activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
      },
    ]);

    expect(catalog.entries()).toEqual([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        category: "Transform",
        inputSchema: { type: "string" },
        outputSchema: { type: "string" },
      },
    ]);

    const workflow = compileWorkflow({
      schema: {
        version: 1,
        name: "catalog-workflow",
        steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" }],
      },
      registry: catalog,
      version: "v2",
    });
    const result = await createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
      workflow,
      workflowId: "catalog-1",
      input: "hello",
    });

    expect(workflow.version).toBe("v2");
    expect(result).toBe("HELLO");
  });
});
