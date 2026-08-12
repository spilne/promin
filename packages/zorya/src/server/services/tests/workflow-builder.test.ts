import { describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import {
  createWorkflowRunner,
  createWorkflowStepCatalog,
  InMemoryWorkflowStorage,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import { ZoryaWorkflowBuilder } from "../workflow-builder.ts";
import { LocalWorkflows } from "../workflows/local-workflows.ts";

describe("ZoryaWorkflowBuilder", () => {
  it("saves, publishes, promotes, and dispatches an authored workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const versionRegistry = new WorkflowVersionRegistry();
    const catalog = createWorkflowStepCatalog([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
      },
    ]);
    const builder = new ZoryaWorkflowBuilder({
      catalog,
      versionRegistry,
      now: () => 1_000,
    });

    await builder.save({
      version: "v1",
      schema: {
        version: 1,
        name: "authored-upper",
        steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" }],
      },
    });
    const published = await builder.publish("authored-upper");
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage, registry: versionRegistry }),
      definitions: {},
      versionRegistry,
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });

    const { workflowId } = await workflows.trigger("authored-upper", "hello", {
      workflowId: "authored-1",
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    const state = await storage.loadWorkflow(workflowId);

    expect(published.status).toBe("published");
    expect(await versionRegistry.findActive("authored-upper")).toMatchObject({ version: "v1" });
    expect(state?.status).toBe("completed");
    expect(state?.result).toBe("HELLO");
  });
});
