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
import { ZoryaServer } from "../../server.ts";

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

  it("executes a builder-published workflow through the HTTP trigger route", async () => {
    const storage = new InMemoryWorkflowStorage();
    const versionRegistry = new WorkflowVersionRegistry();
    const catalog = createWorkflowStepCatalog([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
      },
    ]);
    const workflowBuilder = new ZoryaWorkflowBuilder({
      catalog,
      versionRegistry,
      now: () => 1_000,
    });
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage, registry: versionRegistry }),
      definitions: {},
      versionRegistry,
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows, workflowBuilder });

    await server.handle(
      new Request("http://x/api/workflow-builder/workflows", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          version: "v1",
          schema: {
            version: 1,
            name: "http-authored-upper",
            steps: [
              {
                type: "step",
                name: "upper",
                dependsOn: [],
                activityRef: "transform.uppercase",
              },
            ],
          },
        }),
      }),
    );
    await server.handle(
      new Request("http://x/api/workflow-builder/workflows/http-authored-upper/publish", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ version: "v1", promote: true }),
      }),
    );
    const trigger = await server.handle(
      new Request("http://x/api/runs/trigger/http-authored-upper", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ input: "hello", version: "v1" }),
      }),
    );
    const body = (await trigger.json()) as { workflowId: string };
    await new Promise((resolve) => setTimeout(resolve, 0));
    const detail = await server.handle(new Request(`http://x/api/runs/${body.workflowId}`));
    const run = (await detail.json()) as { status: string; steps: Array<{ stepName: string }> };

    expect(trigger.status).toBe(200);
    expect(detail.status).toBe(200);
    expect(run.status).toBe("completed");
    expect(run.steps.map((step) => step.stepName)).toContain("upper");
  });

  it("marks a published workflow draft again when saved content changes", async () => {
    const versionRegistry = new WorkflowVersionRegistry();
    const catalog = createWorkflowStepCatalog([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
      },
      {
        id: "transform.identity",
        title: "Identity",
        activity: () => (ctx) => Pipeline.succeed(ctx.prev),
      },
    ]);
    const builder = new ZoryaWorkflowBuilder({ catalog, versionRegistry, now: () => 1_000 });
    await builder.save({
      version: "v1",
      schema: {
        version: 1,
        name: "edited-after-publish",
        steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" }],
      },
    });
    await builder.publish("edited-after-publish", "v1");

    const edited = await builder.save({
      version: "v1",
      schema: {
        version: 1,
        name: "edited-after-publish",
        steps: [
          { type: "step", name: "identity", dependsOn: [], activityRef: "transform.identity" },
        ],
      },
    });

    expect(edited.status).toBe("draft");
    expect(edited.publishedAt).toBeUndefined();
  });

  it("deletes authored record and deregisters its workflow version", async () => {
    const versionRegistry = new WorkflowVersionRegistry();
    const catalog = createWorkflowStepCatalog([
      {
        id: "transform.uppercase",
        title: "Uppercase",
        activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
      },
    ]);
    const builder = new ZoryaWorkflowBuilder({ catalog, versionRegistry });
    await builder.save({
      version: "v1",
      schema: {
        version: 1,
        name: "delete-authored",
        steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" }],
      },
    });
    await builder.publish("delete-authored", "v1");

    await builder.delete("delete-authored", "v1");

    expect(await builder.get("delete-authored", "v1")).toBeNull();
    expect(await versionRegistry.resolve("delete-authored", "v1")).toBeUndefined();
  });
});
