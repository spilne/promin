import { describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow, WorkflowVersionRegistry } from "@promin/workflow";
import { getWorkflowDef, listWorkflowDefs, type WorkflowDefsResponse } from "../workflow-defs.ts";

describe("workflow definitions routes", () => {
  it("includes active workflows from the version registry", async () => {
    const registry = new WorkflowVersionRegistry();
    const v1 = workflow<string>({ name: "authored", version: "v1", type: "authored" })
      .step("first", ({ input }) => Pipeline.succeed(input))
      .build();
    const v2 = workflow<string>({ name: "authored", version: "v2", type: "authored" })
      .step("first", ({ input }) => Pipeline.succeed(input))
      .step("second", ({ prev }) => Pipeline.succeed(prev))
      .build();
    registry.register(v1);
    registry.register(v2);
    await registry.promote("authored", "v2");

    const handler = listWorkflowDefs({ versionRegistry: registry });
    const res = await handler();
    const body = (await res.json()) as WorkflowDefsResponse;

    expect(res.status).toBe(200);
    expect(body.workflows).toHaveLength(1);
    expect(body.workflows[0]).toMatchObject({
      name: "authored",
      type: "authored",
      version: "v2",
    });
    expect(body.workflows[0]?.versions).toEqual(["v1", "v2"]);
    expect(body.workflows[0]?.steps.map((step) => step.name)).toEqual(["first", "second"]);
  });

  it("resolves a registered workflow definition by name", async () => {
    const registry = new WorkflowVersionRegistry();
    const wf = workflow<string>({ name: "registered-only", version: "v1" })
      .step("first", ({ input }) => Pipeline.succeed(input))
      .build();
    registry.register(wf);

    const handler = getWorkflowDef({ versionRegistry: registry });
    const res = await handler(new Request("http://x"), { name: "registered-only" });
    const body = (await res.json()) as { name: string; version: string };

    expect(res.status).toBe(200);
    expect(body).toMatchObject({ name: "registered-only", version: "v1" });
  });
});
