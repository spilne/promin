import { describe, expect, it } from "bun:test";
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaServer, ZoryaWorkflowBuilder } from "../../src/index.ts";
import {
  createDemoWorkflowStepCatalog,
  demoWorkflowSteps,
  type PostgresQueryClient,
} from "../workflow-steps/index.ts";

describe("demo workflow step libraries", () => {
  it("composes file-based step arrays into the demo catalog", () => {
    const catalog = createDemoWorkflowStepCatalog({ now: () => new Date("2026-08-15T00:00:00Z") });
    const ids = catalog.entries().map((entry) => entry.id);

    expect(ids).toContain("source.input");
    expect(ids).toContain("json.pick");
    expect(ids).toContain("transform.uppercase");
    expect(ids).toContain("text.template");
    expect(ids).toContain("transform.concat");
    expect(ids).toContain("control.if");
    expect(ids).toContain("control.parallel");
    expect(ids).toContain("predicate.long");
    expect(ids).toContain("system.now");
  });

  it("only exposes dependency-backed steps when the host provides the dependency", () => {
    expect(demoWorkflowSteps().map((step) => step.id)).not.toContain("postgres.query");

    const postgres: PostgresQueryClient = {
      async queryJson() {
        return [{ ok: true }];
      },
    };
    const ids = demoWorkflowSteps({ postgres }).map((step) => step.id);

    expect(ids).toContain("postgres.query");
  });

  it("serves demo step metadata through the workflow-builder API", async () => {
    const storage = new InMemoryWorkflowStorage();
    const server = new ZoryaServer({
      workflows: new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
      }),
      workflowBuilder: new ZoryaWorkflowBuilder({
        catalog: createDemoWorkflowStepCatalog(),
        versionRegistry: new WorkflowVersionRegistry(),
      }),
    });

    const res = await server.handle(new Request("http://test/api/workflow-builder/steps"));
    const body = (await res.json()) as { steps: Array<{ id: string }> };

    expect(res.status).toBe(200);
    expect(body.steps.map((step) => step.id)).toContain("source.input");
    expect(body.steps.map((step) => step.id)).toContain("text.template");
    expect(body.steps.map((step) => step.id)).toContain("control.if");
    expect(body.steps.map((step) => step.id)).toContain("control.parallel");
  });
});
