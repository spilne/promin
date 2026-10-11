// ---------------------------------------------------------------------------
// Workflow-builder HTTP routes — end-to-end against a real ZoryaServer with
// a ZoryaWorkflowBuilder over LocalWorkflows. Pins the CRUD surface, the
// 400s for invalid graphs, the 409s for stale revisions and in-use deletes,
// and that published workflows run, list and version through the server's
// ordinary workflow routes.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import {
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  MapActivityCatalog,
  createWorkflowRunner,
  type WorkflowSchema,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaWorkflowBuilder } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

function bootServer() {
  const storage = new InMemoryWorkflowStorage();
  const versionRegistry = new InMemoryWorkflowVersionRegistry();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    versionRegistry,
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
    orphanRecovery: false,
  });
  const workflowBuilder = new ZoryaWorkflowBuilder({
    workflows,
    activities: new MapActivityCatalog([
      {
        id: "text.upper",
        title: "Uppercase",
        category: "Text",
        inputSchema: { type: "string" },
        outputSchema: { type: "string" },
        factory: () => (ctx) => succeed(String(ctx.prev).toUpperCase()),
      },
    ]),
  });
  const server = new ZoryaServer({ insecure: true, workflows, workflowBuilder });
  return { server, storage, workflows };
}

function send(method: string, path: string, body?: unknown): Request {
  return new Request(`http://test${path}`, {
    method,
    ...(body !== undefined && {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
}

async function call(server: ZoryaServer, req: Request): Promise<{ status: number; body: any }> {
  const res = await server.handle(req);
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

const greet: WorkflowSchema = {
  version: 1,
  name: "greet",
  inputSchema: { type: "string", default: "hello builder" },
  steps: [{ type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" }],
};

async function waitForRun(server: ZoryaServer, id: string): Promise<any> {
  for (let i = 0; i < 500; i++) {
    const { body } = await call(server, send("GET", `/api/runs/${id}`));
    if (body?.status === "completed" || body?.status === "failed") return body;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error(`run ${id} did not end`);
}

describe("workflow-builder routes", () => {
  it("serves the activity palette", async () => {
    const { server } = bootServer();
    const { status, body } = await call(server, send("GET", "/api/workflow-builder/activities"));
    expect(status).toBe(200);
    expect(body.activities.map((a: { id: string }) => a.id)).toEqual(["text.upper"]);
    expect(body.activities[0]).not.toHaveProperty("factory");
  });

  it("creates, saves, publishes, runs and deletes a workflow", async () => {
    const { server } = bootServer();
    const created = await call(
      server,
      send("POST", "/api/workflow-builder/workflows", { schema: greet }),
    );
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({ name: "greet", revision: 1, unpublishedChanges: true });

    const saved = await call(
      server,
      send("PUT", "/api/workflow-builder/workflows/greet", {
        schema: { ...greet, ui: { upper: { x: 10, y: 20 } } },
        revision: 1,
      }),
    );
    expect(saved.status).toBe(200);
    expect(saved.body.revision).toBe(2);

    const published = await call(
      server,
      send("POST", "/api/workflow-builder/workflows/greet/publish", { revision: 2 }),
    );
    expect(published.status).toBe(201);
    expect(published.body).toMatchObject({ version: "1", created: true });
    const republished = await call(
      server,
      send("POST", "/api/workflow-builder/workflows/greet/publish"),
    );
    expect(republished.status).toBe(200);
    expect(republished.body.created).toBe(false);

    // The ordinary workflow surface sees it: definitions (with an inferred
    // sample input), versions, and the trigger route.
    const defs = await call(server, send("GET", "/api/workflows/definitions"));
    expect(defs.body.workflows).toContainEqual(
      expect.objectContaining({ name: "greet", version: "1", sampleInput: "hello builder" }),
    );
    const versions = await call(server, send("GET", "/api/workflows/greet/versions"));
    expect(versions.body.versions).toEqual([
      expect.objectContaining({ version: "1", status: "active" }),
    ]);
    const trigger = await call(server, send("POST", "/api/workflows/greet/runs", { input: "hi" }));
    expect(trigger.status).toBe(202);
    const run = await waitForRun(server, trigger.body.workflowId);
    expect(run).toMatchObject({ status: "completed", result: "HI" });

    const list = await call(server, send("GET", "/api/workflow-builder/workflows"));
    expect(list.body.workflows).toEqual([
      expect.objectContaining({ name: "greet", latestVersion: "1", activeVersion: "1" }),
    ]);

    const deleted = await server.handle(send("DELETE", "/api/workflow-builder/workflows/greet"));
    expect(deleted.status).toBe(204);
    const gone = await call(server, send("GET", "/api/workflow-builder/workflows/greet"));
    expect(gone).toMatchObject({ status: 404, body: { error: "builder_workflow_not_found" } });
    const noRuns = await call(server, send("POST", "/api/workflows/greet/runs", { input: "x" }));
    expect(noRuns.status).toBe(404);
  });

  it("answers invalid graphs with 400 invalid_workflow and one issue per problem", async () => {
    const { server } = bootServer();
    const res = await call(
      server,
      send("POST", "/api/workflow-builder/workflows", {
        schema: {
          version: 1,
          name: "broken",
          steps: [
            { type: "step", name: "a", dependsOn: ["b"], activityRef: "text.upper" },
            { type: "step", name: "b", dependsOn: ["a"], activityRef: "missing.activity" },
            { type: "step", name: "c", dependsOn: ["ghost"], activityRef: "text.upper" },
          ],
        },
      }),
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toBe("invalid_workflow");
    const messages = res.body.issues.map((i: { message: string }) => i.message).join("\n");
    expect(messages).toContain('unknown activity "missing.activity"');
    expect(messages).toContain('depends on "ghost" which does not exist');
    expect(messages).toContain('Dependency cycle between steps: "a", "b"');
    expect(res.body.issues.every((i: { path: string }) => i.path === "schema")).toBe(true);

    const structural = await call(
      server,
      send("POST", "/api/workflow-builder/workflows", { schema: { version: 2, name: "x" } }),
    );
    expect(structural.status).toBe(400);
    expect(structural.body.issues.map((i: { path: string }) => i.path)).toEqual(
      expect.arrayContaining(["schema.version", "schema.steps"]),
    );

    const missing = await call(server, send("POST", "/api/workflow-builder/workflows", {}));
    expect(missing).toMatchObject({ status: 400, body: { error: "invalid_request" } });
  });

  it("validates without saving", async () => {
    const { server } = bootServer();
    const ok = await call(
      server,
      send("POST", "/api/workflow-builder/validate", { schema: greet }),
    );
    expect(ok.body).toEqual({ issues: [] });
    const bad = await call(
      server,
      send("POST", "/api/workflow-builder/validate", {
        schema: { ...greet, steps: [{ ...greet.steps[0], activityRef: "nope" }] },
      }),
    );
    expect(bad.status).toBe(200);
    expect(bad.body.issues).toHaveLength(1);
    const list = await call(server, send("GET", "/api/workflow-builder/workflows"));
    expect(list.body.workflows).toEqual([]);
  });

  it("answers a stale revision with 409 revision_conflict and the current revision", async () => {
    const { server } = bootServer();
    await call(server, send("POST", "/api/workflow-builder/workflows", { schema: greet }));
    await call(
      server,
      send("PUT", "/api/workflow-builder/workflows/greet", { schema: greet, revision: 1 }),
    );
    const stale = await call(
      server,
      send("PUT", "/api/workflow-builder/workflows/greet", { schema: greet, revision: 1 }),
    );
    expect(stale.status).toBe(409);
    expect(stale.body).toMatchObject({ error: "revision_conflict", currentRevision: 2 });

    const stalePublish = await call(
      server,
      send("POST", "/api/workflow-builder/workflows/greet/publish", { revision: 1 }),
    );
    expect(stalePublish.status).toBe(409);
  });

  it("answers a taken name with 409 and a save without a revision with 400", async () => {
    const { server } = bootServer();
    await call(server, send("POST", "/api/workflow-builder/workflows", { schema: greet }));
    const again = await call(
      server,
      send("POST", "/api/workflow-builder/workflows", { schema: greet }),
    );
    expect(again).toMatchObject({ status: 409, body: { error: "workflow_exists" } });
    const noRevision = await call(
      server,
      send("PUT", "/api/workflow-builder/workflows/greet", { schema: greet }),
    );
    expect(noRevision.status).toBe(400);
  });

  it("refuses to delete a workflow with active runs (409 workflow_in_use)", async () => {
    const { server, storage } = bootServer();
    const review: WorkflowSchema = {
      version: 1,
      name: "review",
      steps: [
        { type: "step", name: "upper", dependsOn: [], activityRef: "text.upper" },
        { type: "approval", name: "ok", dependsOn: ["upper"] },
      ],
    };
    await call(server, send("POST", "/api/workflow-builder/workflows", { schema: review }));
    await call(server, send("POST", "/api/workflow-builder/workflows/review/publish"));
    const trigger = await call(server, send("POST", "/api/workflows/review/runs", { input: "x" }));
    for (let i = 0; i < 500; i++) {
      if ((await storage.loadWorkflow(trigger.body.workflowId))?.status === "suspended") break;
      await new Promise((r) => setTimeout(r, 0));
    }

    const refused = await call(server, send("DELETE", "/api/workflow-builder/workflows/review"));
    expect(refused.status).toBe(409);
    expect(refused.body).toMatchObject({ error: "workflow_in_use", activeRuns: 1 });
  });

  it("refuses a non-JSON body with 415", async () => {
    const { server } = bootServer();
    const res = await server.handle(
      new Request("http://test/api/workflow-builder/workflows", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "{}",
      }),
    );
    expect(res.status).toBe(415);
  });

  it("is not mounted without a builder", async () => {
    const storage = new InMemoryWorkflowStorage();
    const server = new ZoryaServer({
      insecure: true,
      workflows: new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
        sleepScanIntervalMs: 0,
      }),
    });
    const res = await call(server, send("GET", "/api/workflow-builder/workflows"));
    expect(res.status).toBe(404);
  });

  it("refuses a builder whose LocalWorkflows layer is not the server's", () => {
    const { workflows } = bootServer();
    const storage = new InMemoryWorkflowStorage();
    const other = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const workflowBuilder = new ZoryaWorkflowBuilder({
      workflows,
      activities: new MapActivityCatalog(),
    });
    expect(() => new ZoryaServer({ insecure: true, workflows: other, workflowBuilder })).toThrow(
      /not part of `workflows`/,
    );
  });
});

describe("workflow-builder routes — request size", () => {
  it("answers an oversized schema with a JSON 413 before the route runs", async () => {
    const { server } = bootServer();
    const listening = server.listen({ port: 0, hostname: "127.0.0.1", maxRequestBodySize: 4_096 });
    try {
      const big: WorkflowSchema = {
        ...greet,
        ui: { upper: { x: 0, y: 0, label: "x".repeat(10_000) } },
      };
      const res = await fetch(`http://127.0.0.1:${listening.port}/api/workflow-builder/workflows`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ schema: big }),
      });
      expect(res.status).toBe(413);
      expect(await res.json()).toMatchObject({ error: "payload_too_large" });
    } finally {
      await listening.stop();
    }
  });
});
