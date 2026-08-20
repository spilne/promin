import { describe, it, expect, beforeEach } from "bun:test";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  createWorkflowStepCatalog,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import { Pipeline } from "@promin/core";
import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { ZoryaServer } from "../../server/server.ts";
import { LocalWorkflows, QueuedWorkflows } from "../../index.ts";
import { ZoryaWorkflowBuilder } from "../../server/services/workflow-builder.ts";

function makeWorkflows(storage: InMemoryWorkflowStorage) {
  return new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
}
import type {
  RunDto,
  RunListResponse,
  TriggerRunResponse,
  MetricsDto,
  WorkersResponse,
} from "../../server/api-types.ts";
import type { NamespacesResponse } from "../../server/routes/namespaces.ts";

async function seedWorkflow(
  storage: InMemoryWorkflowStorage,
  id: string,
  name: string,
  type?: string,
  namespace?: string,
): Promise<void> {
  await storage.createWorkflow({
    workflowId: id,
    workflowName: name,
    workflowType: type,
    ...(namespace !== undefined && { namespace }),
    input: { hello: "world" },
  });
}

describe("ZoryaServer", () => {
  let storage: InMemoryWorkflowStorage;
  let server: ZoryaServer;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
    server = new ZoryaServer({ workflows: makeWorkflows(storage) });
  });

  describe("GET /api/health", () => {
    it("returns ok", async () => {
      const res = await server.handle(new Request("http://x/api/health"));
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ ok: true });
    });

    it("does not serve the SPA fallback for missing API routes", async () => {
      const uiDir = await mkdtemp(join(tmpdir(), "zorya-ui-"));
      await Bun.write(join(uiDir, "index.html"), "<!doctype html><div>ui</div>");
      const serverWithUi = new ZoryaServer({ workflows: makeWorkflows(storage), uiDir });

      const api = await serverWithUi.handle(new Request("http://x/api/does-not-exist"));
      expect(api.status).toBe(404);
      expect(api.headers.get("content-type")).toContain("application/json");
      expect(await api.json()).toEqual({ error: "not_found" });

      const app = await serverWithUi.handle(new Request("http://x/workflow-builder"));
      expect(app.status).toBe(200);
      expect(await app.text()).toContain("<!doctype html>");
    });
  });

  describe("workflow builder registry wiring", () => {
    it("uses the workflow builder registry for workflow definitions when no server registry is passed", async () => {
      const registry = new WorkflowVersionRegistry();
      const builder = new ZoryaWorkflowBuilder({
        versionRegistry: registry,
        catalog: createWorkflowStepCatalog([
          {
            id: "transform.uppercase",
            title: "Uppercase",
            activity: () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),
          },
        ]),
      });
      await builder.save({
        version: "v1",
        schema: {
          version: 1,
          name: "authored-upper",
          inputSchema: {
            type: "object",
            properties: {
              text: { type: "string", default: "hello builder" },
              urgent: { type: "boolean", default: false },
            },
          },
          steps: [
            {
              type: "step",
              name: "upper",
              dependsOn: [],
              activityRef: "transform.uppercase",
            },
          ],
        },
      });
      await builder.publish("authored-upper");

      const s = new ZoryaServer({
        workflows: makeWorkflows(storage),
        workflowBuilder: builder,
      });
      const res = await s.handle(new Request("http://x/api/workflows/definitions"));
      const body = (await res.json()) as {
        workflows: Array<{ name: string; sampleInput?: unknown }>;
      };
      const authored = body.workflows.find((workflow) => workflow.name === "authored-upper");

      expect(res.status).toBe(200);
      expect(body.workflows.map((workflow) => workflow.name)).toContain("authored-upper");
      expect(authored?.sampleInput).toEqual({ text: "hello builder", urgent: false });
    });
  });

  describe("GET /api/runs", () => {
    it("returns empty list when no runs", async () => {
      const res = await server.handle(new Request("http://x/api/runs"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as RunListResponse;
      expect(body.runs).toEqual([]);
    });

    it("returns serialised run summaries", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      await seedWorkflow(storage, "wf-2", "payment");

      const res = await server.handle(new Request("http://x/api/runs"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as RunListResponse;
      expect(body.runs).toHaveLength(2);
      const ids = body.runs.map((r) => r.workflowId);
      expect(ids).toContain("wf-1");
      expect(ids).toContain("wf-2");
      for (const r of body.runs) {
        expect(typeof r.createdAt).toBe("string");
        expect(r.status).toBe("pending");
      }
    });

    it("filters by name", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      await seedWorkflow(storage, "wf-2", "payment");
      const res = await server.handle(new Request("http://x/api/runs?name=order"));
      const body = (await res.json()) as RunListResponse;
      expect(body.runs).toHaveLength(1);
      expect(body.runs[0]!.workflowName).toBe("order");
    });
  });

  describe("GET /api/runs/:id", () => {
    it("returns a single run with steps", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      const res = await server.handle(new Request("http://x/api/runs/wf-1"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as RunDto;
      expect(body.workflowId).toBe("wf-1");
      expect(body.steps).toEqual([]);
    });

    it("returns 404 for missing run", async () => {
      const res = await server.handle(new Request("http://x/api/runs/missing"));
      expect(res.status).toBe(404);
    });
  });

  describe("POST /api/runs/trigger/:name", () => {
    it("returns 400 when workflow is unknown to the configured workflows service", async () => {
      const res = await server.handle(
        new Request("http://x/api/runs/trigger/order", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ input: { a: 1 } }),
        }),
      );
      // LocalWorkflows with empty definitions throws UnknownWorkflowError
      // which the route maps to 400 trigger_failed.
      expect(res.status).toBe(400);
    });

    it("invokes trigger and returns workflow id", async () => {
      // QueuedWorkflows.acceptAny lets us trigger without a definition;
      // the enqueued record is the spy.
      const queue = new (
        await import("../../server/workflow-starts.ts")
      ).InMemoryWorkflowStartQueue();
      const workflows = new QueuedWorkflows({
        storage,
        workflowStarts: queue,
        acceptAny: true,
      });
      const s = new ZoryaServer({ workflows });
      const res = await s.handle(
        new Request("http://x/api/runs/trigger/order", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ input: { a: 1 } }),
        }),
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as TriggerRunResponse;
      expect(body.workflowId).toBeDefined();
      const enqueued = await queue.list();
      expect(enqueued.length).toBe(1);
      expect(enqueued[0]?.workflowName).toBe("order");
      expect(enqueued[0]?.input).toEqual({ a: 1 });
      expect(enqueued[0]?.namespace).toBe("default");
    });

    it("rejects explicit namespaces that are not registered", async () => {
      const queue = new (
        await import("../../server/workflow-starts.ts")
      ).InMemoryWorkflowStartQueue();
      const workflows = new QueuedWorkflows({
        storage,
        workflowStarts: queue,
        acceptAny: true,
      });
      const s = new ZoryaServer({ workflows });
      const res = await s.handle(
        new Request("http://x/api/runs/trigger/order", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ namespace: "missing" }),
        }),
      );
      expect(res.status).toBe(404);
      expect(await queue.list()).toEqual([]);
    });
  });

  describe("POST /api/runs/:id/signal", () => {
    it("returns 400 when no signal name", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      const res = await server.handle(
        new Request("http://x/api/runs/wf-1/signal", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ payload: 1 }),
        }),
      );
      expect(res.status).toBe(400);
    });

    it("delivers signal via storage", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      const res = await server.handle(
        new Request("http://x/api/runs/wf-1/signal", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ signalName: "approve", payload: { ok: true } }),
        }),
      );
      expect(res.status).toBe(200);
    });
  });

  describe("GET /api/metrics", () => {
    it("reports counts across statuses", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      await seedWorkflow(storage, "wf-2", "order");
      const res = await server.handle(new Request("http://x/api/metrics"));
      const m = (await res.json()) as MetricsDto;
      expect(m.total).toBe(2);
      expect(m.byStatus.pending).toBe(2);
      expect(m.byStatus.completed).toBe(0);
    });
  });

  describe("GET /api/workers", () => {
    it("returns empty list by default", async () => {
      const res = await server.handle(new Request("http://x/api/workers"));
      const body = (await res.json()) as WorkersResponse;
      expect(body.workers).toEqual([]);
    });

    it("uses configured provider", async () => {
      const s = new ZoryaServer({
        workflows: makeWorkflows(storage),
        workers: {
          listWorkers: async () => [
            {
              workerId: "w-1",
              status: "online",
              queue: "default",
              activeTasks: 2,
              completedToday: 10,
            },
          ],
        },
      });
      const res = await s.handle(new Request("http://x/api/workers"));
      const body = (await res.json()) as WorkersResponse;
      expect(body.workers).toHaveLength(1);
      expect(body.workers[0]!.workerId).toBe("w-1");
    });
  });

  describe("namespaces", () => {
    it("prebuilds and lists the default namespace", async () => {
      const res = await server.handle(new Request("http://x/api/namespaces"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as NamespacesResponse;
      expect(body.defaultNamespaceId).toBe("default");
      expect(body.namespaces.map((ns) => ns.id)).toContain("default");
      expect(body.namespaces.find((ns) => ns.id === "default")?.capabilities).toEqual({});
    });

    it("backfills namespaces already present in workflow storage", async () => {
      await seedWorkflow(storage, "wf-tenant", "order", undefined, "tenant-a");
      const res = await server.handle(new Request("http://x/api/namespaces"));
      expect(res.status).toBe(200);
      const body = (await res.json()) as NamespacesResponse;
      expect(body.namespaces.map((ns) => ns.id)).toContain("tenant-a");
    });

    it("creates a namespace with capability policy", async () => {
      const create = await server.handle(
        new Request("http://x/api/namespaces", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            id: "acme",
            displayName: "Acme",
            capabilities: { ai: { enabled: true, maxTokensPerTurn: 8192 } },
          }),
        }),
      );
      expect(create.status).toBe(201);

      const res = await server.handle(new Request("http://x/api/namespaces"));
      const body = (await res.json()) as NamespacesResponse;
      const acme = body.namespaces.find((ns) => ns.id === "acme");
      expect(acme?.displayName).toBe("Acme");
      expect(acme?.capabilities.ai?.maxTokensPerTurn).toBe(8192);
    });

    it("rejects invalid namespace status filters", async () => {
      const res = await server.handle(new Request("http://x/api/namespaces?status=deleted"));
      expect(res.status).toBe(400);
      expect((await res.json()) as { error: string }).toEqual({
        error: "invalid_namespace_status",
      });
    });

    it("rejects malformed capability policy", async () => {
      const res = await server.handle(
        new Request("http://x/api/namespaces", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            id: "bad",
            capabilities: { ai: { maxTokensPerTurn: "lots" } },
          }),
        }),
      );
      expect(res.status).toBe(400);
      expect((await res.json()) as { error: string }).toEqual({
        error: "invalid_namespace_capabilities",
      });
    });

    it("rejects explicit run filters for unknown namespaces", async () => {
      const res = await server.handle(new Request("http://x/api/runs?namespace=missing"));
      expect(res.status).toBe(404);
      expect((await res.json()) as { error: string }).toEqual({ error: "namespace_not_found" });
    });
  });

  describe("auth", () => {
    it("rejects /api requests with no key when auth enabled", async () => {
      const s = new ZoryaServer({ workflows: makeWorkflows(storage), apiKeys: ["secret"] });
      const res = await s.handle(new Request("http://x/api/runs"));
      expect(res.status).toBe(401);
    });

    it("accepts request with valid bearer token", async () => {
      const s = new ZoryaServer({ workflows: makeWorkflows(storage), apiKeys: ["secret"] });
      const res = await s.handle(
        new Request("http://x/api/runs", {
          headers: { authorization: "Bearer secret" },
        }),
      );
      expect(res.status).toBe(200);
    });

    it("rejects invalid bearer token", async () => {
      const s = new ZoryaServer({ workflows: makeWorkflows(storage), apiKeys: ["secret"] });
      const res = await s.handle(
        new Request("http://x/api/runs", {
          headers: { authorization: "Bearer wrong" },
        }),
      );
      expect(res.status).toBe(401);
    });
  });

  describe("cancel", () => {
    it("returns ok when storage supports cancel", async () => {
      await seedWorkflow(storage, "wf-1", "order");
      const res = await server.handle(
        new Request("http://x/api/runs/wf-1/cancel", { method: "POST" }),
      );
      expect(res.status).toBe(200);
    });
  });
});
