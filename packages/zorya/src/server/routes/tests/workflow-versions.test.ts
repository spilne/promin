// ---------------------------------------------------------------------------
// Workflow versions routes — thin surface over WorkflowVersionRegistry's
// lifecycle methods. Verifies promote / rollback / findActive / list
// against the in-memory registry.
// ---------------------------------------------------------------------------

import { succeed, sync } from "@spilne/perfect-core";
import { beforeEach, describe, expect, it } from "bun:test";
import {
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { ZoryaServer } from "../../server.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import {
  getActiveWorkflowVersion,
  listWorkflowVersions,
  promoteWorkflowVersion,
  rollbackWorkflow,
  type WorkflowVersionDto,
} from "../workflow-versions.ts";
import { invoke } from "./invoke-handler.ts";

const jsonReq = (body: unknown) =>
  new Request("http://x", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

function makeWf(version: string) {
  return workflow<{ n: number }>({ name: "compute", version })
    .step("double", ({ input }) => succeed(input.n * 2))
    .build();
}

describe("workflow versions routes — registry lifecycle surface", () => {
  let registry: InMemoryWorkflowVersionRegistry;

  beforeEach(() => {
    registry = new InMemoryWorkflowVersionRegistry();
    registry.register(makeWf("v1"));
    registry.register(makeWf("v2"));
  });

  it("listWorkflowVersions returns every registered version", async () => {
    const handler = listWorkflowVersions({ registry });
    const res = await handler(new Request("http://x/api/workflows/compute/versions"), {
      name: "compute",
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { versions: WorkflowVersionDto[] };
    expect(body.versions).toHaveLength(2);
    expect(body.versions.every((d) => d.status === "inactive")).toBe(true);
  });

  it("listWorkflowVersions rejects missing name", async () => {
    const handler = listWorkflowVersions({ registry });
    const res = await invoke(handler, new Request("http://x/api/workflows//versions"), {});
    expect(res.status).toBe(400);
  });

  it("getActiveWorkflowVersion returns 404 before any promote", async () => {
    const handler = getActiveWorkflowVersion({ registry });
    const res = await handler(new Request("http://x"), { name: "compute" });
    expect(res.status).toBe(404);
  });

  it("promote sets a version active and findActive returns it", async () => {
    const promote = promoteWorkflowVersion({ registry });
    const res = await promote(new Request("http://x", { method: "POST" }), {
      name: "compute",
      version: "v2",
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { version: WorkflowVersionDto };
    expect(body.version.version).toBe("v2");
    expect(body.version.status).toBe("active");
    expect(body.version.activeAt).not.toBeNull();

    const active = getActiveWorkflowVersion({ registry });
    const found = await active(new Request("http://x"), { name: "compute" });
    expect(found.status).toBe(200);
    const foundBody = (await found.json()) as WorkflowVersionDto;
    expect(foundBody.version).toBe("v2");
  });

  it("re-promoting demotes prior active to inactive (not archived)", async () => {
    const promote = promoteWorkflowVersion({ registry });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v1" });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v2" });

    const v1 = await registry.getStatus({ name: "compute", version: "v1" });
    expect(v1?.status).toBe("inactive");
    const v2 = await registry.getStatus({ name: "compute", version: "v2" });
    expect(v2?.status).toBe("active");
  });

  it("rollback archives current active and promotes target", async () => {
    const promote = promoteWorkflowVersion({ registry });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v1" });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v2" });

    const rollback = rollbackWorkflow({ registry });
    const res = await rollback(jsonReq({ toVersion: "v1" }), { name: "compute" });
    expect(res.status).toBe(200);

    const v2After = await registry.getStatus({ name: "compute", version: "v2" });
    expect(v2After?.status).toBe("archived");
    expect(v2After?.archivedAt).not.toBeNull();
    const v1After = await registry.getStatus({ name: "compute", version: "v1" });
    expect(v1After?.status).toBe("active");
  });

  it("promote on unregistered version returns 404", async () => {
    const promote = promoteWorkflowVersion({ registry });
    const res = await promote(new Request("http://x", { method: "POST" }), {
      name: "compute",
      version: "v99",
    });
    expect(res.status).toBe(404);
  });

  it("rollback rejects when no active version exists", async () => {
    const rollback = rollbackWorkflow({ registry });
    const res = await rollback(jsonReq({ toVersion: "v1" }), { name: "compute" });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("no_active_version");
  });

  it("listWorkflowVersions returns both versions (order may tie at ms resolution)", async () => {
    const handler = listWorkflowVersions({ registry });
    const res = await handler(new Request("http://x/api/workflows/compute/versions"), {
      name: "compute",
    });
    const body = (await res.json()) as { versions: WorkflowVersionDto[] };
    const versions = body.versions.map((d) => d.version).sort();
    expect(versions).toEqual(["v1", "v2"]);
  });

  it("rollback validates required body fields", async () => {
    const rollback = rollbackWorkflow({ registry });
    const res = await invoke(rollback, jsonReq({}), { name: "compute" });
    expect(res.status).toBe(400);
  });
});

describe("version routes through ZoryaServer — one registry with the runner", () => {
  function versioned(version: string, ran: string[]) {
    return workflow<unknown>({ name: "compute", version })
      .step("body", () => sync(() => ran.push(version)))
      .build() as Workflow<unknown, unknown>;
  }

  async function boot(params: { withRegistry: boolean; apiKeys?: string[] }) {
    const ran: string[] = [];
    const storage = new InMemoryWorkflowStorage();
    const registry = new InMemoryWorkflowVersionRegistry();
    // The host registered the old version; the current one comes from
    // `definitions` and is registered on first use.
    if (params.withRegistry) await registry.register(versioned("v1", ran));
    const workflows = new LocalWorkflows({
      storage,
      runner: createWorkflowRunner({ storage }),
      definitions: { compute: versioned("v2", ran) },
      ...(params.withRegistry && { versionRegistry: registry }),
      sleepScanIntervalMs: 0,
      signalScanIntervalMs: 0,
    });
    const server = new ZoryaServer({
      workflows,
      ...(params.apiKeys ? { apiKeys: params.apiKeys } : { insecure: true }),
    });
    const auth: Record<string, string> = params.apiKeys
      ? { authorization: `Bearer ${params.apiKeys[0]}` }
      : {};
    const send = (method: string, path: string, body?: unknown) =>
      server.handle(
        new Request(`http://x${path}`, {
          method,
          headers: { ...auth, "content-type": "application/json" },
          ...(body !== undefined && { body: JSON.stringify(body) }),
        }),
      );
    return { ran, storage, registry, server, send };
  }

  it("lists the definitions' versions, and a promote changes what new runs execute", async () => {
    const { send, ran, storage, server, registry } = await boot({ withRegistry: true });
    expect(server.versionRegistry).toBe(registry);

    const listed = (await (await send("GET", "/api/workflows/compute/versions")).json()) as {
      versions: WorkflowVersionDto[];
    };
    expect(listed.versions.map((v) => v.version).sort()).toEqual(["v1", "v2"]);

    const promoted = await send("POST", "/api/workflows/compute/versions/v1/promote");
    expect(promoted.status).toBe(200);
    const started = await send("POST", "/api/workflows/compute/runs", { workflowId: "r1" });
    expect(started.status).toBe(202);
    for (let i = 0; i < 2_000 && (await storage.loadWorkflow("r1"))?.status !== "completed"; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    expect(ran).toEqual(["v1"]);
    expect((await storage.loadWorkflow("r1"))?.version).toBe("v1");

    const rolled = await send("POST", "/api/workflows/compute/rollback", { toVersion: "v2" });
    expect(rolled.status).toBe(200);
    expect((await registry.findActive("compute"))?.version).toBe("v2");
  });

  it("maps registry errors: 404 unknown version, 409 nothing to roll back from", async () => {
    const { send } = await boot({ withRegistry: true });
    const unknown = await send("POST", "/api/workflows/compute/versions/v9/promote");
    expect(unknown.status).toBe(404);
    expect(((await unknown.json()) as { error: string }).error).toBe("version_not_registered");

    const noActive = await send("POST", "/api/workflows/compute/rollback", { toVersion: "v1" });
    expect(noActive.status).toBe(409);
    expect(((await noActive.json()) as { error: string }).error).toBe("no_active_version");

    const unregistered = await send("POST", "/api/workflows/compute/rollback", {
      toVersion: "v9",
    });
    expect(unregistered.status).toBe(404);
    expect((await send("POST", "/api/workflows/compute/rollback", {})).status).toBe(400);
  });

  it("without a registry: lists nothing and refuses changes with 501", async () => {
    const { send, server } = await boot({ withRegistry: false });
    expect(server.versionRegistry).toBeUndefined();
    const listed = await send("GET", "/api/workflows/compute/versions");
    expect(listed.status).toBe(200);
    expect(await listed.json()).toEqual({ versions: [] });
    const promoted = await send("POST", "/api/workflows/compute/versions/v2/promote");
    expect(promoted.status).toBe(501);
    expect(((await promoted.json()) as { error: string }).error).toBe("versioning_not_configured");
  });

  it("needs the operator key", async () => {
    const { server, send } = await boot({ withRegistry: true, apiKeys: ["k-0123456789"] });
    const anonymous = await server.handle(
      new Request("http://x/api/workflows/compute/versions/v1/promote", { method: "POST" }),
    );
    expect(anonymous.status).toBe(401);
    expect((await send("POST", "/api/workflows/compute/versions/v1/promote")).status).toBe(200);
  });
});
