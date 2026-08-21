// ---------------------------------------------------------------------------
// Workflow versions routes — thin surface over WorkflowVersionRegistry's
// lifecycle methods. Verifies promote / rollback / findActive / list
// against the in-memory registry.
// ---------------------------------------------------------------------------

import { beforeEach, describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow, WorkflowVersionRegistry } from "@promin/workflow";
import {
  deleteWorkflowVersion,
  getActiveWorkflowVersion,
  listWorkflowVersions,
  promoteWorkflowVersion,
  rollbackWorkflow,
  type WorkflowVersionDto,
} from "../workflow-versions.ts";

const jsonReq = (body: unknown) =>
  new Request("http://x", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

function makeWf(version: string) {
  return workflow<{ n: number }>({ name: "compute", version })
    .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
    .build();
}

describe("workflow versions routes — registry lifecycle surface", () => {
  let registry: WorkflowVersionRegistry;

  beforeEach(() => {
    registry = new WorkflowVersionRegistry();
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
    const res = await handler(new Request("http://x/api/workflows//versions"), {});
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

    const v1 = registry.getStatus("compute", "v1");
    expect(v1?.status).toBe("inactive");
    const v2 = registry.getStatus("compute", "v2");
    expect(v2?.status).toBe("active");
  });

  it("rollback archives current active and promotes target", async () => {
    const promote = promoteWorkflowVersion({ registry });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v1" });
    await promote(new Request("http://x", { method: "POST" }), { name: "compute", version: "v2" });

    const rollback = rollbackWorkflow({ registry });
    const res = await rollback(jsonReq({ toVersion: "v1" }), { name: "compute" });
    expect(res.status).toBe(200);

    const v2After = registry.getStatus("compute", "v2");
    expect(v2After?.status).toBe("archived");
    expect(v2After?.archivedAt).not.toBeNull();
    const v1After = registry.getStatus("compute", "v1");
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
    expect(res.status).toBe(500);
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
    const res = await rollback(jsonReq({}), { name: "compute" });
    expect(res.status).toBe(400);
  });

  it("deleteWorkflowVersion refuses active version without force", async () => {
    registry.promote("compute", "v2");
    const handler = deleteWorkflowVersion({ registry });
    const res = await handler(new Request("http://x/api/workflows/compute/versions/v2"), {
      name: "compute",
      version: "v2",
    });

    expect(res.status).toBe(409);
    expect(registry.resolve("compute", "v2")).toBeDefined();
  });

  it("deleteWorkflowVersion deregisters inactive or forced versions", async () => {
    registry.promote("compute", "v2");
    const handler = deleteWorkflowVersion({ registry });
    const inactive = await handler(new Request("http://x/api/workflows/compute/versions/v1"), {
      name: "compute",
      version: "v1",
    });
    const active = await handler(
      new Request("http://x/api/workflows/compute/versions/v2?force=true"),
      {
        name: "compute",
        version: "v2",
      },
    );

    expect(inactive.status).toBe(200);
    expect(active.status).toBe(200);
    expect(registry.resolve("compute", "v1")).toBeUndefined();
    expect(registry.resolve("compute", "v2")).toBeUndefined();
  });
});
