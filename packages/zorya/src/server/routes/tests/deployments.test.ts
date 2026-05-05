// ---------------------------------------------------------------------------
// Deployments routes — thin surface over WorkflowVersionRegistry's
// lifecycle methods. Verifies promote / rollback / findActive / list
// against the in-memory registry.
// ---------------------------------------------------------------------------

import { beforeEach, describe, expect, it } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow, WorkflowVersionRegistry } from "@promin/workflow";
import {
  getActiveDeployment,
  listDeployments,
  promoteDeployment,
  rollbackDeployment,
  type DeploymentDto,
} from "../deployments.ts";

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

describe("deployments routes — registry lifecycle surface", () => {
  let registry: WorkflowVersionRegistry;

  beforeEach(() => {
    registry = new WorkflowVersionRegistry();
    registry.register(makeWf("v1"));
    registry.register(makeWf("v2"));
  });

  it("listDeployments returns every registered version", async () => {
    const handler = listDeployments({ registry });
    const res = await handler(new Request("http://x/api/deployments?name=compute"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { deployments: DeploymentDto[] };
    expect(body.deployments).toHaveLength(2);
    expect(body.deployments.every((d) => d.status === "inactive")).toBe(true);
  });

  it("listDeployments rejects missing name", async () => {
    const handler = listDeployments({ registry });
    const res = await handler(new Request("http://x/api/deployments"));
    expect(res.status).toBe(400);
  });

  it("findActive returns 404 before any promote", async () => {
    const handler = getActiveDeployment({ registry });
    const res = await handler(new Request("http://x"), { name: "compute" });
    expect(res.status).toBe(404);
  });

  it("promote sets a version active and findActive returns it", async () => {
    const promote = promoteDeployment({ registry });
    const res = await promote(jsonReq({ version: "v2" }), { name: "compute" });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { deployment: DeploymentDto };
    expect(body.deployment.version).toBe("v2");
    expect(body.deployment.status).toBe("active");
    expect(body.deployment.activeAt).not.toBeNull();

    const active = getActiveDeployment({ registry });
    const found = await active(new Request("http://x"), { name: "compute" });
    expect(found.status).toBe(200);
    const foundBody = (await found.json()) as DeploymentDto;
    expect(foundBody.version).toBe("v2");
  });

  it("re-promoting demotes prior active to inactive (not archived)", async () => {
    const promote = promoteDeployment({ registry });
    await promote(jsonReq({ version: "v1" }), { name: "compute" });
    await promote(jsonReq({ version: "v2" }), { name: "compute" });

    const v1 = registry.getStatus("compute", "v1");
    expect(v1?.status).toBe("inactive");
    const v2 = registry.getStatus("compute", "v2");
    expect(v2?.status).toBe("active");
  });

  it("rollback archives current active and promotes target", async () => {
    const promote = promoteDeployment({ registry });
    await promote(jsonReq({ version: "v1" }), { name: "compute" });
    await promote(jsonReq({ version: "v2" }), { name: "compute" });

    const rollback = rollbackDeployment({ registry });
    const res = await rollback(jsonReq({ toVersion: "v1" }), { name: "compute" });
    expect(res.status).toBe(200);

    const v2After = registry.getStatus("compute", "v2");
    expect(v2After?.status).toBe("archived");
    expect(v2After?.archivedAt).not.toBeNull();
    const v1After = registry.getStatus("compute", "v1");
    expect(v1After?.status).toBe("active");
  });

  it("promote on unregistered version returns 404", async () => {
    const promote = promoteDeployment({ registry });
    const res = await promote(jsonReq({ version: "v99" }), { name: "compute" });
    expect(res.status).toBe(404);
  });

  it("rollback rejects when no active version exists", async () => {
    const rollback = rollbackDeployment({ registry });
    // Nothing promoted — rollback should fail.
    const res = await rollback(jsonReq({ toVersion: "v1" }), { name: "compute" });
    expect(res.status).toBe(500);
  });

  it("listDeployments returns both versions (order may tie at ms resolution)", async () => {
    const handler = listDeployments({ registry });
    const res = await handler(new Request("http://x/api/deployments?name=compute"));
    const body = (await res.json()) as { deployments: DeploymentDto[] };
    const versions = body.deployments.map((d) => d.version).sort();
    expect(versions).toEqual(["v1", "v2"]);
  });

  it("promote validates required body fields", async () => {
    const promote = promoteDeployment({ registry });
    const res = await promote(jsonReq({}), { name: "compute" });
    expect(res.status).toBe(400);
  });
});
