import { describe, it, expect } from "bun:test";
import {
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  InMemoryWorkflowStorage,
} from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";
import { DistributedWorkflows } from "../../index.ts";

function makeServer(
  opts: {
    dashboardKeys?: ReadonlyArray<string>;
    workerKeys?: ReadonlyArray<string>;
  } = {},
): ZoryaServer {
  const workflows = new DistributedWorkflows({
    storage: new InMemoryWorkflowStorage(),
    stepQueue: new InMemoryStepQueue(),
    workerRegistry: new InMemoryWorkerRegistry(),
  });
  return new ZoryaServer({
    workflows,
    ...(opts.dashboardKeys && { apiKeys: opts.dashboardKeys }),
    remoteWorkers: opts.workerKeys ? { apiKeys: opts.workerKeys } : {},
  });
}

describe("ZoryaServer worker-protocol auth", () => {
  it("allows worker endpoints when no keys set", async () => {
    const server = makeServer();
    const res = await server.handle(
      new Request("http://x/rpc/worker", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ method: "listWorkers", params: {} }),
      }),
    );
    expect(res.status).toBe(200);
  });

  it("rejects worker calls without a key when keys are configured", async () => {
    const server = makeServer({ workerKeys: ["worker-key"] });
    const res = await server.handle(
      new Request("http://x/rpc/worker", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ method: "listWorkers", params: {} }),
      }),
    );
    expect(res.status).toBe(401);
  });

  it("accepts worker calls with a valid bearer token", async () => {
    const server = makeServer({ workerKeys: ["worker-key"] });
    const res = await server.handle(
      new Request("http://x/rpc/worker", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: "Bearer worker-key",
        },
        body: JSON.stringify({ method: "listWorkers", params: {} }),
      }),
    );
    expect(res.status).toBe(200);
  });

  it("protects /api/advertisements with the worker keyset, not dashboard keyset", async () => {
    const server = makeServer({
      dashboardKeys: ["dashboard-key"],
      workerKeys: ["worker-key"],
    });

    const dashRes = await server.handle(
      new Request("http://x/api/advertisements", {
        headers: { authorization: "Bearer dashboard-key" },
      }),
    );
    expect(dashRes.status).toBe(401);

    const workerRes = await server.handle(
      new Request("http://x/api/advertisements", {
        headers: { authorization: "Bearer worker-key" },
      }),
    );
    expect(workerRes.status).toBe(200);

    const runsUnauth = await server.handle(new Request("http://x/api/runs"));
    expect(runsUnauth.status).toBe(401);
    const runsOk = await server.handle(
      new Request("http://x/api/runs", { headers: { authorization: "Bearer dashboard-key" } }),
    );
    expect(runsOk.status).toBe(200);
  });
});
