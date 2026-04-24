import { describe, it, expect } from "bun:test";
import {
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  InMemoryWorkflowStorage,
} from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";

describe("ZoryaServer worker-protocol auth", () => {
  it("allows worker endpoints when no keys set (backward-compat)", async () => {
    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      workerProtocol: {
        stepQueue: new InMemoryStepQueue(),
        workerRegistry: new InMemoryWorkerRegistry(),
      },
    });
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
    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      workerProtocol: {
        stepQueue: new InMemoryStepQueue(),
        workerRegistry: new InMemoryWorkerRegistry(),
        apiKeys: ["worker-key"],
      },
    });
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
    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      workerProtocol: {
        stepQueue: new InMemoryStepQueue(),
        workerRegistry: new InMemoryWorkerRegistry(),
        apiKeys: ["worker-key"],
      },
    });
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
    const server = new ZoryaServer({
      storage: new InMemoryWorkflowStorage(),
      apiKeys: ["dashboard-key"], // dashboard auth
      workerProtocol: {
        stepQueue: new InMemoryStepQueue(),
        workerRegistry: new InMemoryWorkerRegistry(),
        apiKeys: ["worker-key"],
      },
    });

    // Dashboard key should be rejected on /api/advertisements
    const dashRes = await server.handle(
      new Request("http://x/api/advertisements", {
        headers: { authorization: "Bearer dashboard-key" },
      }),
    );
    expect(dashRes.status).toBe(401);

    // Worker key should be accepted
    const workerRes = await server.handle(
      new Request("http://x/api/advertisements", {
        headers: { authorization: "Bearer worker-key" },
      }),
    );
    expect(workerRes.status).toBe(200);

    // And /api/runs (dashboard surface) still requires the dashboard key
    const runsUnauth = await server.handle(new Request("http://x/api/runs"));
    expect(runsUnauth.status).toBe(401);
    const runsOk = await server.handle(
      new Request("http://x/api/runs", { headers: { authorization: "Bearer dashboard-key" } }),
    );
    expect(runsOk.status).toBe(200);
  });
});
