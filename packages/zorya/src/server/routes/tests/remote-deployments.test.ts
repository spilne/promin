// ---------------------------------------------------------------------------
// Remote-deployment self-registration routes — register / heartbeat /
// unregister / list. Pins the AgentRegistry side-effects: register
// upserts RemoteAgentBackend recipes, unregister deletes them, sweep
// (via list()) cleans up expired registrations' recipes.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry, InMemoryRemoteDeploymentRegistry } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

interface BootOpts {
  /** Inject a clock so tests can fast-forward across the TTL window. */
  now?: () => number;
}

async function bootGateway(opts: BootOpts = {}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const agentRegistry = new InMemoryAgentRegistry();
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const stubAgent = {
    invoke: async () => ({ text: Promise.resolve("ok") }),
    withScope: () => stubAgent,
  };
  const agents = new ZoryaAgents({
    registry: agentRegistry,
    resolve: () => stubAgent as never,
  });
  const remoteDeployments = new InMemoryRemoteDeploymentRegistry(
    opts.now !== undefined ? { now: opts.now } : {},
  );
  const server = new ZoryaServer({
    workflows,
    agents,
    remoteDeployments,
  });
  return { server, agentRegistry, remoteDeployments };
}

describe("remote-deployments — register", () => {
  it("registers + upserts RemoteAgentBackend recipes for each declared agent", async () => {
    const { server, agentRegistry } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          endpoint: "https://server-b.example",
          agents: ["claude-bot", "analyst-bot"],
        }),
      }),
    );
    expect(res.status).toBe(201);
    const body = (await res.json()) as { deploymentId: string; ttlMs: number };
    expect(body.deploymentId).toBeTruthy();
    expect(body.ttlMs).toBeGreaterThan(0);

    // Recipes upserted in the AgentRegistry as remote backends pointing
    // at the supplied endpoint.
    const claude = await agentRegistry.get("claude-bot");
    expect(claude?.backend.type).toBe("remote");
    if (claude?.backend.type === "remote") {
      expect(claude.backend.endpoint).toBe("https://server-b.example");
      expect(claude.backend.remoteAgentId).toBe("claude-bot");
    }
    const analyst = await agentRegistry.get("analyst-bot");
    expect(analyst?.backend.type).toBe("remote");
  });

  it("preserves auth on the upserted recipes when supplied", async () => {
    const { server, agentRegistry } = await bootGateway();
    await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          endpoint: "https://x",
          agents: ["a"],
          auth: { kind: "bearer", token: "tok-123" },
        }),
      }),
    );
    const a = await agentRegistry.get("a");
    if (a?.backend.type === "remote") {
      expect(a.backend.auth?.token).toBe("tok-123");
    }
  });

  it("400 on missing endpoint / empty agents / invalid auth", async () => {
    const { server } = await bootGateway();

    const noEndpoint = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ agents: ["a"] }),
      }),
    );
    expect(noEndpoint.status).toBe(400);

    const emptyAgents = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://x", agents: [] }),
      }),
    );
    expect(emptyAgents.status).toBe(400);

    const badAuth = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          endpoint: "https://x",
          agents: ["a"],
          auth: { kind: "basic" },
        }),
      }),
    );
    expect(badAuth.status).toBe(400);
  });
});

describe("remote-deployments — heartbeat", () => {
  it("updates lastHeartbeat for a known deployment", async () => {
    let nowMs = 1_700_000_000_000;
    const { server } = await bootGateway({ now: () => nowMs });
    const reg = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://x", agents: ["a"] }),
      }),
    );
    const { deploymentId } = (await reg.json()) as { deploymentId: string };

    nowMs += 30_000;
    const hb = await server.handle(
      new Request(
        `http://test/api/remote-deployments/${encodeURIComponent(deploymentId)}/heartbeat`,
        { method: "POST" },
      ),
    );
    expect(hb.status).toBe(200);
    const body = (await hb.json()) as { lastHeartbeat: number };
    expect(body.lastHeartbeat).toBe(nowMs);
  });

  it("returns 410 when the deploymentId is unknown", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/remote-deployments/missing/heartbeat", { method: "POST" }),
    );
    expect(res.status).toBe(410);
  });
});

describe("remote-deployments — unregister", () => {
  it("removes the registration AND deletes the upserted recipes", async () => {
    const { server, agentRegistry } = await bootGateway();
    const reg = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://x", agents: ["a", "b"] }),
      }),
    );
    const { deploymentId } = (await reg.json()) as { deploymentId: string };

    expect(await agentRegistry.get("a")).not.toBeNull();
    expect(await agentRegistry.get("b")).not.toBeNull();

    const del = await server.handle(
      new Request(`http://test/api/remote-deployments/${encodeURIComponent(deploymentId)}`, {
        method: "DELETE",
      }),
    );
    expect(del.status).toBe(204);
    expect(await agentRegistry.get("a")).toBeNull();
    expect(await agentRegistry.get("b")).toBeNull();
  });

  it("idempotent — 204 even when the deployment is unknown", async () => {
    const { server } = await bootGateway();
    const res = await server.handle(
      new Request("http://test/api/remote-deployments/never", { method: "DELETE" }),
    );
    expect(res.status).toBe(204);
  });
});

describe("remote-deployments — list + lazy sweep", () => {
  it("returns all live deployments", async () => {
    const { server } = await bootGateway();
    await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://a", agents: ["x"] }),
      }),
    );
    await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://b", agents: ["y"] }),
      }),
    );
    const res = await server.handle(
      new Request("http://test/api/remote-deployments", { method: "GET" }),
    );
    const body = (await res.json()) as { deployments: Array<{ endpoint: string }> };
    expect(body.deployments.map((d) => d.endpoint).sort()).toEqual(["https://a", "https://b"]);
  });

  it("expires stale registrations on list() and cleans up their recipes", async () => {
    let nowMs = 1_700_000_000_000;
    const { server, agentRegistry } = await bootGateway({ now: () => nowMs });
    await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://x", agents: ["a"], ttlMs: 5_000 }),
      }),
    );
    expect(await agentRegistry.get("a")).not.toBeNull();

    nowMs += 10_000;
    const res = await server.handle(
      new Request("http://test/api/remote-deployments", { method: "GET" }),
    );
    const body = (await res.json()) as { deployments: unknown[] };
    expect(body.deployments).toEqual([]);
    // Recipe was cleaned up by the lazy sweep.
    expect(await agentRegistry.get("a")).toBeNull();
  });
});

describe("remote-deployments — when not configured", () => {
  it("routes are not mounted (404)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows }); // no remoteDeployments
    const res = await server.handle(
      new Request("http://test/api/remote-deployments/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ endpoint: "https://x", agents: ["a"] }),
      }),
    );
    expect(res.status).toBe(404);
  });
});
