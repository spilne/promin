// ---------------------------------------------------------------------------
// Remote-deployment self-registration, through `ZoryaServer.handle`:
//   - register writes owner-tagged RemoteAgentBackend recipes and never
//     shadows an existing agent (409); a same-endpoint restart takes over
//   - unregister / expiry delete only the recipe versions the deployment
//     still owns
//   - bearer tokens are redacted from every GET
//   - the endpoint policy blocks private / metadata targets (SSRF)
//   - expiry runs on the clock-driven sweeper and an expired deployment is
//     never revived by a heartbeat
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryRemoteDeploymentRegistry,
  type OutboundPolicy,
} from "@promin/agent";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import { REDACTED_SECRET } from "../agent-gateway-kit.ts";
import { RemoteDeploymentSweeper } from "../remote-deployments.ts";

const localBackend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "Helpful", tools: [] as string[] } },
};

/** Fake DNS: every `*.example` host resolves to a public address. */
const PUBLIC_DNS: OutboundPolicy = {
  resolveHost: async (host) => {
    if (host === "rebind.example") return ["10.0.0.5"];
    if (host.endsWith(".example")) return ["93.184.216.34"];
    throw new Error("NXDOMAIN");
  },
};

async function boot(opts: { endpointPolicy?: OutboundPolicy } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const agentRegistry = new InMemoryAgentRegistry({ clock });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry: agentRegistry,
    resolve: () => {
      throw new Error("unused");
    },
  });
  const deployments = new InMemoryRemoteDeploymentRegistry({ clock });
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    clock,
    remoteDeployments: {
      registry: deployments,
      endpointPolicy: opts.endpointPolicy ?? PUBLIC_DNS,
      sweepIntervalMs: 1_000,
    },
  });
  return { server, agentRegistry, deployments, clock };
}

function register(server: ZoryaServer, body: unknown): Promise<Response> {
  return server.handle(
    new Request("http://test/api/remote-deployments/register", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
}

async function registerOk(server: ZoryaServer, body: unknown): Promise<string> {
  const res = await register(server, body);
  expect(res.status).toBe(201);
  return ((await res.json()) as { deploymentId: string }).deploymentId;
}

const get = (server: ZoryaServer, path: string) => server.handle(new Request(`http://test${path}`));

describe("remote-deployments — register", () => {
  it("writes an owner-tagged RemoteAgentBackend recipe per declared agent", async () => {
    const { server, agentRegistry } = await boot();
    const id = await registerOk(server, {
      endpoint: "https://server-b.example/",
      agents: ["claude-bot", "analyst-bot"],
      ttlMs: 30_000,
    });
    const claude = await agentRegistry.get("claude-bot");
    expect(claude?.backend).toEqual({
      type: "remote",
      endpoint: "https://server-b.example",
      remoteAgentId: "claude-bot",
    });
    expect(claude?.metadata.tags).toContain(`remote-deployment:${id}`);
    expect((await agentRegistry.get("analyst-bot"))?.backend.type).toBe("remote");
  });

  it("409s instead of shadowing a local agent, and writes nothing", async () => {
    const { server, agentRegistry, deployments } = await boot();
    await agentRegistry.register({ id: "support", backend: localBackend });
    const res = await register(server, {
      endpoint: "https://evil.example",
      agents: ["support", "fresh-bot"],
    });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { agents: string[] }).agents).toEqual(["support"]);
    expect((await agentRegistry.get("support"))?.backend.type).toBe("local");
    expect(await agentRegistry.get("fresh-bot")).toBeNull();
    expect(await deployments.list()).toEqual([]);
  });

  it("409s for an id owned by a live deployment at another endpoint", async () => {
    const { server, agentRegistry } = await boot();
    await registerOk(server, { endpoint: "https://a.example", agents: ["shared"] });
    const res = await register(server, { endpoint: "https://b.example", agents: ["shared"] });
    expect(res.status).toBe(409);
    const recipe = await agentRegistry.get("shared");
    expect(recipe?.backend.type === "remote" && recipe.backend.endpoint).toBe("https://a.example");
  });

  it("a restarted remote (same endpoint) takes over; the old registration's expiry leaves it alone", async () => {
    const { server, agentRegistry, clock } = await boot();
    await registerOk(server, { endpoint: "https://a.example", agents: ["bot"], ttlMs: 5_000 });
    clock.advance(2_000);
    const second = await registerOk(server, {
      endpoint: "https://a.example",
      agents: ["bot"],
      ttlMs: 60_000,
    });
    // First registration expires; the recipe now belongs to the second.
    clock.advance(4_000);
    await get(server, "/api/remote-deployments");
    const recipe = await agentRegistry.get("bot");
    expect(recipe?.metadata.tags).toContain(`remote-deployment:${second}`);
  });

  it("400s on bad bodies: missing endpoint, empty / reserved agent ids, invalid auth or ttl", async () => {
    const { server } = await boot();
    const bad = [
      { agents: ["a"] },
      { endpoint: "https://a.example", agents: [] },
      { endpoint: "https://a.example", agents: ["_catalog"] },
      { endpoint: "https://a.example", agents: ["a"], auth: { kind: "basic" } },
      { endpoint: "https://a.example", agents: ["a"], ttlMs: 10 },
      { endpoint: "https://a.example", agents: ["a"], ttlMs: 1e12 },
    ];
    for (const body of bad) expect((await register(server, body)).status).toBe(400);
  });
});

describe("remote-deployments — endpoint policy (SSRF guard)", () => {
  it("rejects loopback, private, link-local / metadata and internal hosts", async () => {
    const { server, agentRegistry } = await boot();
    for (const endpoint of [
      "http://127.0.0.1:8080",
      "http://2130706433",
      "http://169.254.169.254/latest/meta-data",
      "http://10.1.2.3",
      "http://[::1]/",
      "http://[::ffff:127.0.0.1]/",
      "http://[fd00::1]/",
      "http://localhost:3000",
      "http://metadata.google.internal",
      "http://rebind.example",
      "ftp://a.example",
      "https://user:pw@a.example",
      "https://nxdomain.invalid",
    ]) {
      const res = await register(server, { endpoint, agents: ["x"] });
      expect([endpoint, res.status]).toEqual([endpoint, 400]);
    }
    expect(await agentRegistry.get("x")).toBeNull();
  });

  it("allowPrivateNetworks and allowedHosts are honored", async () => {
    const open = await boot({ endpointPolicy: { allowPrivateNetworks: true } });
    expect(
      (await register(open.server, { endpoint: "http://10.0.0.7", agents: ["x"] })).status,
    ).toBe(201);
    const narrow = await boot({
      endpointPolicy: { ...PUBLIC_DNS, allowedHosts: ["*.trusted.example"] },
    });
    expect(
      (await register(narrow.server, { endpoint: "https://a.trusted.example", agents: ["x"] }))
        .status,
    ).toBe(201);
    expect(
      (await register(narrow.server, { endpoint: "https://a.example", agents: ["y"] })).status,
    ).toBe(400);
  });
});

describe("remote-deployments — token redaction", () => {
  it("never returns the bearer token from any GET", async () => {
    const { server, agentRegistry } = await boot();
    await registerOk(server, {
      endpoint: "https://a.example",
      agents: ["bot"],
      auth: { kind: "bearer", token: "TOPSECRET" },
    });
    // Stored for outbound calls...
    const stored = await agentRegistry.get("bot");
    expect(stored?.backend.type === "remote" && stored.backend.auth?.token).toBe("TOPSECRET");
    // ...but redacted everywhere it is served.
    for (const path of [
      "/api/remote-deployments",
      "/api/agents",
      "/api/agents/bot",
      "/api/agents/bot/versions",
    ]) {
      const text = await (await get(server, path)).text();
      expect([path, text.includes("TOPSECRET")]).toEqual([path, false]);
      expect(text).toContain(REDACTED_SECRET);
    }
  });

  it("an edit that round-trips the redacted token keeps the stored one", async () => {
    const { server, agentRegistry } = await boot();
    await agentRegistry.register({
      id: "proxy",
      backend: {
        type: "remote",
        endpoint: "https://a.example",
        remoteAgentId: "p",
        auth: { kind: "bearer", token: "KEEP" },
      },
    });
    const fetched = (await (await get(server, "/api/agents/proxy")).json()) as {
      backend: unknown;
    };
    const res = await server.handle(
      new Request("http://test/api/agents/proxy", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ backend: { ...(fetched.backend as object), timeoutMs: 5_000 } }),
      }),
    );
    expect(res.status).toBe(200);
    const stored = await agentRegistry.get("proxy");
    expect(stored?.backend).toEqual({
      type: "remote",
      endpoint: "https://a.example",
      remoteAgentId: "p",
      auth: { kind: "bearer", token: "KEEP" },
      timeoutMs: 5_000,
    });
  });
});

describe("remote-deployments — heartbeat, unregister and expiry", () => {
  it("heartbeat extends the TTL; unknown ids answer 410", async () => {
    const { server, deployments, clock } = await boot();
    const id = await registerOk(server, { endpoint: "https://a.example", agents: ["bot"] });
    clock.advance(10_000);
    const res = await server.handle(
      new Request(`http://test/api/remote-deployments/${id}/heartbeat`, { method: "POST" }),
    );
    expect(res.status).toBe(200);
    expect((await deployments.get(id))?.lastHeartbeat).toBe(clock.currentTimeMs());
    const unknown = await server.handle(
      new Request("http://test/api/remote-deployments/nope/heartbeat", { method: "POST" }),
    );
    expect(unknown.status).toBe(410);
  });

  it("an expired deployment is not revived by a late heartbeat; its recipes are gone", async () => {
    const { server, agentRegistry, clock } = await boot();
    const id = await registerOk(server, {
      endpoint: "https://a.example",
      agents: ["bot"],
      ttlMs: 5_000,
    });
    clock.advance(5_000);
    const res = await server.handle(
      new Request(`http://test/api/remote-deployments/${id}/heartbeat`, { method: "POST" }),
    );
    expect(res.status).toBe(410);
    expect(await agentRegistry.get("bot")).toBeNull();
  });

  it("unregister deletes only the versions the deployment owns", async () => {
    const { server, agentRegistry } = await boot();
    const id = await registerOk(server, { endpoint: "https://a.example", agents: ["bot"] });
    // An operator publishes a hand-authored v2 under the same id.
    await agentRegistry.register({ id: "bot", version: "v2", backend: localBackend });
    const res = await server.handle(
      new Request(`http://test/api/remote-deployments/${id}`, { method: "DELETE" }),
    );
    expect(res.status).toBe(204);
    expect((await agentRegistry.versions("bot")).map((v) => v.version)).toEqual(["v2"]);
    // Idempotent.
    const again = await server.handle(
      new Request(`http://test/api/remote-deployments/${id}`, { method: "DELETE" }),
    );
    expect(again.status).toBe(204);
  });

  it("the sweeper expires dead deployments on the clock without any request", async () => {
    const { server, agentRegistry, deployments, clock } = await boot();
    await registerOk(server, { endpoint: "https://a.example", agents: ["bot"], ttlMs: 2_000 });
    const sweeper = new RemoteDeploymentSweeper({
      registry: deployments,
      agents: agentRegistry,
      clock,
      intervalMs: 1_000,
    });
    sweeper.start();
    try {
      const settle = async () => {
        for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0));
      };
      clock.advance(1_000); // a sweep before the TTL keeps it
      await settle();
      expect(await deployments.list()).toHaveLength(1);
      clock.advance(1_000); // the TTL passes; this tick expires it
      for (let i = 0; i < 100 && (await agentRegistry.get("bot")) !== null; i++) {
        await new Promise((r) => setTimeout(r, 0));
      }
      expect(await deployments.list()).toEqual([]);
      expect(await agentRegistry.get("bot")).toBeNull();
    } finally {
      sweeper.stop();
    }
    expect(clock.pendingCount()).toBe(0);
  });

  it("routes are not mounted without a remoteDeployments config", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const server = new ZoryaServer({
      insecure: true,
      workflows: new LocalWorkflows({ storage, runner, definitions: {}, sleepScanIntervalMs: 0 }),
      agents: new ZoryaAgents({
        registry: new InMemoryAgentRegistry(),
        resolve: () => {
          throw new Error("unused");
        },
      }),
    });
    expect((await get(server, "/api/remote-deployments")).status).toBe(404);
  });
});
