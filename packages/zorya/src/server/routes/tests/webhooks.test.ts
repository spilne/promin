// ---------------------------------------------------------------------------
// Webhook ingress route, through `ZoryaServer.handle`:
//   - `zorya-v1` signatures bind timestamp, delivery id, source, agent,
//     namespace and resource to the raw body bytes; a captured delivery
//     can't be retargeted or replayed outside the tolerance window.
//   - dedup is durable (the server's WorkflowStorage by default), keyed per
//     agent, and marks a delivery done only after a successful dispatch —
//     a failed delivery is retried, not answered as a replay.
//   - the agent turn runs under a run id per (delivery, attempt); a
//     delivery whose dispatcher died mid-turn is finished by
//     `WebhookDeliveryRecovery` under the same run id.
//   - disabled agents answer 410; the replay window reads the WallClock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry } from "@promin/agent";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  InMemoryNamespaceRegistry,
  createWorkflowRunner,
  type NamespaceRegistry,
  type WorkflowStorage,
} from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import {
  InMemoryWebhookDeliveryStore,
  WEBHOOK_DELIVERY_WORKFLOW,
  WebhookDeliveryRecovery,
  WorkflowStorageWebhookDeliveryStore,
  signWebhookDelivery,
  webhookRunId,
  type WebhookDelivery,
  type WebhookDeliveryStore,
  type WebhookGatewayDeps,
  type WebhookSourceConfig,
} from "../webhooks.ts";

const SECRET = "test-webhook-secret";
const T0 = Date.parse("2026-01-01T00:00:00Z");

const backend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "Helpful", tools: [] as string[] } },
};

interface BootOpts {
  /** Throw from invoke while this returns true. */
  failInvoke?: () => boolean;
  storage?: WorkflowStorage;
  deliveries?: WebhookDeliveryStore;
  sources?: Record<string, WebhookSourceConfig>;
  register?: boolean;
  namespaces?: NamespaceRegistry;
}

async function boot(opts: BootOpts = {}) {
  const clock = FakeWallClock.create(T0);
  const storage = opts.storage ?? new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const registry = new InMemoryAgentRegistry();
  if (opts.register !== false) {
    await registry.register({ id: "echo-bot", backend });
    await registry.register({ id: "other-bot", backend });
  }
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const dispatched: Array<{ agentId: string; task: string; scope: unknown; runId?: string }> = [];
  const makeAgent = (agentId: string, scope?: unknown) => {
    const agent = {
      invoke: async (input: { task: string }, invokeOpts?: { runId?: string }) => {
        if (opts.failInvoke?.()) throw new Error("db password=hunter2 leaked in error");
        dispatched.push({ agentId, task: input.task, scope, runId: invokeOpts?.runId });
        return { text: Promise.resolve("ok") };
      },
      withScope: (s: unknown) => makeAgent(agentId, s),
    };
    return agent;
  };
  const agents = new ZoryaAgents({
    registry,
    resolve: (recipe) => makeAgent(recipe.id) as never,
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    agents,
    clock,
    webhooks: {
      sources: opts.sources ?? { github: { secret: SECRET } },
      ...(opts.deliveries !== undefined && { deliveries: opts.deliveries }),
    },
    ...(opts.namespaces !== undefined && { namespaces: opts.namespaces }),
  });
  return { server, registry, dispatched, clock, storage };
}

interface DeliveryOpts {
  source?: string;
  agentId?: string;
  namespaceId?: string;
  resourceId?: string;
  deliveryId?: string;
  timestamp?: number;
  body?: string;
  secret?: string;
  /** Sign this scope instead of the one sent (to forge a retarget). */
  signedAs?: { namespaceId?: string; resourceId?: string; agentId?: string };
  omit?: Array<"signature" | "timestamp" | "delivery">;
}

function delivery(o: DeliveryOpts = {}): Request {
  const source = o.source ?? "github";
  const agentId = o.agentId ?? "echo-bot";
  const namespaceId = o.namespaceId ?? "acme";
  const deliveryId = o.deliveryId ?? "d-1";
  const timestamp = o.timestamp ?? T0;
  const body = o.body ?? JSON.stringify({ event: "push" });
  const signature = signWebhookDelivery({
    secret: o.secret ?? SECRET,
    scope: {
      source,
      agentId: o.signedAs?.agentId ?? agentId,
      namespaceId: o.signedAs?.namespaceId ?? namespaceId,
      ...((o.signedAs?.resourceId ?? o.resourceId) !== undefined && {
        resourceId: o.signedAs?.resourceId ?? o.resourceId,
      }),
      deliveryId,
      timestamp,
    },
    body,
  });
  const qs = new URLSearchParams({ namespaceId });
  if (o.resourceId !== undefined) qs.set("resourceId", o.resourceId);
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (!o.omit?.includes("signature")) headers["X-Zorya-Signature"] = signature;
  if (!o.omit?.includes("timestamp")) headers["X-Zorya-Timestamp"] = String(timestamp);
  if (!o.omit?.includes("delivery")) headers["X-Zorya-Delivery"] = deliveryId;
  return new Request(`http://test/webhooks/${source}/${agentId}?${qs}`, {
    method: "POST",
    headers,
    body,
  });
}

describe("webhook ingress — signature", () => {
  it("accepts a signed delivery and dispatches it to the agent in the signed scope", async () => {
    const { server, dispatched } = await boot();
    const res = await server.handle(delivery({ resourceId: "alice" }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({
      accepted: true,
      source: "github",
      agentId: "echo-bot",
      deliveryId: "d-1",
    });
    expect(dispatched).toHaveLength(1);
    expect(dispatched[0]!.task).toContain("Incoming webhook from github");
    expect(dispatched[0]!.scope).toEqual({ namespaceId: "acme", resourceId: "alice" });
  });

  it("401s on a missing signature, a wrong secret, or a body changed in transit", async () => {
    const { server, dispatched } = await boot();
    expect((await server.handle(delivery({ omit: ["signature"] }))).status).toBe(401);
    expect((await server.handle(delivery({ secret: "wrong" }))).status).toBe(401);
    const tampered = delivery();
    const forged = new Request(tampered.url, {
      method: "POST",
      headers: tampered.headers,
      body: JSON.stringify({ event: "delete-repo" }),
    });
    expect((await server.handle(forged)).status).toBe(401);
    expect(dispatched).toEqual([]);
  });

  it("refuses a captured delivery retargeted to another namespace, resource or agent", async () => {
    const { server, dispatched } = await boot();
    const ns = await server.handle(
      delivery({ namespaceId: "victim", signedAs: { namespaceId: "acme" } }),
    );
    expect(ns.status).toBe(401);
    const res = await server.handle(
      delivery({ resourceId: "bob", signedAs: { resourceId: "alice" } }),
    );
    expect(res.status).toBe(401);
    const agent = await server.handle(
      delivery({ agentId: "other-bot", signedAs: { agentId: "echo-bot" } }),
    );
    expect(agent.status).toBe(401);
    expect(dispatched).toEqual([]);
  });

  it("with a namespace registry, refuses unknown and archived namespaces before claiming", async () => {
    const namespaces = new InMemoryNamespaceRegistry();
    const { server, dispatched } = await boot({ namespaces });
    const unknown = await server.handle(delivery());
    expect(unknown.status).toBe(404);
    expect(((await unknown.json()) as { error: string }).error).toBe("namespace_not_found");

    // Not claimed: the sender's retry runs once the namespace exists.
    await namespaces.create({ id: "acme" });
    expect((await server.handle(delivery())).status).toBe(200);
    expect(dispatched).toHaveLength(1);

    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await server.handle(delivery({ deliveryId: "d-2" }));
    expect(archived.status).toBe(409);
    expect(((await archived.json()) as { error: string }).error).toBe("namespace_archived");
    expect(dispatched).toHaveLength(1);
  });

  it("rejects deliveries outside the tolerance window on the server clock", async () => {
    const { server, clock, dispatched } = await boot();
    clock.advance(5 * 60 * 1000 + 1);
    const stale = await server.handle(delivery({ deliveryId: "old" }));
    expect(stale.status).toBe(401);
    expect(((await stale.json()) as { error: string }).error).toBe("timestamp_out_of_window");
    // A fresh timestamp from the same sender passes.
    const fresh = await server.handle(
      delivery({ deliveryId: "new", timestamp: clock.currentTimeMs() }),
    );
    expect(fresh.status).toBe(200);
    expect(dispatched).toHaveLength(1);
  });

  it("400s without a timestamp or delivery id header", async () => {
    const { server } = await boot();
    expect((await server.handle(delivery({ omit: ["timestamp"] }))).status).toBe(400);
    expect((await server.handle(delivery({ omit: ["delivery"] }))).status).toBe(400);
  });

  it("a source can pin its namespace; a request naming another is rejected", async () => {
    const { server, dispatched } = await boot({
      sources: { github: { secret: SECRET, namespaceId: "acme" } },
    });
    expect((await server.handle(delivery({ namespaceId: "acme" }))).status).toBe(200);
    expect(
      (await server.handle(delivery({ namespaceId: "other", deliveryId: "d-2" }))).status,
    ).toBe(400);
    expect(dispatched).toHaveLength(1);
  });

  it("404s an unknown source and 400s a missing namespace", async () => {
    const { server } = await boot();
    expect((await server.handle(delivery({ source: "stripe" }))).status).toBe(404);
    const noNs = new Request("http://test/webhooks/github/echo-bot", {
      method: "POST",
      body: "{}",
    });
    expect((await server.handle(noNs)).status).toBe(400);
  });
});

describe("webhook ingress — dedup and retry semantics", () => {
  it("answers a redelivery as a replay without dispatching again", async () => {
    const { server, dispatched } = await boot();
    expect((await server.handle(delivery())).status).toBe(200);
    const replay = await server.handle(delivery());
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual({
      accepted: true,
      replay: true,
      source: "github",
      deliveryId: "d-1",
    });
    expect(dispatched).toHaveLength(1);
  });

  it("keys dedup by agent: the same delivery id for another agent is dispatched", async () => {
    const { server, dispatched } = await boot();
    expect((await server.handle(delivery({ agentId: "echo-bot" }))).status).toBe(200);
    expect((await server.handle(delivery({ agentId: "other-bot" }))).status).toBe(200);
    expect(dispatched.map((d) => d.agentId)).toEqual(["echo-bot", "other-bot"]);
  });

  it("a failed dispatch is retryable: 500 without internals, then the retry is processed", async () => {
    let failing = true;
    const { server, dispatched } = await boot({ failInvoke: () => failing });
    const first = await server.handle(delivery());
    expect(first.status).toBe(500);
    const text = await first.text();
    expect(text).not.toContain("hunter2");
    expect(JSON.parse(text).error).toBe("dispatch_failed");

    failing = false;
    const retry = await server.handle(delivery());
    expect(retry.status).toBe(200);
    expect(((await retry.json()) as { replay?: boolean }).replay).toBeUndefined();
    expect(dispatched).toHaveLength(1);
  });

  it("a delivery for an agent registered later is processed on retry (404 is not remembered)", async () => {
    const { server, registry, dispatched } = await boot({ register: false });
    expect((await server.handle(delivery())).status).toBe(404);
    await registry.register({ id: "echo-bot", backend });
    expect((await server.handle(delivery())).status).toBe(200);
    expect(dispatched).toHaveLength(1);
  });

  it("410s a disabled agent and processes the retry once it is re-enabled", async () => {
    const { server, registry, dispatched } = await boot();
    await registry.setEnabled({ id: "echo-bot", enabled: false });
    expect((await server.handle(delivery())).status).toBe(410);
    expect(dispatched).toEqual([]);
    await registry.setEnabled({ id: "echo-bot", enabled: true });
    expect((await server.handle(delivery())).status).toBe(200);
    expect(dispatched).toHaveLength(1);
  });

  it("422s a body that is not JSON", async () => {
    const { server } = await boot();
    expect((await server.handle(delivery({ body: "not json" }))).status).toBe(422);
  });

  it("dedup survives a restart: a new server on the same storage answers a replay", async () => {
    const storage = new InMemoryWorkflowStorage();
    const first = await boot({ storage });
    expect((await first.server.handle(delivery())).status).toBe(200);

    const second = await boot({ storage });
    const replay = await second.server.handle(delivery());
    expect(((await replay.json()) as { replay?: boolean }).replay).toBe(true);
    expect(second.dispatched).toEqual([]);

    // The delivery is recorded as a completed webhook run.
    const runs = await storage.listWorkflows({ status: "completed" });
    expect(runs.map((r) => r.workflowName)).toContain(WEBHOOK_DELIVERY_WORKFLOW);
  });

  it("a delivery claimed by another instance answers 409 (retry later)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const other = new WorkflowStorageWebhookDeliveryStore({ storage });
    const key = JSON.stringify(["github", "echo-bot", "acme", null, "d-1"]);
    const scope = {
      source: "github",
      agentId: "echo-bot",
      namespaceId: "acme",
      deliveryId: "d-1",
      timestamp: T0,
    };
    const held = await other.claim({ delivery: { key, scope, task: "t" }, leaseMs: 60_000 });
    expect(held.outcome).toBe("claimed");

    const { server, dispatched } = await boot({ storage });
    const concurrent = await server.handle(delivery());
    expect(concurrent.status).toBe(409);
    expect(dispatched).toEqual([]);

    // The other instance fails its dispatch: the sender's retry now runs here.
    await other.fail({
      key,
      error: "boom",
      ...(held.outcome === "claimed" && held.token !== undefined && { token: held.token }),
    });
    expect((await server.handle(delivery())).status).toBe(200);
    expect(dispatched).toHaveLength(1);
  });

  it("the in-memory store marks done only on complete and retries a failed claim", async () => {
    const clock = FakeWallClock.create(T0);
    const store: WebhookDeliveryStore = new InMemoryWebhookDeliveryStore({
      clock,
      retainMs: 1_000,
    });
    const delivery: WebhookDelivery = {
      key: "k",
      task: "t",
      scope: { source: "s", agentId: "a", namespaceId: "n", deliveryId: "d", timestamp: T0 },
    };
    expect(await store.claim({ delivery, leaseMs: 100 })).toEqual({
      outcome: "claimed",
      attempt: 1,
    });
    expect((await store.claim({ delivery, leaseMs: 100 })).outcome).toBe("in_flight");
    expect(await store.listUnfinished({ limit: 10 })).toEqual([delivery]);
    await store.fail({ key: "k", error: "x" });
    expect(await store.listUnfinished({ limit: 10 })).toEqual([]);
    // A failed dispatch is retried as the next attempt.
    expect(await store.claim({ delivery, leaseMs: 100 })).toEqual({
      outcome: "claimed",
      attempt: 2,
    });
    // An abandoned claim lapses after its lease and continues its attempt.
    clock.advance(100);
    expect(await store.claim({ delivery, leaseMs: 100 })).toEqual({
      outcome: "claimed",
      attempt: 2,
    });
    await store.complete({ key: "k" });
    expect((await store.claim({ delivery, leaseMs: 100 })).outcome).toBe("duplicate");
    // The processed-delivery memory expires on the clock.
    clock.advance(1_000);
    expect((await store.claim({ delivery, leaseMs: 100 })).outcome).toBe("claimed");
  });

  it("is not mounted without a webhooks config", async () => {
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
    expect((await server.handle(delivery())).status).toBe(404);
  });
});

describe("webhook ingress — durable dispatch", () => {
  const key = JSON.stringify(["github", "echo-bot", "acme", null, "d-1"]);

  it("runs the agent turn under a run id per delivery attempt", async () => {
    let failing = true;
    const { server, dispatched } = await boot({ failInvoke: () => failing });
    expect((await server.handle(delivery())).status).toBe(500);
    failing = false;
    expect((await server.handle(delivery())).status).toBe(200);
    // The first (failed) attempt never reached the agent's record; the
    // retry ran as attempt 2.
    expect(dispatched.map((d) => d.runId)).toEqual([webhookRunId({ key, attempt: 2 })]);
    expect(webhookRunId({ key, attempt: 1 })).not.toBe(webhookRunId({ key, attempt: 2 }));
  });

  it("finishes a delivery whose dispatcher died, under the same run id, exactly once", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const deliveries = new WorkflowStorageWebhookDeliveryStore({ storage });
    const registry = new InMemoryAgentRegistry();
    await registry.register({ id: "echo-bot", backend });
    const runIds: Array<string | undefined> = [];
    const agent = {
      invoke: async (_input: { task: string }, opts?: { runId?: string }) => {
        runIds.push(opts?.runId);
        return { text: Promise.resolve("ok") };
      },
      withScope: () => agent,
    };
    const deps: WebhookGatewayDeps = {
      registry,
      resolve: async () => agent,
      sources: { github: { secret: SECRET } },
      deliveries,
      clock,
      claimLeaseMs: 60_000,
    };
    const recorded: WebhookDelivery = {
      key,
      task: "Incoming webhook from github:\n{}",
      scope: {
        source: "github",
        agentId: "echo-bot",
        namespaceId: "acme",
        deliveryId: "d-1",
        timestamp: T0,
      },
    };
    // A dispatcher claims the delivery and dies before finishing it.
    const crashed = await deliveries.claim({ delivery: recorded, leaseMs: 60_000 });
    expect(crashed).toMatchObject({ outcome: "claimed", attempt: 1 });

    const recovery = new WebhookDeliveryRecovery(deps);
    await recovery.tick();
    expect(runIds).toEqual([]); // its claim is still live

    clock.advance(60_001);
    await recovery.tick();
    expect(runIds).toEqual([webhookRunId({ key, attempt: 1 })]);
    expect(await deliveries.listUnfinished({ limit: 10 })).toEqual([]);

    await recovery.tick();
    expect(runIds).toHaveLength(1);
    expect((await deliveries.claim({ delivery: recorded, leaseMs: 1 })).outcome).toBe("duplicate");
  });

  it("does not run a recovered delivery for an agent disabled since it was accepted", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const deliveries = new WorkflowStorageWebhookDeliveryStore({ storage });
    const registry = new InMemoryAgentRegistry();
    await registry.register({ id: "echo-bot", backend });
    const runIds: Array<string | undefined> = [];
    const agent = {
      invoke: async (_input: { task: string }, opts?: { runId?: string }) => {
        runIds.push(opts?.runId);
        return { text: Promise.resolve("ok") };
      },
      withScope: () => agent,
    };
    const errors: unknown[] = [];
    const recovery = new WebhookDeliveryRecovery({
      registry,
      resolve: async () => agent,
      sources: { github: { secret: SECRET } },
      deliveries,
      clock,
      claimLeaseMs: 60_000,
      onError: (err) => errors.push(err),
    });
    const recorded: WebhookDelivery = {
      key,
      task: "Incoming webhook from github:\n{}",
      scope: {
        source: "github",
        agentId: "echo-bot",
        namespaceId: "acme",
        deliveryId: "d-1",
        timestamp: T0,
      },
    };
    await deliveries.claim({ delivery: recorded, leaseMs: 60_000 });
    await registry.setEnabled({ id: "echo-bot", enabled: false });
    clock.advance(60_001);
    await recovery.tick();
    expect(runIds).toEqual([]);
    expect(String(errors[0])).toContain("agent_disabled");
  });
});

describe("webhook recovery — busy turns and paging", () => {
  async function setup(invoke: (runId: string | undefined) => Promise<void>) {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const deliveries = new WorkflowStorageWebhookDeliveryStore({ storage });
    const registry = new InMemoryAgentRegistry();
    await registry.register({ id: "echo-bot", backend });
    const agent = {
      invoke: async (_input: { task: string }, opts?: { runId?: string }) => {
        await invoke(opts?.runId);
        return { text: Promise.resolve("ok") };
      },
      withScope: () => agent,
    };
    const deps: WebhookGatewayDeps = {
      registry,
      resolve: async () => agent,
      sources: { github: { secret: SECRET } },
      deliveries,
      clock,
      claimLeaseMs: 60_000,
    };
    return { clock, deliveries, deps };
  }

  function recorded(deliveryId: string): WebhookDelivery {
    return {
      key: JSON.stringify(["github", "echo-bot", "acme", null, deliveryId]),
      task: "t",
      scope: {
        source: "github",
        agentId: "echo-bot",
        namespaceId: "acme",
        deliveryId,
        timestamp: T0,
      },
    };
  }

  it("leaves a delivery whose turn is still locked elsewhere unfinished, not failed", async () => {
    let locked = true;
    const runs: Array<string | undefined> = [];
    const { clock, deliveries, deps } = await setup(async (runId) => {
      if (locked) throw Object.assign(new Error("locked"), { _tag: "WorkflowLockError" });
      runs.push(runId);
    });
    const d = recorded("d-busy");
    await deliveries.claim({ delivery: d, leaseMs: 60_000 });
    clock.advance(60_001);

    const errors: unknown[] = [];
    const recovery = new WebhookDeliveryRecovery({ ...deps, onError: (e) => errors.push(e) });
    await recovery.tick();
    expect(errors).toEqual([]);
    expect((await deliveries.listUnfinished({ limit: 10 })).map((u) => u.key)).toEqual([d.key]);

    locked = false;
    clock.advance(60_001);
    await recovery.tick();
    expect(runs).toEqual([webhookRunId({ key: d.key, attempt: 1 })]);
    expect(await deliveries.listUnfinished({ limit: 10 })).toEqual([]);
  });

  it("reaches a lapsed delivery behind a full page of live ones", async () => {
    const runs: Array<string | undefined> = [];
    const { clock, deliveries, deps } = await setup(async (runId) => {
      runs.push(runId);
    });
    // Live claims first (long leases), the crashed one last.
    for (const id of ["live-1", "live-2", "live-3"]) {
      await deliveries.claim({ delivery: recorded(id), leaseMs: 10 * 60_000 });
      clock.advance(1);
    }
    const lapsed = recorded("crashed");
    await deliveries.claim({ delivery: lapsed, leaseMs: 1_000 });
    clock.advance(2_000);

    await new WebhookDeliveryRecovery({ ...deps, batch: 2 }).tick();
    expect(runs).toEqual([webhookRunId({ key: lapsed.key, attempt: 1 })]);
  });
});
