// ---------------------------------------------------------------------------
// Namespaces through `ZoryaServer.handle`:
//   - /api/namespaces CRUD over the configured registry, with request
//     validation (ids, capability policy, status) and the archive-then-
//     delete lifecycle (no delete while runs or schedules remain);
//   - with a registry, new runs, schedules, agent invocations, namespace
//     memory, instances and secrets need a registered, active namespace,
//     while reads stay unchecked;
//   - without one, namespaces stay free-form (but well-formed);
//   - at start the server registers the namespaces stored runs use.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemorySecretsStorage } from "@promin/agent";
import {
  FakeWallClock,
  InMemoryNamespaceRegistry,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
  type Workflow,
} from "@promin/workflow";
import { InMemorySchedulerStorage } from "@promin/workflow/scheduler";
import type { ApiError } from "../../api-types.ts";
import { ZoryaServer } from "../../server.ts";
import { ZoryaScheduler } from "../../services/scheduler/index.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { NamespaceDto, NamespacesResponse } from "../namespaces.ts";
import { bootAgents, jsonOf, req } from "./agent-gateway-harness.ts";

function setup(params: { registry?: boolean } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const hello = workflow<unknown>({ name: "hello" })
    .step("body", () => "ok")
    .build() as Workflow<unknown, unknown>;
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: { hello },
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const schedStorage = new InMemorySchedulerStorage({ clock });
  const scheduler = new ZoryaScheduler({
    storage: schedStorage,
    workflows,
    clock,
    namespaces: "all",
    onError: () => {},
  });
  const namespaces = new InMemoryNamespaceRegistry({ clock });
  const secrets = new InMemorySecretsStorage();
  const server = new ZoryaServer({
    insecure: true,
    workflows,
    scheduler,
    secrets,
    clock,
    logger: { log() {}, warn() {}, error() {} },
    ...(params.registry !== false && { namespaces }),
  });
  const send = (method: string, path: string, body?: unknown) =>
    server.handle(
      new Request(`http://x${path}`, {
        method,
        headers: { "content-type": "application/json" },
        ...(body !== undefined && { body: JSON.stringify(body) }),
      }),
    );
  return { clock, storage, schedStorage, namespaces, secrets, server, send };
}

async function errorOf(res: Response): Promise<string> {
  return ((await res.json()) as ApiError).error;
}

describe("/api/namespaces", () => {
  it("creates a namespace with 201, a Location and its capability policy", async () => {
    const { send } = setup();
    const res = await send("POST", "/api/namespaces", {
      id: "acme",
      displayName: "Acme",
      description: "Production tenant",
      capabilities: { ai: { enabled: true, maxTokensPerTurn: 8192 } },
      metadata: { owner: "ops" },
    });
    expect(res.status).toBe(201);
    expect(res.headers.get("location")).toBe("/api/namespaces/acme");
    const created = await jsonOf<NamespaceDto>(res);
    expect(created).toEqual({
      id: "acme",
      displayName: "Acme",
      description: "Production tenant",
      status: "active",
      capabilities: { ai: { enabled: true, maxTokensPerTurn: 8192 } },
      metadata: { owner: "ops" },
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    });

    const got = await send("GET", "/api/namespaces/acme");
    expect(await jsonOf<NamespaceDto>(got)).toEqual(created);
    const list = await jsonOf<NamespacesResponse>(await send("GET", "/api/namespaces"));
    expect(list).toEqual({ configured: true, namespaces: [created] });
  });

  it("refuses a duplicate id with 409 and leaves the first unchanged", async () => {
    const { send } = setup();
    await send("POST", "/api/namespaces", { id: "acme", displayName: "First" });
    const dup = await send("POST", "/api/namespaces", { id: "acme", displayName: "Second" });
    expect(dup.status).toBe(409);
    expect(await errorOf(dup)).toBe("namespace_exists");
    const got = await jsonOf<NamespaceDto>(await send("GET", "/api/namespaces/acme"));
    expect(got.displayName).toBe("First");
  });

  it("validates ids, capability policy and unknown fields", async () => {
    const { send, namespaces } = setup();
    for (const body of [
      { id: "has space" },
      { id: "-leading" },
      { id: "x".repeat(129) },
      {},
      { id: "bad", capabilities: { ai: { maxTokensPerTurn: "lots" } } },
      { id: "bad", capabilities: { ai: { maxTokensPerTurn: 0 } } },
      { id: "bad", capabilities: { billing: { plan: "pro" } } },
      { id: "bad", capabilities: { tools: { shell: true, sudo: true } } },
      { id: "bad", status: "archived" },
      { id: "bad", displayName: "x".repeat(129) },
    ]) {
      const res = await send("POST", "/api/namespaces", body);
      expect(res.status).toBe(400);
      expect(await errorOf(res)).toBe("invalid_request");
    }
    expect(await namespaces.list()).toEqual([]);
  });

  it("filters the list by status and rejects an unknown status", async () => {
    const { send } = setup();
    await send("POST", "/api/namespaces", { id: "live" });
    await send("POST", "/api/namespaces", { id: "old" });
    await send("PATCH", "/api/namespaces/old", { status: "archived" });

    const active = await jsonOf<NamespacesResponse>(
      await send("GET", "/api/namespaces?status=active"),
    );
    expect(active.namespaces.map((ns) => ns.id)).toEqual(["live"]);
    const archived = await jsonOf<NamespacesResponse>(
      await send("GET", "/api/namespaces?status=archived"),
    );
    expect(archived.namespaces.map((ns) => ns.id)).toEqual(["old"]);

    const bad = await send("GET", "/api/namespaces?status=deleted");
    expect(bad.status).toBe(400);
    expect(await errorOf(bad)).toBe("invalid_request");
  });

  it("patches label, description, policy, metadata and status", async () => {
    const { send, clock } = setup();
    await send("POST", "/api/namespaces", {
      id: "acme",
      description: "Tenant",
      metadata: { region: "us" },
    });
    clock.advance(1_000);
    const res = await send("PATCH", "/api/namespaces/acme", {
      displayName: "Acme Corp",
      capabilities: { workflows: { maxConcurrentRuns: 5 } },
    });
    expect(res.status).toBe(200);
    const patched = await jsonOf<NamespaceDto>(res);
    expect(patched).toMatchObject({
      displayName: "Acme Corp",
      description: "Tenant",
      metadata: { region: "us" },
      capabilities: { workflows: { maxConcurrentRuns: 5 } },
      updatedAt: "2026-01-01T00:00:01.000Z",
    });

    const cleared = await jsonOf<NamespaceDto>(
      await send("PATCH", "/api/namespaces/acme", { description: null, status: "archived" }),
    );
    expect(cleared.description).toBeNull();
    expect(cleared.status).toBe("archived");
  });

  it("answers PATCH errors: empty body, bad status, unknown namespace, immutable id", async () => {
    const { send } = setup();
    await send("POST", "/api/namespaces", { id: "acme" });
    expect((await send("PATCH", "/api/namespaces/acme", {})).status).toBe(400);
    expect((await send("PATCH", "/api/namespaces/acme", { status: "deleted" })).status).toBe(400);
    expect((await send("PATCH", "/api/namespaces/acme", { id: "other" })).status).toBe(400);
    const missing = await send("PATCH", "/api/namespaces/missing", { displayName: "x" });
    expect(missing.status).toBe(404);
    expect(await errorOf(missing)).toBe("not_found");
    expect((await send("GET", "/api/namespaces/missing")).status).toBe(404);
  });

  it("deletes only an archived namespace no run or schedule names", async () => {
    const { send, storage, schedStorage } = setup();
    await send("POST", "/api/namespaces", { id: "acme" });

    const active = await send("DELETE", "/api/namespaces/acme");
    expect(active.status).toBe(409);
    expect(await errorOf(active)).toBe("namespace_active");

    // A run names "acme", a schedule names "beta".
    await storage.createWorkflow({
      workflowId: "run-1",
      workflowName: "hello",
      namespace: "acme",
      input: null,
    });
    await send("POST", "/api/namespaces", { id: "beta" });
    await schedStorage.upsertSchedule({ id: "s-1", namespace: "beta", intervalMs: 60_000 });
    await send("PATCH", "/api/namespaces/acme", { status: "archived" });
    await send("PATCH", "/api/namespaces/beta", { status: "archived" });

    const runsLeft = await send("DELETE", "/api/namespaces/acme");
    expect(runsLeft.status).toBe(409);
    const runsError = (await runsLeft.json()) as ApiError;
    expect(runsError.error).toBe("namespace_in_use");
    expect(runsError.message).toContain("runs");

    const schedulesLeft = await send("DELETE", "/api/namespaces/beta");
    expect(schedulesLeft.status).toBe(409);
    expect(((await schedulesLeft.json()) as ApiError).message).toContain("schedules");

    await schedStorage.deleteSchedule("s-1");
    expect((await send("DELETE", "/api/namespaces/beta")).status).toBe(204);
    expect((await send("GET", "/api/namespaces/beta")).status).toBe(404);
    expect((await send("DELETE", "/api/namespaces/beta")).status).toBe(404);
    expect((await send("GET", "/api/namespaces/acme")).status).toBe(200);
  });

  it("without a registry only GET answers, with configured: false", async () => {
    const { send } = setup({ registry: false });
    const res = await send("GET", "/api/namespaces");
    expect(await jsonOf<NamespacesResponse>(res)).toEqual({ configured: false, namespaces: [] });
    expect((await send("POST", "/api/namespaces", { id: "acme" })).status).toBe(405);
  });
});

describe("namespace checks on runs and schedules", () => {
  it("triggers runs only in registered, active namespaces", async () => {
    const { send, namespaces, storage } = setup();
    const unknown = await send("POST", "/api/workflows/hello/runs", { namespace: "missing" });
    expect(unknown.status).toBe(404);
    expect(await errorOf(unknown)).toBe("namespace_not_found");

    await namespaces.create({ id: "acme" });
    const ok = await send("POST", "/api/workflows/hello/runs", { namespace: "acme" });
    expect(ok.status).toBe(202);

    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await send("POST", "/api/workflows/hello/runs", { namespace: "acme" });
    expect(archived.status).toBe(409);
    expect(await errorOf(archived)).toBe("namespace_archived");

    // The global namespace needs no registration.
    expect((await send("POST", "/api/workflows/hello/runs", {})).status).toBe(202);
    const runs = await storage.listWorkflows({});
    expect(runs.map((r) => r.namespace ?? null).sort()).toEqual(["acme", null]);
  });

  it("leaves read filters unchecked: an unknown namespace matches nothing", async () => {
    const { send } = setup();
    const runs = await send("GET", "/api/runs?namespace=missing");
    expect(runs.status).toBe(200);
    expect(((await runs.json()) as { runs: unknown[] }).runs).toEqual([]);
    expect((await send("GET", "/api/schedules?namespace=missing")).status).toBe(200);
  });

  it("creates schedules only in registered, active namespaces", async () => {
    const { send, namespaces } = setup();
    const body = { id: "s-1", namespace: "acme", intervalMs: 60_000, workflowName: "hello" };
    const unknown = await send("POST", "/api/schedules", body);
    expect(unknown.status).toBe(404);
    expect(await errorOf(unknown)).toBe("namespace_not_found");

    await namespaces.create({ id: "acme" });
    expect((await send("POST", "/api/schedules", body)).status).toBe(201);

    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await send("POST", "/api/schedules", { ...body, id: "s-2" });
    expect(archived.status).toBe(409);
  });

  it("validates namespace names the same way with or without a registry", async () => {
    const { send } = setup({ registry: false });
    for (const [method, path, body] of [
      ["POST", "/api/workflows/hello/runs", { namespace: "has space" }],
      ["POST", "/api/schedules", { id: "s-1", namespace: "a/b", intervalMs: 60_000 }],
      ["GET", "/api/runs?namespace=%20padded", undefined],
    ] as const) {
      const res = await send(method, path, body);
      expect(res.status).toBe(400);
      expect(await errorOf(res)).toBe("invalid_request");
    }
    // Free-form namespaces are accepted without a registry.
    expect(
      (await send("POST", "/api/workflows/hello/runs", { namespace: "anything" })).status,
    ).toBe(202);
  });
});

describe("namespace checks on secrets", () => {
  it("stores namespace- and resource-scoped secrets only in active namespaces", async () => {
    const { send, namespaces } = setup();
    const post = (scope: unknown) =>
      send("POST", "/api/secrets", { scope, key: "API_KEY", value: "v" });

    expect((await post({ kind: "namespace", namespaceId: "acme" })).status).toBe(404);
    expect((await post({ kind: "resource", namespaceId: "acme", resourceId: "u" })).status).toBe(
      404,
    );
    expect((await post({ kind: "global" })).status).toBe(201);

    await namespaces.create({ id: "acme" });
    expect((await post({ kind: "namespace", namespaceId: "acme" })).status).toBe(201);

    // Archived: no new secrets, but listing and deleting still work.
    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    expect((await post({ kind: "namespace", namespaceId: "acme" })).status).toBe(409);
    const list = await send("GET", "/api/secrets?scope=namespace&namespaceId=acme");
    expect(((await list.json()) as { keys: string[] }).keys).toEqual(["API_KEY"]);
    expect(
      (await send("DELETE", "/api/secrets/API_KEY?scope=namespace&namespaceId=acme")).status,
    ).toBe(204);
  });
});

describe("namespace checks on agents", () => {
  const invoke = { task: "hi", namespaceId: "acme", resourceId: "alice" };

  it("invokes agents only in registered, active namespaces", async () => {
    const namespaces = new InMemoryNamespaceRegistry();
    const { server } = await bootAgents({ namespaces });
    const unknown = await server.handle(req("POST", "/api/agents/support/invoke", invoke));
    expect(unknown.status).toBe(404);
    expect(await errorOf(unknown)).toBe("namespace_not_found");

    await namespaces.create({ id: "acme" });
    expect((await server.handle(req("POST", "/api/agents/support/invoke", invoke))).status).toBe(
      200,
    );

    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await server.handle(req("POST", "/api/agents/support/invoke", invoke));
    expect(archived.status).toBe(409);
    // Reading the archived namespace's threads still works.
    const threads = await server.handle(
      req("GET", "/api/agents/support/threads?namespaceId=acme&resourceId=alice"),
    );
    expect(threads.status).toBe(200);
  });

  it("writes namespace memory and instances only in registered namespaces", async () => {
    const namespaces = new InMemoryNamespaceRegistry();
    const { server, memory } = await bootAgents({ namespaces });
    const patch = () =>
      server.handle(req("PATCH", "/api/memory/namespace/acme", { staticRules: "be brief" }));
    const fact = () =>
      server.handle(req("POST", "/api/memory/namespace/acme/facts", { text: "a fact" }));
    const instance = () =>
      server.handle(
        req("POST", "/api/agents/support/instances", { namespaceId: "acme", ownerId: "o-1" }),
      );

    expect((await patch()).status).toBe(404);
    expect((await fact()).status).toBe(404);
    expect((await instance()).status).toBe(404);
    expect(await memory.getNamespace("acme")).toBeNull();

    await namespaces.create({ id: "acme" });
    expect((await patch()).status).toBe(200);
    expect((await fact()).status).toBe(201);
    expect((await instance()).status).toBe(200);

    // Inspection is a read: unchecked.
    expect(
      (await server.handle(req("GET", "/api/memory/inspect?namespaceId=elsewhere"))).status,
    ).toBe(200);
  });
});

describe("adopting stored namespaces at start", () => {
  it("registers the namespaces stored runs use, skipping malformed ids", async () => {
    const { server, storage, namespaces, send } = setup();
    for (const [id, namespace] of [
      ["r-1", "tenant-a"],
      ["r-2", "tenant-b"],
      ["r-3", "not well formed"],
    ] as const) {
      await storage.createWorkflow({
        workflowId: id,
        workflowName: "hello",
        namespace,
        input: null,
      });
    }
    await namespaces.create({ id: "tenant-b", displayName: "Tenant B" });
    await namespaces.update({ id: "tenant-b", patch: { status: "archived" } });

    server.start();
    await server.ready();
    try {
      const list = await jsonOf<NamespacesResponse>(await send("GET", "/api/namespaces"));
      expect(list.namespaces.map((ns) => [ns.id, ns.status, ns.displayName])).toEqual([
        ["tenant-a", "active", "tenant-a"],
        // Already registered: left as it was.
        ["tenant-b", "archived", "Tenant B"],
      ]);
      expect(server.serviceStatuses().map((s) => s.name)).toContain("namespaces");
    } finally {
      await server.stop();
    }
  });
});
