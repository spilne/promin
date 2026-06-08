// ---------------------------------------------------------------------------
// Secrets HTTP routes — end-to-end against a real ZoryaServer with an
// InMemorySecretsStorage backend. Pins the wire shape, validation, and
// scope handling.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemorySecretsStorage, SecretScope } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

async function bootServerWithSecrets() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const secrets = new InMemorySecretsStorage();
  const server = new ZoryaServer({ workflows, secrets });
  await server.handle(
    new Request("http://test/api/namespaces", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: "acme", displayName: "Acme" }),
    }),
  );
  return { server, secrets };
}

describe("secrets HTTP routes — POST /api/secrets", () => {
  it("stores a secret at global scope, returns 201", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          key: "anthropic_api_key",
          value: "sk-ant-test",
        }),
      }),
    );
    expect(res.status).toBe(201);
    const body = (await res.json()) as { scope: { kind: string }; key: string };
    expect(body.scope.kind).toBe("global");
    expect(body.key).toBe("anthropic_api_key");
    expect(await secrets.get({ scope: SecretScope.global(), key: "anthropic_api_key" })).toBe(
      "sk-ant-test",
    );
  });

  it("stores at namespace scope when scope object is supplied", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scope: { kind: "namespace", namespaceId: "acme" },
          key: "OPENAI_API_KEY",
          value: "sk-test",
        }),
      }),
    );
    expect(res.status).toBe(201);
    expect(await secrets.get({ scope: SecretScope.namespace("acme"), key: "OPENAI_API_KEY" })).toBe(
      "sk-test",
    );
  });

  it("stores at resource scope", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scope: { kind: "resource", namespaceId: "acme", resourceId: "alice" },
          key: "user.token",
          value: "tok",
        }),
      }),
    );
    expect(res.status).toBe(201);
    expect(
      await secrets.get({
        scope: SecretScope.resource("acme", "alice"),
        key: "user.token",
      }),
    ).toBe("tok");
  });

  it("response NEVER echoes the value", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: "k", value: "very-secret-value" }),
      }),
    );
    const body = (await res.text()) as string;
    expect(body).not.toContain("very-secret-value");
  });

  it("400 on invalid key (special chars)", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: "bad key!", value: "v" }),
      }),
    );
    expect(res.status).toBe(400);
  });

  it("400 on missing namespaceId for namespace scope", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scope: { kind: "namespace" },
          key: "k",
          value: "v",
        }),
      }),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("missing_namespaceId");
  });

  it("400 on missing value", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: "k" }),
      }),
    );
    expect(res.status).toBe(400);
  });
});

describe("secrets HTTP routes — GET /api/secrets", () => {
  it("lists keys at global scope (default), values not returned", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    await secrets.set({ scope: SecretScope.global(), key: "k1", value: "v1" });
    await secrets.set({ scope: SecretScope.global(), key: "k2", value: "v2" });
    const res = await server.handle(new Request("http://test/api/secrets", { method: "GET" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { scope: { kind: string }; keys: string[] };
    expect(body.scope.kind).toBe("global");
    expect(body.keys.sort()).toEqual(["k1", "k2"]);
    // Verify the response body really doesn't contain the values.
    const text = JSON.stringify(body);
    expect(text).not.toContain("v1");
    expect(text).not.toContain("v2");
  });

  it("lists keys at namespace scope when scope=namespace&namespaceId=...", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    await secrets.set({ scope: SecretScope.namespace("acme"), key: "ns_key", value: "v" });
    await secrets.set({ scope: SecretScope.global(), key: "global_key", value: "v" });
    const res = await server.handle(
      new Request("http://test/api/secrets?scope=namespace&namespaceId=acme", {
        method: "GET",
      }),
    );
    const body = (await res.json()) as { keys: string[] };
    expect(body.keys).toEqual(["ns_key"]);
    expect(body.keys).not.toContain("global_key");
  });

  it("400 on namespace scope without namespaceId", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets?scope=namespace", { method: "GET" }),
    );
    expect(res.status).toBe(400);
  });
});

describe("secrets HTTP routes — DELETE /api/secrets/:key", () => {
  it("removes a global secret, returns 204", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    await secrets.set({ scope: SecretScope.global(), key: "tmp", value: "x" });
    const res = await server.handle(
      new Request("http://test/api/secrets/tmp", { method: "DELETE" }),
    );
    expect(res.status).toBe(204);
    expect(await secrets.get({ scope: SecretScope.global(), key: "tmp" })).toBeNull();
  });

  it("removes from namespace scope when ?scope=namespace&namespaceId=...", async () => {
    const { server, secrets } = await bootServerWithSecrets();
    await secrets.set({ scope: SecretScope.namespace("acme"), key: "k", value: "v" });
    await secrets.set({ scope: SecretScope.global(), key: "k", value: "v-global" });
    const res = await server.handle(
      new Request("http://test/api/secrets/k?scope=namespace&namespaceId=acme", {
        method: "DELETE",
      }),
    );
    expect(res.status).toBe(204);
    expect(await secrets.get({ scope: SecretScope.namespace("acme"), key: "k" })).toBeNull();
    // Global was untouched.
    expect(await secrets.get({ scope: SecretScope.global(), key: "k" })).toBe("v-global");
  });

  it("204 even when the key isn't there (idempotent delete)", async () => {
    const { server } = await bootServerWithSecrets();
    const res = await server.handle(
      new Request("http://test/api/secrets/never", { method: "DELETE" }),
    );
    expect(res.status).toBe(204);
  });
});

describe("secrets HTTP routes — when no secrets configured", () => {
  it("routes are not mounted (404 on POST)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows }); // no `secrets`
    const res = await server.handle(
      new Request("http://test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: "k", value: "v" }),
      }),
    );
    expect(res.status).toBe(404);
  });
});
