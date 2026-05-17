// ---------------------------------------------------------------------------
// `SqliteAgentInstanceRegistry` — pins the same contract as the in-memory
// reference plus persistence-across-instances. The shared conformance
// suite hasn't been factored out yet (mirrors the AgentRegistry pattern
// that was retrofitted later); when it is, this file becomes a one-liner
// invocation.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { composeAgentInstanceId, InMemoryMemoryStore, wipeAgentInstance } from "@promin/agent";
import { SqliteAgentInstanceRegistry } from "../agent-instance-registry.ts";

function makeRegistry() {
  return SqliteAgentInstanceRegistry.make({ db: new Database(":memory:") });
}

describe("SqliteAgentInstanceRegistry", () => {
  it("resolveOrCreate is idempotent", async () => {
    const registry = makeRegistry();
    const a = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    const b = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(b.id).toBe(a.id);
    expect(b.createdAt).toBe(a.createdAt);
  });

  it("composes a deterministic id", async () => {
    const registry = makeRegistry();
    const id = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(id.id).toBe(
      composeAgentInstanceId({
        namespaceId: "acme",
        registeredAgentId: "writer",
        ownerId: "alice",
      }),
    );
  });

  it("isolates instances across namespaces", async () => {
    const registry = makeRegistry();
    const acme = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    const globex = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "globex",
      ownerId: "alice",
    });
    expect(acme.id).not.toBe(globex.id);
    const list = await registry.list();
    expect(list).toHaveLength(2);
  });

  it("list orders by createdDesc by default", async () => {
    const registry = makeRegistry();
    const a = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await new Promise((r) => setTimeout(r, 5));
    const b = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });

    const list = await registry.list({ namespaceId: "acme" });
    expect(list[0]!.id).toBe(b.id);
    expect(list[1]!.id).toBe(a.id);
  });

  it("update patches displayName + metadata", async () => {
    const registry = makeRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    const updated = await registry.update(created.id, {
      displayName: "Alice's writer",
      metadata: { color: "blue", priority: 2 },
    });
    expect(updated.displayName).toBe("Alice's writer");
    expect(updated.metadata).toEqual({ color: "blue", priority: 2 });

    const refetched = await registry.get(created.id);
    expect(refetched?.metadata).toEqual({ color: "blue", priority: 2 });
  });

  it("delete is idempotent", async () => {
    const registry = makeRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.delete(created.id);
    expect(await registry.get(created.id)).toBeNull();
    await registry.delete(created.id);
  });
});

describe("SqliteAgentInstanceRegistry — persistence", () => {
  it("instances survive across registry instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteAgentInstanceRegistry.make({ db });
    const created = await r1.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
      displayName: "alice's writer",
      metadata: { color: "blue" },
    });

    const r2 = SqliteAgentInstanceRegistry.make({ db });
    const got = await r2.get(created.id);
    expect(got).not.toBeNull();
    expect(got?.displayName).toBe("alice's writer");
    expect(got?.metadata).toEqual({ color: "blue" });

    // Concurrent resolveOrCreate from a fresh instance returns the same row.
    const same = await r2.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(same.id).toBe(created.id);
    expect(same.createdAt).toBe(created.createdAt);
  });
});

describe("wipeAgentInstance over SqliteAgentInstanceRegistry", () => {
  it("removes the row + clears resource memory", async () => {
    const registry = makeRegistry();
    const memory = new InMemoryMemoryStore();
    const instance = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: instance.id },
      { workingMemory: "draft" },
    );
    await memory.createThread({
      namespaceId: "acme",
      resourceId: instance.id,
      threadId: "t1",
    });

    const result = await wipeAgentInstance({
      registry,
      memory,
      instanceId: instance.id,
    });
    expect(result.threadsDeleted).toBe(1);
    expect(await registry.get(instance.id)).toBeNull();
    const row = await memory.getResource({ namespaceId: "acme", resourceId: instance.id });
    expect(row?.workingMemory).toBeNull();
  });
});
