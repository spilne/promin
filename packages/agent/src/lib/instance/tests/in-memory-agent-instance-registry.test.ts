// ---------------------------------------------------------------------------
// `InMemoryAgentInstanceRegistry` — pins the registry contract:
//   - resolveOrCreate is idempotent for same (registeredAgentId, namespace, ownerId)
//   - distinct triples produce distinct instances
//   - id format is deterministic (composeAgentInstanceId)
//   - list() filters by namespace / ownerId / registeredAgentId
//   - update patches displayName / metadata
//   - delete is idempotent
//
// `wipeAgentInstance` cascades through MemoryStore — verified end-to-end
// with InMemoryMemoryStore.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeClock } from "@promin/core";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { composeAgentInstanceId } from "../types.ts";
import { InMemoryAgentInstanceRegistry } from "../in-memory-agent-instance-registry.ts";
import { wipeAgentInstance } from "../wipe.ts";

describe("InMemoryAgentInstanceRegistry", () => {
  it("resolveOrCreate is idempotent for the same triple", async () => {
    const clock = FakeClock.create(1_000);
    const registry = new InMemoryAgentInstanceRegistry({ clock });
    const first = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    clock.advance(50);
    const second = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(second).toEqual(first);
    // Subsequent resolves do not bump createdAt — same row, same timestamp.
    expect(second.createdAt).toBe(1_000);
  });

  it("distinct triples produce distinct instances", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    const writerForAlice = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    const writerForBob = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    const reviewerForAlice = await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(new Set([writerForAlice.id, writerForBob.id, reviewerForAlice.id]).size).toBe(3);
  });

  it("id format is deterministic", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
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

  it("validates non-empty inputs", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "", namespaceId: "acme", ownerId: "alice" }),
    ).rejects.toThrow();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "", ownerId: "alice" }),
    ).rejects.toThrow();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "acme", ownerId: "" }),
    ).rejects.toThrow();
  });

  it("list filters by namespaceId, ownerId, and registeredAgentId", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "globex",
      ownerId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      ownerId: "alice",
    });

    const acmeOnly = await registry.list({ namespaceId: "acme" });
    expect(acmeOnly.map((r) => r.id).sort()).toEqual(
      ["acme::reviewer::alice", "acme::writer::alice", "acme::writer::bob"].sort(),
    );

    const aliceInAcme = await registry.list({ namespaceId: "acme", ownerId: "alice" });
    expect(aliceInAcme.map((r) => r.id).sort()).toEqual(
      ["acme::reviewer::alice", "acme::writer::alice"].sort(),
    );

    const writerEverywhere = await registry.list({ registeredAgentId: "writer" });
    expect(writerEverywhere).toHaveLength(3);
  });

  it("list orders by createdDesc by default", async () => {
    const clock = FakeClock.create(0);
    const registry = new InMemoryAgentInstanceRegistry({ clock });
    const a = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    clock.advance(100);
    const b = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    const all = await registry.list();
    // Newest-first: bob (createdAt=100) before alice (createdAt=0).
    expect(all[0]!.id).toBe(b.id);
    expect(all[1]!.id).toBe(a.id);
  });

  it("update patches displayName + metadata", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    const updated = await registry.update(created.id, {
      displayName: "Alice's writing buddy",
      metadata: { color: "blue" },
    });
    expect(updated.displayName).toBe("Alice's writing buddy");
    expect(updated.metadata).toEqual({ color: "blue" });
  });

  it("delete is idempotent", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await registry.delete(created.id);
    expect(await registry.get(created.id)).toBeNull();
    // Deleting again does not throw.
    await registry.delete(created.id);
  });
});

describe("wipeAgentInstance — cascades through MemoryStore", () => {
  it("drops the registry row and clears resource-scope state", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    const memory = new InMemoryMemoryStore();
    const instance = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });

    // Stand up state under resourceId = instance.id, mirroring what an
    // agent invocation would create.
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: instance.id },
      { workingMemory: "alice's writing notes" },
    );
    await memory.appendResourceFact(
      { namespaceId: "acme", resourceId: instance.id },
      "alice prefers terse",
    );
    await memory.createThread({
      namespaceId: "acme",
      resourceId: instance.id,
      threadId: "t1",
    });
    await memory.appendMessages({ namespaceId: "acme", resourceId: instance.id, threadId: "t1" }, [
      { role: "user", content: "hi" },
    ]);

    const result = await wipeAgentInstance({
      registry,
      memory,
      instanceId: instance.id,
    });

    expect(result.instanceId).toBe(instance.id);
    expect(result.threadsDeleted).toBe(1);
    expect(result.factsDeleted).toBe(1);

    // Registry row gone.
    expect(await registry.get(instance.id)).toBeNull();
    // Resource working memory cleared.
    const resourceRow = await memory.getResource({
      namespaceId: "acme",
      resourceId: instance.id,
    });
    expect(resourceRow?.workingMemory).toBeNull();
    // Threads gone.
    expect(await memory.listThreads({ namespaceId: "acme", resourceId: instance.id })).toHaveLength(
      0,
    );
    // Facts gone.
    expect(
      await memory.listResourceFacts({ namespaceId: "acme", resourceId: instance.id }),
    ).toHaveLength(0);
  });

  it("is a no-op for an unknown instance id", async () => {
    const registry = new InMemoryAgentInstanceRegistry();
    const memory = new InMemoryMemoryStore();
    const result = await wipeAgentInstance({
      registry,
      memory,
      instanceId: "acme::writer::ghost",
    });
    expect(result).toEqual({
      instanceId: "acme::writer::ghost",
      threadsDeleted: 0,
      factsDeleted: 0,
      episodesDeleted: 0,
    });
  });
});
