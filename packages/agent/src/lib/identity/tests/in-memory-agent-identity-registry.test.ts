// ---------------------------------------------------------------------------
// `InMemoryAgentIdentityRegistry` — pins the registry contract:
//   - resolveOrCreate is idempotent for same (registeredAgentId, namespace, userId)
//   - distinct triples produce distinct identities
//   - id format is deterministic (composeAgentIdentityId)
//   - list() filters by namespace / userId / registeredAgentId
//   - touch updates lastActiveAt without mutating other fields
//   - update patches displayName / metadata
//   - delete is idempotent
//
// `wipeAgentIdentity` cascades through MemoryStore — verified end-to-end
// with InMemoryMemoryStore.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeClock } from "@promin/core";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { composeAgentIdentityId } from "../types.ts";
import { InMemoryAgentIdentityRegistry } from "../in-memory-agent-identity-registry.ts";
import { wipeAgentIdentity } from "../wipe.ts";

describe("InMemoryAgentIdentityRegistry", () => {
  it("resolveOrCreate is idempotent for the same triple", async () => {
    const clock = FakeClock.create(1_000);
    const registry = new InMemoryAgentIdentityRegistry({ clock });
    const first = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    clock.advance(50);
    const second = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    expect(second).toEqual(first);
    // Subsequent resolves do not bump createdAt or lastActiveAt — that's `touch`.
    expect(second.createdAt).toBe(1_000);
    expect(second.lastActiveAt).toBe(1_000);
  });

  it("distinct triples produce distinct identities", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const writerForAlice = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    const writerForBob = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "bob",
    });
    const reviewerForAlice = await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      userId: "alice",
    });
    expect(new Set([writerForAlice.id, writerForBob.id, reviewerForAlice.id]).size).toBe(3);
  });

  it("id format is deterministic", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const id = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    expect(id.id).toBe(
      composeAgentIdentityId({
        namespaceId: "acme",
        registeredAgentId: "writer",
        userId: "alice",
      }),
    );
  });

  it("validates non-empty inputs", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "", namespaceId: "acme", userId: "alice" }),
    ).rejects.toThrow();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "", userId: "alice" }),
    ).rejects.toThrow();
    expect(
      registry.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "acme", userId: "" }),
    ).rejects.toThrow();
  });

  it("list filters by namespaceId, userId, and registeredAgentId", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "bob",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "globex",
      userId: "alice",
    });
    await registry.resolveOrCreate({
      registeredAgentId: "reviewer",
      namespaceId: "acme",
      userId: "alice",
    });

    const acmeOnly = await registry.list({ namespaceId: "acme" });
    expect(acmeOnly.map((r) => r.id).sort()).toEqual(
      ["acme::reviewer::alice", "acme::writer::alice", "acme::writer::bob"].sort(),
    );

    const aliceInAcme = await registry.list({ namespaceId: "acme", userId: "alice" });
    expect(aliceInAcme.map((r) => r.id).sort()).toEqual(
      ["acme::reviewer::alice", "acme::writer::alice"].sort(),
    );

    const writerEverywhere = await registry.list({ registeredAgentId: "writer" });
    expect(writerEverywhere).toHaveLength(3);
  });

  it("list orders by lastActiveDesc by default", async () => {
    const clock = FakeClock.create(0);
    const registry = new InMemoryAgentIdentityRegistry({ clock });
    const a = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    clock.advance(100);
    const b = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "bob",
    });
    // alice's identity is older but we'll touch it last
    clock.advance(100);
    await registry.touch(a.id);

    const all = await registry.list();
    expect(all[0]!.id).toBe(a.id);
    expect(all[1]!.id).toBe(b.id);
  });

  it("touch updates lastActiveAt without mutating other fields", async () => {
    const clock = FakeClock.create(1_000);
    const registry = new InMemoryAgentIdentityRegistry({ clock });
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    clock.advance(500);
    await registry.touch(created.id);
    const after = await registry.get(created.id);
    expect(after?.lastActiveAt).toBe(1_500);
    expect(after?.createdAt).toBe(1_000);
    expect(after?.userId).toBe("alice");
  });

  it("update patches displayName + metadata", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    const updated = await registry.update(created.id, {
      displayName: "Alice's writing buddy",
      metadata: { color: "blue" },
    });
    expect(updated.displayName).toBe("Alice's writing buddy");
    expect(updated.metadata).toEqual({ color: "blue" });
  });

  it("delete is idempotent", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });
    await registry.delete(created.id);
    expect(await registry.get(created.id)).toBeNull();
    // Deleting again does not throw.
    await registry.delete(created.id);
  });
});

describe("wipeAgentIdentity — cascades through MemoryStore", () => {
  it("drops the registry row and clears resource-scope state", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const memory = new InMemoryMemoryStore();
    const identity = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      userId: "alice",
    });

    // Stand up state under resourceId = identity.id, mirroring what an
    // agent invocation would create.
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: identity.id },
      { workingMemory: "alice's writing notes" },
    );
    await memory.appendResourceFact(
      { namespaceId: "acme", resourceId: identity.id },
      "alice prefers terse",
    );
    await memory.createThread({
      namespaceId: "acme",
      resourceId: identity.id,
      threadId: "t1",
    });
    await memory.appendMessages({ namespaceId: "acme", resourceId: identity.id, threadId: "t1" }, [
      { role: "user", content: "hi" },
    ]);

    const result = await wipeAgentIdentity({
      registry,
      memory,
      identityId: identity.id,
    });

    expect(result.identityId).toBe(identity.id);
    expect(result.threadsDeleted).toBe(1);
    expect(result.factsDeleted).toBe(1);

    // Registry row gone.
    expect(await registry.get(identity.id)).toBeNull();
    // Resource working memory cleared.
    const resourceRow = await memory.getResource({
      namespaceId: "acme",
      resourceId: identity.id,
    });
    expect(resourceRow?.workingMemory).toBeNull();
    // Threads gone.
    expect(await memory.listThreads({ namespaceId: "acme", resourceId: identity.id })).toHaveLength(
      0,
    );
    // Facts gone.
    expect(
      await memory.listResourceFacts({ namespaceId: "acme", resourceId: identity.id }),
    ).toHaveLength(0);
  });

  it("is a no-op for an unknown identity id", async () => {
    const registry = new InMemoryAgentIdentityRegistry();
    const memory = new InMemoryMemoryStore();
    const result = await wipeAgentIdentity({
      registry,
      memory,
      identityId: "acme::writer::ghost",
    });
    expect(result).toEqual({
      identityId: "acme::writer::ghost",
      threadsDeleted: 0,
      factsDeleted: 0,
      episodesDeleted: 0,
    });
  });
});
