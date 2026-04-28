// ---------------------------------------------------------------------------
// `SqliteAgentIdentityRegistry` — pins the same contract as the in-memory
// reference plus persistence-across-instances. The shared conformance
// suite hasn't been factored out yet (mirrors the AgentRegistry pattern
// that was retrofitted later); when it is, this file becomes a one-liner
// invocation.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { composeAgentIdentityId, InMemoryMemoryStore, wipeAgentIdentity } from "@promin/agent";
import { SqliteAgentIdentityRegistry } from "../sqlite-agent-identity-registry.ts";

function makeRegistry() {
  return SqliteAgentIdentityRegistry.make({ db: new Database(":memory:") });
}

describe("SqliteAgentIdentityRegistry", () => {
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
      composeAgentIdentityId({
        namespaceId: "acme",
        registeredAgentId: "writer",
        ownerId: "alice",
      }),
    );
  });

  it("isolates identities across namespaces", async () => {
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

  it("list filters and orders by lastActiveDesc", async () => {
    const registry = makeRegistry();
    await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await new Promise((r) => setTimeout(r, 5));
    const second = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    await new Promise((r) => setTimeout(r, 5));
    await registry.touch(second.id);

    const list = await registry.list({ namespaceId: "acme" });
    expect(list[0]!.id).toBe(second.id);
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

describe("SqliteAgentIdentityRegistry — persistence", () => {
  it("identities survive across instances sharing one db (acceptance criterion #2)", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteAgentIdentityRegistry.make({ db });
    const created = await r1.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
      displayName: "alice's writer",
      metadata: { color: "blue" },
    });

    const r2 = SqliteAgentIdentityRegistry.make({ db });
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

describe("SqliteAgentIdentityRegistry — migration from pre-rename schema", () => {
  it("renames user_id → owner_id on first open of a legacy db", async () => {
    const db = new Database(":memory:");
    // Stand up the legacy schema by hand — this is what existed before
    // the rename commit shipped.
    db.run(`
      CREATE TABLE promin_agent_identity (
        id                  TEXT PRIMARY KEY,
        registered_agent_id TEXT NOT NULL,
        namespace_id        TEXT NOT NULL,
        user_id             TEXT NOT NULL,
        display_name        TEXT,
        metadata            TEXT NOT NULL,
        created_at          INTEGER NOT NULL,
        last_active_at      INTEGER NOT NULL
      )
    `);
    db.run(
      `CREATE INDEX promin_agent_identity_ns_user ON promin_agent_identity (namespace_id, user_id)`,
    );
    db.run(
      `INSERT INTO promin_agent_identity
         (id, registered_agent_id, namespace_id, user_id, display_name, metadata, created_at, last_active_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      "acme::writer::alice",
      "writer",
      "acme",
      "alice",
      "Alice's writer",
      "{}",
      1_000,
      1_000,
    );

    // Constructor runs _setup which should rename the column in place.
    const registry = SqliteAgentIdentityRegistry.make({ db });
    const got = await registry.get("acme::writer::alice");
    expect(got).not.toBeNull();
    expect(got?.ownerId).toBe("alice");
    expect(got?.displayName).toBe("Alice's writer");

    // The new index exists.
    const indexes = db
      .query<{ name: string }>(`SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?`)
      .all("promin_agent_identity")
      .map((r) => r.name);
    expect(indexes).toContain("promin_agent_identity_ns_owner");
    expect(indexes).not.toContain("promin_agent_identity_ns_user");
  });

  it("is idempotent — running setup twice on a migrated db is a no-op", async () => {
    const db = new Database(":memory:");
    db.run(`
      CREATE TABLE promin_agent_identity (
        id                  TEXT PRIMARY KEY,
        registered_agent_id TEXT NOT NULL,
        namespace_id        TEXT NOT NULL,
        user_id             TEXT NOT NULL,
        display_name        TEXT,
        metadata            TEXT NOT NULL,
        created_at          INTEGER NOT NULL,
        last_active_at      INTEGER NOT NULL
      )
    `);
    SqliteAgentIdentityRegistry.make({ db }); // first run migrates
    SqliteAgentIdentityRegistry.make({ db }); // second run must not throw or re-rename
    const registry = SqliteAgentIdentityRegistry.make({ db });
    const created = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "bob",
    });
    expect(created.ownerId).toBe("bob");
  });
});

describe("wipeAgentIdentity over SqliteAgentIdentityRegistry", () => {
  it("removes the row + clears resource memory", async () => {
    const registry = makeRegistry();
    const memory = new InMemoryMemoryStore();
    const identity = await registry.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    await memory.upsertResource(
      { namespaceId: "acme", resourceId: identity.id },
      { workingMemory: "draft" },
    );
    await memory.createThread({
      namespaceId: "acme",
      resourceId: identity.id,
      threadId: "t1",
    });

    const result = await wipeAgentIdentity({
      registry,
      memory,
      identityId: identity.id,
    });
    expect(result.threadsDeleted).toBe(1);
    expect(await registry.get(identity.id)).toBeNull();
    const row = await memory.getResource({ namespaceId: "acme", resourceId: identity.id });
    expect(row?.workingMemory).toBeNull();
  });
});
