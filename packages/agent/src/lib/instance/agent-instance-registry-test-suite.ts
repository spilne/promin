// ---------------------------------------------------------------------------
// Portable `AgentInstanceRegistry` conformance suite. Every implementation
// must pass: in-memory, SQLite, Postgres.
//
// Usage:
//   import { agentInstanceRegistryTestSuite } from "@promin/agent/testing";
//   agentInstanceRegistryTestSuite(() => new InMemoryAgentInstanceRegistry());
//
// The factory must return a registry on a real (wall) clock — the
// list-ordering test relies on `createdAt` advancing between creates.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "../memory/in-memory-memory-store.ts";
import { composeAgentInstanceId, type AgentInstanceRegistry } from "./types.ts";
import { wipeAgentInstance } from "./wipe.ts";

const wait = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export function agentInstanceRegistryTestSuite(
  factory: () => AgentInstanceRegistry | Promise<AgentInstanceRegistry>,
): void {
  const make = (): Promise<AgentInstanceRegistry> => Promise.resolve(factory());

  describe("AgentInstanceRegistry conformance", () => {
    it("resolveOrCreate is idempotent for the same triple", async () => {
      const reg = await make();
      const a = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      const b = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      expect(b.id).toBe(a.id);
      expect(b.createdAt).toBe(a.createdAt);
      expect(await reg.list()).toHaveLength(1);
    });

    it("composes a deterministic id from the triple", async () => {
      const reg = await make();
      const inst = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      expect(inst.id).toBe(
        composeAgentInstanceId({
          namespaceId: "acme",
          registeredAgentId: "writer",
          ownerId: "alice",
        }),
      );
    });

    it("distinct triples produce distinct instances — namespace isolation", async () => {
      const reg = await make();
      const acme = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      const globex = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "globex",
        ownerId: "alice",
      });
      expect(acme.id).not.toBe(globex.id);
      expect(await reg.list()).toHaveLength(2);
    });

    it("displayName + metadata from the input apply only on creation", async () => {
      const reg = await make();
      await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
        displayName: "first",
        metadata: { v: 1 },
      });
      // Re-resolving the same triple must NOT overwrite displayName / metadata.
      const again = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
        displayName: "second",
        metadata: { v: 2 },
      });
      expect(again.displayName).toBe("first");
      expect(again.metadata).toEqual({ v: 1 });
    });

    it("list filters by namespace / ownerId / registeredAgentId", async () => {
      const reg = await make();
      await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      await reg.resolveOrCreate({
        registeredAgentId: "reviewer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "bob",
      });
      await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "globex",
        ownerId: "alice",
      });

      expect(await reg.list({ namespaceId: "acme" })).toHaveLength(3);
      expect(await reg.list({ ownerId: "alice" })).toHaveLength(3);
      expect(await reg.list({ registeredAgentId: "writer" })).toHaveLength(3);
      expect(
        await reg.list({ namespaceId: "acme", ownerId: "alice", registeredAgentId: "writer" }),
      ).toHaveLength(1);
    });

    it("list orders by createdDesc by default, createdAsc on request", async () => {
      const reg = await make();
      const a = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      await wait(8);
      const b = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "bob",
      });

      expect((await reg.list()).map((i) => i.id)).toEqual([b.id, a.id]);
      expect((await reg.list({ order: "createdAsc" })).map((i) => i.id)).toEqual([a.id, b.id]);
    });

    it("list respects limit", async () => {
      const reg = await make();
      await reg.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "acme", ownerId: "a" });
      await reg.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "acme", ownerId: "b" });
      await reg.resolveOrCreate({ registeredAgentId: "writer", namespaceId: "acme", ownerId: "c" });
      expect(await reg.list({ limit: 2 })).toHaveLength(2);
    });

    it("update patches displayName + metadata; the change persists", async () => {
      const reg = await make();
      const created = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      const updated = await reg.update(created.id, {
        displayName: "Alice's writer",
        metadata: { color: "blue", priority: 2 },
      });
      expect(updated.displayName).toBe("Alice's writer");
      expect(updated.metadata).toEqual({ color: "blue", priority: 2 });
      expect((await reg.get(created.id))?.metadata).toEqual({ color: "blue", priority: 2 });
    });

    it("update throws on an unknown instance", async () => {
      const reg = await make();
      await expect(reg.update("acme::writer::ghost", { displayName: "x" })).rejects.toThrow();
    });

    it("get returns null for an unknown id", async () => {
      const reg = await make();
      expect(await reg.get("acme::writer::nobody")).toBeNull();
    });

    it("delete removes the row and is idempotent", async () => {
      const reg = await make();
      const created = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      await reg.delete(created.id);
      expect(await reg.get(created.id)).toBeNull();
      await reg.delete(created.id); // no throw
    });

    it("wipeAgentInstance drops the row and clears resource memory", async () => {
      const reg = await make();
      const memory = new InMemoryMemoryStore();
      const inst = await reg.resolveOrCreate({
        registeredAgentId: "writer",
        namespaceId: "acme",
        ownerId: "alice",
      });
      await memory.upsertResource(
        { namespaceId: "acme", resourceId: inst.id },
        { workingMemory: "draft" },
      );
      await memory.createThread({ namespaceId: "acme", resourceId: inst.id, threadId: "t1" });

      const result = await wipeAgentInstance({ registry: reg, memory, instanceId: inst.id });
      expect(result.threadsDeleted).toBe(1);
      expect(await reg.get(inst.id)).toBeNull();
      const row = await memory.getResource({ namespaceId: "acme", resourceId: inst.id });
      expect(row?.workingMemory).toBeNull();
    });
  });
}
