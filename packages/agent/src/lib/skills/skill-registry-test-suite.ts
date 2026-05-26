// ---------------------------------------------------------------------------
// Portable `SkillRegistry` conformance suite. Every implementation
// (in-memory, future Postgres) must pass. Mirrors `agentRegistryTestSuite`.
//
// Usage:
//   import { skillRegistryTestSuite } from "@promin/agent/testing";
//   skillRegistryTestSuite(() => new InMemorySkillRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { RegisterSkillInput, SkillRegistry } from "./types.ts";

function input(id: string, patch?: Partial<RegisterSkillInput>): RegisterSkillInput {
  return {
    id,
    description: `${id} description`,
    whenToUse: `use ${id} when relevant`,
    body: `# ${id}\n\nInstructions for ${id}.`,
    ...patch,
  };
}

export function skillRegistryTestSuite(factory: () => SkillRegistry | Promise<SkillRegistry>) {
  async function make(): Promise<SkillRegistry> {
    return factory();
  }

  describe("SkillRegistry conformance", () => {
    // --- register + get -----------------------------------------------
    describe("register + get", () => {
      it("returns a row with content fields + createdAt + updatedAt + default version", async () => {
        const r = await make();
        const row = await r.register(input("structured-debugging"));
        expect(row.id).toBe("structured-debugging");
        expect(row.version).toBe("v1");
        expect(row.description).toBe("structured-debugging description");
        expect(row.whenToUse).toBe("use structured-debugging when relevant");
        expect(row.body).toContain("Instructions for structured-debugging");
        expect(typeof row.createdAt).toBe("number");
        expect(typeof row.updatedAt).toBe("number");
      });

      it("defaults metadata to empty capabilities + tags", async () => {
        const r = await make();
        const row = await r.register(input("writing-style"));
        expect(row.metadata.capabilities).toEqual([]);
        expect(row.metadata.tags).toEqual([]);
        expect(row.metadata.enabled).toBeUndefined();
      });

      it("respects a caller-supplied version", async () => {
        const r = await make();
        await r.register(input("writing-style", { version: "v2" }));
        expect((await r.get("writing-style", "v2"))?.version).toBe("v2");
      });

      it("get(id) returns the most recently updated version", async () => {
        const r = await make();
        await r.register(input("writing-style", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("writing-style", { version: "v2" }));
        const latest = await r.get("writing-style");
        expect(latest?.version).toBe("v2");
      });

      it("get returns null when not found", async () => {
        const r = await make();
        expect(await r.get("missing")).toBeNull();
        expect(await r.get("missing", "v1")).toBeNull();
      });

      it("re-registering same (id, version) replaces content + metadata, preserves createdAt", async () => {
        const r = await make();
        const first = await r.register(input("writing-style"));
        await new Promise((res) => setTimeout(res, 2));
        const second = await r.register(
          input("writing-style", {
            body: "# updated body",
            metadata: { tags: ["beta"], capabilities: ["writing"] },
          }),
        );
        expect(second.createdAt).toBe(first.createdAt);
        expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
        expect(second.body).toBe("# updated body");
        expect(second.metadata.tags).toEqual(["beta"]);
        expect(second.metadata.capabilities).toEqual(["writing"]);
      });

      it("carries the enabled kill-switch through", async () => {
        const r = await make();
        const row = await r.register(input("writing-style", { metadata: { enabled: false } }));
        expect(row.metadata.enabled).toBe(false);
      });
    });

    // --- list ---------------------------------------------------------
    describe("list", () => {
      it("returns all rows when no filters", async () => {
        const r = await make();
        await r.register(input("a"));
        await r.register(input("b"));
        const all = await r.list();
        expect(all.map((x) => x.id).sort()).toEqual(["a", "b"]);
      });

      it("filters by capability", async () => {
        const r = await make();
        await r.register(input("a", { metadata: { capabilities: ["rag"] } }));
        await r.register(input("b", { metadata: { capabilities: ["codegen"] } }));
        const rag = await r.list({ capability: "rag" });
        expect(rag.map((x) => x.id)).toEqual(["a"]);
      });

      it("filters by tag", async () => {
        const r = await make();
        await r.register(input("a", { metadata: { tags: ["beta"] } }));
        await r.register(input("b", { metadata: { tags: ["stable"] } }));
        const beta = await r.list({ tag: "beta" });
        expect(beta.map((x) => x.id)).toEqual(["a"]);
      });

      it("orders by createdAsc / createdDesc", async () => {
        const r = await make();
        await r.register(input("a"));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("b"));
        const asc = await r.list({ order: "createdAsc" });
        expect(asc.map((x) => x.id)).toEqual(["a", "b"]);
        const desc = await r.list({ order: "createdDesc" });
        expect(desc.map((x) => x.id)).toEqual(["b", "a"]);
      });

      it("respects limit + cursor pagination", async () => {
        const r = await make();
        for (const id of ["a", "b", "c"]) await r.register(input(id));
        const page1 = await r.list({ order: "idAsc", limit: 2 });
        expect(page1.map((x) => x.id)).toEqual(["a", "b"]);
        const page2 = await r.list({ order: "idAsc", limit: 2, cursor: "2" });
        expect(page2.map((x) => x.id)).toEqual(["c"]);
      });
    });

    // --- versions -----------------------------------------------------
    describe("versions", () => {
      it("returns all versions of one id, oldest first", async () => {
        const r = await make();
        await r.register(input("writing-style", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("writing-style", { version: "v2" }));
        const vs = await r.versions("writing-style");
        expect(vs.map((x) => x.version)).toEqual(["v1", "v2"]);
      });

      it("returns empty when id not found", async () => {
        const r = await make();
        expect(await r.versions("missing")).toEqual([]);
      });
    });

    // --- unregister --------------------------------------------------
    describe("unregister", () => {
      it("removes a single (id, version)", async () => {
        const r = await make();
        await r.register(input("writing-style", { version: "v1" }));
        await r.register(input("writing-style", { version: "v2" }));
        await r.unregister("writing-style", "v1");
        expect(await r.get("writing-style", "v1")).toBeNull();
        expect(await r.get("writing-style", "v2")).not.toBeNull();
      });

      it("removes all versions when version is omitted", async () => {
        const r = await make();
        await r.register(input("writing-style", { version: "v1" }));
        await r.register(input("writing-style", { version: "v2" }));
        await r.unregister("writing-style");
        expect(await r.versions("writing-style")).toEqual([]);
      });

      it("is a no-op when nothing matches", async () => {
        const r = await make();
        await r.unregister("never-existed");
        await r.unregister("never-existed", "v3");
      });
    });
  });
}
