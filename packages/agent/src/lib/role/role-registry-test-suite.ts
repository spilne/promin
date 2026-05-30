// ---------------------------------------------------------------------------
// Portable `RoleRegistry` conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass. Mirrors the AgentRegistry suite.
//
// Usage:
//   import { roleRegistryTestSuite } from "@promin/agent/testing";
//   roleRegistryTestSuite(() => new InMemoryRoleRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { RegisterRoleInput, RoleDefinition, RoleRegistry } from "./types.ts";

function definition(patch?: Partial<RoleDefinition>): RoleDefinition {
  return {
    systemPrompt: "You are a helpful assistant.",
    tools: [],
    ...patch,
  };
}

function input(id: string, patch?: Partial<RegisterRoleInput>): RegisterRoleInput {
  return {
    id,
    definition: definition(),
    ...patch,
  };
}

export function roleRegistryTestSuite(factory: () => RoleRegistry | Promise<RoleRegistry>) {
  async function make(): Promise<RoleRegistry> {
    return factory();
  }

  describe("RoleRegistry conformance", () => {
    // --- register + get -----------------------------------------------
    describe("register + get", () => {
      it("returns a row with createdAt + updatedAt + default version", async () => {
        const r = await make();
        const row = await r.register(input("debugger"));
        expect(row.id).toBe("debugger");
        expect(row.version).toBe("v1");
        expect(typeof row.createdAt).toBe("number");
        expect(typeof row.updatedAt).toBe("number");
      });

      it("respects a caller-supplied version", async () => {
        const r = await make();
        await r.register(input("debugger", { version: "v2" }));
        expect((await r.get("debugger", "v2"))?.version).toBe("v2");
      });

      it("get(id) returns the most recently updated version", async () => {
        const r = await make();
        await r.register(input("debugger", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("debugger", { version: "v2" }));
        const latest = await r.get("debugger");
        expect(latest?.version).toBe("v2");
      });

      it("get returns null when not found", async () => {
        const r = await make();
        expect(await r.get("missing")).toBeNull();
        expect(await r.get("missing", "v1")).toBeNull();
      });

      it("round-trips the layered prompt + tools + skills + capabilities", async () => {
        const r = await make();
        await r.register(
          input("git-master", {
            definition: definition({
              systemPrompt: { base: "you are a git master", layers: ["findings-table"] },
              tools: ["bash"],
              skills: [{ id: "structured-debugging", version: "v2" }],
              capabilities: ["chat", "live"],
            }),
            metadata: { description: "Git expert", tags: ["role"], suggestedSecrets: ["GH_TOKEN"] },
          }),
        );
        const got = await r.get("git-master");
        expect(got?.definition.systemPrompt).toEqual({
          base: "you are a git master",
          layers: ["findings-table"],
        });
        expect(got?.definition.tools).toEqual(["bash"]);
        expect(got?.definition.skills).toEqual([{ id: "structured-debugging", version: "v2" }]);
        expect(got?.definition.capabilities).toEqual(["chat", "live"]);
        expect(got?.metadata.description).toBe("Git expert");
        expect(got?.metadata.suggestedSecrets).toEqual(["GH_TOKEN"]);
      });

      it("re-registering same (id, version) updates definition + metadata, preserves createdAt", async () => {
        const r = await make();
        const first = await r.register(input("debugger"));
        await new Promise((res) => setTimeout(res, 2));
        const second = await r.register(
          input("debugger", {
            definition: definition({ tools: ["bash"] }),
            metadata: { tags: ["beta"] },
          }),
        );
        expect(second.createdAt).toBe(first.createdAt);
        expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
        expect(second.definition.tools).toEqual(["bash"]);
        expect(second.metadata.tags).toEqual(["beta"]);
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
        await r.register(input("a", { definition: definition({ capabilities: ["summarize"] }) }));
        await r.register(input("b", { definition: definition({ capabilities: ["codegen"] }) }));
        const summarizers = await r.list({ capability: "summarize" });
        expect(summarizers.map((x) => x.id)).toEqual(["a"]);
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
        await r.register(input("debugger", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("debugger", { version: "v2" }));
        const vs = await r.versions("debugger");
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
        await r.register(input("debugger", { version: "v1" }));
        await r.register(input("debugger", { version: "v2" }));
        await r.unregister("debugger", "v1");
        expect(await r.get("debugger", "v1")).toBeNull();
        expect(await r.get("debugger", "v2")).not.toBeNull();
      });

      it("removes all versions when version is omitted", async () => {
        const r = await make();
        await r.register(input("debugger", { version: "v1" }));
        await r.register(input("debugger", { version: "v2" }));
        await r.unregister("debugger");
        expect(await r.versions("debugger")).toEqual([]);
      });

      it("is a no-op when nothing matches", async () => {
        const r = await make();
        await r.unregister("never-existed");
        await r.unregister("never-existed", "v3");
      });
    });
  });
}
