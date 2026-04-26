// ---------------------------------------------------------------------------
// Portable `AgentRegistry` conformance suite. Every implementation
// (in-memory, SQLite, future Postgres) must pass.
//
// Usage:
//   import { agentRegistryTestSuite } from "@promin/agent/testing";
//   agentRegistryTestSuite(() => new InMemoryAgentRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { AgentBackend, AgentRegistry, RegisterAgentInput } from "./types.ts";

function localBackend(patch?: Partial<Extract<AgentBackend, { type: "local" }>>): AgentBackend {
  return {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: null,
    tools: [],
    ...patch,
  };
}

function input(id: string, patch?: Partial<RegisterAgentInput>): RegisterAgentInput {
  return {
    id,
    backend: localBackend(),
    ...patch,
  };
}

export function agentRegistryTestSuite(factory: () => AgentRegistry | Promise<AgentRegistry>) {
  async function make(): Promise<AgentRegistry> {
    return factory();
  }

  describe("AgentRegistry conformance", () => {
    // --- register + get -----------------------------------------------
    describe("register + get", () => {
      it("returns a row with createdAt + updatedAt + default version", async () => {
        const r = await make();
        const row = await r.register(input("support"));
        expect(row.id).toBe("support");
        expect(row.version).toBe("v1");
        expect(typeof row.createdAt).toBe("number");
        expect(typeof row.updatedAt).toBe("number");
      });

      it("respects a caller-supplied version", async () => {
        const r = await make();
        await r.register(input("support", { version: "v2" }));
        expect((await r.get("support", "v2"))?.version).toBe("v2");
      });

      it("get(id) returns the most recently updated version", async () => {
        const r = await make();
        await r.register(input("support", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("support", { version: "v2" }));
        const latest = await r.get("support");
        expect(latest?.version).toBe("v2");
      });

      it("get returns null when not found", async () => {
        const r = await make();
        expect(await r.get("missing")).toBeNull();
        expect(await r.get("missing", "v1")).toBeNull();
      });

      it("re-registering same (id, version) updates backend + metadata, preserves createdAt", async () => {
        const r = await make();
        const first = await r.register(input("support"));
        await new Promise((res) => setTimeout(res, 2));
        const second = await r.register(
          input("support", {
            backend: localBackend({ tools: ["search"] }),
            metadata: { tags: ["beta"] },
          }),
        );
        expect(second.createdAt).toBe(first.createdAt);
        expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
        if (second.backend.type === "local") {
          expect(second.backend.tools).toEqual(["search"]);
        }
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
        await r.register(input("a", { metadata: { capabilities: ["summarize"] } }));
        await r.register(input("b", { metadata: { capabilities: ["codegen"] } }));
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

      it("filters by backendType", async () => {
        const r = await make();
        await r.register(input("a"));
        const all = await r.list({ backendType: "local" });
        expect(all).toHaveLength(1);
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
        await r.register(input("support", { version: "v1" }));
        await new Promise((res) => setTimeout(res, 2));
        await r.register(input("support", { version: "v2" }));
        const vs = await r.versions("support");
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
        await r.register(input("support", { version: "v1" }));
        await r.register(input("support", { version: "v2" }));
        await r.unregister("support", "v1");
        expect(await r.get("support", "v1")).toBeNull();
        expect(await r.get("support", "v2")).not.toBeNull();
      });

      it("removes all versions when version is omitted", async () => {
        const r = await make();
        await r.register(input("support", { version: "v1" }));
        await r.register(input("support", { version: "v2" }));
        await r.unregister("support");
        expect(await r.versions("support")).toEqual([]);
      });

      it("is a no-op when nothing matches", async () => {
        const r = await make();
        await r.unregister("never-existed");
        await r.unregister("never-existed", "v3");
      });
    });
  });
}
