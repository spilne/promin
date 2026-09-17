// ---------------------------------------------------------------------------
// Portable `DagRegistry` conformance suite. Every implementation
// (in-memory, SQLite, future Postgres) must pass identically.
//
// Usage:
//   import { dagRegistryTestSuite } from "@promin/agent/testing";
//   dagRegistryTestSuite(() => new InMemoryDagRegistry());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { DagValidationError } from "./types.ts";
import type { DagRegistry, RegisterDagInput } from "./registry.ts";

const VALID: Omit<RegisterDagInput, "id" | "version"> = {
  nodes: [
    { id: "a", agentId: "x", inputs: { task: { kind: "initial", path: "task" } } },
    { id: "b", agentId: "x", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
  ],
  edges: [{ from: "a", to: "b" }],
  entry: ["a"],
  terminals: ["b"],
};

const BAD: Omit<RegisterDagInput, "id" | "version"> = {
  nodes: [{ id: "a", agentId: "x", inputs: {} }],
  edges: [{ from: "a", to: "ghost" }],
  entry: ["a"],
  terminals: ["a"],
};

export function dagRegistryTestSuite(factory: () => DagRegistry | Promise<DagRegistry>) {
  async function make(): Promise<DagRegistry> {
    return factory();
  }

  describe("DagRegistry conformance", () => {
    it("register + get round-trips", async () => {
      const r = await make();
      const row = await r.register({ id: "research", version: "v1", ...VALID });
      expect(row.id).toBe("research");
      expect(row.version).toBe("v1");
      expect(typeof row.createdAt).toBe("number");
      expect(typeof row.updatedAt).toBe("number");

      const fetched = await r.get("research", "v1");
      expect(fetched).not.toBeNull();
      expect(fetched!.nodes).toHaveLength(2);
    });

    it("default version is v1 when omitted", async () => {
      const r = await make();
      const row = await r.register({ id: "x", ...VALID });
      expect(row.version).toBe("v1");
    });

    it("get with no version returns latest by updatedAt", async () => {
      const r = await make();
      await r.register({ id: "r", version: "v1", ...VALID });
      // Small delay so timestamps differ even on fast clocks. Some impls
      // compress to ms granularity; sleeping > 1ms ensures monotonic order.
      await new Promise((res) => setTimeout(res, 5));
      await r.register({ id: "r", version: "v2", ...VALID });

      const latest = await r.get("r");
      expect(latest!.version).toBe("v2");
    });

    it("versions returns ascending by createdAt", async () => {
      const r = await make();
      await r.register({ id: "x", version: "v1", ...VALID });
      await new Promise((res) => setTimeout(res, 5));
      await r.register({ id: "x", version: "v2", ...VALID });
      await new Promise((res) => setTimeout(res, 5));
      await r.register({ id: "x", version: "v3", ...VALID });

      const vs = await r.versions("x");
      expect(vs.map((v) => v.version)).toEqual(["v1", "v2", "v3"]);
    });

    it("re-register same version preserves createdAt; updatedAt advances", async () => {
      const r = await make();
      const a = await r.register({ id: "y", version: "v1", ...VALID });
      await new Promise((res) => setTimeout(res, 10));
      const b = await r.register({
        id: "y",
        version: "v1",
        ...VALID,
        metadata: { description: "updated" },
      });
      expect(b.createdAt).toBe(a.createdAt);
      expect(b.updatedAt).toBeGreaterThan(a.updatedAt);
      expect(b.metadata?.description).toBe("updated");
    });

    it("list returns latest per id", async () => {
      const r = await make();
      await r.register({ id: "research", version: "v1", ...VALID });
      await new Promise((res) => setTimeout(res, 5));
      await r.register({ id: "research", version: "v2", ...VALID });
      await r.register({ id: "support", version: "v1", ...VALID });

      const all = await r.list();
      expect(all.map((x) => `${x.id}@${x.version}`).sort()).toEqual(["research@v2", "support@v1"]);
    });

    it("list filters by tag", async () => {
      const r = await make();
      await r.register({
        id: "a",
        version: "v1",
        ...VALID,
        metadata: { tags: ["analysis", "stable"] },
      });
      await r.register({
        id: "b",
        version: "v1",
        ...VALID,
        metadata: { tags: ["routing"] },
      });

      const stable = await r.list({ tag: "stable" });
      expect(stable.map((x) => x.id)).toEqual(["a"]);

      const routing = await r.list({ tag: "routing" });
      expect(routing.map((x) => x.id)).toEqual(["b"]);
    });

    it("unregister(id, version) drops one row; unregister(id) drops all", async () => {
      const r = await make();
      await r.register({ id: "z", version: "v1", ...VALID });
      await r.register({ id: "z", version: "v2", ...VALID });

      await r.unregister("z", "v1");
      expect(await r.get("z", "v1")).toBeNull();
      expect(await r.get("z", "v2")).not.toBeNull();

      await r.unregister("z");
      expect(await r.versions("z")).toHaveLength(0);
    });

    it("rejects invalid graphs at register time", async () => {
      const r = await make();
      await expect(
        r.register({ id: "broken", version: "v1", ...BAD } as RegisterDagInput),
      ).rejects.toBeInstanceOf(DagValidationError);
    });

    it("get returns null for missing id / missing version", async () => {
      const r = await make();
      expect(await r.get("nope")).toBeNull();
      expect(await r.get("nope", "v1")).toBeNull();
      await r.register({ id: "exists", version: "v1", ...VALID });
      expect(await r.get("exists", "v999")).toBeNull();
    });

    it("metadata round-trips through encode + decode", async () => {
      const r = await make();
      const stored = await r.register({
        id: "m",
        version: "v1",
        ...VALID,
        metadata: {
          description: "round-trip test",
          tags: ["a", "b", "c"],
        },
      });
      const fetched = await r.get("m", "v1");
      expect(fetched!.metadata).toEqual(stored.metadata);
    });
  });
}
