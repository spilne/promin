// ---------------------------------------------------------------------------
// evalSpecRegistryTestSuite — shared conformance suite for EvalSpecRegistry.
// Every backend (InMemory / Sqlite / Postgres) runs this exact suite.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { EvalSpecRegistry, RegisterEvalSpecInput } from "./types.ts";

/** A minimal valid registration input, overriding only what a test needs. */
function input(over: Partial<RegisterEvalSpecInput> = {}): RegisterEvalSpecInput {
  return {
    id: "spec",
    dataset: { kind: "inline", id: "ds", cases: [{ id: "1", input: "a", expected: "A" }] },
    targets: [{ kind: "recipe", recipeId: "bot" }],
    scorers: [{ kind: "exactMatch" }],
    ...over,
  };
}

/** Run the EvalSpecRegistry conformance suite against a fresh registry per test. */
export function evalSpecRegistryTestSuite(makeRegistry: () => EvalSpecRegistry): void {
  describe("EvalSpecRegistry conformance", () => {
    it("registers a spec and defaults the version to v1", async () => {
      const registry = makeRegistry();
      const spec = await registry.register(input());
      expect(spec.version).toBe("v1");
      expect(spec.createdAt).toBeGreaterThan(0);
      const got = await registry.get("spec");
      expect(got?.id).toBe("spec");
    });

    it("returns null for an unknown id", async () => {
      expect(await makeRegistry().get("nope")).toBeNull();
    });

    it("preserves createdAt across a re-register, bumps updatedAt", async () => {
      const registry = makeRegistry();
      const first = await registry.register(input({ description: "v1 desc" }));
      const second = await registry.register(input({ description: "v1 desc edited" }));
      expect(second.createdAt).toBe(first.createdAt);
      expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
      expect(second.description).toBe("v1 desc edited");
    });

    it("get without a version returns the most recently updated one", async () => {
      const registry = makeRegistry();
      await registry.register(input({ version: "v1" }));
      await registry.register(input({ version: "v2", description: "newer" }));
      const got = await registry.get("spec");
      expect(got?.version).toBe("v2");
    });

    it("lists the newest version per id", async () => {
      const registry = makeRegistry();
      await registry.register(input({ id: "a", version: "v1" }));
      await registry.register(input({ id: "a", version: "v2" }));
      await registry.register(input({ id: "b" }));
      const all = await registry.list();
      expect(all.map((s) => s.id).sort()).toEqual(["a", "b"]);
    });

    it("versions returns every version of one id", async () => {
      const registry = makeRegistry();
      await registry.register(input({ version: "v1" }));
      await registry.register(input({ version: "v2" }));
      const versions = await registry.versions("spec");
      expect(versions.map((s) => s.version)).toEqual(["v1", "v2"]);
    });

    it("unregister without a version removes every version", async () => {
      const registry = makeRegistry();
      await registry.register(input({ version: "v1" }));
      await registry.register(input({ version: "v2" }));
      await registry.unregister("spec");
      expect(await registry.get("spec")).toBeNull();
      expect((await registry.versions("spec")).length).toBe(0);
    });

    it("unregister with a version removes only that row", async () => {
      const registry = makeRegistry();
      await registry.register(input({ version: "v1" }));
      await registry.register(input({ version: "v2" }));
      await registry.unregister("spec", "v1");
      expect(await registry.get("spec", "v1")).toBeNull();
      expect(await registry.get("spec", "v2")).not.toBeNull();
    });
  });
}
