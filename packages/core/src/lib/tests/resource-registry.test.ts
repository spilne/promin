import { describe, expect, it } from "bun:test";
import { InMemoryResourceRegistry } from "../resource-registry.ts";

describe("InMemoryResourceRegistry", () => {
  it("replaces, lists, gets, and deletes keyed resources", () => {
    const registry = new InMemoryResourceRegistry<{ id: string; rank: number }, string>({
      keyOf: (resource) => resource.id,
      compare: (a, b) => a.rank - b.rank,
    });

    registry.set({ id: "slow", rank: 2 });
    registry.set({ id: "fast", rank: 1 });
    registry.set({ id: "fast", rank: 0 });

    expect(registry.get("fast")).toEqual({ id: "fast", rank: 0 });
    expect(registry.list().map((resource) => resource.id)).toEqual(["fast", "slow"]);
    expect(registry.delete("slow")).toBe(true);
    expect(registry.delete("slow")).toBe(false);
    expect(registry.list()).toEqual([{ id: "fast", rank: 0 }]);
  });
});
