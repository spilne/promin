import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemoryEvalSpecRegistry } from "../in-memory-registry.ts";
import { evalSpecRegistryTestSuite } from "../registry-test-suite.ts";

evalSpecRegistryTestSuite(() => new InMemoryEvalSpecRegistry());

describe("InMemoryEvalSpecRegistry", () => {
  it("stamps createdAt / updatedAt from the clock", async () => {
    const registry = new InMemoryEvalSpecRegistry({ clock: FakeWallClock.create(4242) });
    const spec = await registry.register({
      id: "s",
      dataset: { kind: "inline", id: "d", cases: [] },
      targets: [],
      scorers: [],
    });
    expect(spec.createdAt).toBe(4242);
    expect(spec.updatedAt).toBe(4242);
  });
});
