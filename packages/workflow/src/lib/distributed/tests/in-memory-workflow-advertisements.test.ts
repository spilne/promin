import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";
import { workflowAdvertisementRegistryTestSuite } from "../workflow-advertisements-test-suite.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

workflowAdvertisementRegistryTestSuite(() => new InMemoryWorkflowAdvertisementRegistry());

describe("InMemoryWorkflowAdvertisementRegistry — advertisedAt on an injected clock", () => {
  it("stamps advertisedAt from the clock and refreshes it on re-advertise", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryWorkflowAdvertisementRegistry({ clock });
    const workflows = [{ name: "wf", steps: [] }];

    await registry.upsert("w-1", workflows);
    expect((await registry.list())[0]?.advertisedAt.toISOString()).toBe("2026-01-01T00:00:00.000Z");

    clock.advance(5_000);
    await registry.upsert("w-1", workflows);
    expect((await registry.list())[0]?.advertisedAt.toISOString()).toBe("2026-01-01T00:00:05.000Z");
  });
});
