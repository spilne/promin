import { describe, it, expect } from "bun:test";
import { FakeClock } from "@promin/core";
import { skillRegistryTestSuite } from "../skill-registry-test-suite.ts";
import { InMemorySkillRegistry } from "../in-memory-skill-registry.ts";

skillRegistryTestSuite(() => new InMemorySkillRegistry());

describe("InMemorySkillRegistry — clock", () => {
  it("stamps createdAt/updatedAt from the injected clock", async () => {
    const clock = FakeClock.create(1000);
    const r = new InMemorySkillRegistry({ clock });
    const created = await r.register({
      id: "structured-debugging",
      description: "A disciplined debugging loop.",
      whenToUse: "When a bug resists a quick fix.",
      body: "# Structured debugging\n\n1. Reproduce. 2. Bisect. 3. Hypothesize.",
    });
    expect(created.createdAt).toBe(1000);
    expect(created.updatedAt).toBe(1000);

    clock.advance(500);
    const updated = await r.register({
      id: "structured-debugging",
      description: "A disciplined debugging loop (v2).",
      whenToUse: "When a bug resists a quick fix.",
      body: "# Structured debugging\n\nUpdated steps.",
    });
    expect(updated.createdAt).toBe(1000);
    expect(updated.updatedAt).toBe(1500);
  });
});
