import { describe, it, expect } from "bun:test";
import { InMemorySkillRegistry } from "../in-memory-skill-registry.ts";
import { createLoadSkillTool } from "../load-skill-tool.ts";
import type { ResolvedSkillEntry } from "../resolve-skill-catalog.ts";

const catalog: ResolvedSkillEntry[] = [
  { id: "writing-style", version: "v1", description: "Rubric.", whenToUse: "Drafting." },
];

async function seedRegistry() {
  const registry = new InMemorySkillRegistry();
  await registry.register({
    id: "writing-style",
    version: "v1",
    description: "Rubric.",
    whenToUse: "Drafting.",
    body: "# Writing\nBe concise (v1).",
  });
  return registry;
}

describe("createLoadSkillTool", () => {
  it("returns the pinned skill body for a catalog id", async () => {
    const registry = await seedRegistry();
    const t = createLoadSkillTool({ registry, catalog });
    const out = await t.execute({ id: "writing-style" });
    expect(out).toEqual({
      id: "writing-style",
      version: "v1",
      body: "# Writing\nBe concise (v1).",
    });
    expect(t.toModelOutput?.(out)).toContain("Be concise (v1)");
    expect(t.toResultMetadata?.(out)).toEqual({ skillId: "writing-style", skillVersion: "v1" });
  });

  it("returns an error result (not throw) for an id outside the catalog", async () => {
    const registry = await seedRegistry();
    const t = createLoadSkillTool({ registry, catalog });
    const out = await t.execute({ id: "not-in-catalog" });
    expect("error" in out).toBe(true);
    if ("error" in out) expect(out.error).toContain("Unknown skill");
    expect(t.toModelOutput?.(out)).toContain("Unknown skill");
    expect(t.toResultMetadata?.(out)).toBeUndefined();
  });

  it("loads the catalog-pinned version even when a newer one exists", async () => {
    const registry = await seedRegistry();
    // Publish a newer version after the catalog pinned v1.
    await registry.register({
      id: "writing-style",
      version: "v2",
      description: "Rubric v2.",
      whenToUse: "Drafting.",
      body: "# Writing\nBe concise (v2).",
    });
    const t = createLoadSkillTool({ registry, catalog }); // catalog still pins v1
    const out = await t.execute({ id: "writing-style" });
    if ("error" in out) throw new Error("expected a body");
    expect(out.version).toBe("v1");
    expect(out.body).toContain("(v1)");
  });

  it("returns an error result when the pinned version was unregistered mid-session", async () => {
    const registry = await seedRegistry();
    await registry.unregister("writing-style", "v1");
    const t = createLoadSkillTool({ registry, catalog });
    const out = await t.execute({ id: "writing-style" });
    expect("error" in out).toBe(true);
    if ("error" in out) expect(out.error).toContain("no longer available");
  });

  it("reports no skills available when the catalog is empty", async () => {
    const registry = await seedRegistry();
    const t = createLoadSkillTool({ registry, catalog: [] });
    expect(t.usage).toContain("No skills");
    const out = await t.execute({ id: "writing-style" });
    expect("error" in out).toBe(true);
  });
});
