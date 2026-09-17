import { describe, it, expect } from "bun:test";
import { InMemorySkillRegistry } from "../in-memory-skill-registry.ts";
import {
  buildSkillCatalogPrompt,
  resolveSkillCatalog,
  skillAllowedByCapabilities,
  type ResolvedSkillEntry,
} from "../resolve-skill-catalog.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import { inlineRoleDefinition } from "../../role/resolve-role.ts";
import type { SkillRef } from "../types.ts";

/**
 * Resolve a recipe's skill catalog, sourcing the role's skills the way a host
 * does: the bound role's `definition.skills` are passed to `resolveSkillCatalog`.
 */
function catalogFor(
  recipe: RegisteredAgent,
  opts: { registry: InMemorySkillRegistry; onMissing?: "throw" | "skip" },
): Promise<ResolvedSkillEntry[]> {
  const skills =
    recipe.backend.type === "local" ? inlineRoleDefinition(recipe.backend.role)?.skills : undefined;
  return resolveSkillCatalog({
    recipe,
    registry: opts.registry,
    skills,
    ...(opts.onMissing !== undefined && { onMissing: opts.onMissing }),
  });
}

function recipe(skills?: ReadonlyArray<SkillRef>): RegisteredAgent {
  return {
    id: "writer-bot",
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: {
        inline: {
          systemPrompt: "You are a writer.",
          tools: [],
          ...(skills !== undefined && { skills }),
        },
      },
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };
}

async function seed(registry: InMemorySkillRegistry) {
  await registry.register({
    id: "writing-style",
    description: "Plain-language writing rubric.",
    whenToUse: "Drafting prose for humans.",
    body: "# Writing\nBe concise.",
  });
  await registry.register({
    id: "structured-debugging",
    description: "A disciplined debugging loop.",
    whenToUse: "A bug resists a quick fix.",
    body: "# Debug\nReproduce first.",
  });
}

describe("resolveSkillCatalog", () => {
  it("returns [] when the recipe declares no skills", async () => {
    const registry = new InMemorySkillRegistry();
    expect(await catalogFor(recipe(), { registry })).toEqual([]);
    expect(await catalogFor(recipe([]), { registry })).toEqual([]);
  });

  it("resolves refs to entries with description + whenToUse + pinned version (no body)", async () => {
    const registry = new InMemorySkillRegistry();
    await seed(registry);
    const catalog = await catalogFor(
      recipe([{ id: "writing-style" }, { id: "structured-debugging" }]),
      { registry },
    );
    expect(catalog).toEqual([
      {
        id: "writing-style",
        version: "v1",
        description: "Plain-language writing rubric.",
        whenToUse: "Drafting prose for humans.",
      },
      {
        id: "structured-debugging",
        version: "v1",
        description: "A disciplined debugging loop.",
        whenToUse: "A bug resists a quick fix.",
      },
    ]);
    // No body leaks into the catalog.
    expect(JSON.stringify(catalog)).not.toContain("Be concise");
  });

  it("pins the concrete version when the ref omits one", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({
      id: "writing-style",
      version: "v1",
      description: "v1",
      whenToUse: "x",
      body: "old",
    });
    await new Promise((r) => setTimeout(r, 2));
    await registry.register({
      id: "writing-style",
      version: "v2",
      description: "v2",
      whenToUse: "x",
      body: "new",
    });
    const catalog = await catalogFor(recipe([{ id: "writing-style" }]), { registry });
    expect(catalog[0]!.version).toBe("v2"); // latest pinned to its concrete version
  });

  it("honors an explicit version pin", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({
      id: "writing-style",
      version: "v1",
      description: "v1",
      whenToUse: "x",
      body: "old",
    });
    await registry.register({
      id: "writing-style",
      version: "v2",
      description: "v2",
      whenToUse: "x",
      body: "new",
    });
    const catalog = await catalogFor(recipe([{ id: "writing-style", version: "v1" }]), {
      registry,
    });
    expect(catalog[0]!.version).toBe("v1");
  });

  it("throws on a missing ref by default, skips with onMissing: 'skip'", async () => {
    const registry = new InMemorySkillRegistry();
    await seed(registry);
    const r = recipe([{ id: "writing-style" }, { id: "ghost" }]);
    await expect(catalogFor(r, { registry })).rejects.toThrow(/ghost/);
    const skipped = await catalogFor(r, { registry, onMissing: "skip" });
    expect(skipped.map((e) => e.id)).toEqual(["writing-style"]);
  });

  it("treats a disabled skill like a missing ref", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({
      id: "writing-style",
      description: "x",
      whenToUse: "x",
      body: "x",
      metadata: { enabled: false },
    });
    const r = recipe([{ id: "writing-style" }]);
    await expect(catalogFor(r, { registry })).rejects.toThrow(/disabled/);
    expect(await catalogFor(r, { registry, onMissing: "skip" })).toEqual([]);
  });

  it("returns [] for a non-local backend", async () => {
    const registry = new InMemorySkillRegistry();
    const remote: RegisteredAgent = {
      id: "remote-bot",
      version: "v1",
      backend: { type: "remote", endpoint: "http://x", remoteAgentId: "y" },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
    expect(await catalogFor(remote, { registry })).toEqual([]);
  });
});

describe("buildSkillCatalogPrompt", () => {
  it("returns empty string for an empty catalog", () => {
    expect(buildSkillCatalogPrompt([])).toBe("");
  });

  it("renders a block with the loadSkill instruction and one line per skill", () => {
    const entries: ResolvedSkillEntry[] = [
      { id: "writing-style", version: "v1", description: "Rubric.", whenToUse: "Drafting." },
      { id: "structured-debugging", version: "v3", description: "Loop.", whenToUse: "Hard bug." },
    ];
    const block = buildSkillCatalogPrompt(entries);
    expect(block).toContain("## Skills");
    expect(block).toContain("loadSkill");
    expect(block).toContain("`writing-style` (v1): Rubric. Use when: Drafting.");
    expect(block).toContain("`structured-debugging` (v3): Loop. Use when: Hard bug.");
  });
});

describe("skillAllowedByCapabilities", () => {
  it("allows ungated skills (no declared capabilities)", () => {
    expect(skillAllowedByCapabilities([], [])).toBe(true);
    expect(skillAllowedByCapabilities([], ["anything"])).toBe(true);
  });

  it("requires the agent to hold at least one declared capability", () => {
    expect(skillAllowedByCapabilities(["rag"], ["rag", "chat"])).toBe(true);
    expect(skillAllowedByCapabilities(["rag", "search"], ["search"])).toBe(true);
    expect(skillAllowedByCapabilities(["rag"], ["chat"])).toBe(false);
    expect(skillAllowedByCapabilities(["rag"], [])).toBe(false);
  });
});

describe("resolveSkillCatalog — capability gating", () => {
  function recipeWithCaps(caps: string[], skills: ReadonlyArray<SkillRef>): RegisteredAgent {
    return {
      id: "gated-bot",
      version: "v1",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: "x", tools: [], skills } },
      },
      metadata: { description: null, capabilities: caps, tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
  }

  async function seedGated(registry: InMemorySkillRegistry) {
    await registry.register({
      id: "rag-skill",
      description: "RAG technique.",
      whenToUse: "Retrieval.",
      body: "# RAG",
      metadata: { capabilities: ["rag"], tags: [] },
    });
    await registry.register({
      id: "open-skill",
      description: "Ungated.",
      whenToUse: "Anytime.",
      body: "# Open",
    });
  }

  it("excludes a gated skill from an agent lacking the capability (silent, even with onMissing: throw)", async () => {
    const registry = new InMemorySkillRegistry();
    await seedGated(registry);
    const catalog = await catalogFor(
      recipeWithCaps([], [{ id: "rag-skill" }, { id: "open-skill" }]),
      { registry, onMissing: "throw" },
    );
    // rag-skill is policy-excluded (no throw); ungated open-skill remains.
    expect(catalog.map((e) => e.id)).toEqual(["open-skill"]);
  });

  it("drops a needs-review skill silently (trust gate, fail-closed)", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({
      id: "review-pending",
      description: "x",
      whenToUse: "x",
      body: "x",
      metadata: { tags: [], capabilities: [], trust: "needs-review" },
    });
    const catalog = await catalogFor(recipeWithCaps([], [{ id: "review-pending" }]), {
      registry,
      onMissing: "throw", // even with throw, trust-gate is silent
    });
    expect(catalog).toEqual([]);
  });

  it("includes a gated skill when the agent holds the capability", async () => {
    const registry = new InMemorySkillRegistry();
    await seedGated(registry);
    const catalog = await catalogFor(
      recipeWithCaps(["rag"], [{ id: "rag-skill" }, { id: "open-skill" }]),
      { registry },
    );
    expect(catalog.map((e) => e.id).sort()).toEqual(["open-skill", "rag-skill"]);
  });
});
