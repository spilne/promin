// ---------------------------------------------------------------------------
// SkillScanner — markdown skill discovery (the SKILL.md ecosystem drop-in).
//
// Pins:
//   - a folder SKILL.md is discovered; id falls back to the directory name
//   - a bare *.md with frontmatter is discovered
//   - README.md (no frontmatter) is silently skipped
//   - bundled scripts beside a SKILL.md are ignored, with a warning
//   - markdown skills and .ts-module skills coexist in one scan
//   - a discovered md skill flows through the registry + catalog end-to-end
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { SkillScanner, applyDiscoveredSkills } from "../skill-scanner.ts";
import { InMemorySkillRegistry } from "../../skills/in-memory-skill-registry.ts";
import {
  resolveSkillCatalog,
  buildSkillCatalogPrompt,
} from "../../skills/resolve-skill-catalog.ts";
import { inlineRoleDefinition } from "../../role/resolve-role.ts";
import type { RegisteredAgent } from "../../registry/types.ts";

const ROOT = join(tmpdir(), `skill-scanner-md-${process.pid}-${Date.now()}`);

beforeAll(async () => {
  // Folder skill: skills/structured-debugging/SKILL.md (+ a bundled script).
  const folder = join(ROOT, "structured-debugging");
  await mkdir(join(folder, "scripts"), { recursive: true });
  await writeFile(
    join(folder, "SKILL.md"),
    [
      "---",
      "name: Structured Debugging",
      "description: A disciplined debugging loop.",
      "tags: [engineering]",
      // Declare trust so the catalog-flow test doesn't get blocked by the
      // scanner's needs-review default (that policy is exercised in the
      // skill-scanner.test.ts "newly-discovered skill" cases).
      "trust: trusted",
      "---",
      "",
      "# Structured debugging",
      "Reproduce, bisect, hypothesize.",
    ].join("\n"),
  );
  await writeFile(join(folder, "scripts", "helper.py"), "print('ignored')");

  // Bare md skill with an explicit id via frontmatter name.
  await writeFile(
    join(ROOT, "plain-writing.md"),
    [
      "---",
      "name: plain-writing",
      "description: Plain-language rubric.",
      "---",
      "Lead with the point.",
    ].join("\n"),
  );

  // README — no frontmatter, must be skipped silently.
  await writeFile(join(ROOT, "README.md"), "# Skills\n\nThis folder holds skills.");

  // A .ts-module skill, to prove coexistence.
  await writeFile(
    join(ROOT, "code-review.skill.ts"),
    `export const s = { id: "code-review", description: "Review checklist.", whenToUse: "Reviewing a diff.", body: "# Review" };`,
  );
});

afterAll(async () => {
  await rm(ROOT, { recursive: true, force: true });
});

describe("SkillScanner — markdown", () => {
  it("discovers SKILL.md, bare .md, and .ts skills together; skips README", async () => {
    const { skills } = await SkillScanner.scanFolder(ROOT);
    const ids = skills.map((s) => s.id).sort();
    expect(ids).toEqual(["code-review", "plain-writing", "structured-debugging"]);
  });

  it("warns that bundled scripts beside a SKILL.md were ignored", async () => {
    const { skills, warnings } = await SkillScanner.scanFolder(ROOT);
    // The skill itself is still discovered (instructions only)...
    expect(skills.find((s) => s.id === "structured-debugging")).toBeDefined();
    // ...and the bundled script is flagged as ignored.
    expect(warnings.some((w) => w.includes("bundled files") && w.includes("scripts"))).toBe(true);
  });

  it("parses frontmatter into catalog fields and keeps the body for loadSkill", async () => {
    const { skills } = await SkillScanner.scanFolder(ROOT);
    const debug = skills.find((s) => s.id === "structured-debugging")!;
    expect(debug.description).toBe("A disciplined debugging loop.");
    expect(debug.metadata?.tags).toEqual(["engineering"]);
    expect(debug.body).toContain("Reproduce, bisect, hypothesize.");
  });

  it("flows through the registry + catalog end-to-end (whenToUse falls back to description)", async () => {
    const registry = new InMemorySkillRegistry();
    const { skills } = await SkillScanner.scanFolder(ROOT);
    await applyDiscoveredSkills(registry, skills);

    const recipe: RegisteredAgent = {
      id: "skilled-bot",
      version: "v1",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: {
          inline: { systemPrompt: "x", tools: [], skills: [{ id: "structured-debugging" }] },
        },
      },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
    const catalog = await resolveSkillCatalog({
      recipe,
      registry,
      skills:
        recipe.backend.type === "local"
          ? inlineRoleDefinition(recipe.backend.role)?.skills
          : undefined,
    });
    expect(catalog[0]!.whenToUse).toBe("A disciplined debugging loop."); // fallback
    const prompt = buildSkillCatalogPrompt(catalog);
    // Description shown once; no duplicated "Use when:" when it equals description.
    expect(prompt).toContain("`structured-debugging` (v1): A disciplined debugging loop.");
    expect(prompt).not.toContain("Use when:");
  });
});
