// ---------------------------------------------------------------------------
// `applyDiscovered` against operator choices and broken files:
//   - a file never undoes an operator's promote; a changed file waits as a
//     draft while an operator's version is active
//   - an empty / unreadable file is a scan error, so sync deletes nothing
//   - a pinned fragment layer never resolves to different text after the
//     fragment was removed and re-created
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeWallClock } from "@promin/workflow";
import { resolveSystemPrompt } from "../../fragments/resolve-prompt.ts";
import {
  InMemoryAgentRegistry,
  InMemoryFragmentRegistry,
} from "../../versioned-registry/in-memory-registries.ts";
import { applyDiscoveredAgents } from "../agent-scanner.ts";
import { applyDiscoveredFragments, startFragmentScanLoop } from "../fragment-scanner.ts";
import { SkillScanner } from "../skill-scanner.ts";

const backend = (model: string) => ({
  type: "local" as const,
  model: { provider: "mock", id: model },
  role: { inline: { systemPrompt: null, tools: [] } },
});

describe("applyDiscovered — operator choices", () => {
  it("an unchanged file never undoes an operator's promote", async () => {
    const registry = new InMemoryAgentRegistry();
    const file = [{ id: "bot", backend: backend("a") }];
    await applyDiscoveredAgents({ registry, agents: file });
    // The operator publishes their own version.
    await registry.register({ id: "bot", backend: backend("operator"), promote: true });
    const again = await applyDiscoveredAgents({ registry, agents: file });
    expect(again.changed).toEqual([]);
    expect(await registry.get({ id: "bot" })).toMatchObject({ version: "v2" });
    expect(await registry.versions({ id: "bot" })).toHaveLength(2);
  });

  it("a changed file waits as a draft while canAutoPromote refuses its id (a gate)", async () => {
    const registry = new InMemoryAgentRegistry();
    await applyDiscoveredAgents({ registry, agents: [{ id: "bot", backend: backend("a") }] });
    const asked: string[] = [];
    const changed = await applyDiscoveredAgents({
      registry,
      agents: [{ id: "bot", backend: backend("b") }],
      canAutoPromote: ({ id }) => {
        asked.push(id);
        return false;
      },
    });
    expect(asked).toEqual(["bot"]);
    expect(changed.warnings.join()).toContain("promotion gate");
    expect((await registry.get({ id: "bot" }))?.version).toBe("v1");
    expect((await registry.versions({ id: "bot" })).map((v) => v.version)).toEqual(["v1", "v2"]);
    // Allowed: the file's new version becomes active as before.
    await applyDiscoveredAgents({
      registry,
      agents: [{ id: "bot", backend: backend("c") }],
      canAutoPromote: () => true,
    });
    expect((await registry.get({ id: "bot" }))?.version).toBe("v3");
  });

  it("a changed file waits as a draft while an operator's version is active", async () => {
    const registry = new InMemoryAgentRegistry();
    await applyDiscoveredAgents({ registry, agents: [{ id: "bot", backend: backend("a") }] });
    await registry.register({ id: "bot", backend: backend("operator"), promote: true });
    const changed = await applyDiscoveredAgents({
      registry,
      agents: [{ id: "bot", backend: backend("b") }],
    });
    expect(changed.changed).toEqual(["bot"]);
    expect(changed.warnings.join()).toContain("waits as a draft");
    expect((await registry.get({ id: "bot" }))?.version).toBe("v2");
    expect((await registry.versions({ id: "bot" })).map((v) => [v.version, v.source])).toEqual([
      ["v1", "file"],
      ["v2", undefined],
      ["v3", "file"],
    ]);
  });

  it("a changed file replaces the file's own active version", async () => {
    const registry = new InMemoryAgentRegistry();
    await applyDiscoveredAgents({ registry, agents: [{ id: "bot", backend: backend("a") }] });
    await applyDiscoveredAgents({ registry, agents: [{ id: "bot", backend: backend("b") }] });
    expect(await registry.get({ id: "bot" })).toMatchObject({ version: "v2", source: "file" });
    // File-managed is stored: every replica sees it.
    const managed = await registry.list({ source: "file" });
    expect(managed.items.map((e) => e.id)).toEqual(["bot"]);
  });
});

describe("applyDiscovered — broken files", () => {
  it("an emptied fragment file is a scan error; sync keeps the fragment", async () => {
    const root = join(tmpdir(), `frag-empty-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    const registry = new InMemoryFragmentRegistry();
    const loop = startFragmentScanLoop({
      registry,
      root,
      sync: true,
      clock: FakeWallClock.create(0),
    });
    try {
      await writeFile(join(root, "tone.md"), "Be polite.");
      await loop.tick();
      await writeFile(join(root, "tone.md"), ""); // emptied mid-save
      const tick = await loop.tick();
      expect(tick.errors).toHaveLength(1);
      expect(tick.deleted).toEqual([]);
      expect((await registry.get({ id: "tone" }))?.content).toBe("Be polite.");
    } finally {
      loop.stop();
      await rm(root, { recursive: true, force: true });
    }
  });

  it("a skill markdown file without frontmatter is a scan error (README.md excepted)", async () => {
    const root = join(tmpdir(), `skill-nofm-${process.pid}-${Date.now()}`);
    await mkdir(join(root, "debug"), { recursive: true });
    try {
      await writeFile(join(root, "debug", "SKILL.md"), "# lost its frontmatter mid-edit");
      await writeFile(join(root, "README.md"), "# Skills in this folder");
      const scan = await SkillScanner.scanFolder(root);
      expect(scan.errors).toHaveLength(1);
      expect(scan.errors[0]).toContain("SKILL.md");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("a pinned layer never resolves to other text after its fragment was removed and re-created", async () => {
    const fragments = new InMemoryFragmentRegistry();
    await applyDiscoveredFragments({
      registry: fragments,
      fragments: [{ id: "tone", content: "Be polite." }],
      sync: true,
    });
    const systemPrompt = { base: "base", layers: ["tone@v1"] };
    expect(await resolveSystemPrompt({ systemPrompt, fragments })).toBe("base\n\nBe polite.");
    await applyDiscoveredFragments({ registry: fragments, fragments: [], sync: true });
    await applyDiscoveredFragments({
      registry: fragments,
      fragments: [{ id: "tone", content: "Be rude." }],
      sync: true,
    });
    expect((await fragments.get({ id: "tone" }))?.version).toBe("v2");
    const pinned = await resolveSystemPrompt({ systemPrompt, fragments, onMissingLayer: "skip" });
    expect(pinned).toBe("base");
  });
});
