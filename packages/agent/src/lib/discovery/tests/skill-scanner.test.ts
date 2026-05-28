// ---------------------------------------------------------------------------
// `SkillScanner` + `applyDiscoveredSkills` — directory-walked skill manifests
// reconciled into a `SkillRegistry`. Sibling of the agent-scanner test.
//
// Pins:
//   - detects exports matching the structural shape (id + description + whenToUse + body)
//   - flattens both single-export and array-export modules
//   - skips test/bench/.d.ts files
//   - ignores non-conforming exports without warning
//   - duplicate ids across files emit a warning, last-wins
//   - applyDiscoveredSkills upserts; sync mode deletes the rest; idPrefix narrows
//   - the scan loop hot-reloads added files and (with sync) removes deleted ones
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { FakeClock } from "@promin/core";
import { SkillScanner, applyDiscoveredSkills, startSkillScanLoop } from "../skill-scanner.ts";
import { InMemorySkillRegistry } from "../../skills/in-memory-skill-registry.ts";

const FIXTURE_ROOT = join(tmpdir(), `skill-scanner-${process.pid}-${Date.now()}`);

const debuggingSkillSrc = `
export const debugging = {
  id: "structured-debugging",
  description: "A disciplined debugging loop.",
  whenToUse: "When a bug resists a quick fix.",
  body: "# Structured debugging\\n\\n1. Reproduce. 2. Bisect. 3. Hypothesize.",
  metadata: { tags: ["engineering"] },
};
`;

const arrayExportSrc = `
export const all = [
  { id: "writing-style", description: "Plain-language writing rubric.", whenToUse: "When drafting prose.", body: "# Writing\\nBe concise." },
  { id: "code-review", description: "A review checklist.", whenToUse: "When reviewing a diff.", body: "# Review\\nCheck edges." },
];
`;

const noisySrc = `
export const notASkill = { id: "missing-fields" }; // no description/whenToUse/body
export const stringExport = "string-export";
export const tenantOnly = {
  id: "tenant-acme:tone",
  description: "Acme tone guide.",
  whenToUse: "When writing for Acme.",
  body: "# Tone\\nWarm and direct.",
};
`;

beforeAll(async () => {
  await mkdir(FIXTURE_ROOT, { recursive: true });
  await writeFile(join(FIXTURE_ROOT, "debugging.skill.ts"), debuggingSkillSrc);
  await writeFile(join(FIXTURE_ROOT, "bundle.skill.ts"), arrayExportSrc);
  await writeFile(join(FIXTURE_ROOT, "noisy.skill.ts"), noisySrc);
  // Ignored file.
  await writeFile(
    join(FIXTURE_ROOT, "ignored.test.ts"),
    `export const skip = { id: "skip", description: "x", whenToUse: "x", body: "x" };`,
  );
});

afterAll(async () => {
  await rm(FIXTURE_ROOT, { recursive: true, force: true });
});

describe("SkillScanner", () => {
  it("discovers exports matching the structural shape", async () => {
    const result = await SkillScanner.scanFolder(FIXTURE_ROOT);
    const ids = result.skills.map((s) => s.id).sort();
    expect(ids).toEqual([
      "code-review",
      "structured-debugging",
      "tenant-acme:tone",
      "writing-style",
    ]);
  });

  it("skips files matching .test. / .bench. / .d.ts", async () => {
    const result = await SkillScanner.scanFolder(FIXTURE_ROOT);
    expect(result.skills.find((s) => s.id === "skip")).toBeUndefined();
  });

  it("ignores non-conforming exports without warning", async () => {
    const result = await SkillScanner.scanFolder(FIXTURE_ROOT);
    expect(result.skills.find((s) => s.id === "missing-fields")).toBeUndefined();
  });
});

describe("applyDiscoveredSkills", () => {
  it("upserts discovered skills into the registry", async () => {
    const registry = new InMemorySkillRegistry();
    const { skills } = await SkillScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredSkills(registry, skills);
    expect(result.upserted.sort()).toEqual([
      "code-review",
      "structured-debugging",
      "tenant-acme:tone",
      "writing-style",
    ]);
    expect(result.added.length).toBe(4);
    expect((await registry.get("structured-debugging"))?.id).toBe("structured-debugging");
  });

  it("idPrefix narrows reconciliation to a subset", async () => {
    const registry = new InMemorySkillRegistry();
    const { skills } = await SkillScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredSkills(registry, skills, { idPrefix: "tenant-acme:" });
    expect(result.upserted).toEqual(["tenant-acme:tone"]);
    expect(await registry.get("structured-debugging")).toBeNull();
  });

  it("defaults a newly-discovered skill to needs-review (trust step)", async () => {
    const registry = new InMemorySkillRegistry();
    await applyDiscoveredSkills(registry, [
      { id: "unreviewed", description: "x", whenToUse: "x", body: "x" },
    ]);
    const row = await registry.get("unreviewed");
    expect(row?.metadata.trust).toBe("needs-review");
  });

  it("respects a manifest-declared trust on first discovery", async () => {
    const registry = new InMemorySkillRegistry();
    await applyDiscoveredSkills(registry, [
      {
        id: "first-party",
        description: "x",
        whenToUse: "x",
        body: "x",
        metadata: { tags: [], capabilities: [], trust: "trusted" },
      },
    ]);
    const row = await registry.get("first-party");
    expect(row?.metadata.trust).toBe("trusted");
  });

  it("preserves operator-approved trust on re-scan (no revert to needs-review)", async () => {
    const registry = new InMemorySkillRegistry();
    // First scan → needs-review.
    await applyDiscoveredSkills(registry, [
      { id: "to-approve", description: "x", whenToUse: "x", body: "x" },
    ]);
    expect((await registry.get("to-approve"))?.metadata.trust).toBe("needs-review");
    // Operator approves.
    await registry.register({
      id: "to-approve",
      description: "x",
      whenToUse: "x",
      body: "x",
      metadata: { tags: [], capabilities: [], trust: "trusted" },
    });
    // Re-scan with no declared trust — preserves operator's "trusted".
    await applyDiscoveredSkills(registry, [
      { id: "to-approve", description: "x", whenToUse: "x", body: "x" },
    ]);
    expect((await registry.get("to-approve"))?.metadata.trust).toBe("trusted");
  });

  it("sync mode deletes registry entries not in the scan", async () => {
    const registry = new InMemorySkillRegistry();
    await registry.register({
      id: "stale-skill",
      description: "x",
      whenToUse: "x",
      body: "x",
    });
    const { skills } = await SkillScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredSkills(registry, skills, { sync: true });
    expect(result.deleted).toContain("stale-skill");
    expect(await registry.get("stale-skill")).toBeNull();
  });
});

describe("startSkillScanLoop", () => {
  it("registers a skill added between ticks (hot-reload)", async () => {
    const root = join(tmpdir(), `skill-scan-loop-add-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    try {
      const registry = new InMemorySkillRegistry();
      const clock = FakeClock.create(0);
      const loop = startSkillScanLoop({ registry, root, intervalMs: 1_000, clock });

      const t1 = await loop.tick();
      expect(t1.added).toEqual([]);
      expect(await registry.list()).toEqual([]);

      await writeFile(
        join(root, "s.ts"),
        `export const s = { id: "writing-style", description: "rubric", whenToUse: "drafting", body: "# Writing" };`,
      );
      const t2 = await loop.tick();
      expect(t2.added).toEqual(["writing-style"]);
      expect((await registry.get("writing-style"))?.id).toBe("writing-style");
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("sync removes a skill whose file was deleted", async () => {
    const root = join(tmpdir(), `skill-scan-loop-del-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    const skillPath = join(root, "live.ts");
    await writeFile(
      skillPath,
      `export const a = { id: "live-only", description: "x", whenToUse: "x", body: "x" };`,
    );
    try {
      const registry = new InMemorySkillRegistry();
      const clock = FakeClock.create(0);
      const loop = startSkillScanLoop({ registry, root, intervalMs: 1_000, sync: true, clock });
      const t1 = await loop.tick();
      expect(t1.added).toEqual(["live-only"]);

      await rm(skillPath);
      const t2 = await loop.tick();
      expect(t2.deleted).toContain("live-only");
      expect(await registry.get("live-only")).toBeNull();
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("coalesces overlapping ticks", async () => {
    const root = join(tmpdir(), `skill-scan-loop-coalesce-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "x.ts"),
      `export const x = { id: "x", description: "x", whenToUse: "x", body: "x" };`,
    );
    try {
      const registry = new InMemorySkillRegistry();
      const clock = FakeClock.create(0);
      const loop = startSkillScanLoop({ registry, root, intervalMs: 1_000, clock });
      const [a, b] = await Promise.all([loop.tick(), loop.tick()]);
      expect(a).toBe(b);
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
