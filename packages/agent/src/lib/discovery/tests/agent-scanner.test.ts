// ---------------------------------------------------------------------------
// `AgentScanner` + `applyDiscoveredAgents` — directory-walked agent recipes
// reconciled into an `AgentRegistry`.
//
// Pins:
//   - detects exports matching the structural shape (id + backend.type)
//   - flattens both single-export and array-export modules
//   - skips test/bench/.d.ts files
//   - duplicate ids across files emit a warning, last-wins
//   - applyDiscoveredAgents registers changed recipes as promoted versions
//     and leaves unchanged ones alone
//   - sync removes only scanner-written recipes, and nothing after a scan
//     that could not import every file
//   - idPrefix filter narrows reconciliation to a subset
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { FakeWallClock } from "@promin/workflow";
import { AgentScanner, applyDiscoveredAgents, startAgentScanLoop } from "../agent-scanner.ts";
import type { RegisterAgentInput, RegisteredAgent } from "../../registry/types.ts";
import { InMemoryAgentRegistry } from "../../versioned-registry/in-memory-registries.ts";

/** Counts writes, to tell an unchanged recipe was left alone. */
class CountingAgentRegistry extends InMemoryAgentRegistry {
  registers = 0;
  override async register(input: RegisterAgentInput): Promise<RegisteredAgent> {
    this.registers += 1;
    return super.register(input);
  }
}

const LOCAL = {
  type: "local" as const,
  model: { provider: "anthropic", id: "x" },
  role: { inline: { systemPrompt: null, tools: [] } },
};

const FIXTURE_ROOT = join(tmpdir(), `agent-scanner-${process.pid}-${Date.now()}`);

const supportAgentSrc = `
export const support = {
  id: "support",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    role: { inline: { systemPrompt: "Helpful", tools: ["search"] } },
  },
  metadata: { capabilities: ["chat"] },
};
`;

const arrayExportSrc = `
export const all = [
  {
    id: "research",
    backend: { type: "local", model: { provider: "openai", id: "gpt-x" }, role: { inline: { systemPrompt: null, tools: [] } } },
  },
  {
    id: "summarize",
    backend: { type: "local", model: { provider: "openai", id: "gpt-x" }, role: { inline: { systemPrompt: null, tools: [] } } },
  },
];
`;

const noisySrc = `
export const someConfig = { id: "not-an-agent" }; // no backend
export const otherConfig = "string-export";
export const tenantOnly = {
  id: "tenant-acme:billing",
  backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } },
};
`;

beforeAll(async () => {
  await mkdir(FIXTURE_ROOT, { recursive: true });
  await writeFile(join(FIXTURE_ROOT, "support.agent.ts"), supportAgentSrc);
  await writeFile(join(FIXTURE_ROOT, "bundle.agent.ts"), arrayExportSrc);
  await writeFile(join(FIXTURE_ROOT, "noisy.agent.ts"), noisySrc);
  // Ignored files
  await writeFile(
    join(FIXTURE_ROOT, "ignored.test.ts"),
    `export const skip = { id: "skip", backend: { type: "local" } };`,
  );
});

afterAll(async () => {
  await rm(FIXTURE_ROOT, { recursive: true, force: true });
});

describe("AgentScanner", () => {
  it("discovers exports matching the structural shape", async () => {
    const result = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const ids = result.agents.map((a) => a.id).sort();
    expect(ids).toEqual(["research", "summarize", "support", "tenant-acme:billing"]);
  });

  it("skips files matching .test. / .bench. / .d.ts", async () => {
    const result = await AgentScanner.scanFolder(FIXTURE_ROOT);
    expect(result.agents.find((a) => a.id === "skip")).toBeUndefined();
  });

  it("ignores non-conforming exports without warning", async () => {
    const result = await AgentScanner.scanFolder(FIXTURE_ROOT);
    expect(result.agents.find((a) => a.id === "not-an-agent")).toBeUndefined();
  });
});

describe("applyDiscoveredAgents", () => {
  it("upserts discovered agents into the registry", async () => {
    const registry = new InMemoryAgentRegistry();
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents({ registry, agents });
    expect(result.upserted.sort()).toEqual([
      "research",
      "summarize",
      "support",
      "tenant-acme:billing",
    ]);
    expect(result.added.length).toBe(4);
    expect(await registry.get({ id: "support" })).toMatchObject({
      id: "support",
      version: "v1",
      source: "file",
    });
  });

  it("leaves unchanged recipes alone and promotes a changed one as a new version", async () => {
    const registry = new CountingAgentRegistry();
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    await applyDiscoveredAgents({ registry, agents });
    const writes = registry.registers;

    const again = await applyDiscoveredAgents({ registry, agents });
    expect(registry.registers).toBe(writes);
    expect(again.changed).toEqual([]);
    expect(again.upserted).toHaveLength(4);

    const edited = agents.map((a) =>
      a.id === "support" ? { ...a, metadata: { ...a.metadata, tags: ["edited"] } } : a,
    );
    const third = await applyDiscoveredAgents({ registry, agents: edited });
    expect(third.changed).toEqual(["support"]);
    expect(registry.registers).toBe(writes + 1);
    expect((await registry.versions({ id: "support" })).map((v) => [v.version, v.active])).toEqual([
      ["v1", false],
      ["v2", true],
    ]);
  });

  it("idPrefix narrows reconciliation to a subset", async () => {
    const registry = new InMemoryAgentRegistry();
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents({ registry, agents, idPrefix: "tenant-acme:" });
    expect(result.upserted).toEqual(["tenant-acme:billing"]);
    expect(await registry.get({ id: "support" })).toBeNull();
  });

  it("sync removes scanner-written recipes not in the scan, never operator-authored ones", async () => {
    const registry = new InMemoryAgentRegistry();
    // A recipe an earlier scan wrote, whose file is gone.
    await registry.register({ id: "stale-agent", backend: LOCAL, source: "file" });
    // A recipe an operator authored through the API.
    await registry.register({ id: "operator-agent", backend: LOCAL });
    // Operator edits on top of a scanned recipe survive; the file versions go.
    await registry.register({ id: "mixed", backend: LOCAL, source: "file" });
    await registry.register({
      id: "mixed",
      backend: { ...LOCAL, model: { provider: "anthropic", id: "y" } },
      promote: true,
    });
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents({ registry, agents, sync: true });
    expect(result.deleted.sort()).toEqual(["mixed", "stale-agent"]);
    expect(await registry.get({ id: "stale-agent" })).toBeNull();
    expect(await registry.get({ id: "operator-agent" })).not.toBeNull();
    expect((await registry.versions({ id: "mixed" })).map((v) => v.version)).toEqual(["v2"]);
  });

  it("sync deletes nothing after a scan that could not read every file", async () => {
    const registry = new InMemoryAgentRegistry();
    await registry.register({ id: "stale-agent", backend: LOCAL, source: "file" });
    const result = await applyDiscoveredAgents({
      registry,
      agents: [],
      sync: true,
      complete: false,
    });
    expect(result.deleted).toEqual([]);
    expect(await registry.get({ id: "stale-agent" })).not.toBeNull();
  });
});

describe("startAgentScanLoop", () => {
  it("registers a recipe added between ticks (hot-reload)", async () => {
    const root = join(tmpdir(), `agent-scan-loop-add-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeWallClock.create(0);
      const loop = startAgentScanLoop({ registry, root, intervalMs: 1_000, clock });

      // First tick: empty folder.
      const t1 = await loop.tick();
      expect(t1.added).toEqual([]);
      expect((await registry.list()).items).toEqual([]);

      // Drop a recipe in and tick again.
      await writeFile(
        join(root, "support.ts"),
        `export const s = { id: "support", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
      );
      const t2 = await loop.tick();
      expect(t2.added).toEqual(["support"]);
      expect((await registry.get({ id: "support" }))?.id).toBe("support");
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("sync removes a recipe whose file was deleted", async () => {
    const root = join(tmpdir(), `agent-scan-loop-del-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    const recipePath = join(root, "live.ts");
    await writeFile(
      recipePath,
      `export const a = { id: "live-only", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
    );
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeWallClock.create(0);
      const loop = startAgentScanLoop({
        registry,
        root,
        intervalMs: 1_000,
        sync: true,
        clock,
      });
      const t1 = await loop.tick();
      expect(t1.added).toEqual(["live-only"]);

      await rm(recipePath);
      const t2 = await loop.tick();
      expect(t2.deleted).toContain("live-only");
      expect(await registry.get({ id: "live-only" })).toBeNull();
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("filters discovered agents through filterAgents", async () => {
    const root = join(tmpdir(), `agent-scan-loop-filter-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "a.ts"),
      `export const a = { id: "keep-me", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };
       export const b = { id: "drop-me", backend: { type: "local", model: { provider: "anthropic", id: "y" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
    );
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeWallClock.create(0);
      const loop = startAgentScanLoop({
        registry,
        root,
        intervalMs: 1_000,
        clock,
        filterAgents: (agents) => agents.filter((a) => a.id === "keep-me"),
      });
      const t = await loop.tick();
      expect(t.added).toEqual(["keep-me"]);
      expect(await registry.get({ id: "drop-me" })).toBeNull();
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("a file that fails to import keeps sync from deleting its recipe", async () => {
    const root = join(tmpdir(), `agent-scan-loop-broken-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    const recipePath = join(root, "live.ts");
    await writeFile(
      recipePath,
      `export const a = { id: "live", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
    );
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeWallClock.create(0);
      const loop = startAgentScanLoop({ registry, root, intervalMs: 1_000, sync: true, clock });
      expect((await loop.tick()).added).toEqual(["live"]);

      // The recipe moves into a new file that does not import (a syntax
      // error mid-edit): the scan cannot tell it was not just removed.
      await rm(recipePath);
      await writeFile(join(root, "moved.ts"), `export const a = { id: "live", backend: {`);
      const t2 = await loop.tick();
      expect(t2.errors).toHaveLength(1);
      expect(t2.deleted).toEqual([]);
      expect(await registry.get({ id: "live" })).not.toBeNull();
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("coalesces overlapping ticks", async () => {
    const root = join(tmpdir(), `agent-scan-loop-coalesce-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "x.ts"),
      `export const x = { id: "x", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
    );
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeWallClock.create(0);
      const loop = startAgentScanLoop({ registry, root, intervalMs: 1_000, clock });
      // Two parallel ticks should resolve to the same in-flight scan.
      const [a, b] = await Promise.all([loop.tick(), loop.tick()]);
      expect(a).toBe(b);
      loop.stop();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
