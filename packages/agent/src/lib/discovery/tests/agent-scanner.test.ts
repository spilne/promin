// ---------------------------------------------------------------------------
// `AgentScanner` + `applyDiscoveredAgents` — directory-walked agent recipes
// reconciled into an `AgentRegistry`.
//
// Pins:
//   - detects exports matching the structural shape (id + backend.type)
//   - flattens both single-export and array-export modules
//   - skips test/bench/.d.ts files
//   - duplicate ids across files emit a warning, last-wins
//   - applyDiscoveredAgents upserts; sync mode deletes the rest
//   - idPrefix filter narrows reconciliation to a subset
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { FakeClock } from "@promin/core";
import { AgentScanner, applyDiscoveredAgents, startAgentScanLoop } from "../agent-scanner.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";

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
    const result = await applyDiscoveredAgents(registry, agents);
    expect(result.upserted.sort()).toEqual([
      "research",
      "summarize",
      "support",
      "tenant-acme:billing",
    ]);
    expect(result.added.length).toBe(4);
    expect((await registry.get("support"))?.id).toBe("support");
  });

  it("idPrefix narrows reconciliation to a subset", async () => {
    const registry = new InMemoryAgentRegistry();
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents(registry, agents, { idPrefix: "tenant-acme:" });
    expect(result.upserted).toEqual(["tenant-acme:billing"]);
    expect(await registry.get("support")).toBeNull();
  });

  it("sync mode deletes registry entries not in the scan", async () => {
    const registry = new InMemoryAgentRegistry();
    // Pre-populate with an entry that won't be in the scan.
    await registry.register({
      id: "stale-agent",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "x" },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
    });
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents(registry, agents, { sync: true });
    expect(result.deleted).toContain("stale-agent");
    expect(await registry.get("stale-agent")).toBeNull();
  });
});

describe("startAgentScanLoop", () => {
  it("registers a recipe added between ticks (hot-reload)", async () => {
    const root = join(tmpdir(), `agent-scan-loop-add-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    try {
      const registry = new InMemoryAgentRegistry();
      const clock = FakeClock.create(0);
      const loop = startAgentScanLoop({ registry, root, intervalMs: 1_000, clock });

      // First tick: empty folder.
      const t1 = await loop.tick();
      expect(t1.added).toEqual([]);
      expect(await registry.list()).toEqual([]);

      // Drop a recipe in and tick again.
      await writeFile(
        join(root, "support.ts"),
        `export const s = { id: "support", backend: { type: "local", model: { provider: "anthropic", id: "x" }, role: { inline: { systemPrompt: null, tools: [] } } } };`,
      );
      const t2 = await loop.tick();
      expect(t2.added).toEqual(["support"]);
      expect((await registry.get("support"))?.id).toBe("support");
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
      const clock = FakeClock.create(0);
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
      expect(await registry.get("live-only")).toBeNull();
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
      const clock = FakeClock.create(0);
      const loop = startAgentScanLoop({
        registry,
        root,
        intervalMs: 1_000,
        clock,
        filterAgents: (agents) => agents.filter((a) => a.id === "keep-me"),
      });
      const t = await loop.tick();
      expect(t.added).toEqual(["keep-me"]);
      expect(await registry.get("drop-me")).toBeNull();
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
      const clock = FakeClock.create(0);
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
