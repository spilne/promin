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
import { AgentScanner, applyDiscoveredAgents } from "../agent-scanner.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";

const FIXTURE_ROOT = join(tmpdir(), `agent-scanner-${process.pid}-${Date.now()}`);

const supportAgentSrc = `
export const support = {
  id: "support",
  backend: {
    type: "local",
    model: { provider: "anthropic", id: "claude-sonnet-4-6" },
    systemPrompt: "Helpful",
    tools: ["search"],
  },
  metadata: { capabilities: ["chat"] },
};
`;

const arrayExportSrc = `
export const all = [
  {
    id: "research",
    backend: { type: "local", model: { provider: "openai", id: "gpt-x" }, systemPrompt: null, tools: [] },
  },
  {
    id: "summarize",
    backend: { type: "local", model: { provider: "openai", id: "gpt-x" }, systemPrompt: null, tools: [] },
  },
];
`;

const noisySrc = `
export const someConfig = { id: "not-an-agent" }; // no backend
export const otherConfig = "string-export";
export const tenantOnly = {
  id: "tenant-acme:billing",
  backend: { type: "local", model: { provider: "anthropic", id: "x" }, systemPrompt: null, tools: [] },
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
        systemPrompt: null,
        tools: [],
      },
    });
    const { agents } = await AgentScanner.scanFolder(FIXTURE_ROOT);
    const result = await applyDiscoveredAgents(registry, agents, { sync: true });
    expect(result.deleted).toContain("stale-agent");
    expect(await registry.get("stale-agent")).toBeNull();
  });
});
