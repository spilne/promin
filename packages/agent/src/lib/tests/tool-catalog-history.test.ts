// ---------------------------------------------------------------------------
// Tool catalog history — InMemoryToolHistoryStore (conformance + exact
// FakeClock timing) and the AgentToolCatalogHistory snapshot loop.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeClock } from "@promin/core";
import { toolHistoryStoreTestSuite } from "../tool-history/tool-history-store-test-suite.ts";
import { InMemoryToolHistoryStore } from "../tool-history/in-memory-tool-history-store.ts";
import { AgentToolCatalogHistory } from "../tool-history/agent-tool-catalog-history.ts";
import type { AgentToolCatalog, ToolCatalogEntry } from "../tool-catalog.ts";

toolHistoryStoreTestSuite(() => new InMemoryToolHistoryStore());

const observation = {
  name: "search",
  sourceKind: "in-process" as const,
  sourceDetail: "",
  schemaHash: "hash-a",
  description: "Search",
};

describe("InMemoryToolHistoryStore — FakeClock timing", () => {
  it("stamps firstSeenAt and lastSeenAt with the clock at insert time", async () => {
    const clock = FakeClock.create(1_000);
    const store = new InMemoryToolHistoryStore({ clock });
    await store.recordSnapshot([observation]);

    const [r] = await store.list();
    expect(r?.firstSeenAt).toBe(1_000);
    expect(r?.lastSeenAt).toBe(1_000);
  });

  it("advances lastSeenAt on re-record but holds firstSeenAt", async () => {
    const clock = FakeClock.create(1_000);
    const store = new InMemoryToolHistoryStore({ clock });
    await store.recordSnapshot([observation]);

    clock.advance(5_000);
    await store.recordSnapshot([observation]);

    const [r] = await store.list();
    expect(r?.firstSeenAt).toBe(1_000);
    expect(r?.lastSeenAt).toBe(6_000);
  });
});

// --- AgentToolCatalogHistory --------------------------------------------

function entry(over: Partial<ToolCatalogEntry> = {}): ToolCatalogEntry {
  return {
    name: "search",
    description: "Search the web",
    parameters: { type: "object", properties: { q: { type: "string" } } },
    source: { kind: "in-process" },
    enabled: true,
    requiredSecrets: [],
    usesMemory: false,
    ...over,
  };
}

/** Mutable fake catalog — tests swap `entries` between snapshots. */
class FakeCatalog implements AgentToolCatalog {
  entries: ToolCatalogEntry[] = [];
  async listAll(): Promise<ToolCatalogEntry[]> {
    return this.entries;
  }
}

describe("AgentToolCatalogHistory", () => {
  it("snapshot() persists every catalog entry", async () => {
    const catalog = new FakeCatalog();
    catalog.entries = [entry({ name: "search" }), entry({ name: "fetch" })];
    const store = new InMemoryToolHistoryStore();
    const history = new AgentToolCatalogHistory({ catalog, store });

    await history.snapshot();

    expect((await store.list()).map((r) => r.name).sort()).toEqual(["fetch", "search"]);
  });

  it("maps file / mcp sources to their source detail", async () => {
    const catalog = new FakeCatalog();
    catalog.entries = [
      entry({ name: "lint", source: { kind: "file", path: "/tools/lint.ts" } }),
      entry({ name: "web:search", source: { kind: "mcp", server: "web" } }),
    ];
    const store = new InMemoryToolHistoryStore();
    await new AgentToolCatalogHistory({ catalog, store }).snapshot();

    const byName = new Map((await store.list()).map((r) => [r.name, r]));
    expect(byName.get("lint")).toMatchObject({
      sourceKind: "file",
      sourceDetail: "/tools/lint.ts",
    });
    expect(byName.get("web:search")).toMatchObject({
      sourceKind: "mcp",
      sourceDetail: "web",
    });
  });

  it("a parameter-schema change lands as a second history row", async () => {
    const catalog = new FakeCatalog();
    const store = new InMemoryToolHistoryStore();
    const history = new AgentToolCatalogHistory({ catalog, store });

    catalog.entries = [entry({ parameters: { type: "object", properties: { a: {} } } })];
    await history.snapshot();
    // Same name, different param shape -> different schema hash.
    catalog.entries = [entry({ parameters: { type: "object", properties: { b: {} } } })];
    await history.snapshot();

    expect(await store.list({ name: "search" })).toHaveLength(2);
  });

  it("an unchanged catalog re-snapshots into the same single row", async () => {
    const catalog = new FakeCatalog();
    catalog.entries = [entry()];
    const store = new InMemoryToolHistoryStore();
    const history = new AgentToolCatalogHistory({ catalog, store });

    await history.snapshot();
    await history.snapshot();

    expect(await store.list()).toHaveLength(1);
  });

  it("start() drives periodic snapshots; stop() halts them", async () => {
    const clock = FakeClock.create(0);
    const catalog = new FakeCatalog();
    catalog.entries = [entry()];
    const store = new InMemoryToolHistoryStore({ clock });
    const history = new AgentToolCatalogHistory({ catalog, store, clock });

    history.start(1_000);
    clock.advance(1_000);
    await flush();
    expect(await store.list()).toHaveLength(1);

    history.stop();
    catalog.entries = [entry({ name: "added-after-stop" })];
    clock.advance(5_000);
    await flush();
    // stop() halted the loop — the new tool was never snapshotted.
    expect((await store.list()).map((r) => r.name)).toEqual(["search"]);
  });
});

/** Let the interval callback's async snapshot() settle. */
async function flush(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}
