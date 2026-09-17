// ---------------------------------------------------------------------------
// GET /api/agents/_catalog/tools/history — durable tool-catalog audit trail.
//
// Pins:
//   - returns the store's records when a tool-history store is wired
//   - empty array when no store is wired (route still mounted)
//   - name / source query params filter the result
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry, InMemoryToolHistoryStore } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { ZoryaServer } from "../../server/server.ts";
import { LocalWorkflows, ZoryaAgents } from "../../index.ts";

function makeServer(history: InMemoryToolHistoryStore | null) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return new ZoryaServer({
    workflows: new LocalWorkflows({ storage, runner, definitions: {}, sleepScanIntervalMs: 0 }),
    agents: new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("tool-history route test should not resolve agents");
      },
      ...(history !== null && { toolHistory: history }),
    }),
  });
}

const obs = (
  over: Partial<Parameters<InMemoryToolHistoryStore["recordSnapshot"]>[0][number]> = {},
) => ({
  name: "search",
  sourceKind: "in-process" as const,
  sourceDetail: "",
  schemaHash: "hash-a",
  description: "Search",
  ...over,
});

describe("GET /api/agents/_catalog/tools/history", () => {
  it("returns the store's recorded history", async () => {
    const store = new InMemoryToolHistoryStore();
    await store.recordSnapshot([obs({ name: "search" }), obs({ name: "fetch" })]);
    const server = makeServer(store);

    const res = await server.handle(new Request("http://test/api/agents/_catalog/tools/history"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { history: Array<{ name: string }> };
    expect(body.history.map((r) => r.name).sort()).toEqual(["fetch", "search"]);
  });

  it("returns an empty array when no tool-history store is wired", async () => {
    const server = makeServer(null);
    const res = await server.handle(new Request("http://test/api/agents/_catalog/tools/history"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ history: [] });
  });

  it("filters by the name query param", async () => {
    const store = new InMemoryToolHistoryStore();
    await store.recordSnapshot([obs({ name: "search" }), obs({ name: "fetch" })]);
    const server = makeServer(store);

    const res = await server.handle(
      new Request("http://test/api/agents/_catalog/tools/history?name=fetch"),
    );
    const body = (await res.json()) as { history: Array<{ name: string }> };
    expect(body.history.map((r) => r.name)).toEqual(["fetch"]);
  });

  it("filters by the source query param", async () => {
    const store = new InMemoryToolHistoryStore();
    await store.recordSnapshot([
      obs({ name: "search", sourceKind: "in-process" }),
      obs({ name: "web:fetch", sourceKind: "mcp", sourceDetail: "web" }),
    ]);
    const server = makeServer(store);

    const res = await server.handle(
      new Request("http://test/api/agents/_catalog/tools/history?source=mcp"),
    );
    const body = (await res.json()) as { history: Array<{ name: string }> };
    expect(body.history.map((r) => r.name)).toEqual(["web:fetch"]);
  });
});
