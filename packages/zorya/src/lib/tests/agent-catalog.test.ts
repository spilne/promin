// ---------------------------------------------------------------------------
// GET /api/agents/_catalog/models — designer UI's model dropdown source.
//
// Pins:
//   - returns the catalog's serialize() projection (no llm field on the wire)
//   - empty array when no catalog is wired
//   - JSON-safe round-trip
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryModelCatalog,
  type LLMProvider,
  type ModelCatalogItem,
} from "@promin/agent";
import { ZoryaServer } from "../../server/server.ts";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../index.ts";

const stubLLM = (label: string): LLMProvider => ({
  chat: async () => ({ content: label, finishReason: "stop" }),
});

function makeServer(items: ModelCatalogItem[] | null) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return new ZoryaServer({
    workflows: new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    }),
    agents: new ZoryaAgents({
      registry: new InMemoryAgentRegistry(),
      resolve: () => {
        throw new Error("catalog test should not resolve agents");
      },
      ...(items !== null && { models: new InMemoryModelCatalog(items) }),
    }),
  });
}

describe("GET /api/agents/_catalog/models", () => {
  it("returns the catalog's serialized projection (no llm field on the wire)", async () => {
    const server = makeServer([
      {
        provider: "anthropic",
        id: "claude-sonnet-4-6",
        displayName: "Claude Sonnet 4.6",
        contextLimit: 200_000,
        capabilities: ["chat", "tools"],
        costTier: "mid",
        llm: stubLLM("anthropic-sonnet"),
      },
      {
        provider: "openai",
        id: "gpt-4o",
        displayName: "GPT-4o",
        capabilities: ["chat"],
        costTier: "high",
        llm: stubLLM("openai-gpt4o"),
      },
    ]);

    const res = await server.handle(new Request("http://test/api/agents/_catalog/models"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      models: Array<{ provider: string; id: string; llm?: unknown }>;
    };
    expect(body.models).toHaveLength(2);
    expect(body.models[0]!.provider).toBe("anthropic");
    expect(body.models[1]!.id).toBe("gpt-4o");
    // llm field is runtime-only and must NOT cross the network boundary —
    // its closure isn't JSON-serializable, and exposing it would leak
    // implementation details (api key bindings) to the client.
    expect("llm" in body.models[0]!).toBe(false);
    expect("llm" in body.models[1]!).toBe(false);
  });

  it("returns an empty list when no catalog is wired (graceful, not 404)", async () => {
    const server = makeServer(null);
    const res = await server.handle(new Request("http://test/api/agents/_catalog/models"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { models: unknown[] };
    expect(body.models).toEqual([]);
  });

  it("does not collide with /api/agents/:id (literal _catalog wins)", async () => {
    // /api/agents/some-agent must still reach getAgent — the _catalog path
    // is registered first in server.ts so it's matched as a literal.
    const server = makeServer([]);
    const res = await server.handle(
      new Request("http://test/api/agents/some-agent-that-does-not-exist"),
    );
    // 404 from the registry, not from the catalog handler — proves the
    // routes don't shadow each other.
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error?: string };
    expect(body.error).toBe("agent_not_found");
  });
});
