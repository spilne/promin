import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";

import type { AgenticDagRecipe } from "../types.ts";
import type { DecisionAnswer, DecisionProvider } from "../../decision/types.ts";
import {
  DecisionModelRefusedError,
  InMemoryDecisionModelCatalog,
  createCatalogDecisionResolver,
  type DecisionResolver,
} from "../../decision/decision-model-catalog.ts";
import { scriptedDecisionProvider } from "../../testing/scripted-decision-provider.ts";
import { InMemorySecretsStorage } from "../../secrets/in-memory-secrets-storage.ts";
import { SecretScope } from "../../secrets/types.ts";
import { mockLLM } from "../../testing/mock-llm.ts";
import { invokeAgent, localAgents, makeResolver, runDag, stubAgent } from "./dag-test-kit.ts";

const model = { provider: "test", id: "decision-v1" } as const;
const refundQuestion = {
  refund: { type: "yesno", instructions: "Should this be refunded?" },
} as const;

function decisions(
  provider: DecisionProvider,
  rates?: { readonly inputPerMillion?: number; readonly outputPerMillion?: number },
): DecisionResolver {
  return async () => ({ provider, ...(rates !== undefined && { rates }) });
}

function routeAgent(id: string, condition: AgenticDagRecipe["edges"][number]["condition"]) {
  return {
    node: {
      id,
      kind: "agent" as const,
      agentId: id,
      inputs: { task: { kind: "literal" as const, value: id } },
    },
    edge: { from: "triage", to: id, condition },
  };
}

function refundDag(answerPaths = false): AgenticDagRecipe {
  const refunds = routeAgent("refunds", {
    kind: "compare",
    path: "refund.yes",
    op: "gte",
    value: 0.7,
  });
  const other = routeAgent("other", {
    kind: "compare",
    path: "refund.yes",
    op: "lt",
    value: 0.3,
  });
  return {
    id: answerPaths ? "refund-paths" : "refund-route",
    version: "v1",
    nodes: [
      {
        id: "triage",
        kind: "decide",
        model,
        questions: refundQuestion,
        inputs: {
          message: { kind: "initial", path: "message" },
          source: { kind: "literal", value: "support" },
        },
      },
      refunds.node,
      other.node,
    ],
    edges: [refunds.edge, other.edge],
    entry: ["triage"],
    terminals: ["refunds", "other"],
  };
}

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 1_000 && !condition(); attempt++) {
    await Promise.resolve();
  }
  if (!condition()) throw new Error("condition was not observed");
}

describe("decide nodes", () => {
  it("routes on a yes/no probability", async () => {
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.97, probabilitySource: "model" },
    }));
    const { result } = await runDag({
      dag: refundDag(),
      resolver: makeResolver({ refunds: stubAgent(), other: stubAgent() }),
      initialInput: { message: "charged twice" },
      config: { decisions: decisions(provider) },
    });

    expect(result.nodeOutputs.triage).toEqual({
      refund: { yes: 0.97, probabilitySource: "model" },
    });
    expect(result.nodeOutputs.refunds).toBe("refunds");
    expect(result.skipped).toEqual(["other"]);
  });

  it("routes an unsure band through an agent and any-join merge", async () => {
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.5, probabilitySource: "model" },
    }));
    const base = refundDag(true);
    const ask = routeAgent("ask-llm", {
      kind: "and",
      all: [
        { kind: "compare", path: "refund.yes", op: "gte", value: 0.3 },
        { kind: "compare", path: "refund.yes", op: "lt", value: 0.7 },
      ],
    });
    const dag: AgenticDagRecipe = {
      ...base,
      nodes: [
        ...base.nodes,
        ask.node,
        {
          id: "merge",
          kind: "transform",
          join: "any",
          inputs: {
            refund: { kind: "node", nodeId: "refunds", path: "" },
            other: { kind: "node", nodeId: "other", path: "" },
            unsure: { kind: "node", nodeId: "ask-llm", path: "" },
          },
          ops: [
            { op: "collect", fields: ["refund", "other", "unsure"] },
            { op: "compact" },
            { op: "take", count: 1 },
          ],
        },
      ],
      edges: [
        ...base.edges,
        ask.edge,
        { from: "refunds", to: "merge" },
        { from: "other", to: "merge" },
        { from: "ask-llm", to: "merge" },
      ],
      terminals: ["merge"],
    };
    const { result } = await runDag({
      dag,
      resolver: makeResolver({
        refunds: stubAgent(),
        other: stubAgent(),
        "ask-llm": stubAgent(),
      }),
      initialInput: { message: "unclear" },
      config: { decisions: decisions(provider) },
    });

    expect(result.nodeOutputs["ask-llm"]).toBe("ask-llm");
    expect(result.nodeOutputs.merge).toEqual(["ask-llm"]);
    expect(result.skipped).toEqual(["refunds", "other"]);
  });

  it("routes on a choice and a score", async () => {
    const questions = {
      team: {
        type: "choice",
        instructions: "Which team?",
        options: [{ name: "billing" }, { name: "technical" }],
      },
      urgency: {
        type: "score",
        instructions: "How urgent?",
        levels: ["low", "medium", "high"],
      },
    } as const;
    const provider = scriptedDecisionProvider(() => ({
      team: {
        choice: "billing",
        probability: 0.9,
        probabilities: { billing: 0.9, technical: 0.1 },
        probabilitySource: "model",
      },
      urgency: {
        score: 1.5,
        normalized: 0.75,
        probabilities: [0.1, 0.3, 0.6],
        probabilitySource: "model",
      },
    }));
    const billing = routeAgent("billing", {
      kind: "equals",
      path: "team.choice",
      value: "billing",
    });
    const urgent = routeAgent("urgent", {
      kind: "compare",
      path: "urgency.normalized",
      op: "gte",
      value: 0.5,
    });
    const dag: AgenticDagRecipe = {
      id: "choice-score-route",
      version: "v1",
      nodes: [
        { id: "triage", kind: "decide", model, questions, inputs: {} },
        billing.node,
        urgent.node,
      ],
      edges: [billing.edge, urgent.edge],
      entry: ["triage"],
      terminals: ["billing", "urgent"],
    };
    const { result } = await runDag({
      dag,
      resolver: makeResolver({ billing: stubAgent(), urgent: stubAgent() }),
      config: { decisions: decisions(provider) },
    });

    expect(result.nodeOutputs.billing).toBe("billing");
    expect(result.nodeOutputs.urgent).toBe("urgent");
  });

  it("passes the resolved node inputs as state", async () => {
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.97, probabilitySource: "model" },
    }));
    await runDag({
      dag: refundDag(),
      resolver: makeResolver({ refunds: stubAgent(), other: stubAgent() }),
      initialInput: { message: "charged twice" },
      config: { decisions: decisions(provider) },
    });

    expect(provider.calls[0]?.state).toEqual({ message: "charged twice", source: "support" });
  });

  it("replays the journaled decision", async () => {
    const storage = new InMemoryWorkflowStorage();
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.97, probabilitySource: "model" },
    }));
    const params = {
      dag: refundDag(),
      resolver: makeResolver({ refunds: stubAgent(), other: stubAgent() }),
      initialInput: { message: "charged twice" },
      workflowId: "decision-replay",
      storage,
      config: { decisions: decisions(provider) },
    };

    await runDag(params);
    await runDag(params);

    expect(provider.calls).toHaveLength(1);
  });

  it("turns malformed answers into a node error", async () => {
    const malformed: Readonly<Record<string, DecisionAnswer>> = {
      team: {
        choice: "sales",
        probability: 1,
        probabilities: { sales: 1 },
        probabilitySource: "model",
      },
    };
    const provider = scriptedDecisionProvider(() => malformed);
    const dag: AgenticDagRecipe = {
      id: "malformed-decision",
      version: "v1",
      nodes: [
        {
          id: "triage",
          kind: "decide",
          model,
          questions: {
            team: {
              type: "choice",
              instructions: "Which team?",
              options: [{ name: "billing" }, { name: "technical" }],
            },
          },
          inputs: {},
          onError: "skip",
        },
        routeAgent("billing", {
          kind: "equals",
          path: "team.choice",
          value: "billing",
        }).node,
      ],
      edges: [{ from: "triage", to: "billing" }],
      entry: ["triage"],
      terminals: ["billing"],
    };
    const { result } = await runDag({
      dag,
      resolver: makeResolver({ billing: stubAgent() }),
      config: { decisions: decisions(provider) },
    });

    expect(result.errors.triage).toMatch(/^decision_failed:/);
    expect(result.nodeOutputs.billing).toBeUndefined();
  });

  it("reports an unknown model as a node error", async () => {
    const resolver: DecisionResolver = async () => {
      throw new DecisionModelRefusedError("decision_model_not_found: missing test model");
    };
    const { result } = await runDag({
      dag: refundDag(),
      resolver: makeResolver({}),
      initialInput: { message: "charged twice" },
      config: { decisions: resolver },
    });

    expect(result.errors.triage).toMatch(/^decision_failed: decision_model_not_found/);
  });

  it("reports when no decision models are configured", async () => {
    const dag = refundDag();
    const decideOnly: AgenticDagRecipe = {
      ...dag,
      nodes: [dag.nodes[0]!],
      edges: [],
      terminals: ["triage"],
    };
    const { result } = await runDag({
      dag: decideOnly,
      resolver: makeResolver({}),
    });

    expect(result.errors.triage).toBe("decision_failed: no decision models are configured");
  });

  it("gives up after three transient provider failures", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const provider: DecisionProvider = {
      async decide() {
        calls += 1;
        throw new TypeError("fetch failed");
      },
    };
    const pending = runDag({
      dag: refundDag(),
      resolver: makeResolver({}),
      initialInput: { message: "charged twice" },
      config: { decisions: decisions(provider), clock },
    });

    await waitUntil(() => calls === 1 && clock.pendingCount() > 0);
    clock.advance(250);
    await waitUntil(() => calls === 2 && clock.pendingCount() > 0);
    clock.advance(500);
    const { result } = await pending;

    expect(calls).toBe(3);
    expect(result.errors.triage).toMatch(/^decision_failed:/);
  });

  it("cancels a retry wait when a sibling aborts the run", async () => {
    let calls = 0;
    const provider: DecisionProvider = {
      async decide() {
        calls += 1;
        throw new TypeError("fetch failed");
      },
    };
    const dag: AgenticDagRecipe = {
      id: "decision-abort",
      version: "v1",
      nodes: [
        {
          id: "triage",
          kind: "decide",
          model,
          questions: refundQuestion,
          inputs: {},
        },
        {
          id: "stop",
          kind: "agent",
          agentId: "stop",
          inputs: { task: { kind: "literal", value: "stop" } },
        },
      ],
      edges: [],
      entry: ["triage", "stop"],
      terminals: ["triage", "stop"],
    };
    const stop = invokeAgent(async () => {
      throw new Error("stop now");
    });
    const { result } = await runDag({
      dag,
      resolver: makeResolver({ stop }),
      config: { decisions: decisions(provider) },
    });

    expect(calls).toBeLessThan(3);
    expect(result.nodeOutputs.triage).toBeUndefined();
  });

  it("counts a decision and an agent call against the run budget", async () => {
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.97, probabilitySource: "model" },
    }));
    const llm = mockLLM([
      {
        content: "refunds",
        finishReason: "stop",
        usage: { inputTokens: 10, outputTokens: 2 },
      },
    ]);
    const agents = await localAgents({ llms: { refunds: llm } });
    const dag = refundDag();
    const oneBranch: AgenticDagRecipe = {
      ...dag,
      nodes: dag.nodes.filter((node) => node.id !== "other"),
      edges: dag.edges.filter((edge) => edge.to !== "other"),
      terminals: ["refunds"],
    };
    const { result } = await runDag({
      dag: oneBranch,
      resolver: agents.boot().resolver,
      storage: agents.storage,
      initialInput: { message: "charged twice" },
      budget: { maxLlmCalls: 2 },
      config: { decisions: decisions(provider) },
    });

    expect(result.ok).toBe(true);
    expect(result.spend.llmCalls).toBe(2);
  });

  it("refuses an unpriced decision model under a cost limit", async () => {
    const provider = scriptedDecisionProvider(() => ({
      refund: { yes: 0.97, probabilitySource: "model" },
    }));
    const dag = refundDag();
    const decideOnly: AgenticDagRecipe = {
      ...dag,
      nodes: [dag.nodes[0]!],
      edges: [],
      terminals: ["triage"],
    };
    const { result } = await runDag({
      dag: decideOnly,
      resolver: makeResolver({}),
      budget: { maxCostUsd: 1 },
      config: { decisions: decisions(provider) },
    });

    expect(result.errors.triage).toMatch(/^model_unpriced:/);
    expect(provider.calls).toHaveLength(0);
  });
});

describe("decision model catalogue", () => {
  it("indexes models and rejects duplicate keys", () => {
    const item = {
      provider: "test",
      id: "m",
      create: () => scriptedDecisionProvider(() => ({})),
    };
    const catalog = new InMemoryDecisionModelCatalog([item]);

    expect(catalog.get("test", "m")).toBe(item);
    expect(catalog.list()).toEqual([item]);
    expect(() => new InMemoryDecisionModelCatalog([item, item])).toThrow("duplicate");
  });

  it("resolves a scoped credential before creating the provider", async () => {
    const secrets = new InMemorySecretsStorage();
    await secrets.set({
      scope: SecretScope.resource("acme", "r-1"),
      key: "decision-key",
      value: "secret-value",
    });
    let receivedKey: string | undefined;
    const provider = scriptedDecisionProvider(() => ({}));
    const catalog = new InMemoryDecisionModelCatalog([
      {
        provider: "test",
        id: "m",
        create: ({ apiKey }) => {
          receivedKey = apiKey;
          return provider;
        },
      },
    ]);
    const resolve = createCatalogDecisionResolver({ catalog, secrets });

    await resolve({
      model: { provider: "test", id: "m", credentialRef: "decision-key" },
      scope: { namespaceId: "acme", resourceId: "r-1" },
    });

    expect(receivedKey).toBe("secret-value");
  });
});
