// ---------------------------------------------------------------------------
// createDagWorkflow — a run budget shared between nodes: a starting node
// gets an equal share of what is free among itself and the nodes not
// started yet, the share rides in the node's child input, and the run
// reports what its nodes spent. A node that runs out fails.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { InMemoryAgentRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import { reactiveLLM } from "../../tests/engine-harness.ts";
import { createDagWorkflow, dagNodeRunId } from "../durable-executor.ts";
import { createRegistryAgentResolver } from "../registry-agent-resolver.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import type { AgenticDagRecipe } from "../types.ts";

const SCOPE = { namespaceId: "acme", resourceId: "r-1" } as const;

const diamond: AgenticDagRecipe = {
  id: "diamond-budget",
  version: "v1",
  nodes: [
    { id: "n1", agentId: "worker", inputs: { task: { kind: "initial", path: "task" } } },
    { id: "a", agentId: "worker", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
    { id: "b", agentId: "worker", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
    {
      id: "c",
      agentId: "worker",
      inputs: {
        left: { kind: "node", nodeId: "a", path: "" },
        right: { kind: "node", nodeId: "b", path: "" },
      },
    },
  ],
  edges: [
    { from: "n1", to: "a" },
    { from: "n1", to: "b" },
    { from: "a", to: "c" },
    { from: "b", to: "c" },
  ],
  entry: ["n1"],
  terminals: ["c"],
};

async function setup() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const dags = new InMemoryDagRegistry();
  await dags.register({ ...diamond });
  const agents = new InMemoryAgentRegistry();
  await agents.register({
    id: "worker",
    backend: {
      type: "local",
      model: { provider: "test", id: "m" },
      role: { inline: { systemPrompt: null, tools: [] } },
    },
  });
  const llm = reactiveLLM(() => ({
    content: "ok",
    finishReason: "stop",
    usage: { inputTokens: 100, outputTokens: 0 },
  }));
  const resolver = createRegistryAgentResolver({
    registry: agents,
    resolve: (recipe) => resolveLocalAgent(recipe, { runner, llm: () => llm, tools: {} }),
  });
  return { storage, runner, llm, workflow: createDagWorkflow({ registry: dags, resolver }) };
}

describe("createDagWorkflow — run budget", () => {
  it("shares the run budget between nodes and reports what they spent", async () => {
    const { storage, runner, workflow } = await setup();

    const result = await runner.run({
      workflow,
      workflowId: "dag-budget",
      input: {
        dagId: diamond.id,
        version: diamond.version,
        initialInput: { task: "go" },
        scope: SCOPE,
        budget: { maxTotalTokens: 4_000 },
      },
    });

    expect(result.ok).toBe(true);
    expect(result.spend).toMatchObject({ inputTokens: 400, llmCalls: 4 });
    const budgetOf = async (nodeId: string) => {
      const run = await storage.loadWorkflow(dagNodeRunId({ dagRunId: "dag-budget", nodeId }));
      return (run?.input as { budget?: unknown } | undefined)?.budget;
    };
    // n1: a quarter of 4000. a and b: 3900 free between three waiting
    // nodes, the second one after the first took its 1300. c: what is left.
    expect(await budgetOf("n1")).toEqual({ maxTotalTokens: 1_000 });
    expect(await budgetOf("a")).toEqual({ maxTotalTokens: 1_300 });
    expect(await budgetOf("b")).toEqual({ maxTotalTokens: 1_300 });
    expect(await budgetOf("c")).toEqual({ maxTotalTokens: 3_700 });
  });

  it("records the run's spend under the run id, once", async () => {
    const { runner } = await setup();
    const dags = new InMemoryDagRegistry();
    await dags.register({ ...diamond });
    const records: Array<{ workflowId: string; namespaceId: string; inputTokens: number }> = [];
    const agents = new InMemoryAgentRegistry();
    await agents.register({
      id: "worker",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
    });
    const llm = reactiveLLM(() => ({
      content: "ok",
      finishReason: "stop",
      usage: { inputTokens: 100, outputTokens: 0 },
    }));
    const workflow = createDagWorkflow({
      registry: dags,
      resolver: createRegistryAgentResolver({
        registry: agents,
        resolve: (recipe) => resolveLocalAgent(recipe, { runner, llm: () => llm, tools: {} }),
      }),
      usageRecorder: {
        record: async ({ workflowId, scope, usage }) => {
          records.push({
            workflowId,
            namespaceId: scope.namespaceId,
            inputTokens: usage.inputTokens,
          });
        },
      },
    });
    const input = {
      dagId: diamond.id,
      version: diamond.version,
      initialInput: { task: "go" },
      scope: SCOPE,
    };

    await runner.run({ workflow, workflowId: "dag-rec", input });
    await runner.run({ workflow, workflowId: "dag-rec", input });

    expect(records).toEqual([{ workflowId: "dag-rec", namespaceId: "acme", inputTokens: 400 }]);
  });

  it("fails a node whose share can't cover its turn", async () => {
    const { runner, llm, workflow } = await setup();

    const result = await runner.run({
      workflow,
      workflowId: "dag-budget-out",
      input: {
        dagId: diamond.id,
        version: diamond.version,
        initialInput: { task: "go" },
        scope: SCOPE,
        budget: { maxLlmCalls: 3 },
      },
    });

    // Four nodes, three calls: n1's share rounds down to none.
    expect(result.ok).toBe(false);
    expect(result.errors.n1).toContain("budget_exceeded");
    expect(llm.calls).toHaveLength(0);
  });
});
