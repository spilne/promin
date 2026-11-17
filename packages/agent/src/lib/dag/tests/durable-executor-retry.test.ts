import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

import type { Agent, AgentRunOutput } from "../../agent/types.ts";
import type { TurnBudget } from "../../budget/budget.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { createDagWorkflow } from "../durable-executor.ts";
import type { AgentRefusedError, AgenticDagRecipe, ForEach, Retry } from "../types.ts";
import { AgentRefusedError as Refused } from "../types.ts";
import { SCOPE, makeResolver, runDag, stubAgent, textOnly } from "./dag-test-kit.ts";

function customAgent(invoke: Agent["invoke"]): Agent {
  return { ...stubAgent(), invoke };
}

function output(params: {
  readonly text: string;
  readonly finishReason?: "stop" | "budget_exceeded" | "cancelled" | "output_invalid";
  readonly inputTokens?: number;
}): AgentRunOutput {
  return {
    ...textOnly(params.text),
    finishReason: Promise.resolve(params.finishReason ?? "stop"),
    usage: Promise.resolve({ inputTokens: params.inputTokens ?? 0, outputTokens: 0 }),
  };
}

function retryDag(params: {
  readonly id: string;
  readonly retry: Retry;
  readonly timeoutMs?: number;
  readonly forEach?: ForEach;
  readonly outputSchema?: Record<string, unknown>;
  readonly extraNodes?: AgenticDagRecipe["nodes"];
}): AgenticDagRecipe {
  const first = {
    id: "n1",
    kind: "agent" as const,
    agentId: "worker",
    inputs: { task: { kind: "literal" as const, value: "work" } },
    retry: params.retry,
    ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
    ...(params.forEach !== undefined && { forEach: params.forEach }),
    ...(params.outputSchema !== undefined && { output: { schema: params.outputSchema } }),
  };
  const extraNodes = params.extraNodes ?? [];
  return {
    id: params.id,
    version: "v1",
    nodes: [first, ...extraNodes],
    edges: [],
    entry: [first.id, ...extraNodes.map((node) => node.id)],
    terminals: [first.id, ...extraNodes.map((node) => node.id)],
  };
}

async function childIds(storage: InMemoryWorkflowStorage, parentId: string): Promise<string[]> {
  return (await storage.listWorkflows({ parentId })).map((run) => run.workflowId).sort();
}

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 1_000 && !condition(); attempt++) await Promise.resolve();
  expect(condition()).toBe(true);
}

describe("createDagWorkflow — per-node retry", () => {
  it("retries a failed attempt with a distinct child id, then succeeds", async () => {
    let calls = 0;
    const worker = customAgent(async () => {
      calls++;
      if (calls === 1) throw new Error("first failed");
      return textOnly("done");
    });
    const { result, storage } = await runDag({
      dag: retryDag({ id: "retry-once", retry: { maxAttempts: 3 } }),
      resolver: makeResolver({ worker }),
      workflowId: "retry-once",
    });

    expect(result.nodeOutputs.n1).toBe("done");
    expect(result.nodeStats.n1?.attempts).toBe(2);
    expect(await childIds(storage, "retry-once")).toEqual([
      "retry-once.node-n1",
      "retry-once.node-n1.try-2",
    ]);
  });

  it("returns the last error after maxAttempts", async () => {
    let calls = 0;
    const { result, storage } = await runDag({
      dag: retryDag({ id: "retry-twice", retry: { maxAttempts: 2 } }),
      resolver: makeResolver({
        worker: customAgent(async () => {
          calls++;
          throw new Error(`failed ${calls}`);
        }),
      }),
      workflowId: "retry-twice",
    });

    expect(result.errors.n1).toContain("failed 2");
    expect(result.nodeStats.n1?.attempts).toBe(2);
    expect(await childIds(storage, "retry-twice")).toHaveLength(2);
  });

  it("retries an output-schema mismatch", async () => {
    let calls = 0;
    const { result } = await runDag({
      dag: retryDag({
        id: "retry-output",
        retry: { maxAttempts: 2 },
        outputSchema: {
          type: "object",
          properties: { value: { type: "string" } },
          required: ["value"],
          additionalProperties: false,
        },
      }),
      resolver: makeResolver({
        worker: customAgent(async () => {
          calls++;
          return textOnly(calls === 1 ? "not json" : '{"value":"ok"}');
        }),
      }),
    });

    expect(result.nodeOutputs.n1).toEqual({ value: "ok" });
    expect(result.nodeStats.n1?.attempts).toBe(2);
  });

  it("retries after an attempt times out", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const worker = customAgent(async (_input, opts) => {
      calls++;
      if (calls === 1) {
        await new Promise<void>((resolve) => {
          if (opts?.signal?.aborted) resolve();
          else opts?.signal?.addEventListener("abort", () => resolve(), { once: true });
        });
        return output({ text: "stopped", finishReason: "cancelled" });
      }
      return textOnly("recovered");
    });
    const pending = runDag({
      dag: retryDag({
        id: "retry-timeout",
        retry: { maxAttempts: 2 },
        timeoutMs: 1_000,
      }),
      resolver: makeResolver({ worker }),
      config: { clock },
    });

    await waitUntil(() => calls === 1 && clock.pendingCount() === 1);
    clock.advance(1_000);
    const { result } = await pending;
    expect(result.nodeOutputs.n1).toBe("recovered");
    expect(result.nodeStats.n1?.attempts).toBe(2);
  });

  it("does not retry a disabled agent refusal", async () => {
    const refusal: AgentRefusedError = new Refused("agent_disabled", 'agent "worker" is disabled');
    const { result, storage } = await runDag({
      dag: retryDag({ id: "no-retry-disabled", retry: { maxAttempts: 3 } }),
      resolver: async () => {
        throw refusal;
      },
      workflowId: "no-retry-disabled",
    });

    expect(result.errors.n1).toMatch(/^agent_disabled:/);
    expect(result.nodeStats.n1?.attempts).toBeUndefined();
    expect(await childIds(storage, "no-retry-disabled")).toEqual([]);
  });

  it("does not retry a turn that spent its budget", async () => {
    let calls = 0;
    const { result, storage } = await runDag({
      dag: retryDag({ id: "no-retry-budget", retry: { maxAttempts: 3 } }),
      resolver: makeResolver({
        worker: customAgent(async () => {
          calls++;
          return output({ text: "spent", finishReason: "budget_exceeded" });
        }),
      }),
      workflowId: "no-retry-budget",
    });

    expect(result.errors.n1).toMatch(/^budget_exceeded:/);
    expect(calls).toBe(1);
    expect(await childIds(storage, "no-retry-budget")).toHaveLength(1);
  });

  it("subtracts earlier spend from every later attempt budget", async () => {
    let calls = 0;
    const schema = {
      type: "object",
      properties: { value: { type: "string" } },
      required: ["value"],
      additionalProperties: false,
    };
    const { result, storage } = await runDag({
      dag: retryDag({
        id: "retry-budget",
        retry: { maxAttempts: 3 },
        outputSchema: schema,
      }),
      resolver: makeResolver({
        worker: customAgent(async () => {
          calls++;
          if (calls < 3) {
            return output({ text: "invalid", finishReason: "output_invalid", inputTokens: 100 });
          }
          return output({ text: '{"value":"ok"}', inputTokens: 50 });
        }),
      }),
      workflowId: "retry-budget",
      budget: { maxTotalTokens: 250 },
    });

    expect(result.nodeOutputs.n1).toEqual({ value: "ok" });
    expect(result.spend.inputTokens).toBe(250);
    const third = await storage.loadWorkflow("retry-budget.node-n1.try-3");
    expect((third?.input as { budget?: TurnBudget } | undefined)?.budget).toEqual({
      maxTotalTokens: 50,
    });
  });

  it("retries each fan-out item independently", async () => {
    const calls = new Map<number, number>();
    const worker = customAgent(async (input) => {
      const item = (JSON.parse(input.task) as { item: number }).item;
      const count = (calls.get(item) ?? 0) + 1;
      calls.set(item, count);
      if (count === 1) throw new Error(`retry item ${item}`);
      return textOnly(`x${item}`);
    });
    const { result, storage } = await runDag({
      dag: retryDag({
        id: "retry-items",
        retry: { maxAttempts: 2 },
        forEach: {
          items: { kind: "initial", path: "items" },
          as: "item",
          maxItems: 3,
        },
      }),
      resolver: makeResolver({ worker }),
      initialInput: { items: [0, 1, 2] },
      workflowId: "retry-items",
    });

    expect(result.nodeOutputs.n1).toEqual(["x0", "x1", "x2"]);
    expect(calls).toEqual(
      new Map([
        [0, 2],
        [1, 2],
        [2, 2],
      ]),
    );
    expect(await childIds(storage, "retry-items")).toEqual([
      "retry-items.node-n1.item-0",
      "retry-items.node-n1.item-0.try-2",
      "retry-items.node-n1.item-1",
      "retry-items.node-n1.item-1.try-2",
      "retry-items.node-n1.item-2",
      "retry-items.node-n1.item-2.try-2",
    ]);
  });

  it("durably waits for backoff while an independent sibling completes", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const registry = new InMemoryDagRegistry();
    const dag = retryDag({
      id: "retry-backoff",
      retry: { maxAttempts: 2, backoffMs: 2_000 },
      extraNodes: [{ id: "side", kind: "agent", agentId: "side", inputs: {} }],
    });
    await registry.register(dag);
    let calls = 0;
    const resolver = makeResolver({
      worker: customAgent(async () => {
        calls++;
        if (calls === 1) throw new Error("retry me");
        return textOnly("done");
      }),
      side: stubAgent(() => "side done"),
    });
    const workflow = createDagWorkflow({ registry, resolver, clock });
    const runner = createWorkflowRunner({ storage, clock });
    const run = () =>
      runner.run({
        workflow,
        workflowId: "retry-backoff",
        input: {
          dagId: dag.id,
          version: dag.version,
          initialInput: {},
          scope: SCOPE,
        },
      });

    await expect(run()).rejects.toThrow(/sleeping until/);
    expect(await storage.loadWorkflow("retry-backoff.node-n1.try-2")).toBeNull();
    expect((await storage.loadWorkflow("retry-backoff.node-side"))?.status).toBe("completed");
    clock.advance(1_999);
    await expect(run()).rejects.toThrow(/sleeping until/);
    expect(await storage.loadWorkflow("retry-backoff.node-n1.try-2")).toBeNull();
    clock.advance(1);

    const result = await run();
    expect(result.nodeOutputs.n1).toBe("done");
    expect(result.nodeOutputs.side).toBe("side done");
    expect(await storage.loadWorkflow("retry-backoff.node-n1.try-2")).not.toBeNull();
  });
});
