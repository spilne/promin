// ---------------------------------------------------------------------------
// Budgets on the agent gateway: a turn on a thread or namespace whose
// budget is used up answers 429 `budget_exceeded` (send, stream, invoke);
// the usage routes show spend against the limits; a recipe's
// `backend.budget` is validated and caps its turns; a DAG run takes a
// budget.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryUsageLedger } from "@promin/agent";
import type {
  BudgetExceededResponse,
  NamespaceUsageResponse,
  ThreadInvokeResponse,
  ThreadUsageResponse,
} from "../agents/agent-gateway-deps.ts";
import { bootAgents, jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

const body = { task: "hi", namespaceId: "ns", resourceId: "alice" };
const DAY_MS = 86_400_000;

function usageLLM() {
  return {
    chat: async () => ({
      content: "ok",
      finishReason: "stop" as const,
      usage: { inputTokens: 400, outputTokens: 20 },
    }),
  };
}

describe("agent gateway — budgets", () => {
  it("refuses a thread send and a stream with 429 once the namespace budget is used up", async () => {
    const ledger = new InMemoryUsageLedger();
    const { server } = await bootAgents({
      llm: usageLLM(),
      budgets: { ledger, namespace: { limits: { maxLlmCalls: 1 }, windowMs: DAY_MS } },
    });

    const first = await server.handle(req("POST", "/api/agents/support/threads/t1", body));
    expect(first.status).toBe(200);

    const refused = await server.handle(req("POST", "/api/agents/support/threads/t2", body));
    expect(refused.status).toBe(429);
    expect(await jsonOf<BudgetExceededResponse>(refused)).toMatchObject({
      error: "budget_exceeded",
      scope: "namespace",
      dimension: "maxLlmCalls",
      limit: 1,
      used: 1,
    });

    const stream = await server.handle(req("POST", "/api/agents/support/threads/t3/stream", body));
    expect(stream.status).toBe(429);
    const invoke = await server.handle(req("POST", "/api/agents/support/invoke", body));
    expect(invoke.status).toBe(429);
    const oneShotStream = await server.handle(req("POST", "/api/agents/support/stream", body));
    expect(oneShotStream.status).toBe(429);

    // Another namespace has its own window.
    const other = await server.handle(
      req("POST", "/api/agents/support/threads/t1", { ...body, namespaceId: "ns2" }),
    );
    expect(other.status).toBe(200);
  });

  it("refuses a thread over its cumulative budget, and shows spend against the limits", async () => {
    const ledger = new InMemoryUsageLedger();
    const { server } = await bootAgents({
      llm: usageLLM(),
      budgets: {
        ledger,
        thread: { maxTotalTokens: 400 },
        namespace: { limits: { maxLlmCalls: 10 }, windowMs: DAY_MS },
      },
    });
    expect((await server.handle(req("POST", "/api/agents/support/threads/t1", body))).status).toBe(
      200,
    );
    const refused = await server.handle(req("POST", "/api/agents/support/threads/t1", body));
    expect(refused.status).toBe(429);
    expect(await jsonOf<BudgetExceededResponse>(refused)).toMatchObject({ scope: "thread" });

    const usage = await server.handle(
      req("GET", "/api/agents/support/threads/t1/usage?namespaceId=ns&resourceId=alice"),
    );
    expect(usage.status).toBe(200);
    expect(await jsonOf<ThreadUsageResponse>(usage)).toEqual({
      threadId: "t1",
      enabled: true,
      thread: {
        spent: {
          inputTokens: 400,
          outputTokens: 20,
          cacheReadTokens: 0,
          cacheWriteTokens: 0,
          costUsd: 0,
          llmCalls: 1,
          toolCalls: 0,
        },
        limits: { maxTotalTokens: 400 },
      },
      namespace: {
        limits: { maxLlmCalls: 10 },
        windowMs: DAY_MS,
        spent: expect.objectContaining({ llmCalls: 1 }),
      },
    });

    const ns = await server.handle(req("GET", "/api/agents/_usage?namespaceId=ns"));
    expect(await jsonOf<NamespaceUsageResponse>(ns)).toMatchObject({
      namespaceId: "ns",
      enabled: true,
      namespace: { windowMs: DAY_MS, spent: { llmCalls: 1 } },
    });
    // Another thread of the namespace is not refused by t1's budget.
    expect((await server.handle(req("POST", "/api/agents/support/threads/t2", body))).status).toBe(
      200,
    );
  });

  it("reports budgets as disabled when none are configured", async () => {
    const { server } = await bootAgents();
    await server.handle(req("POST", "/api/agents/support/threads/t1", body));
    const usage = await server.handle(
      req("GET", "/api/agents/support/threads/t1/usage?namespaceId=ns&resourceId=alice"),
    );
    expect(await jsonOf<ThreadUsageResponse>(usage)).toEqual({ threadId: "t1", enabled: false });
    const ns = await server.handle(req("GET", "/api/agents/_usage?namespaceId=ns"));
    expect(await jsonOf<NamespaceUsageResponse>(ns)).toEqual({ namespaceId: "ns", enabled: false });
  });

  it("checks the caller may see the namespace's usage", async () => {
    const { server } = await bootAgents({
      authorizeScope: ({ scope }) => "namespaceId" in scope && scope.namespaceId === "mine",
    });
    const forbidden = await server.handle(req("GET", "/api/agents/_usage?namespaceId=ns"));
    expect(forbidden.status).toBe(403);
    const missing = await server.handle(req("GET", "/api/agents/_usage"));
    expect(missing.status).toBe(400);
  });
});

describe("agent recipes — backend.budget", () => {
  it("validates the budget and caps the recipe's turns with it", async () => {
    const { server } = await bootAgents({ llm: usageLLM() });
    const invalid = await server.handle(
      req("POST", "/api/agents", {
        id: "capped",
        backend: { ...localBackend, budget: { maxLlmCalls: -1 } },
      }),
    );
    expect(invalid.status).toBe(400);
    const unknownField = await server.handle(
      req("POST", "/api/agents", {
        id: "capped",
        backend: { ...localBackend, budget: { maxDollars: 1 } },
      }),
    );
    expect(unknownField.status).toBe(400);

    const created = await server.handle(
      req("POST", "/api/agents", {
        id: "capped",
        backend: { ...localBackend, budget: { maxLlmCalls: 0 } },
      }),
    );
    expect(created.status).toBe(201);
    const res = await server.handle(req("POST", "/api/agents/capped/threads/t1", body));
    expect(res.status).toBe(200);
    expect(await jsonOf<ThreadInvokeResponse>(res)).toMatchObject({
      finishReason: "budget_exceeded",
      text: "(budget exhausted)",
    });
  });
});

describe("agent recipes — cost budget without a price", () => {
  it("answers 400 model_unpriced, not a server error", async () => {
    const { server } = await bootAgents({ llm: usageLLM() });
    await server.handle(
      req("POST", "/api/agents", {
        id: "costly",
        backend: { ...localBackend, budget: { maxCostUsd: 1 } },
      }),
    );
    const res = await server.handle(req("POST", "/api/agents/costly/threads/t1", body));
    expect(res.status).toBe(400);
    expect(await jsonOf<{ error: string }>(res)).toMatchObject({ error: "model_unpriced" });
  });
});

describe("DAG runs — budget", () => {
  async function registerOneNodeDag(
    dags: NonNullable<Awaited<ReturnType<typeof bootAgents>>["dags"]>,
  ) {
    await dags.registry.register({
      id: "one",
      version: "v1",
      nodes: [
        { id: "n1", agentId: "support", inputs: { task: { kind: "initial", path: "task" } } },
      ],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    });
  }
  const runBody = { namespaceId: "ns", resourceId: "alice", initialInput: { task: "go" } };

  it("admits a run against its namespace budget and records its spend under the run id", async () => {
    const ledger = new InMemoryUsageLedger();
    const { server, dags, storage } = await bootAgents({
      dags: true,
      llm: usageLLM(),
      budgets: { ledger, namespace: { limits: { maxTotalTokens: 1_000 }, windowMs: DAY_MS } },
    });
    await registerOneNodeDag(dags!);

    const started = await server.handle(
      req("POST", "/api/dags/one/run", { ...runBody, workflowId: "dag-run-1" }),
    );
    expect(started.status).toBe(202);
    // The run is capped by what the namespace has left.
    const run = await storage.loadWorkflow("dag-run-1");
    expect((run?.input as { budget?: unknown } | undefined)?.budget).toEqual({
      maxTotalTokens: 1_000,
    });
    let spent = 0;
    for (let i = 0; i < 5_000 && spent === 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
      spent = (await ledger.total({ namespaceId: "ns" })).inputTokens;
    }
    expect(spent).toBe(400);

    // Once the window is used up, a run answers 429.
    await ledger.record({
      turnId: "elsewhere",
      namespaceId: "ns",
      usage: { ...(await ledger.total({ namespaceId: "none" })), inputTokens: 600 },
    });
    const refused = await server.handle(req("POST", "/api/dags/one/run", runBody));
    expect(refused.status).toBe(429);
    expect(await jsonOf<BudgetExceededResponse>(refused)).toMatchObject({
      error: "budget_exceeded",
      scope: "namespace",
    });
  });

  it("rejects a malformed run budget", async () => {
    const { server, dags } = await bootAgents({ dags: true });
    await dags!.registry.register({
      id: "one",
      version: "v1",
      nodes: [
        { id: "n1", agentId: "support", inputs: { task: { kind: "initial", path: "task" } } },
      ],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    });
    const res = await server.handle(
      req("POST", "/api/dags/one/run", {
        namespaceId: "ns",
        resourceId: "alice",
        initialInput: { task: "go" },
        budget: { maxTotalTokens: "lots" },
      }),
    );
    expect(res.status).toBe(400);
    expect(await jsonOf<{ error: string }>(res)).toMatchObject({ error: "invalid_budget" });
  });
});
