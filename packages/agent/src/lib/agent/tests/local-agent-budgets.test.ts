// ---------------------------------------------------------------------------
// LocalAgent thread and namespace budgets:
//   - a thread's cumulative budget and a namespace's rolling window refuse
//     a turn once used up (BudgetExceededError), and cap an admitted turn
//     at what is left;
//   - concurrent turns of a namespace all count, once each;
//   - a committed turn is recorded once per turn workflow id: a retried
//     send and a crash recovered on another worker don't count it twice;
//   - a one-shot invoke counts against its namespace.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { AgentBudgets } from "../../budget/agent-budgets.ts";
import { BudgetExceededError, EMPTY_USAGE } from "../../budget/budget.ts";
import { InMemoryUsageLedger, type UsageEntry } from "../../budget/usage-ledger.ts";
import type { LLMResponse } from "../../llm-provider.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { deferred, reactiveLLM, until } from "../../tests/engine-harness.ts";
import { tool } from "../../tool.ts";
import { z } from "zod";
import { LocalAgent, threadTurnWorkflowId } from "../local-agent.ts";

/** Lock lease of `InMemoryWorkflowStorage` runs, plus a margin. */
const PAST_LOCK_LEASE_MS = 121_000;
const DAY_MS = 86_400_000;

const reply = (tokens: number): LLMResponse => ({
  content: "ok",
  finishReason: "stop",
  usage: { inputTokens: tokens, outputTokens: 0 },
});

function makeAgent(params: {
  budgets: AgentBudgets;
  tokens?: number;
  storage?: InMemoryWorkflowStorage;
  memory?: InMemoryMemoryStore;
}) {
  const llm = reactiveLLM(() => reply(params.tokens ?? 100));
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner: createWorkflowRunner({ storage }),
    memory: params.memory ?? new InMemoryMemoryStore(),
    agent: { name: "budgeted", llm, llmRetry: false },
    budgets: params.budgets,
  });
  return { agent, llm, storage };
}

describe("LocalAgent — thread budget", () => {
  it("refuses a turn once the thread's budget is used up, and caps the turn before it", async () => {
    const ledger = new InMemoryUsageLedger();
    const { agent, llm, storage } = makeAgent({
      tokens: 1_000,
      budgets: { ledger, thread: { maxTotalTokens: 2_500 } },
    });
    const thread = await agent.thread("t-1");

    await thread.send({ task: "one" }, { turnKey: "k1" });
    await thread.send({ task: "two" }, { turnKey: "k2" });
    // The third turn starts with 500 left: its input carries that ceiling.
    // Its one call goes over, but a text answer still ends the turn.
    const third = await thread.send({ task: "three" }, { turnKey: "k3" });
    expect(await third.text).toBe("ok");
    const run = await storage.loadWorkflow(
      threadTurnWorkflowId({ namespaceId: "acme", threadId: "t-1", turnKey: "k3" }),
    );
    expect((run?.input as { budget?: unknown } | undefined)?.budget).toEqual({
      maxTotalTokens: 500,
    });
    expect((await ledger.total({ namespaceId: "acme", threadId: "t-1" })).inputTokens).toBe(3_000);

    const refused = await thread.send({ task: "four" }, { turnKey: "k4" }).catch((e: unknown) => e);
    expect(refused).toBeInstanceOf(BudgetExceededError);
    expect(refused).toMatchObject({
      code: "budget_exceeded",
      scope: "thread",
      dimension: "maxTotalTokens",
      limit: 2_500,
      used: 3_000,
    });
    expect(llm.calls).toHaveLength(3);

    // Another thread has its own budget.
    const other = await agent.thread("t-2");
    expect(await (await other.send({ task: "hi" })).text).toBe("ok");
  });
});

describe("LocalAgent — budget edges", () => {
  it("refuses a thread with less left than one useful call instead of answering empty turns", async () => {
    const ledger = new InMemoryUsageLedger();
    const llm = reactiveLLM(() => ({
      content: "ok",
      finishReason: "stop",
      usage: { inputTokens: 10, outputTokens: 600 },
    }));
    const agent = new LocalAgent({
      namespaceId: "acme",
      resourceId: "alice",
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      memory: new InMemoryMemoryStore(),
      agent: { name: "capped", llm, llmRetry: false, maxTokens: 500 },
      budgets: { ledger, thread: { maxOutputTokens: 1_000 } },
    });
    const thread = await agent.thread("t");

    expect(await (await thread.send({ task: "one" })).finishReason).toBe("stop");
    // 400 output tokens left: the call goes out capped at 400, not refused
    // for the 500 `maxTokens` would ask for.
    expect(await (await thread.send({ task: "two" })).finishReason).toBe("stop");
    expect(llm.calls.map((c) => c.maxTokens)).toEqual([500, 400]);
    // Nothing useful left: refused (a 429), never an empty "(budget exhausted)" turn.
    const third = await thread.send({ task: "three" }).catch((e: unknown) => e);
    expect(third).toBeInstanceOf(BudgetExceededError);
    expect(llm.calls).toHaveLength(2);
  });

  it("refuses a thread whose remaining tokens are below minCallTokens", async () => {
    const ledger = new InMemoryUsageLedger();
    await ledger.record({
      turnId: "earlier",
      namespaceId: "acme",
      threadId: "t-1",
      usage: { ...EMPTY_USAGE, inputTokens: 900 },
    });
    const { agent, llm } = makeAgent({
      budgets: { ledger, thread: { maxTotalTokens: 1_000 }, minCallTokens: 200 },
    });
    const refused = await (await agent.thread("t-1")).send({ task: "go" }).catch((e: unknown) => e);
    expect(refused).toMatchObject({ code: "budget_exceeded", dimension: "maxTotalTokens" });
    expect(llm.calls).toHaveLength(0);
  });

  it("records the spend of a turn that fails after paid steps", async () => {
    const ledger = new InMemoryUsageLedger();
    let calls = 0;
    const llm = reactiveLLM(() => {
      calls++;
      if (calls === 2) throw new Error("provider down");
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: `c${calls}`, name: "noop", input: {} }],
        usage: { inputTokens: 5_000, outputTokens: 0 },
      };
    });
    const noop = tool({
      name: "noop",
      description: "does nothing",
      parameters: z.object({}),
      execute: async () => "ok",
    });
    const agent = new LocalAgent({
      namespaceId: "acme",
      resourceId: "alice",
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      memory: new InMemoryMemoryStore(),
      agent: { name: "failing", llm, llmRetry: false, tools: { noop } },
      budgets: { ledger, namespace: { limits: { maxInputTokens: 5_200 }, windowMs: DAY_MS } },
    });

    const failed = await (await agent.thread("t")).send({ task: "go" }).catch((e: unknown) => e);
    expect(failed).toBeInstanceOf(Error);
    expect(await ledger.total({ namespaceId: "acme", windowMs: DAY_MS })).toMatchObject({
      inputTokens: 5_000,
      llmCalls: 1,
      toolCalls: 1,
    });
    // The failed turn's spend counts: the namespace now refuses.
    const refused = await (await agent.thread("t2")).send({ task: "go" }).catch((e: unknown) => e);
    expect(refused).toBeInstanceOf(BudgetExceededError);
  });
});

describe("LocalAgent — namespace budget", () => {
  it("enforces a rolling window across threads", async () => {
    const clock = FakeWallClock.create(1_000_000);
    const ledger = new InMemoryUsageLedger({ clock });
    const { agent } = makeAgent({
      budgets: { ledger, namespace: { limits: { maxLlmCalls: 2 }, windowMs: DAY_MS } },
    });

    await (await agent.thread("a")).send({ task: "1" });
    await (await agent.thread("b")).send({ task: "2" });
    const refused = await (await agent.thread("c")).send({ task: "3" }).catch((e: unknown) => e);
    expect(refused).toMatchObject({ code: "budget_exceeded", scope: "namespace" });

    // A day later the window has moved past both turns.
    clock.advance(DAY_MS);
    expect(await (await (await agent.thread("c")).send({ task: "3" })).text).toBe("ok");
  });

  it("counts every concurrent turn once", async () => {
    const ledger = new InMemoryUsageLedger();
    const { agent } = makeAgent({
      budgets: { ledger, namespace: { limits: { maxLlmCalls: 100 }, windowMs: DAY_MS } },
    });

    await Promise.all(
      Array.from({ length: 8 }, async (_, i) =>
        (await agent.thread(`t-${i}`)).send({ task: "go" }),
      ),
    );

    const total = await ledger.total({ namespaceId: "acme", windowMs: DAY_MS });
    expect(total.llmCalls).toBe(8);
    expect(total.inputTokens).toBe(800);
  });

  it("counts a one-shot invoke against its namespace", async () => {
    const ledger = new InMemoryUsageLedger();
    const { agent } = makeAgent({
      budgets: { ledger, namespace: { limits: { maxLlmCalls: 1 }, windowMs: DAY_MS } },
    });
    await agent.invoke({ task: "go" });
    expect((await ledger.total({ namespaceId: "acme" })).llmCalls).toBe(1);
    await expect(agent.invoke({ task: "again" })).rejects.toBeInstanceOf(BudgetExceededError);
  });
});

describe("LocalAgent — idempotent accounting", () => {
  it("a send retried with the same turn key is recorded once", async () => {
    const ledger = new InMemoryUsageLedger();
    const { agent } = makeAgent({ budgets: { ledger, thread: { maxLlmCalls: 10 } } });
    const thread = await agent.thread("t-1");
    await thread.send({ task: "go" }, { turnKey: "same" });
    await thread.send({ task: "go" }, { turnKey: "same" });
    expect((await ledger.total({ namespaceId: "acme", threadId: "t-1" })).llmCalls).toBe(1);
  });

  it("a crash after the record, recovered on another worker, records the turn once", async () => {
    const gate = deferred();
    let parked = false;
    const records: boolean[] = [];
    class StallingLedger extends InMemoryUsageLedger {
      override async record(entry: UsageEntry): Promise<boolean> {
        const recorded = await super.record(entry);
        records.push(recorded);
        if (records.length === 1) {
          parked = true;
          await gate.promise;
        }
        return recorded;
      }
    }
    const ledger = new StallingLedger();
    const clock = FakeWallClock.create(Date.now());
    const storage = new InMemoryWorkflowStorage({ clock });
    const memory = new InMemoryMemoryStore();
    const budgets: AgentBudgets = { ledger, thread: { maxLlmCalls: 10 } };
    const workerA = makeAgent({ budgets, storage, memory }).agent;
    const workerB = makeAgent({ budgets, storage, memory }).agent;

    const runA = (await workerA.thread("t-1")).send({ task: "go" }, { turnKey: "crash" }).then(
      () => undefined,
      () => undefined,
    );
    await until(() => parked);
    clock.set(clock.currentTimeMs() + PAST_LOCK_LEASE_MS);
    const recovered = await (
      await workerB.thread("t-1")
    ).send({ task: "go" }, { turnKey: "crash" });
    gate.resolve();
    await runA;

    expect(await recovered.text).toBe("ok");
    expect(records).toEqual([true, false]);
    expect((await ledger.total({ namespaceId: "acme", threadId: "t-1" })).llmCalls).toBe(1);
  });
});
