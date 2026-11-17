import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

import type { Agent, AgentInput, AgentInvokeOpts, AgentRunOutput } from "../../agent/types.ts";
import type { DecisionProvider } from "../../decision/types.ts";
import type { LLMResponse } from "../../llm-provider.ts";
import { reactiveLLM } from "../../tests/engine-harness.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { createDagWorkflow } from "../durable-executor.ts";
import type { AgenticDagRecipe, ForEach } from "../types.ts";
import { SCOPE, localAgents, makeResolver, runDag, stubAgent, textOnly } from "./dag-test-kit.ts";

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 1_000 && !condition(); attempt++) {
    await Promise.resolve();
  }
  expect(condition()).toBe(true);
}

function timedDag(params: {
  readonly id: string;
  readonly timeoutMs?: number;
  readonly forEach?: ForEach;
}): AgenticDagRecipe {
  return {
    id: params.id,
    version: "v1",
    nodes: [
      {
        id: "n1",
        kind: "agent",
        agentId: "worker",
        inputs: { task: { kind: "literal", value: "work" } },
        ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
        ...(params.forEach !== undefined && { forEach: params.forEach }),
      },
    ],
    edges: [],
    entry: ["n1"],
    terminals: ["n1"],
  };
}

function cancellableAgent(params: { readonly onStart?: (input: AgentInput) => void }): Agent {
  return {
    ...stubAgent(),
    invoke: async (input: AgentInput, opts?: AgentInvokeOpts): Promise<AgentRunOutput> => {
      params.onStart?.(input);
      await new Promise<void>((resolve) => {
        if (opts?.signal?.aborted) resolve();
        else
          opts?.signal?.addEventListener("abort", () => resolve(), {
            once: true,
          });
      });
      return {
        ...textOnly("stopped"),
        finishReason: Promise.resolve("cancelled"),
      };
    },
  };
}

function customAgent(invoke: Agent["invoke"]): Agent {
  return { ...stubAgent(), invoke };
}

describe("createDagWorkflow — per-attempt timeout", () => {
  it("times out a hanging local agent and cancels its child run", async () => {
    const clock = FakeWallClock.create(0);
    let starts = 0;
    const storage = new InMemoryWorkflowStorage({ clock });
    const pending = runDag({
      dag: timedDag({ id: "timeout", timeoutMs: 5_000 }),
      resolver: makeResolver({
        worker: cancellableAgent({ onStart: () => starts++ }),
      }),
      storage,
      config: { clock },
      workflowId: "timeout-run",
    });

    await waitUntil(() => starts === 1 && clock.pendingCount() === 1);
    clock.advance(5_000);
    const { result } = await pending;

    expect(result.errors.n1).toBe('node_timeout: node "n1" ran past 5000ms');
    expect((await storage.loadWorkflow("timeout-run.node-n1"))?.status).toBe("completed");
  });

  it("clears the deadline when an attempt finishes before it", async () => {
    const clock = FakeWallClock.create(0);
    const { result } = await runDag({
      dag: timedDag({ id: "before-timeout", timeoutMs: 5_000 }),
      resolver: makeResolver({ worker: stubAgent(() => "done") }),
      config: { clock },
    });

    clock.advance(4_999);
    expect(result.nodeOutputs.n1).toBe("done");
    expect(clock.pendingCount()).toBe(0);
  });

  it("keeps the original deadline when a new runner resumes the attempt", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const registry = new InMemoryDagRegistry();
    const dag = timedDag({ id: "resume-timeout", timeoutMs: 5_000 });
    await registry.register(dag);
    let starts = 0;
    const llm = reactiveLLM(
      (_messages, params) =>
        new Promise<LLMResponse>((_resolve, reject) => {
          starts++;
          params.signal?.addEventListener("abort", () => reject(params.signal?.reason), {
            once: true,
          });
        }),
    );
    const agents = await localAgents({
      llms: { worker: llm },
      storage,
    });
    const input = {
      dagId: dag.id,
      version: dag.version,
      initialInput: {},
      scope: SCOPE,
    };
    const boot = (durationMs: number) => {
      const runner = createWorkflowRunner({
        storage,
        lock: { durationMs, heartbeatIntervalMs: 500 },
      });
      const workflow = createDagWorkflow({
        registry,
        resolver: agents.boot().resolver,
        clock,
      });
      return { runner, workflow };
    };
    const firstBoot = boot(2_000);
    const first = firstBoot.runner
      .run({
        workflow: firstBoot.workflow,
        workflowId: "resume-timeout",
        input,
      })
      .catch(() => undefined);
    await waitUntil(() => starts === 1 && clock.pendingCount() === 1);

    clock.set(3_000);
    const secondBoot = boot(10_000);
    const recovered = secondBoot.runner.run({
      workflow: secondBoot.workflow,
      workflowId: "resume-timeout",
      input: undefined,
      useStoredInput: true,
    });
    await waitUntil(() => starts === 2 && clock.pendingCount() >= 2);
    clock.advance(1_999);
    expect(clock.currentTimeMs()).toBe(4_999);
    clock.advance(1);

    expect((await recovered).errors.n1).toBe('node_timeout: node "n1" ran past 5000ms');
    await first;
  });

  it("applies a separate timeout to every fan-out item", async () => {
    const clock = FakeWallClock.create(0);
    let starts = 0;
    const worker = customAgent(async (input, opts) => {
      const parsed = JSON.parse(input.task) as { item: number };
      starts++;
      if (parsed.item === 1) {
        await new Promise<void>((resolve) => {
          if (opts?.signal?.aborted) resolve();
          else
            opts?.signal?.addEventListener("abort", () => resolve(), {
              once: true,
            });
        });
        return {
          ...textOnly("stopped"),
          finishReason: Promise.resolve("cancelled" as const),
        };
      }
      return textOnly(`x${parsed.item}`);
    });
    const pending = runDag({
      dag: timedDag({
        id: "fan-timeout",
        timeoutMs: 1_000,
        forEach: {
          items: { kind: "initial", path: "items" },
          as: "item",
          maxItems: 3,
        },
      }),
      resolver: makeResolver({ worker }),
      initialInput: { items: [0, 1, 2] },
      config: { clock },
    });

    await waitUntil(() => starts === 3 && clock.pendingCount() === 1);
    clock.advance(1_000);
    const { result } = await pending;

    expect(result.nodeOutputs.n1).toEqual(["x0", null, "x2"]);
    expect(result.errors["n1.item-1"]).toBe('node_timeout: node "n1" ran past 1000ms');
  });

  it("aborts a hanging decision provider at its deadline", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const provider: DecisionProvider = {
      decide: async (request) => {
        calls++;
        await new Promise<void>((_resolve, reject) => {
          request.signal?.addEventListener("abort", () => reject(request.signal?.reason), {
            once: true,
          });
        });
        throw new Error("unreachable");
      },
    };
    const dag: AgenticDagRecipe = {
      id: "decision-timeout",
      version: "v1",
      nodes: [
        {
          id: "pick",
          kind: "decide",
          model: { provider: "test", id: "m" },
          questions: {
            answer: { type: "yesno", instructions: "Proceed?" },
          },
          inputs: {},
          timeoutMs: 1_000,
        },
      ],
      edges: [],
      entry: ["pick"],
      terminals: ["pick"],
    };
    const pending = runDag({
      dag,
      resolver: makeResolver({}),
      config: {
        clock,
        decisions: async () => ({ provider }),
      },
    });

    await waitUntil(() => calls === 1 && clock.pendingCount() === 1);
    clock.advance(1_000);
    const { result } = await pending;
    expect(result.errors.pick).toMatch(/^decision_failed:/);
  });

  it("does not report a timeout when a sibling aborts first", async () => {
    const clock = FakeWallClock.create(0);
    let waitingStarted = false;
    const waiting = cancellableAgent({
      onStart: () => (waitingStarted = true),
    });
    const failing = customAgent(async () => {
      await waitUntil(() => waitingStarted);
      throw new Error("sibling failed");
    });
    const dag: AgenticDagRecipe = {
      id: "abort-before-timeout",
      version: "v1",
      nodes: [
        {
          id: "wait",
          kind: "agent",
          agentId: "waiting",
          inputs: {},
          timeoutMs: 5_000,
        },
        { id: "fail", kind: "agent", agentId: "failing", inputs: {} },
      ],
      edges: [],
      entry: ["wait", "fail"],
      terminals: ["wait", "fail"],
    };
    const { result } = await runDag({
      dag,
      resolver: makeResolver({ waiting, failing }),
      config: { clock },
    });

    expect(result.errors.wait).toContain("cancelled");
    expect(result.errors.wait).not.toContain("node_timeout");
    expect(clock.pendingCount()).toBe(0);
  });
});
