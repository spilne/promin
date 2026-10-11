// ---------------------------------------------------------------------------
// createDagWorkflow — nodes as child workflows:
//
//   - every node runs its agent's turn as a child run `<dag run>.node-<id>`
//     linked to the DAG run;
//   - nodes whose upstream nodes have settled run in parallel;
//   - after a restart, driving the DAG run again re-attaches to the node
//     that was running and resumes it from its journal; finished nodes are
//     not run again. (Zorya's tests cover recovery finding the run by the
//     DAG workflow's registered name.)
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { Agent, AgentInput, AgentRunOutput } from "../../agent/types.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import { deferred, lastMessage, reactiveLLM, until } from "../../tests/engine-harness.ts";
import { z } from "zod";
import { LocalAgent } from "../../agent/local-agent.ts";
import type { AgentTool } from "../../tool.ts";
import {
  DAG_APPROVAL_REFUSED,
  DAG_WORKFLOW_NAME,
  createDagWorkflow,
  dagNodeRunId,
  type DurableDagInput,
} from "../durable-executor.ts";
import { createRegistryAgentResolver } from "../registry-agent-resolver.ts";
import type { AgentResolver, AgenticDagRecipe, DagRunScope } from "../types.ts";
import { InMemoryDagRegistry } from "../registry.ts";

/** DAGs run by reference: register `dag` and name it in the run input. */
const dags = new InMemoryDagRegistry();
async function runInput(params: {
  dag: AgenticDagRecipe;
  initialInput: Record<string, unknown>;
  scope: DagRunScope;
}): Promise<DurableDagInput> {
  const { dag } = params;
  await dags.register({
    id: dag.id,
    version: dag.version,
    nodes: dag.nodes,
    edges: dag.edges,
    entry: dag.entry,
    terminals: dag.terminals,
  });
  return {
    dagId: dag.id,
    version: dag.version,
    initialInput: params.initialInput,
    scope: params.scope,
  };
}

const SCOPE = { namespaceId: "acme", resourceId: "r-1" } as const;

/** Lock lease of `InMemoryWorkflowStorage` runs, plus a margin. */
const PAST_LOCK_LEASE_MS = 121_000;

function textOnly(text: string): AgentRunOutput {
  return {
    text: Promise.resolve(text),
    finishReason: Promise.resolve("stop" as const),
    messages: Promise.resolve([]),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    steps: Promise.resolve([]),
  } as unknown as AgentRunOutput;
}

function invokeAgent(invoke: (input: AgentInput) => Promise<AgentRunOutput>): Agent {
  const agent = {
    invoke,
    withScope: () => agent,
  } as unknown as Agent;
  return agent;
}

/** The user message the LLM was asked to answer. */
function taskOf(messages: Parameters<Parameters<typeof reactiveLLM>[0]>[0]): string {
  const last = lastMessage(messages);
  return typeof last?.content === "string" ? last.content : "";
}

const linear: AgenticDagRecipe = {
  id: "linear",
  version: "v1",
  nodes: [
    { id: "n1", agentId: "upper", inputs: { task: { kind: "initial", path: "task" } } },
    { id: "n2", agentId: "exclaim", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
    { id: "n3", agentId: "wrap", inputs: { task: { kind: "node", nodeId: "n2", path: "" } } },
  ],
  edges: [
    { from: "n1", to: "n2" },
    { from: "n2", to: "n3" },
  ],
  entry: ["n1"],
  terminals: ["n3"],
};

describe("createDagWorkflow — nodes as child runs", () => {
  it("runs nodes whose upstream nodes settled in parallel, each as a linked child run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    // a and b both depend on n1 only: they must be running at the same time.
    const diamond: AgenticDagRecipe = {
      id: "diamond",
      version: "v1",
      nodes: [
        { id: "n1", agentId: "echo", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "a", agentId: "side", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
        { id: "b", agentId: "side", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
        {
          id: "c",
          agentId: "echo",
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
    let sideStarted = 0;
    const bothStarted = deferred();
    const resolver: AgentResolver = async ({ agentId }) =>
      agentId === "side"
        ? invokeAgent(async (input) => {
            sideStarted++;
            if (sideStarted === 2) bothStarted.resolve();
            await bothStarted.promise;
            return textOnly(`side(${input.task})`);
          })
        : invokeAgent(async (input) => textOnly(input.task));

    const result = await runner.run({
      workflow: createDagWorkflow({ registry: dags, resolver }),
      workflowId: "dag-par",
      input: await runInput({ dag: diamond, initialInput: { task: "go" }, scope: SCOPE }),
    });

    expect(result.ok).toBe(true);
    expect(sideStarted).toBe(2);
    expect(JSON.parse(result.outputs.c as string)).toEqual({ left: "side(go)", right: "side(go)" });
    const children = await storage.listWorkflows({ parentId: "dag-par" });
    expect(children.map((c) => c.workflowId).sort()).toEqual(
      ["n1", "a", "b", "c"].map((nodeId) => dagNodeRunId({ dagRunId: "dag-par", nodeId })).sort(),
    );
  });

  it("recovers a DAG run after a restart: resumes the running node, never re-runs finished ones", async () => {
    const clock = FakeWallClock.create(Date.now());
    const storage = new InMemoryWorkflowStorage({ clock });
    const agents = new InMemoryAgentRegistry();
    for (const id of ["upper", "exclaim", "wrap"]) {
      await agents.register({
        id,
        backend: {
          type: "local",
          model: { provider: "test", id: "m" },
          role: { inline: { systemPrompt: null, tools: [] } },
        },
      });
    }
    const gate = deferred();
    const calls: Record<string, number> = { upper: 0, exclaim: 0, wrap: 0 };
    const answer = (content: string): LLMResponse => ({ content, finishReason: "stop" });
    const llms: Record<string, LLMProvider> = {
      upper: reactiveLLM((m) => {
        calls.upper!++;
        return answer(taskOf(m).toUpperCase());
      }),
      exclaim: reactiveLLM(async (m) => {
        calls.exclaim!++;
        // The first worker stalls inside n2's LLM call and loses its lease.
        if (calls.exclaim === 1) await gate.promise;
        return answer(`${taskOf(m)}!`);
      }),
      wrap: reactiveLLM((m) => {
        calls.wrap!++;
        return answer(`<<${taskOf(m)}>>`);
      }),
    };
    // The host's boot: one DAG workflow, registered under its fixed name.
    const boot = () => {
      const runner = createWorkflowRunner({ storage });
      const resolver = createRegistryAgentResolver({
        registry: agents,
        resolve: (recipe) =>
          resolveLocalAgent(recipe, { runner, llm: () => llms[recipe.id]!, tools: {} }),
      });
      return { runner, dag: createDagWorkflow({ registry: dags, resolver }) };
    };

    const first = boot();
    const crashed = first.runner
      .run({
        workflow: first.dag,
        workflowId: "dag-r",
        input: await runInput({ dag: linear, initialInput: { task: "hi" }, scope: SCOPE }),
      })
      .catch(() => undefined);
    await until(() => calls.exclaim === 1);

    // Restart: a new process drives the orphaned run with its stored input.
    clock.set(clock.currentTimeMs() + PAST_LOCK_LEASE_MS);
    const second = boot();
    await second.runner.run({
      workflow: second.dag,
      workflowId: "dag-r",
      input: undefined,
      useStoredInput: true,
    });
    gate.resolve();
    await crashed;

    const run = await storage.loadWorkflow("dag-r");
    expect(run?.workflowName).toBe(DAG_WORKFLOW_NAME);
    expect(run?.status).toBe("completed");
    expect((run?.result as { outputs?: Record<string, unknown> } | undefined)?.outputs).toEqual({
      n3: "<<HI!>>",
    });
    // n1 ran once; n2's interrupted LLM call ran again; n3 ran once.
    expect(calls).toEqual({ upper: 1, exclaim: 2, wrap: 1 });
    const children = await storage.listWorkflows({ parentId: "dag-r" });
    expect(children.map((c) => c.workflowId).sort()).toEqual([
      "dag-r.node-n1",
      "dag-r.node-n2",
      "dag-r.node-n3",
    ]);
    expect(children.every((c) => c.status === "completed")).toBe(true);
  });
});

describe("createDagWorkflow — scheduling and abort", () => {
  it("starts a node as soon as its own upstream nodes finish, not when the wave does", async () => {
    // a → c, and b on its own: c must start while b is still running.
    const dag: AgenticDagRecipe = {
      id: "ready",
      version: "v1",
      nodes: [
        { id: "a", agentId: "echo", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "b", agentId: "slow", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "c", agentId: "echo", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
      ],
      edges: [{ from: "a", to: "c" }],
      entry: ["a", "b"],
      terminals: ["b", "c"],
    };
    const cStarted = deferred();
    const resolver: AgentResolver = async ({ agentId }) =>
      agentId === "slow"
        ? invokeAgent(async () => {
            await cStarted.promise;
            return textOnly("slow done");
          })
        : invokeAgent(async (input) => {
            if (input.task === "go!") cStarted.resolve();
            return textOnly(`${input.task}!`);
          });

    const result = await createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
      workflow: createDagWorkflow({ registry: dags, resolver }),
      workflowId: "dag-ready",
      input: await runInput({ dag, initialInput: { task: "go" }, scope: SCOPE }),
    });

    expect(result.ok).toBe(true);
    expect(result.outputs).toEqual({ b: "slow done", c: "go!!" });
  });

  it("an aborting node stops its running siblings and starts nothing new", async () => {
    // a fails (abort) while b runs; c depends on b, d on a.
    const dag: AgenticDagRecipe = {
      id: "abort",
      version: "v1",
      nodes: [
        { id: "a", agentId: "fails", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "b", agentId: "waits", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "c", agentId: "echo", inputs: { task: { kind: "node", nodeId: "b", path: "" } } },
        { id: "d", agentId: "echo", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
      ],
      edges: [
        { from: "b", to: "c" },
        { from: "a", to: "d" },
      ],
      entry: ["a", "b"],
      terminals: ["c", "d"],
    };
    const bStarted = deferred();
    let echoes = 0;
    const resolver: AgentResolver = async ({ agentId }) => {
      if (agentId === "fails") {
        return invokeAgent(async () => {
          await bStarted.promise;
          throw new Error("a broke");
        });
      }
      if (agentId === "waits") {
        const agent = {
          invoke: async (_input: AgentInput, opts?: { signal?: AbortSignal }) => {
            bStarted.resolve();
            await new Promise<void>((resolve) =>
              opts?.signal?.addEventListener("abort", () => resolve(), { once: true }),
            );
            return {
              ...textOnly("stopped"),
              finishReason: Promise.resolve("cancelled" as const),
            } as unknown as AgentRunOutput;
          },
          withScope: () => agent,
        } as unknown as Agent;
        return agent;
      }
      return invokeAgent(async (input) => {
        echoes++;
        return textOnly(input.task);
      });
    };
    const storage = new InMemoryWorkflowStorage();

    const result = await createWorkflowRunner({ storage }).run({
      workflow: createDagWorkflow({ registry: dags, resolver }),
      workflowId: "dag-abort",
      input: await runInput({ dag, initialInput: { task: "go" }, scope: SCOPE }),
    });

    expect(result.ok).toBe(false);
    expect(result.errors["a"]).toContain("a broke");
    expect(result.errors["b"]).toContain("cancelled");
    expect(result.nodeOutputs).toEqual({});
    expect(echoes).toBe(0);
    const children = (await storage.listWorkflows({ parentId: "dag-abort" })).map(
      (c) => c.workflowId,
    );
    expect(children.sort()).toEqual(["dag-abort.node-a", "dag-abort.node-b"]);
  });

  it("fails the DAG step, not the node, when the resolver hits an infrastructure error", async () => {
    const dag: AgenticDagRecipe = {
      id: "infra",
      version: "v1",
      nodes: [{ id: "n1", agentId: "x", inputs: { task: { kind: "initial", path: "task" } } }],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    };
    const resolver: AgentResolver = async () => {
      throw new Error("registry unreachable");
    };
    await expect(
      createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
        workflow: createDagWorkflow({ registry: dags, resolver }),
        workflowId: "dag-infra",
        input: await runInput({ dag, initialInput: { task: "go" }, scope: SCOPE }),
      }),
    ).rejects.toThrow("registry unreachable");
  });

  it("runs only registered DAGs", async () => {
    await expect(
      createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }).run({
        workflow: createDagWorkflow({
          registry: dags,
          resolver: async () => invokeAgent(async () => textOnly("x")),
        }),
        workflowId: "dag-unknown",
        input: { dagId: "nope", version: "v1", initialInput: {}, scope: SCOPE },
      }),
    ).rejects.toThrow("dag_not_found");
  });
});

describe("createDagWorkflow — approvals", () => {
  it("refuses a node's approval-gated tool call instead of suspending the DAG", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let ran = 0;
    const guarded: AgentTool<Record<string, never>, string> = {
      name: "guarded",
      description: "needs approval",
      parameters: z.object({}),
      requireApproval: true,
      execute: async () => {
        ran++;
        return "ran";
      },
    };
    const llm = reactiveLLM((m) =>
      lastMessage(m)?.role === "tool"
        ? { content: `done: ${String(lastMessage(m)?.content)}`, finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "g1", name: "guarded", input: {} }],
          },
    );
    const resolver: AgentResolver = async ({ scope }) =>
      new LocalAgent({
        ...(scope.namespaceId !== undefined && { namespaceId: scope.namespaceId }),
        runner,
        agent: { name: "worker", llm, tools: { guarded } },
      });
    const dag: AgenticDagRecipe = {
      id: "gated",
      version: "v1",
      nodes: [{ id: "n1", agentId: "worker", inputs: { task: { kind: "initial", path: "task" } } }],
      edges: [],
      entry: ["n1"],
      terminals: ["n1"],
    };

    const result = await runner.run({
      workflow: createDagWorkflow({ registry: dags, resolver }),
      workflowId: "dag-gated",
      input: await runInput({ dag, initialInput: { task: "x" }, scope: SCOPE }),
    });

    expect(result.ok).toBe(true);
    expect(result.outputs["n1"]).toBe(`done: Rejected: ${DAG_APPROVAL_REFUSED}`);
    expect(ran).toBe(0);
    expect(await storage.listWorkflows({ status: "suspended", limit: 10 })).toEqual([]);
  });
});
