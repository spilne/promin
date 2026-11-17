import { describe, expect, it } from "bun:test";
import type { Agent, AgentInput, AgentRunOutput } from "../../agent/types.ts";
import type { BudgetUsage } from "../../budget/budget.ts";
import { InMemoryDagRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { deferred } from "../../tests/engine-harness.ts";
import { MAX_CHILD_RUNS, MAX_DAG_DEPTH, type DagUsageRecorder } from "../durable-executor.ts";
import {
  DagValidationError,
  type AgentResolver,
  type AgenticDagRecipe,
  type SubDagNode,
} from "../types.ts";
import { invokeAgent, makeResolver, runDag, textOnly } from "./dag-test-kit.ts";

function agentOutput(params: {
  readonly text: string;
  readonly inputTokens?: number;
  readonly finishReason?: "stop" | "cancelled";
}): AgentRunOutput<unknown> {
  return {
    ...textOnly(params.text),
    usage: Promise.resolve({ inputTokens: params.inputTokens ?? 0, outputTokens: 0 }),
    finishReason: Promise.resolve(params.finishReason ?? "stop"),
  };
}

function customAgent(invoke: Agent["invoke"]): Agent {
  const agent: Agent = {
    invoke,
    stream: () => {
      throw new Error("stub: stream");
    },
    thread: async () => {
      throw new Error("stub: thread");
    },
    listThreads: async () => [],
    compactThread: async () => {
      throw new Error("stub: compactThread");
    },
    distillThread: async () => {
      throw new Error("stub: distillThread");
    },
    withScope: () => agent,
  };
  return agent;
}

function chainDag(id = "child"): AgenticDagRecipe {
  return {
    id,
    version: "v1",
    nodes: [
      {
        id: "first",
        kind: "agent",
        agentId: "echo",
        inputs: { topic: { kind: "initial", path: "topic" } },
      },
      {
        id: "t",
        kind: "agent",
        agentId: "echo",
        inputs: { text: { kind: "node", nodeId: "first", path: "" } },
      },
    ],
    edges: [{ from: "first", to: "t" }],
    entry: ["first"],
    terminals: ["t"],
  };
}

function parentDag(
  params: {
    readonly id?: string;
    readonly childId?: string;
    readonly node?: Partial<Omit<SubDagNode, "id" | "kind" | "dagId" | "version">>;
  } = {},
): AgenticDagRecipe {
  return {
    id: params.id ?? "parent",
    version: "v1",
    nodes: [
      {
        id: "sub",
        kind: "dag",
        dagId: params.childId ?? "child",
        version: "v1",
        inputs: { topic: { kind: "initial", path: "topic" } },
        ...params.node,
      },
    ],
    edges: [],
    entry: ["sub"],
    terminals: ["sub"],
  };
}

const echo = invokeAgent(async (input) => textOnly(input.task));

describe("createDagWorkflow — sub-DAG nodes", () => {
  it("uses a sub-DAG's terminal outputs as the node output", async () => {
    const { result, storage } = await runDag({
      dag: parentDag(),
      extraDags: [chainDag()],
      resolver: makeResolver({ echo }),
      initialInput: { topic: "tea" },
      workflowId: "wf",
    });

    expect(result.nodeOutputs.sub).toEqual({ t: "tea" });
    const child = await storage.loadWorkflow("wf.node-sub");
    expect(child?.parentWorkflowId).toBe("wf");
    expect(result.childRuns).toBe(3);
  });

  it("uses the node's resolved inputs as the child's initial input", async () => {
    const seen: string[] = [];
    const resolver = makeResolver({
      echo: invokeAgent(async (input) => {
        seen.push(input.task);
        return textOnly(input.task);
      }),
    });
    await runDag({
      dag: parentDag(),
      extraDags: [chainDag()],
      resolver,
      initialInput: { topic: "oolong" },
    });

    expect(seen[0]).toBe("oolong");
  });

  it("turns a failed sub-DAG into a node error", async () => {
    const { result } = await runDag({
      dag: parentDag(),
      extraDags: [chainDag()],
      resolver: makeResolver({
        echo: invokeAgent(async () => {
          throw new Error("child broke");
        }),
      }),
      initialInput: { topic: "tea" },
    });

    expect(result.ok).toBe(false);
    expect(result.errors.sub).toContain("child broke");
  });

  it("rejects a DAG that contains itself", async () => {
    const registry = new InMemoryDagRegistry();
    await expect(
      registry.register(parentDag({ id: "self", childId: "self" })),
    ).rejects.toBeInstanceOf(DagValidationError);
  });

  it("refuses an ancestor cycle at run time", async () => {
    const a = parentDag({ id: "a", childId: "b" });
    const b = parentDag({ id: "b", childId: "a" });
    const { result } = await runDag({
      dag: a,
      extraDags: [b],
      resolver: makeResolver({}),
    });

    expect(result.errors.sub).toMatch(/^sub_dag_failed:.*dag_cycle/s);
  });

  it("caps nesting depth at four", async () => {
    const dags = Array.from({ length: MAX_DAG_DEPTH + 2 }, (_, index) =>
      parentDag({ id: `d${index}`, childId: `d${index + 1}` }),
    );
    const terminal = chainDag(`d${MAX_DAG_DEPTH + 2}`);
    const { result } = await runDag({
      dag: dags[0]!,
      extraDags: [...dags.slice(1), terminal],
      resolver: makeResolver({ echo }),
    });

    expect(result.errors.sub).toMatch(/^sub_dag_failed:.*dag_too_deep/s);
  });

  it("reports unknown and disabled sub-DAGs", async () => {
    const missing = await runDag({
      dag: parentDag({ childId: "nope" }),
      resolver: makeResolver({}),
    });
    expect(missing.result.errors.sub).toMatch(/^dag_not_found:/);

    const disabled = { ...chainDag("off"), enabled: false };
    const off = await runDag({
      dag: parentDag({ id: "disabled-parent", childId: "off" }),
      extraDags: [disabled],
      resolver: makeResolver({ echo }),
    });
    expect(off.result.errors.sub).toMatch(/^dag_disabled:/);
  });

  it("propagates a parent abort into the child's running nodes", async () => {
    const childStarted = deferred();
    let laterStarts = 0;
    const waiting = customAgent(async (_input, opts) => {
      childStarted.resolve();
      await new Promise<void>((resolve) => {
        if (opts?.signal?.aborted) resolve();
        else opts?.signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      return agentOutput({ text: "stopped", finishReason: "cancelled" });
    });
    const child: AgenticDagRecipe = {
      id: "waiting-child",
      version: "v1",
      nodes: [
        { id: "wait", kind: "agent", agentId: "waiting", inputs: {} },
        {
          id: "later",
          kind: "agent",
          agentId: "later",
          inputs: { value: { kind: "node", nodeId: "wait", path: "" } },
        },
      ],
      edges: [{ from: "wait", to: "later" }],
      entry: ["wait"],
      terminals: ["later"],
    };
    const parent: AgenticDagRecipe = {
      id: "aborting-parent",
      version: "v1",
      nodes: [
        {
          id: "sub",
          kind: "dag",
          dagId: child.id,
          version: child.version,
          inputs: {},
        },
        { id: "fail", kind: "agent", agentId: "fail", inputs: {} },
      ],
      edges: [],
      entry: ["sub", "fail"],
      terminals: ["sub"],
    };
    const { result, storage } = await runDag({
      dag: parent,
      extraDags: [child],
      resolver: makeResolver({
        waiting,
        later: invokeAgent(async () => {
          laterStarts++;
          return textOnly("late");
        }),
        fail: invokeAgent(async () => {
          await childStarted.promise;
          throw new Error("stop parent");
        }),
      }),
      workflowId: "abort-parent",
    });

    expect(result.errors.fail).toContain("stop parent");
    expect(laterStarts).toBe(0);
    expect((await storage.loadWorkflow("abort-parent.node-sub.node-wait"))?.status).toBe(
      "completed",
    );
  });

  it("records parent and child spend once each", async () => {
    const records: Array<{ workflowId: string; usage: BudgetUsage }> = [];
    const usageRecorder: DagUsageRecorder = {
      record: async ({ workflowId, usage }) => {
        records.push({ workflowId, usage });
      },
    };
    const parent: AgenticDagRecipe = {
      id: "spend-parent",
      version: "v1",
      nodes: [
        { id: "own", kind: "agent", agentId: "spent", inputs: {} },
        {
          id: "sub",
          kind: "dag",
          dagId: "spend-child",
          version: "v1",
          inputs: { value: { kind: "node", nodeId: "own", path: "" } },
        },
      ],
      edges: [{ from: "own", to: "sub" }],
      entry: ["own"],
      terminals: ["sub"],
    };
    const child: AgenticDagRecipe = {
      id: "spend-child",
      version: "v1",
      nodes: [{ id: "work", kind: "agent", agentId: "spent", inputs: {} }],
      edges: [],
      entry: ["work"],
      terminals: ["work"],
    };
    const { result } = await runDag({
      dag: parent,
      extraDags: [child],
      resolver: makeResolver({
        spent: invokeAgent(async () => agentOutput({ text: "ok", inputTokens: 100 })),
      }),
      workflowId: "spend-root",
      config: { usageRecorder },
    });

    expect(result.spend.inputTokens).toBe(200);
    expect(
      records
        .map(({ workflowId, usage }) => [workflowId, usage.inputTokens] as const)
        .sort(([a], [b]) => a.localeCompare(b)),
    ).toEqual([
      ["spend-root", 100],
      ["spend-root.node-sub", 100],
    ]);
  });

  it("records a failed child's spend", async () => {
    const records: Array<{ workflowId: string; inputTokens: number }> = [];
    const child: AgenticDagRecipe = {
      id: "failed-spend-child",
      version: "v1",
      nodes: [
        {
          id: "bad",
          kind: "agent",
          agentId: "invalid",
          inputs: {},
          output: {
            schema: {
              type: "object",
              properties: { value: { type: "number" } },
              required: ["value"],
            },
          },
        },
      ],
      edges: [],
      entry: ["bad"],
      terminals: ["bad"],
    };
    const { result } = await runDag({
      dag: parentDag({ id: "failed-spend-parent", childId: child.id }),
      extraDags: [child],
      resolver: makeResolver({
        invalid: invokeAgent(async () => agentOutput({ text: "not json", inputTokens: 100 })),
      }),
      workflowId: "failed-spend-root",
      config: {
        usageRecorder: {
          record: async ({ workflowId, usage }) => {
            records.push({ workflowId, inputTokens: usage.inputTokens });
          },
        },
      },
    });

    expect(result.errors.sub).toContain("output_schema_mismatch");
    expect(records).toContainEqual({ workflowId: "failed-spend-root.node-sub", inputTokens: 100 });
  });

  it("enforces the child-run ceiling across nesting levels", async () => {
    const items = Array.from({ length: 100 }, (_, index) => index);
    const child: AgenticDagRecipe = {
      id: "ceiling-child",
      version: "v1",
      nodes: [
        {
          id: "work",
          kind: "agent",
          agentId: "echo",
          inputs: {},
          forEach: {
            items: { kind: "literal", value: items },
            as: "item",
            maxItems: 100,
            concurrency: 16,
            onItemError: "fail",
          },
        },
      ],
      edges: [],
      entry: ["work"],
      terminals: ["work"],
    };
    const parent = parentDag({
      id: "ceiling-parent",
      childId: child.id,
      node: {
        forEach: {
          items: { kind: "literal", value: items },
          as: "item",
          maxItems: 100,
          concurrency: 16,
        },
      },
    });
    const { result } = await runDag({
      dag: parent,
      extraDags: [child],
      resolver: makeResolver({ echo }),
      workflowId: "ceiling-root",
    });

    expect(result.childRuns).toBeLessThanOrEqual(MAX_CHILD_RUNS);
    expect(result.errors.sub).toMatch(/too_many_child_runs/);
  });

  it("supports forEach over a sub-DAG", async () => {
    const parent = parentDag({
      id: "foreach-parent",
      node: {
        inputs: {},
        forEach: {
          items: { kind: "literal", value: ["a", "b", "c"] },
          as: "topic",
          maxItems: 3,
        },
      },
    });
    const { result, storage } = await runDag({
      dag: parent,
      extraDags: [chainDag()],
      resolver: makeResolver({ echo }),
      workflowId: "foreach-sub",
    });

    expect(result.nodeOutputs.sub).toEqual([{ t: "a" }, { t: "b" }, { t: "c" }]);
    for (let index = 0; index < 3; index++) {
      expect(await storage.loadWorkflow(`foreach-sub.node-sub.item-${index}`)).not.toBeNull();
    }
  });

  it("supports repeat over a sub-DAG and passes the previous outputs", async () => {
    const writerInputs: Array<Record<string, unknown>> = [];
    let reviews = 0;
    const round: AgenticDagRecipe = {
      id: "round",
      version: "v1",
      nodes: [
        {
          id: "writer",
          kind: "agent",
          agentId: "writer",
          inputs: {
            topic: { kind: "initial", path: "topic" },
            previous: { kind: "initial", path: "previous" },
          },
        },
        {
          id: "review",
          kind: "agent",
          agentId: "reviewer",
          inputs: { draft: { kind: "node", nodeId: "writer", path: "" } },
          output: {
            schema: {
              type: "object",
              properties: { verdict: { type: "string" } },
              required: ["verdict"],
            },
          },
        },
      ],
      edges: [{ from: "writer", to: "review" }],
      entry: ["writer"],
      terminals: ["writer", "review"],
    };
    const parent = parentDag({
      id: "repeat-parent",
      childId: round.id,
      node: {
        repeat: {
          until: { kind: "equals", path: "review.verdict", value: "pass" },
          maxIterations: 3,
          onExhausted: "continue",
        },
      },
    });
    const resolver: AgentResolver = makeResolver({
      writer: invokeAgent(async (input: AgentInput) => {
        const parsed = JSON.parse(input.task) as Record<string, unknown>;
        writerInputs.push(parsed);
        return textOnly(`draft-${writerInputs.length}`);
      }),
      reviewer: invokeAgent(async () =>
        textOnly(JSON.stringify({ verdict: reviews++ === 0 ? "revise" : "pass" })),
      ),
    });
    const { result } = await runDag({
      dag: parent,
      extraDags: [round],
      resolver,
      initialInput: { topic: "tea" },
      workflowId: "repeat-sub",
    });

    expect(result.nodeStats.sub.iterations).toBe(2);
    expect(result.nodeOutputs.sub).toMatchObject({ review: { verdict: "pass" } });
    expect(writerInputs[1]?.previous).toMatchObject({ review: { verdict: "revise" } });
  });
});
