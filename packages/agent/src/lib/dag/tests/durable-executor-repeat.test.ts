import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";

import { RESPOND_TOOL_NAME } from "../../agent-action.ts";
import { deferred, reactiveLLM, scriptedLLM, until } from "../../tests/engine-harness.ts";
import { validateDag } from "../validate.ts";
import { DagValidationError, type AgenticDagRecipe, type Repeat } from "../types.ts";
import {
  invokeAgent,
  localAgents,
  makeResolver,
  runDag,
  taskOf,
  textOnly,
} from "./dag-test-kit.ts";

const verdictSchema = {
  type: "object",
  properties: { verdict: { type: "string" } },
  required: ["verdict"],
};

const untilPass = {
  until: { kind: "equals" as const, path: "verdict", value: "pass" },
  maxIterations: 5,
};

function repeatDag(repeat: Repeat): AgenticDagRecipe {
  return {
    id: "repeat",
    version: "v1",
    nodes: [
      {
        id: "review",
        kind: "agent",
        agentId: "reviewer",
        inputs: { topic: { kind: "initial", path: "topic" } },
        output: { schema: verdictSchema },
        repeat,
      },
    ],
    edges: [],
    entry: ["review"],
    terminals: ["review"],
  };
}

function repeatInput(task: string): Record<string, unknown> {
  const marker = "\n\nReply with one JSON object and nothing else.";
  const markerIndex = task.indexOf(marker);
  const json = markerIndex === -1 ? task : task.slice(0, markerIndex);
  return JSON.parse(json) as Record<string, unknown>;
}

describe("createDagWorkflow — repeat", () => {
  it("stops when until holds", async () => {
    const verdicts = ["revise", "revise", "pass"];
    let calls = 0;
    const reviewer = invokeAgent(async () =>
      textOnly(JSON.stringify({ verdict: verdicts[calls++] })),
    );
    const { result, storage } = await runDag({
      dag: repeatDag(untilPass),
      resolver: makeResolver({ reviewer }),
      initialInput: { topic: "tea" },
      workflowId: "repeat-pass",
    });

    expect(result.nodeOutputs.review).toEqual({ verdict: "pass" });
    expect(result.nodeStats.review.iterations).toBe(3);
    for (let iteration = 0; iteration < 3; iteration++) {
      expect(
        await storage.loadWorkflow(`repeat-pass.node-review.iter-${iteration}`),
      ).not.toBeNull();
    }
  });

  it("passes the previous output and iteration number", async () => {
    const inputs: Array<Record<string, unknown>> = [];
    const verdicts = ["revise-0", "revise-1", "pass"];
    const reviewer = invokeAgent(async (input) => {
      inputs.push(repeatInput(input.task));
      return textOnly(JSON.stringify({ verdict: verdicts[inputs.length - 1] }));
    });
    await runDag({
      dag: repeatDag(untilPass),
      resolver: makeResolver({ reviewer }),
      initialInput: { topic: "tea" },
    });

    expect(inputs).toEqual([
      { topic: "tea", iteration: 0 },
      { topic: "tea", iteration: 1, previous: { verdict: "revise-0" } },
      { topic: "tea", iteration: 2, previous: { verdict: "revise-1" } },
    ]);
  });

  it("fails by default when maxIterations is exhausted", async () => {
    const reviewer = invokeAgent(async () => textOnly(JSON.stringify({ verdict: "revise" })));
    const { result } = await runDag({
      dag: repeatDag({ ...untilPass, maxIterations: 2 }),
      resolver: makeResolver({ reviewer }),
      initialInput: { topic: "tea" },
    });

    expect(result.errors.review).toMatch(/^repeat_exhausted:/);
  });

  it("continues with the last output when exhaustion is allowed", async () => {
    let calls = 0;
    const reviewer = invokeAgent(async () =>
      textOnly(JSON.stringify({ verdict: `revise-${calls++}` })),
    );
    const { result } = await runDag({
      dag: repeatDag({
        ...untilPass,
        maxIterations: 2,
        onExhausted: "continue",
      }),
      resolver: makeResolver({ reviewer }),
      initialInput: { topic: "tea" },
    });

    expect(result.nodeOutputs.review).toEqual({ verdict: "revise-1" });
    expect(result.ok).toBe(true);
  });

  it("treats the budget pool running dry as exhaustion", async () => {
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r0", name: RESPOND_TOOL_NAME, input: { verdict: "revise-0" } }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { verdict: "revise-1" } }],
      },
    ]);
    const agents = await localAgents({ llms: { reviewer: llm } });
    const { result, storage } = await runDag({
      dag: repeatDag({ ...untilPass, onExhausted: "continue" }),
      resolver: agents.boot().resolver,
      storage: agents.storage,
      initialInput: { topic: "tea" },
      workflowId: "repeat-budget",
      budget: { maxLlmCalls: 2 },
    });

    expect(result.nodeOutputs.review).toEqual({ verdict: "revise-1" });
    expect(result.nodeStats.review.iterations).toBe(2);
    expect(await storage.loadWorkflow("repeat-budget.node-review.iter-2")).toBeNull();
  });

  it("fails when an iteration fails", async () => {
    let calls = 0;
    const reviewer = invokeAgent(async () => {
      if (calls++ === 1) throw new Error("iteration failed");
      return textOnly(JSON.stringify({ verdict: "revise" }));
    });
    const { result } = await runDag({
      dag: repeatDag(untilPass),
      resolver: makeResolver({ reviewer }),
      initialInput: { topic: "tea" },
    });

    expect(result.errors.review).toContain("iteration failed");
  });

  it("refuses forEach and repeat on the same node", () => {
    const dag = repeatDag(untilPass);
    const node = dag.nodes[0];
    if (node?.kind !== "agent") throw new Error("fixture must contain an agent");
    const invalid: AgenticDagRecipe = {
      ...dag,
      nodes: [
        {
          ...node,
          forEach: {
            items: { kind: "initial", path: "items" },
            as: "item",
            maxItems: 4,
          },
        },
      ],
    };

    expect(() => validateDag({ dag: invalid })).toThrow(DagValidationError);
  });

  it("resumes mid loop without re-running finished iterations", async () => {
    const clock = FakeWallClock.create(1_000);
    const storage = new InMemoryWorkflowStorage({ clock });
    const stalled = deferred();
    const calls = [0, 0];
    const reviewerLlm = reactiveLLM(async (messages) => {
      const iteration = Number(repeatInput(taskOf(messages)).iteration);
      calls[iteration]!++;
      if (iteration === 1 && calls[iteration] === 1) await stalled.promise;
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [
          {
            id: `r${iteration}`,
            name: RESPOND_TOOL_NAME,
            input: { verdict: iteration === 0 ? "revise" : "pass" },
          },
        ],
      };
    });
    const agents = await localAgents({
      llms: { reviewer: reviewerLlm },
      storage,
    });
    const run = () =>
      runDag({
        dag: repeatDag(untilPass),
        resolver: agents.boot().resolver,
        storage,
        config: { clock },
        initialInput: { topic: "tea" },
        workflowId: "recover-repeat",
      });
    const first = run().catch(() => undefined);
    await until(() => calls[0] === 1 && calls[1] === 1);

    clock.set(clock.currentTimeMs() + 121_000);
    const recovered = await run();
    stalled.resolve();
    await first;

    expect(recovered.result.nodeOutputs.review).toEqual({ verdict: "pass" });
    expect(calls).toEqual([1, 2]);
  });
});
