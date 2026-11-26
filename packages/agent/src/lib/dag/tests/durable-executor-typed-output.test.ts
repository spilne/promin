import { describe, expect, it } from "bun:test";

import { RESPOND_TOOL_NAME } from "../../agent-action.ts";
import { LocalAgent } from "../../agent/local-agent.ts";
import { freshRunner, scriptedLLM } from "../../tests/engine-harness.ts";
import type { AgenticDagRecipe } from "../types.ts";
import {
  invokeAgent,
  localAgents,
  makeResolver,
  runDag,
  stubAgent,
  textOnly,
} from "./dag-test-kit.ts";

const outputSchema = {
  type: "object",
  properties: {
    questions: { type: "array", items: { type: "string" } },
  },
  required: ["questions"],
};

function typedDag(withConsumer = false): AgenticDagRecipe {
  return {
    id: withConsumer ? "typed-with-consumer" : "typed",
    version: "v1",
    nodes: [
      {
        id: "plan",
        kind: "agent",
        agentId: "planner",
        inputs: { topic: { kind: "initial", path: "topic" } },
        output: { schema: outputSchema },
      },
      ...(withConsumer
        ? [
            {
              id: "use",
              kind: "agent" as const,
              agentId: "consumer",
              inputs: { question: { kind: "node" as const, nodeId: "plan", path: "questions.1" } },
            },
          ]
        : []),
    ],
    edges: withConsumer ? [{ from: "plan", to: "use" }] : [],
    entry: ["plan"],
    terminals: [withConsumer ? "use" : "plan"],
  };
}

describe("createDagWorkflow — typed agent output", () => {
  it("a turn agent's node output is the validated object", async () => {
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [
          {
            id: "respond",
            name: RESPOND_TOOL_NAME,
            input: { questions: ["a", "b"] },
          },
        ],
      },
    ]);
    const agents = await localAgents({ llms: { planner: llm } });
    const local = agents.boot().resolver;
    let downstreamTask = "";
    const consumer = stubAgent((task) => {
      downstreamTask = task;
      return task;
    });
    const { result } = await runDag({
      dag: typedDag(true),
      storage: agents.storage,
      initialInput: { topic: "x" },
      resolver: async (params) =>
        params.agentId === "consumer" ? consumer.withScope(params.scope) : local(params),
    });

    expect(result.nodeOutputs.plan).toEqual({ questions: ["a", "b"] });
    expect(downstreamTask).toBe("b");
  });

  it("an invoke agent is told the schema and its JSON answer is validated", async () => {
    let task = "";
    const agent = invokeAgent(async (input) => {
      task = input.task;
      return textOnly('```json\n{"questions":["a"]}\n```');
    });
    const { result } = await runDag({
      dag: typedDag(),
      resolver: makeResolver({ planner: agent }),
      initialInput: { topic: "x" },
    });

    expect(result.nodeOutputs.plan).toEqual({ questions: ["a"] });
    expect(task).toContain("It must match this JSON Schema:");
  });

  it("an answer that does not match is a node error with the turn's spend counted", async () => {
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: {} }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r2", name: RESPOND_TOOL_NAME, input: {} }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r3", name: RESPOND_TOOL_NAME, input: {} }],
      },
    ]);
    const agents = await localAgents({ llms: { planner: llm } });
    const { result } = await runDag({
      dag: typedDag(),
      storage: agents.storage,
      resolver: agents.boot().resolver,
      initialInput: { topic: "x" },
    });

    expect(result.errors.plan).toMatch(/^output_schema_mismatch/);
    expect(result.spend.llmCalls).toBe(3);
  });

  it("a step-limit answer is never passed downstream as the typed value", async () => {
    const llm = scriptedLLM([{ content: "not structured", finishReason: "stop" }]);
    const planner = new LocalAgent({
      agent: { name: "planner", llm, maxSteps: 1 },
      runner: freshRunner().runner,
    });
    const { result } = await runDag({
      dag: typedDag(),
      resolver: makeResolver({ planner }),
      initialInput: { topic: "x" },
    });

    expect(result.errors.plan).toBeDefined();
    expect(result.nodeOutputs.plan).toBeUndefined();
  });
});
