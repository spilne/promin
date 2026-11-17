import { describe, expect, it } from "bun:test";
import type { Workflow } from "@promin/workflow";
import { z } from "zod";

import {
  agentAction,
  OUTPUT_INVALID_ANSWER,
  RESPOND_TOOL_NAME,
  type AgentInput,
  type AgentResult,
} from "../agent-action.ts";
import type { LLMResponse } from "../llm-provider.ts";
import { freshRunner, scriptedLLM } from "./engine-harness.ts";

const schema = {
  type: "object",
  properties: { verdict: { type: "string" } },
  required: ["verdict"],
};

const respond = (input: unknown): LLMResponse => ({
  content: null,
  finishReason: "tool_calls",
  toolCalls: [{ id: "c1", name: RESPOND_TOOL_NAME, input }],
});

async function run(
  workflow: Workflow<AgentInput, AgentResult>,
  input: AgentInput,
): Promise<AgentResult> {
  return freshRunner().runner.run({ workflow, workflowId: "turn-schema", input });
}

describe("agentAction per-turn output schema", () => {
  it("offers _respond with the turn's schema and returns the validated value", async () => {
    const llm = scriptedLLM([respond({ verdict: "pass" })]);
    const result = await run(agentAction({ name: "a", llm }), {
      task: "t",
      outputJsonSchema: schema,
    });

    expect(result.finishReason).toBe("stop");
    expect(result.output).toEqual({ verdict: "pass" });
    expect(
      llm.calls[0]!.tools?.find((tool) => tool.name === RESPOND_TOOL_NAME)?.parameters,
    ).toEqual(schema);
  });

  it("returns output_invalid with its spend when repairs run out", async () => {
    const llm = scriptedLLM([respond({}), respond({}), respond({})]);
    const result = await run(agentAction({ name: "a", llm }), {
      task: "t",
      outputJsonSchema: schema,
    });

    expect(result.answer).toBe(OUTPUT_INVALID_ANSWER);
    expect(result.finishReason).toBe("output_invalid");
    expect(result.output).toBeUndefined();
    expect(result.spend?.llmCalls).toBe(3);
  });

  it("the turn's schema replaces the configured one", async () => {
    const llm = scriptedLLM([respond({ verdict: "pass" })]);
    const result = await run(
      agentAction({ name: "a", llm, outputSchema: z.object({ n: z.number() }) }),
      { task: "t", outputJsonSchema: schema },
    );

    expect(result.finishReason).toBe("stop");
    expect(result.output).toEqual({ verdict: "pass" });
  });

  it("a configured schema still throws when repairs run out", async () => {
    const llm = scriptedLLM([respond({}), respond({}), respond({})]);
    await expect(
      run(agentAction({ name: "a", llm, outputSchema: z.object({ n: z.number() }) }), {
        task: "t",
      }),
    ).rejects.toThrow("structured output failed schema validation");
  });
});
