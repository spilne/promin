import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction, StructuredOutputParseError } from "../agent-action.ts";
import type { LLMResponse } from "../llm-provider.ts";

async function runAction(
  workflow: ReturnType<typeof agentAction>,
  task: string,
  workflowId = "so-wf-1",
) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return runner.run({ workflow, workflowId, input: { task } });
}

function mockLLM(responses: LLMResponse[]) {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

describe("agentAction structured output", () => {
  it("returns parsed output when LLM calls _respond", async () => {
    const ContactSchema = z.object({
      name: z.string(),
      email: z.string(),
    });

    const action = agentAction({
      name: "extract-contact",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [
            { id: "tc-1", name: "_respond", input: { name: "Alice", email: "alice@example.com" } },
          ],
        },
      ]),
      outputSchema: ContactSchema,
    });

    const result = await runAction(action, "Extract contact from: Alice <alice@example.com>");

    expect(result.output).toEqual({ name: "Alice", email: "alice@example.com" });
    expect(result.answer).toContain("Alice");
  });

  it("infers TypeScript type from outputSchema", async () => {
    const Schema = z.object({ count: z.number(), label: z.string() });

    const action = agentAction({
      name: "typed-test",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "_respond", input: { count: 42, label: "hello" } }],
        },
      ]),
      outputSchema: Schema,
    });

    const result = await runAction(action, "give me count and label");
    // TypeScript should infer result.output as { count: number; label: string }
    const output = result.output;
    expect(output.count).toBe(42);
    expect(output.label).toBe("hello");
  });

  it("throws StructuredOutputParseError when LLM returns invalid data", async () => {
    const StrictSchema = z.object({ required: z.string() });

    const action = agentAction({
      name: "parse-error-test",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          // 'required' is missing — should fail validation
          toolCalls: [{ id: "tc-1", name: "_respond", input: { wrong: "field" } }],
        },
      ]),
      outputSchema: StrictSchema,
    });

    await expect(runAction(action, "task")).rejects.toThrow(
      "structured output failed schema validation",
    );
  });

  it("injects _respond tool in llm.chat calls", async () => {
    let capturedTools: unknown[] = [];

    const action = agentAction({
      name: "tool-injection-test",
      llm: {
        chat: async (params) => {
          capturedTools = params.tools ?? [];
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "_respond", input: { value: "x" } }],
          };
        },
      },
      outputSchema: z.object({ value: z.string() }),
    });

    await runAction(action, "task");

    const respondDef = (capturedTools as any[]).find((t) => t.name === "_respond");
    expect(respondDef).toBeDefined();
    expect(respondDef.parameters.type).toBe("object");
    expect(respondDef.parameters.properties.value).toBeDefined();
  });

  it("injects structured output system message", async () => {
    let capturedMessages: unknown[] = [];

    const action = agentAction({
      name: "system-msg-test",
      llm: {
        chat: async (params) => {
          capturedMessages = params.messages;
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "_respond", input: { ok: true } }],
          };
        },
      },
      outputSchema: z.object({ ok: z.boolean() }),
    });

    await runAction(action, "task");

    const hasInstruction = capturedMessages.some(
      (m: any) => m.role === "system" && m.content?.includes("_respond"),
    );
    expect(hasInstruction).toBe(true);
  });

  it("without outputSchema, result.output is undefined", async () => {
    const action = agentAction({
      name: "no-schema-test",
      llm: mockLLM([{ content: "plain text answer", finishReason: "stop" }]),
    });

    const result = await runAction(action, "task");
    expect(result.output).toBeUndefined();
    expect(result.answer).toBe("plain text answer");
  });

  it("can still call other tools before _respond", async () => {
    const action = agentAction({
      name: "multi-step-structured",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "lookup", input: { key: "foo" } }],
        },
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-2", name: "_respond", input: { result: "bar" } }],
        },
      ]),
      tools: {
        lookup: {
          name: "lookup",
          description: "look up a key",
          parameters: z.object({ key: z.string() }),
          execute: async ({ key }) => `value-of-${key}`,
        },
      },
      outputSchema: z.object({ result: z.string() }),
    });

    const res = await runAction(action, "look up foo then respond");
    expect(res.output).toEqual({ result: "bar" });
  });
});
