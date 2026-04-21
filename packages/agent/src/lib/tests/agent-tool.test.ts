import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { agentTool } from "../agent-tool.ts";
import type { LLMResponse } from "../llm-provider.ts";

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

function makeRunner() {
  const storage = new InMemoryWorkflowStorage();
  return createWorkflowRunner({ storage });
}

describe("agentTool", () => {
  it("exposes the correct name and description", () => {
    const agent = agentAction({
      name: "sub-agent",
      llm: mockLLM([{ content: "done", finishReason: "stop" }]),
    });

    const t = agentTool({
      agent,
      runner: makeRunner(),
      name: "my-specialist",
      description: "Does specialist work",
    });

    expect(t.name).toBe("my-specialist");
    expect(t.description).toBe("Does specialist work");
  });

  it("execute() runs the subagent and returns its answer", async () => {
    const agent = agentAction({
      name: "answer-agent",
      llm: mockLLM([{ content: "42", finishReason: "stop" }]),
    });

    const t = agentTool({
      agent,
      runner: makeRunner(),
      name: "answerer",
      description: "Answers questions",
    });

    const answer = await t.execute({ task: "What is 6 * 7?" });
    expect(answer).toBe("42");
  });

  it("each execute() call gets a unique workflowId", async () => {
    const seenIds: string[] = [];

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const agent = agentAction({
      name: "id-tracking-agent",
      llm: {
        chat: async (params) => {
          return { content: "ok", finishReason: "stop" as const };
        },
      },
    });

    const originalRun = runner.run.bind(runner);
    const patchedRunner = {
      ...runner,
      run: async (params: Parameters<typeof runner.run>[0]) => {
        if ("workflowId" in params) {
          seenIds.push(params.workflowId);
        }
        return originalRun(params);
      },
    };

    const t = agentTool({
      agent,
      runner: patchedRunner as typeof runner,
      name: "unique-id-tool",
      description: "Tracks workflow IDs",
    });

    await t.execute({ task: "first call" });
    await t.execute({ task: "second call" });

    expect(seenIds).toHaveLength(2);
    expect(seenIds[0]).not.toBe(seenIds[1]);
  });

  it("workflowIdPrefix customizes the workflow ID prefix", async () => {
    const seenIds: string[] = [];

    const storage = new InMemoryWorkflowStorage();
    const baseRunner = createWorkflowRunner({ storage });

    const agent = agentAction({
      name: "prefix-agent",
      llm: mockLLM([
        { content: "a", finishReason: "stop" },
        { content: "b", finishReason: "stop" },
      ]),
    });

    const originalRun = baseRunner.run.bind(baseRunner);
    const patchedRunner = {
      ...baseRunner,
      run: async (params: Parameters<typeof baseRunner.run>[0]) => {
        if ("workflowId" in params) {
          seenIds.push(params.workflowId);
        }
        return originalRun(params);
      },
    };

    const t = agentTool({
      agent,
      runner: patchedRunner as typeof baseRunner,
      name: "my-tool",
      description: "uses prefix",
      workflowIdPrefix: "custom-prefix",
    });

    await t.execute({ task: "task 1" });
    await t.execute({ task: "task 2" });

    expect(seenIds[0]).toMatch(/^custom-prefix-/);
    expect(seenIds[1]).toMatch(/^custom-prefix-/);
    expect(seenIds[0]).not.toBe(seenIds[1]);
  });

  it("parameters schema is { task: string }", () => {
    const agent = agentAction({
      name: "schema-check-agent",
      llm: mockLLM([{ content: "done", finishReason: "stop" }]),
    });

    const t = agentTool({
      agent,
      runner: makeRunner(),
      name: "schema-tool",
      description: "check schema",
    });

    const schema = t.parameters as z.ZodObject<{ task: z.ZodString }>;
    const valid = schema.safeParse({ task: "hello" });
    expect(valid.success).toBe(true);

    const missing = schema.safeParse({});
    expect(missing.success).toBe(false);

    const wrongType = schema.safeParse({ task: 123 });
    expect(wrongType.success).toBe(false);
  });
});
