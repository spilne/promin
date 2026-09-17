import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { createAgentTool } from "../tools/agent-tool-factory.ts";
import { tool } from "../tool.ts";
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

describe("createAgentTool", () => {
  it("exposes correct name and description", () => {
    const t = createAgentTool({
      runner: makeRunner(),
      llm: mockLLM([{ content: "done", finishReason: "stop" }]),
      name: "myAgent",
      description: "Does specialist work",
    });
    expect(t.name).toBe("myAgent");
    expect(t.description).toBe("Does specialist work");
  });

  it("execute() delegates to sub-agent and returns its answer", async () => {
    const t = createAgentTool({
      runner: makeRunner(),
      llm: mockLLM([{ content: "42", finishReason: "stop" }]),
      name: "answerer",
    });
    const answer = await t.execute({ task: "What is 6*7?" });
    expect(answer).toBe("42");
  });

  it("parameters schema requires task:string", () => {
    const t = createAgentTool({
      runner: makeRunner(),
      llm: mockLLM([]),
      name: "schema-check",
    });
    const schema = t.parameters as z.ZodObject<{ task: z.ZodString }>;
    expect(schema.safeParse({ task: "hello" }).success).toBe(true);
    expect(schema.safeParse({}).success).toBe(false);
  });

  describe("auto-approve ref", () => {
    it("skips prompt when autoApproveRef.value is true", async () => {
      const approvalTool = tool({
        name: "guarded",
        description: "requires approval",
        parameters: z.object({ x: z.string() }),
        execute: async ({ x }) => `got:${x}`,
        requireApproval: true,
      });

      const askCalls: string[] = [];
      const autoApproveRef = { value: true };

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "test" } }],
          },
          { content: "done", finishReason: "stop" },
        ]),
        name: "autoApproveTest",
        tools: { guarded: approvalTool },
        ask: async (q) => {
          askCalls.push(q);
          return "y";
        },
        autoApproveRef,
      });

      await t.execute({ task: "do it" });
      expect(askCalls).toHaveLength(0);
    });

    it("prompts when autoApproveRef.value is false", async () => {
      const approvalTool = tool({
        name: "guarded",
        description: "requires approval",
        parameters: z.object({ x: z.string() }),
        execute: async ({ x }) => `got:${x}`,
        requireApproval: true,
      });

      const askCalls: string[] = [];
      const autoApproveRef = { value: false };

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "payload" } }],
          },
          { content: "done", finishReason: "stop" },
        ]),
        name: "promptTest",
        tools: { guarded: approvalTool },
        ask: async (q) => {
          askCalls.push(q);
          return "y";
        },
        autoApproveRef,
      });

      await t.execute({ task: "do it" });
      expect(askCalls).toHaveLength(1);
      expect(askCalls[0]).toContain("guarded");
      expect(askCalls[0]).toContain("[y/n/always]");
    });

    it("abbreviated param appears in approval prompt", async () => {
      const approvalTool = tool({
        name: "writer",
        description: "writes something",
        parameters: z.object({ content: z.string() }),
        execute: async () => "written",
        requireApproval: true,
      });

      const askCalls: string[] = [];
      const autoApproveRef = { value: false };

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "writer", input: { content: "short text" } }],
          },
          { content: "done", finishReason: "stop" },
        ]),
        name: "abbrevTest",
        tools: { writer: approvalTool },
        ask: async (q) => {
          askCalls.push(q);
          return "y";
        },
        autoApproveRef,
      });

      await t.execute({ task: "write" });
      expect(askCalls[0]).toContain("short text");
    });

    it("typing 'always' sets autoApproveRef.value to true", async () => {
      const approvalTool = tool({
        name: "guarded",
        description: "requires approval",
        parameters: z.object({ x: z.string() }),
        execute: async () => "ok",
        requireApproval: true,
      });

      const autoApproveRef = { value: false };
      let callCount = 0;

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "a" } }],
          },
          { content: "done", finishReason: "stop" },
        ]),
        name: "alwaysTest",
        tools: { guarded: approvalTool },
        ask: async () => {
          callCount++;
          return "always";
        },
        autoApproveRef,
      });

      await t.execute({ task: "go" });
      expect(autoApproveRef.value).toBe(true);
    });

    it("denying approval makes the tool return an error to the sub-agent", async () => {
      const sensitiveInfo: string[] = [];

      const approvalTool = tool({
        name: "dangerous",
        description: "dangerous op",
        parameters: z.object({ x: z.string() }),
        execute: async () => {
          sensitiveInfo.push("executed!");
          return "done";
        },
        requireApproval: true,
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "dangerous", input: { x: "payload" } }],
          },
          { content: "understood, skipping", finishReason: "stop" },
        ]),
        name: "denyTest",
        tools: { dangerous: approvalTool },
        ask: async () => "n",
      });

      const answer = await t.execute({ task: "do dangerous thing" });
      expect(sensitiveInfo).toHaveLength(0);
      expect(answer).toBe("understood, skipping");
    });
  });

  describe("onStep callback", () => {
    it("fires after each successful tool execution", async () => {
      const steps: Array<{ tool: string; failed: boolean }> = [];

      const echoTool = tool({
        name: "echo",
        description: "echoes",
        parameters: z.object({ msg: z.string() }),
        execute: async ({ msg }) => msg,
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "echo", input: { msg: "hello" } }],
          },
          { content: "done", finishReason: "stop" },
        ]),
        name: "stepTest",
        tools: { echo: echoTool },
        onStep: (s) => steps.push({ tool: s.tool, failed: s.failed }),
      });

      await t.execute({ task: "echo hello" });
      expect(steps).toHaveLength(1);
      expect(steps[0]).toEqual({ tool: "echo", failed: false });
    });

    it("reports failed:true when tool throws", async () => {
      const steps: Array<{ tool: string; failed: boolean }> = [];

      const faultyTool = tool({
        name: "faulty",
        description: "throws",
        parameters: z.object({ x: z.string() }),
        execute: async () => {
          throw new Error("boom");
        },
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "faulty", input: { x: "y" } }],
          },
          { content: "tool failed", finishReason: "stop" },
        ]),
        name: "failStepTest",
        tools: { faulty: faultyTool },
        onStep: (s) => steps.push({ tool: s.tool, failed: s.failed }),
      });

      await t.execute({ task: "run faulty" });
      expect(steps).toHaveLength(1);
      expect(steps[0]).toEqual({ tool: "faulty", failed: true });
    });

    it("includes durationMs in onStep callback", async () => {
      const steps: Array<{ durationMs: number }> = [];

      const slowTool = tool({
        name: "slow",
        description: "waits briefly",
        parameters: z.object({ x: z.string() }),
        execute: async () => {
          await new Promise((r) => setTimeout(r, 10));
          return "done";
        },
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "slow", input: { x: "go" } }],
          },
          { content: "ok", finishReason: "stop" },
        ]),
        name: "durationTest",
        tools: { slow: slowTool },
        onStep: (s) => steps.push({ durationMs: s.durationMs }),
      });

      await t.execute({ task: "go slow" });
      expect(steps[0]!.durationMs).toBeGreaterThanOrEqual(10);
    });

    it("includes abbreviated param in onStep callback", async () => {
      const steps: Array<{ param: string }> = [];

      const echoTool = tool({
        name: "echo",
        description: "echoes",
        parameters: z.object({ msg: z.string() }),
        execute: async ({ msg }) => msg,
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "echo", input: { msg: "short" } }],
          },
          { content: "ok", finishReason: "stop" },
        ]),
        name: "paramTest",
        tools: { echo: echoTool },
        onStep: (s) => steps.push({ param: s.param }),
      });

      await t.execute({ task: "echo" });
      expect(steps[0]!.param).toBe("short");
    });

    it("truncates long param to ~42 chars in onStep", async () => {
      const steps: Array<{ param: string }> = [];

      const echoTool = tool({
        name: "echo",
        description: "echoes",
        parameters: z.object({ msg: z.string() }),
        execute: async ({ msg }) => msg,
      });

      const longMsg = "a".repeat(100);

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "echo", input: { msg: longMsg } }],
          },
          { content: "ok", finishReason: "stop" },
        ]),
        name: "truncTest",
        tools: { echo: echoTool },
        onStep: (s) => steps.push({ param: s.param }),
      });

      await t.execute({ task: "echo long" });
      expect(steps[0]!.param.length).toBeLessThanOrEqual(45);
      expect(steps[0]!.param).toEndWith("…");
    });
  });

  describe("timeout", () => {
    it("rejects when sub-agent exceeds timeoutMs", async () => {
      const hangingTool = tool({
        name: "hang",
        description: "hangs forever",
        parameters: z.object({ x: z.string() }),
        execute: async () => new Promise<string>(() => {}),
      });

      const t = createAgentTool({
        runner: makeRunner(),
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "hang", input: { x: "y" } }],
          },
        ]),
        name: "timeoutTest",
        tools: { hang: hangingTool },
        timeoutMs: 100,
      });

      await expect(t.execute({ task: "hang" })).rejects.toThrow(/timed out/);
    });
  });
});
