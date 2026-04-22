import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { tool } from "../tool.ts";
import type { AgentSession } from "../agent-loop.ts";
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

async function makeSession(config: Parameters<typeof agentLoop>[0], sessionId = "api-s1") {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return agentLoop(config).session({ runner, sessionId });
}

/** Poll session.status() until it matches target or timeout. */
async function waitForStatus(session: AgentSession, target: string, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const st = await session.status();
    if (st === target) return st;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`Timed out waiting for status "${target}"`);
}

const needsApprovalTool = tool({
  name: "guarded",
  description: "Requires approval before running",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => `ran: ${x}`,
  requireApproval: true,
});

// ---- messages() ----

describe("messages()", () => {
  it("returns empty array before any turn", async () => {
    const session = await makeSession({
      name: "msg-empty",
      llm: mockLLM([{ content: "hi", finishReason: "stop" }]),
    });
    expect(session.messages()).toEqual([]);
    await session.close();
  });

  it("returns full conversation after a turn", async () => {
    const session = await makeSession({
      name: "msg-after-turn",
      llm: mockLLM([{ content: "world", finishReason: "stop" }]),
    });
    await session.send("hello");
    const msgs = session.messages();
    expect(msgs.length).toBeGreaterThanOrEqual(2);
    expect(msgs.some((m) => m.role === "user" && m.content === "hello")).toBe(true);
    expect(msgs.some((m) => m.role === "assistant" && m.content === "world")).toBe(true);
    await session.close();
  });

  it("accumulates messages across multiple turns", async () => {
    const session = await makeSession({
      name: "msg-accumulate",
      llm: mockLLM([
        { content: "first reply", finishReason: "stop" },
        { content: "second reply", finishReason: "stop" },
      ]),
    });
    await session.send("turn 1");
    const after1 = session.messages().length;
    await session.send("turn 2");
    const after2 = session.messages().length;
    expect(after2).toBeGreaterThan(after1);
    await session.close();
  });
});

// ---- status() ----

describe("status()", () => {
  it("returns idle before any send", async () => {
    const session = await makeSession({
      name: "status-idle",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });
    expect(await session.status()).toBe("idle");
    await session.close();
  });

  it("returns idle after a completed turn", async () => {
    const session = await makeSession({
      name: "status-idle-post",
      llm: mockLLM([{ content: "done", finishReason: "stop" }]),
    });
    await session.send("go");
    expect(await session.status()).toBe("idle");
    await session.close();
  });

  it("returns idle after close", async () => {
    const session = await makeSession({
      name: "status-closed",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });
    await session.send("go");
    await session.close();
    expect(await session.status()).toBe("idle");
  });

  it("returns waiting_approval when suspended at an approval gate", async () => {
    const session = await makeSession({
      name: "status-approval",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-st-1", name: "guarded", input: { x: "y" } }],
        },
        { content: "all done", finishReason: "stop" },
      ]),
      tools: { guarded: needsApprovalTool },
    });

    const sendPromise = session.send("use guarded tool");
    await waitForStatus(session, "waiting_approval");

    expect(await session.status()).toBe("waiting_approval");

    await session.approve("tc-st-1");
    await sendPromise;
    await session.close();
  });
});

// ---- approve() ----

describe("approve()", () => {
  it("resumes a turn suspended at a tool approval gate", async () => {
    const session = await makeSession({
      name: "approve-resume",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-approve-1", name: "guarded", input: { x: "hello" } }],
        },
        { content: "approved and done", finishReason: "stop" },
      ]),
      tools: { guarded: needsApprovalTool },
    });

    const sendPromise = session.send("run guarded tool");
    await waitForStatus(session, "waiting_approval");

    await session.approve("tc-approve-1");
    const answer = await sendPromise;
    expect(answer).toBe("approved and done");
    await session.close();
  });

  it("tool is executed after approval", async () => {
    const executed: string[] = [];
    const recordTool = tool({
      name: "recorder",
      description: "Records calls",
      parameters: z.object({ val: z.string() }),
      execute: async ({ val }) => {
        executed.push(val);
        return `recorded: ${val}`;
      },
      requireApproval: true,
    });

    const session = await makeSession({
      name: "approve-executes-tool",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-rec-1", name: "recorder", input: { val: "test-value" } }],
        },
        { content: "recorded", finishReason: "stop" },
      ]),
      tools: { recorder: recordTool },
    });

    const sendPromise = session.send("record something");
    await waitForStatus(session, "waiting_approval");
    await session.approve("tc-rec-1");
    await sendPromise;

    expect(executed).toEqual(["test-value"]);
    await session.close();
  });
});

// ---- reject() ----

describe("reject()", () => {
  it("cancels the tool and the turn continues with a rejection message", async () => {
    const executed: string[] = [];
    const guardedExec = tool({
      name: "guarded2",
      description: "Needs approval",
      parameters: z.object({ x: z.string() }),
      execute: async ({ x }) => {
        executed.push(x);
        return `ran: ${x}`;
      },
      requireApproval: true,
    });

    let toolResultSeen: string | undefined;
    const session = await makeSession({
      name: "reject-cancels",
      llm: {
        chat: async (params) => {
          const toolResult = params.messages.find((m) => m.role === "tool");
          if (toolResult) {
            toolResultSeen = (toolResult as any).content;
            return { content: "understood, skipped", finishReason: "stop" };
          }
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-rej-1", name: "guarded2", input: { x: "nope" } }],
          };
        },
      },
      tools: { guarded2: guardedExec },
    });

    const sendPromise = session.send("try tool");
    await waitForStatus(session, "waiting_approval");
    await session.reject("tc-rej-1", "not allowed");
    const answer = await sendPromise;

    expect(executed).toHaveLength(0);
    expect(toolResultSeen).toContain("Rejected");
    expect(answer).toBe("understood, skipped");
    await session.close();
  });

  it("reject reason is included in the tool result message", async () => {
    let toolResultContent = "";
    const session = await makeSession({
      name: "reject-reason",
      llm: {
        chat: async (params) => {
          const toolResult = params.messages.find((m) => m.role === "tool");
          if (toolResult) {
            toolResultContent = (toolResult as any).content;
            return { content: "ok", finishReason: "stop" };
          }
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-rej-2", name: "guarded", input: { x: "y" } }],
          };
        },
      },
      tools: { guarded: needsApprovalTool },
    });

    const sendPromise = session.send("try");
    await waitForStatus(session, "waiting_approval");
    await session.reject("tc-rej-2", "security policy");
    await sendPromise;

    expect(toolResultContent).toContain("security policy");
    await session.close();
  });
});
