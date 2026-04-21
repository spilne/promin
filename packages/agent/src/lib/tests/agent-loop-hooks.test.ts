import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type { Message } from "../message.ts";
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

async function makeSession(config: Parameters<typeof agentLoop>[0], sessionId = "hooks-s1") {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return agentLoop(config).session({ runner, sessionId });
}

describe("agentLoop hooks", () => {
  it("beforeTurn receives task and messages", async () => {
    const captured: { task: string; messageCount: number }[] = [];

    const session = await makeSession({
      name: "before-hook-test",
      llm: mockLLM([
        { content: "reply 1", finishReason: "stop" },
        { content: "reply 2", finishReason: "stop" },
      ]),
      hooks: {
        beforeTurn: async ({ task, messages }) => {
          captured.push({ task, messageCount: messages.length });
        },
      },
    });

    await session.send("first");
    await session.send("second");
    await session.close();

    expect(captured).toHaveLength(2);
    expect(captured[0]!.task).toBe("first");
    expect(captured[1]!.task).toBe("second");
    // second turn should have more messages (first user+assistant pair)
    expect(captured[1]!.messageCount).toBeGreaterThan(captured[0]!.messageCount);
  });

  it("beforeTurn can inject additional context into messages", async () => {
    const seenMessages: Message[][] = [];

    const session = await makeSession({
      name: "before-inject-test",
      llm: {
        chat: async (params) => {
          seenMessages.push(params.messages as Message[]);
          return { content: "ok", finishReason: "stop" };
        },
      },
      hooks: {
        beforeTurn: async ({ messages }) => {
          return [...messages, { role: "system" as const, content: "injected-context" }];
        },
      },
    });

    await session.send("hello");
    await session.close();

    const hasInjected = seenMessages[0]?.some(
      (m) => m.role === "system" && m.content === "injected-context",
    );
    expect(hasInjected).toBe(true);
  });

  it("afterTurn receives task, answer, and full message list", async () => {
    const captured: { task: string; answer: string; msgCount: number }[] = [];

    const session = await makeSession({
      name: "after-hook-test",
      llm: mockLLM([{ content: "the answer", finishReason: "stop" }]),
      hooks: {
        afterTurn: async ({ task, answer, messages }) => {
          captured.push({ task, answer, msgCount: messages.length });
        },
      },
    });

    await session.send("question");
    await session.close();

    expect(captured).toHaveLength(1);
    expect(captured[0]!.task).toBe("question");
    expect(captured[0]!.answer).toBe("the answer");
    expect(captured[0]!.msgCount).toBeGreaterThan(0);
  });

  it("onClose is called when session is closed", async () => {
    let closeCalled = false;

    const session = await makeSession({
      name: "on-close-test",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      hooks: {
        onClose: async () => {
          closeCalled = true;
        },
      },
    });

    await session.send("hi");
    expect(closeCalled).toBe(false);
    await session.close();
    expect(closeCalled).toBe(true);
  });

  it("afterTurn and onClose both fire in sequence", async () => {
    const events: string[] = [];

    const session = await makeSession({
      name: "sequence-test",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      hooks: {
        afterTurn: async () => {
          events.push("afterTurn");
        },
        onClose: async () => {
          events.push("onClose");
        },
      },
    });

    await session.send("hi");
    await session.close();

    expect(events).toEqual(["afterTurn", "onClose"]);
  });

  it("close is still async-safe when no onClose hook is set", async () => {
    const session = await makeSession({
      name: "no-hook-close",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });

    await session.send("hi");
    await expect(session.close()).resolves.toBeUndefined();
  });
});
