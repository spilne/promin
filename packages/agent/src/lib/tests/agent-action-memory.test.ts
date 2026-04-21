import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { InMemoryMemoryStore } from "../memory-store.ts";
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

async function runAction(
  workflow: ReturnType<typeof agentAction>,
  task: string,
  workflowId = "mem-wf-1",
) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return runner.run({ workflow, workflowId, input: { task } });
}

describe("agentAction memory", () => {
  it("injects memories into system prompt before thinking", async () => {
    const store = new InMemoryMemoryStore();
    await store.save({ content: "user prefers bullet lists" });

    let seenMessages: unknown[] = [];

    const action = agentAction({
      name: "mem-inject-test",
      llm: {
        chat: async (params) => {
          seenMessages = params.messages;
          return { content: "ok", finishReason: "stop" };
        },
      },
      // Use a searchQuery that overlaps with stored content ("user" appears in both)
      memory: { store, injectLimit: 5, searchQuery: "user preferences" },
    });

    await runAction(action, "hello");

    const hasMemory = seenMessages.some(
      (m: any) => m.role === "system" && m.content?.includes("user prefers bullet lists"),
    );
    expect(hasMemory).toBe(true);
  });

  it("skips memory injection when store is empty", async () => {
    const store = new InMemoryMemoryStore();
    let callCount = 0;

    const action = agentAction({
      name: "empty-mem-test",
      llm: {
        chat: async () => {
          callCount++;
          return { content: "ok", finishReason: "stop" };
        },
      },
      memory: { store, injectLimit: 5 },
    });

    await runAction(action, "hello");
    expect(callCount).toBe(1);
  });

  it("uses task text as default search query", async () => {
    const store = new InMemoryMemoryStore();
    await store.save({ content: "typescript is statically typed" });

    let seenMessages: unknown[] = [];

    const action = agentAction({
      name: "query-test",
      llm: {
        chat: async (params) => {
          seenMessages = params.messages;
          return { content: "ok", finishReason: "stop" };
        },
      },
      memory: { store },
    });

    // task matches memory content keyword "typescript"
    await runAction(action, "tell me about typescript");

    const hasMemory = seenMessages.some(
      (m: any) => m.role === "system" && m.content?.includes("typescript is statically typed"),
    );
    expect(hasMemory).toBe(true);
  });

  it("saves final answer to memory when saveOnComplete is true", async () => {
    const store = new InMemoryMemoryStore();

    const action = agentAction({
      name: "save-test",
      llm: mockLLM([{ content: "the final answer", finishReason: "stop" }]),
      memory: { store, saveOnComplete: true },
    });

    await runAction(action, "what is the answer?");

    const memories = await store.list();
    expect(memories.some((m) => m.content === "the final answer")).toBe(true);
  });

  it("does not save when saveOnComplete is false", async () => {
    const store = new InMemoryMemoryStore();

    const action = agentAction({
      name: "no-save-test",
      llm: mockLLM([{ content: "answer", finishReason: "stop" }]),
      memory: { store, saveOnComplete: false },
    });

    await runAction(action, "task");

    expect(await store.list()).toHaveLength(0);
  });

  it("does not save by default (saveOnComplete defaults to false)", async () => {
    const store = new InMemoryMemoryStore();

    const action = agentAction({
      name: "default-save-test",
      llm: mockLLM([{ content: "answer", finishReason: "stop" }]),
      memory: { store },
    });

    await runAction(action, "task");

    expect(await store.list()).toHaveLength(0);
  });

  it("memory injection uses custom searchQuery when provided", async () => {
    const store = new InMemoryMemoryStore();
    await store.save({ content: "user is an expert in Rust" });

    let seenMessages: unknown[] = [];

    const action = agentAction({
      name: "custom-query-test",
      llm: {
        chat: async (params) => {
          seenMessages = params.messages;
          return { content: "ok", finishReason: "stop" };
        },
      },
      memory: { store, searchQuery: "user expertise Rust" },
    });

    await runAction(action, "unrelated task");

    const hasMemory = seenMessages.some(
      (m: any) => m.role === "system" && m.content?.includes("user is an expert in Rust"),
    );
    expect(hasMemory).toBe(true);
  });
});
