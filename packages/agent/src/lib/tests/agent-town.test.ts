import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { createAgentTown } from "../agent-town.ts";
import { InMemoryMemoryStore } from "../memory-store.ts";
import type { LLMProvider, LLMResponse, LLMChatParams } from "../llm-provider.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async (_params: LLMChatParams): Promise<LLMResponse> => {
      const resp = responses[i++];
      if (!resp) throw new Error(`Mock LLM exhausted at call ${i}`);
      return resp;
    },
  };
}

function makeRunner() {
  return createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
}

// ---- validation ----

describe("createAgentTown — validation", () => {
  it("throws when mayor is not in the agents map", () => {
    expect(() =>
      createAgentTown({
        runner: makeRunner(),
        mayor: "ghost",
        agents: { alice: { llm: mockLLM([]), prompt: "Alice." } },
      }),
    ).toThrow('AgentTown: mayor "ghost" is not in the agents map');
  });
});

// ---- lifecycle ----

describe("createAgentTown — lifecycle", () => {
  it("close() resolves cleanly with idle daemons", async () => {
    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: { llm: mockLLM([]), prompt: "Coordinate." },
        worker: { llm: mockLLM([]), prompt: "Work." },
      },
    });
    await town.close();
  });

  it("ask() returns the mayor LLM response", async () => {
    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: {
          llm: mockLLM([{ content: "Task complete!", finishReason: "stop" }]),
          prompt: "Coordinate.",
        },
      },
    });
    const answer = await town.ask("Do something");
    expect(answer).toBe("Task complete!");
    await town.close();
  });
});

// ---- messaging ----

describe("createAgentTown — messaging", () => {
  it("sendMessage delivers to peer inbox and daemon processes it", async () => {
    let resolveWorkerCalled!: (msg: string) => void;
    const workerReceived = new Promise<string>((r) => (resolveWorkerCalled = r));

    const workerLLM: LLMProvider = {
      chat: async (params) => {
        const last = params.messages.at(-1);
        const text = typeof last?.content === "string" ? last.content : "";
        resolveWorkerCalled(text);
        return { content: "ack", finishReason: "stop" };
      },
    };

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: {
          llm: mockLLM([
            {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [
                { id: "tc-1", name: "sendMessage", input: { to: "worker", content: "ping" } },
              ],
            },
            { content: "sent", finishReason: "stop" },
          ]),
          prompt: "Coordinate.",
        },
        worker: { llm: workerLLM, prompt: "Work." },
      },
    });

    await town.ask("go");
    const workerMsg = await workerReceived;
    expect(workerMsg).toContain("ping");
    await town.close();
  });

  it("sendMessage to unknown recipient returns error string", async () => {
    let toolResult = "";
    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: {
          llm: {
            chat: async (params) => {
              const lastToolResult = params.messages.findLast((m) => m.role === "tool");
              if (lastToolResult) {
                toolResult =
                  typeof lastToolResult.content === "string" ? lastToolResult.content : "";
                return { content: "ok", finishReason: "stop" };
              }
              return {
                content: null,
                finishReason: "tool_calls",
                toolCalls: [
                  { id: "tc-1", name: "sendMessage", input: { to: "nobody", content: "hey" } },
                ],
              };
            },
          },
          prompt: "Coordinate.",
        },
      },
    });

    await town.ask("go");
    expect(toolResult).toContain('Unknown recipient "nobody"');
    await town.close();
  });
});

// ---- private memory ----

describe("createAgentTown — private memory", () => {
  it("injects saveMemory/searchMemory when agent.memory is provided", async () => {
    const store = new InMemoryMemoryStore();
    let toolResult = "";

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: {
          memory: store,
          llm: {
            chat: async (params) => {
              const lastTool = params.messages.findLast((m) => m.role === "tool");
              if (lastTool) {
                toolResult = typeof lastTool.content === "string" ? lastTool.content : "";
                return { content: "remembered", finishReason: "stop" };
              }
              return {
                content: null,
                finishReason: "tool_calls",
                toolCalls: [
                  { id: "tc-1", name: "saveMemory", input: { content: "TypeScript rocks" } },
                ],
              };
            },
          },
          prompt: "Coordinate.",
        },
      },
    });

    await town.ask("remember something");
    expect(toolResult).toContain("Saved to memory");
    const entries = await store.list();
    expect(entries).toHaveLength(1);
    expect(entries[0]!.content).toBe("TypeScript rocks");
    await town.close();
  });

  it("private memory is isolated per agent", async () => {
    const coordStore = new InMemoryMemoryStore();
    const workerStore = new InMemoryMemoryStore();
    let resolveWorkerDone!: () => void;
    const workerDone = new Promise<void>((r) => (resolveWorkerDone = r));

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      agents: {
        coord: {
          memory: coordStore,
          llm: mockLLM([
            {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [{ id: "tc-1", name: "saveMemory", input: { content: "coord secret" } }],
            },
            {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [
                {
                  id: "tc-2",
                  name: "sendMessage",
                  input: { to: "worker", content: "check memory" },
                },
              ],
            },
            { content: "done", finishReason: "stop" },
          ]),
          prompt: "Coordinate.",
        },
        worker: {
          memory: workerStore,
          llm: {
            chat: async () => {
              resolveWorkerDone();
              return { content: "ack", finishReason: "stop" };
            },
          },
          prompt: "Work.",
        },
      },
    });

    await town.ask("go");
    await workerDone;

    const coordEntries = await coordStore.list();
    const workerEntries = await workerStore.list();

    expect(coordEntries).toHaveLength(1);
    expect(coordEntries[0]!.content).toBe("coord secret");
    expect(workerEntries).toHaveLength(0);
    await town.close();
  });
});

// ---- shared memory ----

describe("createAgentTown — shared memory", () => {
  it("injects saveSharedMemory/searchSharedMemory when sharedMemory is provided", async () => {
    const shared = new InMemoryMemoryStore();
    let toolResult = "";

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      sharedMemory: shared,
      agents: {
        coord: {
          llm: {
            chat: async (params) => {
              const lastTool = params.messages.findLast((m) => m.role === "tool");
              if (lastTool) {
                toolResult = typeof lastTool.content === "string" ? lastTool.content : "";
                return { content: "saved", finishReason: "stop" };
              }
              return {
                content: null,
                finishReason: "tool_calls",
                toolCalls: [
                  { id: "tc-1", name: "saveSharedMemory", input: { content: "shared fact" } },
                ],
              };
            },
          },
          prompt: "Coordinate.",
        },
      },
    });

    await town.ask("save something shared");
    expect(toolResult).toContain("Saved to shared memory");
    const entries = await shared.list();
    expect(entries).toHaveLength(1);
    expect(entries[0]!.content).toBe("shared fact");
    await town.close();
  });

  it("shared memory is readable by all agents", async () => {
    const shared = new InMemoryMemoryStore();
    await shared.save({ content: "global fact" });

    let workerSearchResult = "";
    let resolveWorkerDone!: () => void;
    const workerDone = new Promise<void>((r) => (resolveWorkerDone = r));

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      sharedMemory: shared,
      agents: {
        coord: {
          llm: mockLLM([
            {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [
                {
                  id: "tc-1",
                  name: "sendMessage",
                  input: { to: "worker", content: "search shared" },
                },
              ],
            },
            { content: "done", finishReason: "stop" },
          ]),
          prompt: "Coordinate.",
        },
        worker: {
          llm: {
            chat: async (params) => {
              const lastTool = params.messages.findLast((m) => m.role === "tool");
              if (lastTool) {
                workerSearchResult = typeof lastTool.content === "string" ? lastTool.content : "";
                resolveWorkerDone();
                return { content: "found", finishReason: "stop" };
              }
              return {
                content: null,
                finishReason: "tool_calls",
                toolCalls: [
                  {
                    id: "tc-2",
                    name: "searchSharedMemory",
                    input: { query: "global fact", limit: 5 },
                  },
                ],
              };
            },
          },
          prompt: "Work.",
        },
      },
    });

    await town.ask("check");
    await workerDone;
    expect(workerSearchResult).toContain("global fact");
    await town.close();
  });

  it("private memory is not exposed in shared memory", async () => {
    const shared = new InMemoryMemoryStore();
    const coordPrivate = new InMemoryMemoryStore();
    await coordPrivate.save({ content: "coord private" });

    const town = createAgentTown({
      runner: makeRunner(),
      mayor: "coord",
      sharedMemory: shared,
      agents: {
        coord: {
          memory: coordPrivate,
          llm: mockLLM([{ content: "done", finishReason: "stop" }]),
          prompt: "Coordinate.",
        },
      },
    });

    await town.ask("go");
    const sharedEntries = await shared.list();
    expect(sharedEntries).toHaveLength(0);
    await town.close();
  });
});
