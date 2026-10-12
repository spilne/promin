import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { combineProcessors } from "../processors.ts";
import type { LLMResponse } from "../llm-provider.ts";
import type { Message } from "../message.ts";

function mockLLM(responses: LLMResponse[]) {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("exhausted");
      return r;
    },
  };
}

async function runAction(wf: ReturnType<typeof agentAction>, task: string) {
  const storage = new InMemoryWorkflowStorage();
  return createWorkflowRunner({ storage }).run({
    workflow: wf,
    workflowId: "p-1",
    input: { task },
  });
}

describe("processors — agentAction", () => {
  it("beforeLLM can inject a system message", async () => {
    const seen: Message[][] = [];

    const action = agentAction({
      name: "proc-before",
      llm: {
        chat: async (p) => {
          seen.push(p.messages as Message[]);
          return { content: "ok", finishReason: "stop" };
        },
      },
      processors: {
        beforeLLM: (msgs) => [...msgs, { role: "system", content: "injected" }],
      },
    });

    await runAction(action, "hello");
    expect(seen[0]?.some((m) => m.role === "system" && m.content === "injected")).toBe(true);
  });

  it("afterLLM can modify the response content", async () => {
    const action = agentAction({
      name: "proc-after",
      llm: mockLLM([{ content: "raw answer", finishReason: "stop" }]),
      processors: {
        afterLLM: (resp) => ({ ...resp, content: resp.content?.toUpperCase() ?? null }),
      },
    });

    const result = await runAction(action, "hello");
    expect(result.answer).toBe("RAW ANSWER");
  });

  it("receives correct ProcessorContext (step, turn, workflowId)", async () => {
    const ctxs: unknown[] = [];

    const action = agentAction({
      name: "proc-ctx",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      processors: {
        beforeLLM: (msgs, ctx) => {
          ctxs.push(ctx);
          return msgs;
        },
      },
    });

    await runAction(action, "hello");
    expect(ctxs[0]).toMatchObject({ step: 0, turn: 0 });
  });
});

describe("combineProcessors", () => {
  it("chains multiple beforeLLM in order", async () => {
    const order: string[] = [];

    const p1 = {
      beforeLLM: (msgs: Message[]) => {
        order.push("p1");
        return msgs;
      },
    };
    const p2 = {
      beforeLLM: (msgs: Message[]) => {
        order.push("p2");
        return msgs;
      },
    };
    const combined = combineProcessors(p1, p2);

    await combined.beforeLLM!([], { step: 0, turn: 0, workflowId: "w", isReplay: false });
    expect(order).toEqual(["p1", "p2"]);
  });

  it("chains multiple afterLLM in order", async () => {
    const order: string[] = [];
    const resp: LLMResponse = { content: "x", finishReason: "stop" };

    const p1 = {
      afterLLM: (r: LLMResponse) => {
        order.push("p1");
        return r;
      },
    };
    const p2 = {
      afterLLM: (r: LLMResponse) => {
        order.push("p2");
        return r;
      },
    };
    const combined = combineProcessors(p1, p2);

    await combined.afterLLM!(resp, { step: 0, turn: 0, workflowId: "w", isReplay: false });
    expect(order).toEqual(["p1", "p2"]);
  });
});
