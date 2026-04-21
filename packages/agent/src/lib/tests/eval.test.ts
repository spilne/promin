import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { runEval, exactMatch, containsAll, llmJudge } from "../eval.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { EvalCase } from "../eval.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
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

describe("runEval", () => {
  it("exactMatch: correct answer scores 1, wrong answer scores 0", async () => {
    const agent = agentAction({
      name: "exact-match-test",
      llm: mockLLM([
        { content: "Paris", finishReason: "stop" },
        { content: "London", finishReason: "stop" },
      ]),
    });

    const cases: EvalCase[] = [
      { input: "capital of France?", expected: "paris" },
      { input: "capital of France?", expected: "paris" },
    ];

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases,
      scorers: [exactMatch],
    });

    expect(results[0].scores[0].score).toBe(1);
    expect(results[1].scores[0].score).toBe(0);
  });

  it("containsAll: output with all substrings scores 1, missing one scores 0", async () => {
    const agent = agentAction({
      name: "contains-all-test",
      llm: mockLLM([
        { content: "TypeScript is a typed superset of JavaScript", finishReason: "stop" },
        { content: "TypeScript is great", finishReason: "stop" },
      ]),
    });

    const scorer = containsAll(["typescript", "javascript"]);

    const cases: EvalCase[] = [
      { input: "describe TypeScript" },
      { input: "describe TypeScript" },
    ];

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases,
      scorers: [scorer],
    });

    expect(results[0].scores[0].score).toBe(1);
    expect(results[1].scores[0].score).toBe(0);
  });

  it("llmJudge: mock LLM returns 'Score: 8', expects score ≈ 0.8", async () => {
    const agentLLM = mockLLM([{ content: "The answer is 42", finishReason: "stop" }]);
    const judgeLLM: LLMProvider = {
      chat: async () => ({ content: "Score: 8", finishReason: "stop" }),
    };

    const agent = agentAction({ name: "llm-judge-test", llm: agentLLM });

    const scorer = llmJudge({ llm: judgeLLM, rubric: "Rate 0-10 how correct the answer is." });

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases: [{ input: "What is 6 * 7?", expected: "42" }],
      scorers: [scorer],
    });

    expect(results[0].scores[0].score).toBeCloseTo(0.8);
  });

  it("error handling: agent that throws sets error field and scores of 0", async () => {
    const throwingLLM: LLMProvider = {
      chat: async () => {
        throw new Error("LLM failure");
      },
    };

    const agent = agentAction({ name: "error-test", llm: throwingLLM });

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases: [{ input: "cause error", expected: "anything" }],
      scorers: [exactMatch],
    });

    expect(results[0].error).toBeDefined();
    expect(results[0].scores[0].score).toBe(0);
  });

  it("concurrency: run 4 cases with concurrency 2, all complete successfully", async () => {
    const agent = agentAction({
      name: "concurrency-test",
      llm: {
        chat: async () => ({ content: "ok", finishReason: "stop" }),
      },
    });

    const cases: EvalCase[] = Array.from({ length: 4 }, (_, i) => ({
      input: `task ${i}`,
      expected: "ok",
    }));

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases,
      scorers: [exactMatch],
      concurrency: 2,
    });

    expect(results).toHaveLength(4);
    for (const r of results) {
      expect(r.error).toBeUndefined();
      expect(r.scores[0].score).toBe(1);
    }
  });

  it("results array length matches cases array length", async () => {
    const agent = agentAction({
      name: "length-test",
      llm: {
        chat: async () => ({ content: "answer", finishReason: "stop" }),
      },
    });

    const cases: EvalCase[] = Array.from({ length: 6 }, (_, i) => ({ input: `question ${i}` }));

    const results = await runEval({
      agent,
      runner: makeRunner(),
      cases,
      scorers: [exactMatch],
    });

    expect(results).toHaveLength(6);
  });
});
