import { describe, expect, it } from "bun:test";

import type { LLMChatParams, LLMProvider, LLMResponse } from "../../llm-provider.ts";
import { mockLLM } from "../../testing/mock-llm.ts";
import { createLlmDecisionProvider, DECIDE_TOOL_NAME } from "../llm-decision-provider.ts";
import { DecisionAnswerError, type DecisionRequest } from "../types.ts";

const questions = {
  team: {
    type: "choice",
    instructions: "Which team?",
    options: [{ name: "billing", description: "Payment problems" }, { name: "tech" }],
  },
  urgency: {
    type: "score",
    instructions: "How urgent?",
    levels: ["low", "mid", "high"],
  },
  refund: { type: "yesno", instructions: "Refund?" },
} as const;

const request: DecisionRequest = {
  state: { ticket: "charged twice" },
  questions,
};

function response(params: { readonly input?: unknown; readonly content?: string }): LLMResponse {
  return {
    content: params.content ?? null,
    ...(params.input !== undefined && {
      toolCalls: [{ id: "decision-1", name: DECIDE_TOOL_NAME, input: params.input }],
    }),
    finishReason: params.input === undefined ? "stop" : "tool_calls",
    usage: { inputTokens: 12, outputTokens: 3 },
  };
}

describe("createLlmDecisionProvider", () => {
  it("answers from the forced tool call", async () => {
    const llm = mockLLM([response({ input: { team: "billing", urgency: 2, refund: true } })]);

    await expect(
      createLlmDecisionProvider({ llm, model: "small" }).decide(request),
    ).resolves.toEqual({
      answers: {
        team: {
          choice: "billing",
          probability: 1,
          probabilities: { billing: 1, tech: 0 },
          probabilitySource: "none",
        },
        urgency: {
          score: 2,
          normalized: 1,
          probabilities: [0, 0, 1],
          probabilitySource: "none",
        },
        refund: { yes: 1, probabilitySource: "none" },
      },
      model: "small",
      usage: { inputTokens: 12, outputTokens: 3 },
    });
  });

  it("forces the tool and lists every question", async () => {
    let call: LLMChatParams | undefined;
    const reply = response({ input: { team: "billing", urgency: 2, refund: true } });
    const llm: LLMProvider = {
      async chat(params) {
        call = params;
        return reply;
      },
    };

    await createLlmDecisionProvider({ llm, model: "small" }).decide(request);

    expect(call?.toolChoice).toEqual({ name: DECIDE_TOOL_NAME });
    expect(call?.temperature).toBe(0);
    expect(call?.tools).toHaveLength(1);
    expect(call?.tools?.[0]?.parameters).toMatchObject({
      type: "object",
      required: ["team", "urgency", "refund"],
      properties: {
        team: { type: "string", enum: ["billing", "tech"] },
        urgency: { type: "integer", minimum: 0, maximum: 2 },
        refund: { type: "boolean" },
      },
    });
    expect(call?.messages[1]?.content).toContain("Payment problems");
    expect(call?.messages[1]?.content).toContain("low");
  });

  it("accepts a prose JSON reply", async () => {
    const llm = mockLLM([
      response({
        content: '```json\n{"team":"tech","urgency":0,"refund":false}\n```',
      }),
    ]);

    const result = await createLlmDecisionProvider({ llm, model: "small" }).decide(request);

    expect(result.answers.team).toMatchObject({ choice: "tech", probability: 1 });
    expect(result.answers.refund).toEqual({ yes: 0, probabilitySource: "none" });
  });

  it("refuses a reply that is not JSON", async () => {
    const llm = mockLLM([response({ content: "I think billing" })]);

    await expect(
      createLlmDecisionProvider({ llm, model: "small" }).decide(request),
    ).rejects.toBeInstanceOf(DecisionAnswerError);
  });

  it("refuses a choice outside the options", async () => {
    const llm = mockLLM([response({ input: { team: "sales", urgency: 2, refund: true } })]);

    await expect(
      createLlmDecisionProvider({ llm, model: "small" }).decide(request),
    ).rejects.toBeInstanceOf(DecisionAnswerError);
  });
});
