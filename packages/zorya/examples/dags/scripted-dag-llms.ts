import {
  RESPOND_TOOL_NAME,
  type LLMChatParams,
  type LLMProvider,
  type LLMResponse,
} from "@promin/agent";

function inputOf(params: LLMChatParams): Record<string, unknown> {
  for (let index = params.messages.length - 1; index >= 0; index--) {
    const message = params.messages[index]!;
    if (message.role !== "user") continue;
    try {
      return JSON.parse(message.content) as Record<string, unknown>;
    } catch {
      return { topic: message.content };
    }
  }
  return {};
}

function textProvider(answer: (input: Record<string, unknown>) => string): LLMProvider {
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => ({
      content: answer(inputOf(params)),
      finishReason: "stop",
      usage: { inputTokens: 10, outputTokens: 10 },
    }),
  };
}

/** Deterministic demo DAG models: no credentials or network required. */
export function scriptedDagLlms(): Record<
  "dag-planner" | "dag-researcher" | "dag-synthesizer" | "dag-writer" | "dag-reviewer",
  LLMProvider
> {
  return {
    "dag-planner": {
      chat: async (params: LLMChatParams): Promise<LLMResponse> => {
        const topic = String(inputOf(params).topic ?? "the topic");
        return {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [
            {
              id: "dag-plan",
              name: RESPOND_TOOL_NAME,
              input: {
                questions: [
                  `History of ${topic}?`,
                  `State of ${topic} today?`,
                  `The case against ${topic}?`,
                ],
              },
            },
          ],
          usage: { inputTokens: 10, outputTokens: 10 },
        };
      },
    },
    "dag-researcher": textProvider(
      (input) => `Finding: ${String(input.question ?? "unknown question")}`,
    ),
    "dag-synthesizer": textProvider((input) => {
      const findings = Array.isArray(input.findings) ? input.findings : [];
      return `Report on ${String(input.topic ?? "the topic")} from ${findings.length} findings.`;
    }),
    "dag-writer": textProvider((input) => {
      const previous = input.previous as { review?: { notes?: unknown } } | undefined;
      const revision = previous?.review?.notes
        ? ` Revised to address: ${String(previous.review.notes)}.`
        : "";
      return `Draft about ${String(input.topic ?? "the topic")}.${revision}`;
    }),
    "dag-reviewer": {
      chat: async (params: LLMChatParams): Promise<LLMResponse> => {
        const firstRound = inputOf(params).previous === undefined;
        return {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [
            {
              id: "dag-review",
              name: RESPOND_TOOL_NAME,
              input: firstRound
                ? { verdict: "revise", notes: "Add one concrete editorial improvement." }
                : { verdict: "pass", notes: "The revision addresses the review." },
            },
          ],
          usage: { inputTokens: 10, outputTokens: 10 },
        };
      },
    },
  };
}
