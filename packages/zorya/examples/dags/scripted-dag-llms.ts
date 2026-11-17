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
  "dag-planner" | "dag-researcher" | "dag-synthesizer",
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
  };
}
