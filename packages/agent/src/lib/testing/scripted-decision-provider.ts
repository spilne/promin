import {
  checkAnswers,
  type DecisionAnswer,
  type DecisionProvider,
  type DecisionRequest,
} from "../decision/types.ts";

/** Deterministic decision provider that records each request. */
export function scriptedDecisionProvider(
  answer: (request: DecisionRequest) => Readonly<Record<string, DecisionAnswer>>,
  options: { readonly model?: string } = {},
): DecisionProvider & { readonly calls: DecisionRequest[] } {
  const calls: DecisionRequest[] = [];
  return {
    calls,
    decide: async (request) => {
      calls.push(request);
      return {
        answers: checkAnswers({ questions: request.questions, answers: answer(request) }),
        model: options.model ?? "scripted-decision",
      };
    },
  };
}
