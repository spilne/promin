import {
  httpErrorFromResponse,
  type LLMHttpOptions,
  resolveHttpOptions,
  startLLMRequest,
} from "../adapters/llm-http.ts";
import {
  checkAnswers,
  type DecisionAnswer,
  type DecisionProvider,
  type DecisionQuestion,
  type DecisionRequest,
} from "./types.ts";

/** Default wall-clock limit for one System One request. */
export const DEFAULT_DECISION_TIMEOUT_MS = 30_000;

export interface SystemOneOptions extends LLMHttpOptions {
  readonly baseUrl: string;
  readonly model: string;
  readonly apiKey?: string;
}

interface WireChoiceAnswer {
  readonly type: "choice";
  readonly choice: string;
  readonly confidence?: number;
  readonly probabilities: Readonly<Record<string, number>>;
}

interface WireScoreAnswer {
  readonly type: "score";
  readonly score: number;
  readonly confidence?: number;
  readonly probabilities: Readonly<Record<string, number>>;
}

interface WireNoulAnswer {
  readonly type: "noul";
  readonly noul: number;
}

type WireAnswer = WireChoiceAnswer | WireScoreAnswer | WireNoulAnswer;

interface WireResponse {
  readonly model: string;
  readonly answers: Readonly<Record<string, WireAnswer>>;
  readonly usage?: { readonly input_tokens: number; readonly output_tokens?: number };
}

function wireQuestion(question: DecisionQuestion): Record<string, unknown> {
  if (question.type === "choice") {
    return {
      type: "choice",
      instructions: question.instructions,
      criteria: Object.fromEntries(
        question.options.map((option) => [option.name, option.description ?? null]),
      ),
    };
  }
  if (question.type === "score") {
    return {
      type: "score",
      instructions: question.instructions,
      criteria: question.levels,
    };
  }
  return { type: "noul", instructions: question.instructions };
}

function wireQuestions(
  questions: DecisionRequest["questions"],
): Readonly<Record<string, Record<string, unknown>>> {
  return Object.fromEntries(
    Object.entries(questions).map(([name, question]) => [name, wireQuestion(question)]),
  );
}

function answerForQuestion(params: {
  readonly question: DecisionQuestion;
  readonly wire: WireAnswer;
}): DecisionAnswer {
  if (params.question.type === "choice") {
    const wire = params.wire as WireChoiceAnswer;
    return {
      choice: wire.choice,
      probability: wire.probabilities?.[wire.choice],
      probabilities: wire.probabilities,
      probabilitySource: "model",
      ...(wire.confidence !== undefined && { confidence: wire.confidence }),
    };
  }
  if (params.question.type === "score") {
    const wire = params.wire as WireScoreAnswer;
    return {
      score: wire.score,
      normalized: wire.score / (params.question.levels.length - 1),
      probabilities: params.question.levels.map(
        (_, index) => wire.probabilities?.[String(index)] ?? 0,
      ),
      probabilitySource: "model",
      ...(wire.confidence !== undefined && { confidence: wire.confidence }),
    };
  }
  const wire = params.wire as WireNoulAnswer;
  return { yes: wire.noul, probabilitySource: "model" };
}

function resultFromWire(params: {
  readonly request: DecisionRequest;
  readonly wire: WireResponse;
}): Awaited<ReturnType<DecisionProvider["decide"]>> {
  const answers = Object.fromEntries(
    Object.entries(params.request.questions).map(([name, question]) => [
      name,
      answerForQuestion({ question, wire: params.wire.answers?.[name] }),
    ]),
  );
  return {
    answers: checkAnswers({ questions: params.request.questions, answers }),
    model: params.wire.model,
    ...(params.wire.usage !== undefined && {
      usage: {
        inputTokens: params.wire.usage.input_tokens,
        ...(params.wire.usage.output_tokens !== undefined && {
          outputTokens: params.wire.usage.output_tokens,
        }),
      },
    }),
  };
}

/** Create a decision provider for a System One-compatible HTTP server. */
export function createSystemOneDecisionProvider(options: SystemOneOptions): DecisionProvider {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const providerName = `System One at ${baseUrl}`;
  const http = resolveHttpOptions({
    timeoutMs: options.timeoutMs ?? DEFAULT_DECISION_TIMEOUT_MS,
    ...(options.clock !== undefined && { clock: options.clock }),
    ...(options.fetch !== undefined && { fetch: options.fetch }),
  });

  return {
    async decide(request: DecisionRequest) {
      const scope = startLLMRequest({
        provider: providerName,
        signal: request.signal,
        timeoutMs: http.timeoutMs,
        clock: http.clock,
      });
      try {
        const response = await http.fetch(`${baseUrl}/v1/systemone`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...(options.apiKey !== undefined && { authorization: `Bearer ${options.apiKey}` }),
          },
          body: JSON.stringify({
            model: options.model,
            state: request.state,
            questions: wireQuestions(request.questions),
          }),
          signal: scope.signal,
        });
        if (!response.ok) {
          throw await httpErrorFromResponse({
            provider: providerName,
            response,
            clock: http.clock,
          });
        }
        const wire = (await response.json()) as WireResponse;
        return resultFromWire({ request, wire });
      } catch (error) {
        const mapped = scope.mapError(error);
        if (mapped instanceof TypeError) {
          throw new TypeError(`${providerName} request failed: ${mapped.message}`, {
            cause: mapped,
          });
        }
        throw mapped;
      } finally {
        scope.dispose();
      }
    },
  };
}
