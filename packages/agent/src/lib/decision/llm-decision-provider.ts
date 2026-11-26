import type { LLMProvider, LLMToolDefinition } from "../llm-provider.ts";
import { extractJson } from "../dag/output-schema.ts";
import {
  checkAnswers,
  DecisionAnswerError,
  type DecisionAnswer,
  type DecisionProvider,
  type DecisionQuestion,
} from "./types.ts";

export const DECIDE_TOOL_NAME = "decide";

function propertyFor(question: DecisionQuestion): Record<string, unknown> {
  if (question.type === "choice") {
    return { type: "string", enum: question.options.map((option) => option.name) };
  }
  if (question.type === "score") {
    return { type: "integer", minimum: 0, maximum: question.levels.length - 1 };
  }
  return { type: "boolean" };
}

function decisionTool(questions: Readonly<Record<string, DecisionQuestion>>): LLMToolDefinition {
  return {
    name: DECIDE_TOOL_NAME,
    description: "Answer every decision question with the requested value.",
    parameters: {
      type: "object",
      properties: Object.fromEntries(
        Object.entries(questions).map(([name, question]) => [name, propertyFor(question)]),
      ),
      required: Object.keys(questions),
      additionalProperties: false,
    },
  };
}

function questionText(params: {
  readonly name: string;
  readonly question: DecisionQuestion;
}): string {
  const prefix = `${params.name}: ${params.question.instructions}`;
  if (params.question.type === "choice") {
    const options = params.question.options
      .map((option) => `${option.name}${option.description ? ` — ${option.description}` : ""}`)
      .join("; ");
    return `${prefix}\nOptions: ${options}`;
  }
  if (params.question.type === "score") {
    return `${prefix}\nLevels: ${params.question.levels
      .map((level, index) => `${index} — ${level}`)
      .join("; ")}`;
  }
  return `${prefix}\nAnswer true or false.`;
}

function userPrompt(params: {
  readonly state: unknown;
  readonly questions: Readonly<Record<string, DecisionQuestion>>;
}): string {
  const questions = Object.entries(params.questions)
    .map(([name, question]) => questionText({ name, question }))
    .join("\n\n");
  return `State:\n${JSON.stringify(params.state, null, 2)}\n\nQuestions:\n${questions}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function invalid(question: string, reason: string): never {
  throw new DecisionAnswerError(`Invalid LLM decision for question "${question}": ${reason}`);
}

function answerFromValue(params: {
  readonly name: string;
  readonly question: DecisionQuestion;
  readonly value: unknown;
}): DecisionAnswer {
  if (params.question.type === "choice") {
    const names = params.question.options.map((option) => option.name);
    if (typeof params.value !== "string" || !names.includes(params.value)) {
      invalid(params.name, "choice is not one of the options");
    }
    return {
      choice: params.value,
      probability: 1,
      probabilities: Object.fromEntries(names.map((name) => [name, name === params.value ? 1 : 0])),
      probabilitySource: "none",
    };
  }
  if (params.question.type === "score") {
    if (
      typeof params.value !== "number" ||
      !Number.isInteger(params.value) ||
      params.value < 0 ||
      params.value >= params.question.levels.length
    ) {
      invalid(params.name, "score is not an integer level");
    }
    return {
      score: params.value,
      normalized: params.value / (params.question.levels.length - 1),
      probabilities: params.question.levels.map((_, index) => (index === params.value ? 1 : 0)),
      probabilitySource: "none",
    };
  }
  if (typeof params.value !== "boolean") {
    invalid(params.name, "yes/no answer is not a boolean");
  }
  return { yes: params.value ? 1 : 0, probabilitySource: "none" };
}

function checkedAnswers(params: {
  readonly raw: unknown;
  readonly questions: Readonly<Record<string, DecisionQuestion>>;
}): Readonly<Record<string, DecisionAnswer>> {
  const raw = params.raw;
  if (!isRecord(raw)) invalid("answers", "reply is not an object");
  const expected = Object.keys(params.questions).sort();
  const received = Object.keys(raw).sort();
  if (
    expected.length !== received.length ||
    expected.some((name, index) => name !== received[index])
  ) {
    invalid("answers", "reply must contain exactly the question names");
  }
  const answers = Object.fromEntries(
    Object.entries(params.questions).map(([name, question]) => [
      name,
      answerFromValue({ name, question, value: raw[name] }),
    ]),
  );
  return checkAnswers({ questions: params.questions, answers });
}

function proseInput(content: string | null): unknown {
  if (content === null) invalid("answers", "reply contained neither a decision tool call nor JSON");
  try {
    return extractJson(content);
  } catch (error) {
    throw new DecisionAnswerError(
      `Invalid LLM decision for question "answers": ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/** Create a deterministic decision provider over an existing chat model. */
export function createLlmDecisionProvider(options: {
  readonly llm: LLMProvider;
  readonly model: string;
}): DecisionProvider {
  return {
    async decide(request) {
      const response = await options.llm.chat({
        messages: [
          {
            role: "system",
            content:
              "Answer the supplied decision questions from the state. Use the decide tool and answer every question.",
          },
          { role: "user", content: userPrompt(request) },
        ],
        tools: [decisionTool(request.questions)],
        toolChoice: { name: DECIDE_TOOL_NAME },
        temperature: 0,
        ...(request.signal !== undefined && { signal: request.signal }),
      });
      const toolCall = response.toolCalls?.find((call) => call.name === DECIDE_TOOL_NAME);
      if (toolCall?.inputError !== undefined) invalid("answers", toolCall.inputError);
      const answers = checkedAnswers({
        raw: toolCall?.input ?? proseInput(response.content),
        questions: request.questions,
      });
      return {
        answers,
        model: options.model,
        ...(response.usage !== undefined && {
          usage: {
            inputTokens: response.usage.inputTokens,
            outputTokens: response.usage.outputTokens,
          },
        }),
      };
    },
  };
}
