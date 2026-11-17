export type DecisionQuestion =
  | {
      readonly type: "choice";
      readonly instructions: string;
      readonly options: ReadonlyArray<{ readonly name: string; readonly description?: string }>;
    }
  | {
      readonly type: "score";
      readonly instructions: string;
      readonly levels: ReadonlyArray<string>;
    }
  | { readonly type: "yesno"; readonly instructions: string };

export type ProbabilitySource = "model" | "none";

export type DecisionAnswer =
  | {
      readonly choice: string;
      readonly probability: number;
      readonly probabilities: Readonly<Record<string, number>>;
      readonly probabilitySource: ProbabilitySource;
      readonly confidence?: number;
    }
  | {
      readonly score: number;
      readonly normalized: number;
      readonly probabilities: ReadonlyArray<number>;
      readonly probabilitySource: ProbabilitySource;
      readonly confidence?: number;
    }
  | {
      readonly yes: number;
      readonly probabilitySource: ProbabilitySource;
    };

export interface DecisionRequest {
  readonly state: unknown;
  readonly questions: Readonly<Record<string, DecisionQuestion>>;
  readonly signal?: AbortSignal;
}

export interface DecisionResult {
  readonly answers: Readonly<Record<string, DecisionAnswer>>;
  readonly model: string;
  readonly usage?: { readonly inputTokens: number; readonly outputTokens?: number };
}

export interface DecisionProvider {
  decide(request: DecisionRequest): Promise<DecisionResult>;
}

export class DecisionAnswerError extends Error {
  readonly _tag = "DecisionAnswerError";
}

function invalid(question: string, reason: string): never {
  throw new DecisionAnswerError(`Invalid answer for question "${question}": ${reason}`);
}

function probability(params: { readonly question: string; readonly value: unknown }): number {
  if (
    typeof params.value !== "number" ||
    !Number.isFinite(params.value) ||
    params.value < 0 ||
    params.value > 1
  ) {
    invalid(params.question, "probabilities must be finite numbers from 0 to 1");
  }
  return params.value;
}

function probabilitiesSumToOne(params: {
  readonly question: string;
  readonly values: ReadonlyArray<number>;
}): void {
  const sum = params.values.reduce((total, value) => total + value, 0);
  if (Math.abs(sum - 1) > 0.02) invalid(params.question, `probabilities sum to ${sum}, not 1`);
}

function answerRecord(question: string, answer: DecisionAnswer): Record<string, unknown> {
  if (typeof answer !== "object" || answer === null || Array.isArray(answer)) {
    invalid(question, "answer must be an object");
  }
  return answer as unknown as Record<string, unknown>;
}

function checkProbabilitySource(question: string, answer: Record<string, unknown>): void {
  if (answer.probabilitySource !== "model" && answer.probabilitySource !== "none") {
    invalid(question, "probabilitySource must be model or none");
  }
  if (answer.confidence !== undefined) probability({ question, value: answer.confidence });
}

function checkChoice(params: {
  readonly name: string;
  readonly question: Extract<DecisionQuestion, { readonly type: "choice" }>;
  readonly answer: DecisionAnswer;
}): void {
  const answer = answerRecord(params.name, params.answer);
  checkProbabilitySource(params.name, answer);
  const names = params.question.options.map((option) => option.name);
  if (typeof answer.choice !== "string" || !names.includes(answer.choice)) {
    invalid(params.name, `choice ${String(answer.choice)} is not an option`);
  }
  if (
    typeof answer.probabilities !== "object" ||
    answer.probabilities === null ||
    Array.isArray(answer.probabilities)
  ) {
    invalid(params.name, "probabilities must be an object");
  }
  const entries = Object.entries(answer.probabilities);
  const keys = entries.map(([key]) => key).sort();
  if (keys.length !== names.length || keys.some((key, index) => key !== [...names].sort()[index])) {
    invalid(params.name, "probabilities must contain exactly the option names");
  }
  const values = entries.map(([, value]) => probability({ question: params.name, value }));
  probabilitiesSumToOne({ question: params.name, values });
  const selected = (answer.probabilities as Record<string, unknown>)[answer.choice];
  const picked = probability({ question: params.name, value: answer.probability });
  if (picked !== selected) invalid(params.name, "probability does not match the selected choice");
}

function checkScore(params: {
  readonly name: string;
  readonly question: Extract<DecisionQuestion, { readonly type: "score" }>;
  readonly answer: DecisionAnswer;
}): void {
  const answer = answerRecord(params.name, params.answer);
  checkProbabilitySource(params.name, answer);
  if (!Array.isArray(answer.probabilities)) {
    invalid(params.name, "probabilities must be an array");
  }
  if (answer.probabilities.length !== params.question.levels.length) {
    invalid(params.name, "probabilities must have one value per level");
  }
  const values = answer.probabilities.map((value) => probability({ question: params.name, value }));
  probabilitiesSumToOne({ question: params.name, values });
  if (
    typeof answer.score !== "number" ||
    !Number.isFinite(answer.score) ||
    answer.score < 0 ||
    answer.score > params.question.levels.length - 1
  ) {
    invalid(params.name, "score is outside the level range");
  }
  const expected = answer.score / (params.question.levels.length - 1);
  if (
    typeof answer.normalized !== "number" ||
    !Number.isFinite(answer.normalized) ||
    Math.abs(answer.normalized - expected) > 1e-9
  ) {
    invalid(params.name, "normalized does not match score");
  }
}

function checkYesNo(params: { readonly name: string; readonly answer: DecisionAnswer }): void {
  const answer = answerRecord(params.name, params.answer);
  checkProbabilitySource(params.name, answer);
  probability({ question: params.name, value: answer.yes });
}

/** Check answers against their questions, throwing with the invalid question's name. */
export function checkAnswers(params: {
  readonly questions: Readonly<Record<string, DecisionQuestion>>;
  readonly answers: Readonly<Record<string, DecisionAnswer>>;
}): Readonly<Record<string, DecisionAnswer>> {
  const questionNames = Object.keys(params.questions).sort();
  const answerNames = Object.keys(params.answers).sort();
  const mismatch =
    questionNames.find((name, index) => name !== answerNames[index]) ??
    answerNames.find((name, index) => name !== questionNames[index]);
  if (mismatch !== undefined || questionNames.length !== answerNames.length) {
    throw new DecisionAnswerError(
      `Decision answers must contain exactly the questions; mismatch at "${mismatch ?? "unknown"}"`,
    );
  }

  for (const name of questionNames) {
    const question = params.questions[name]!;
    const answer = params.answers[name]!;
    if (question.type === "choice") checkChoice({ name, question, answer });
    else if (question.type === "score") checkScore({ name, question, answer });
    else checkYesNo({ name, answer });
  }
  return params.answers;
}
