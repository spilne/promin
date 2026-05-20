// ---------------------------------------------------------------------------
// llmJudge — a rubric-driven LLM-as-judge scorer.
//
// A thin preset over `createLLMScorer`: it builds a standard judging prompt
// from a rubric and (when present) the case's reference answer, and asks the
// judge for a structured 0..1 score.
// ---------------------------------------------------------------------------

import type { LLMProvider } from "@promin/agent";
import type { Scorer } from "../types.ts";
import { createLLMScorer } from "./llm-scorer.ts";
import { stringifyValue } from "./stringify.ts";

export interface LLMJudgeConfig {
  /** The LLM that judges. */
  readonly judge: LLMProvider;
  /** The rubric — what distinguishes a good answer from a bad one. */
  readonly rubric: string;
  /** Scorer id — default `"llmJudge"`. */
  readonly id?: string;
  /** Pass cutoff on the judge's score. Default 0.5. */
  readonly threshold?: number;
  readonly required?: boolean;
}

/** Build a rubric-driven LLM-as-judge `Scorer`. */
export function llmJudge(config: LLMJudgeConfig): Scorer {
  const id = config.id ?? "llmJudge";
  return createLLMScorer({
    id,
    judge: config.judge,
    threshold: config.threshold ?? 0.5,
    ...(config.required !== undefined && { required: config.required }),
    prompt: ({ input, expected, output }) => ({
      system:
        `You are an evaluation judge. Score the OUTPUT against this rubric:\n${config.rubric}\n\n` +
        `Reply with ONLY a JSON object: {"value": <number between 0 and 1>, "reason": "<one sentence>"}.`,
      user: [
        `Input: ${stringifyValue(input)}`,
        expected !== undefined ? `Reference answer: ${stringifyValue(expected)}` : undefined,
        `Output: ${stringifyValue(output.output)}`,
      ]
        .filter((line): line is string => line !== undefined)
        .join("\n"),
    }),
  });
}
