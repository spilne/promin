// ---------------------------------------------------------------------------
// createLLMScorer — builder for LLM-as-judge scorers.
//
// The core `Scorer` interface stays a single `score()` method; this builder
// is the opt-in convenience for the common shape: render a prompt for the
// case, ask a judge LLM, parse a structured `{ value, reason }` back. The
// default parser reads a JSON object and falls back to the first number in
// the reply, so a judge that ignores the format still produces a score.
// ---------------------------------------------------------------------------

import type { LLMProvider, Message } from "@promin/agent";
import type { Scorer, ScorerInput } from "../types.ts";

/** A prompt for the judge — a bare user string, or split system / user. */
export type JudgePrompt = string | { readonly system?: string; readonly user: string };

/** What a reply parser extracts. `value` is clamped to 0..1 by the scorer. */
export interface ParsedJudgement {
  readonly value: number;
  readonly reason?: string;
  readonly label?: string;
}

export interface LLMScorerConfig {
  readonly id: string;
  /** The LLM that judges. */
  readonly judge: LLMProvider;
  /** Pass cutoff on `value`. Default 0.5 — a judge score is graded, not binary. */
  readonly threshold?: number;
  readonly required?: boolean;
  /** Build the judge prompt from the scorer input. */
  readonly prompt: (input: ScorerInput) => JudgePrompt;
  /**
   * Parse the judge's reply. Default: read a JSON object
   * `{ "value": 0..1, "reason"?, "label"? }`, falling back to the first
   * number in the text (an integer above 1 is read as an N/10 rating).
   */
  readonly parse?: (reply: string) => ParsedJudgement;
}

/** Build an LLM-as-judge `Scorer`. */
export function createLLMScorer(config: LLMScorerConfig): Scorer {
  const parse = config.parse ?? defaultParse;
  return {
    id: config.id,
    threshold: config.threshold ?? 0.5,
    ...(config.required !== undefined && { required: config.required }),
    async score(input) {
      const response = await config.judge.chat({ messages: toMessages(config.prompt(input)) });
      const parsed = parse(response.content ?? "");
      return {
        scorerId: config.id,
        value: clamp01(parsed.value),
        ...(parsed.reason !== undefined && { reason: parsed.reason }),
        ...(parsed.label !== undefined && { label: parsed.label }),
      };
    },
  };
}

function toMessages(prompt: JudgePrompt): Message[] {
  if (typeof prompt === "string") return [{ role: "user", content: prompt }];
  const messages: Message[] = [];
  if (prompt.system !== undefined) messages.push({ role: "system", content: prompt.system });
  messages.push({ role: "user", content: prompt.user });
  return messages;
}

/** Read `{ value | score, reason?, label? }` JSON, else the first number. */
function defaultParse(reply: string): ParsedJudgement {
  const jsonSlice = reply.match(/\{[\s\S]*\}/);
  if (jsonSlice) {
    try {
      const obj = JSON.parse(jsonSlice[0]) as Record<string, unknown>;
      const raw = typeof obj.value === "number" ? obj.value : obj.score;
      if (typeof raw === "number") {
        return {
          value: raw,
          ...(typeof obj.reason === "string" && { reason: obj.reason }),
          ...(typeof obj.label === "string" && { label: obj.label }),
        };
      }
    } catch {
      // Not valid JSON — fall through to the numeric scan.
    }
  }
  const numeric = reply.match(/\d+(?:\.\d+)?/);
  if (numeric) {
    const raw = Number.parseFloat(numeric[0]);
    return { value: raw > 1 ? raw / 10 : raw, reason: "parsed from a non-JSON judge reply" };
  }
  return { value: 0, reason: "judge reply was not parseable" };
}

function clamp01(value: number): number {
  if (Number.isNaN(value)) return 0;
  return Math.max(0, Math.min(1, value));
}
