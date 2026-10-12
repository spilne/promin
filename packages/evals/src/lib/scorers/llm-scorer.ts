// ---------------------------------------------------------------------------
// createLLMScorer — builder for LLM-as-judge scorers.
//
// The core `Scorer` interface stays a single `score()` method; this builder
// is the opt-in convenience for the common shape: render a prompt for the
// case, ask a judge LLM, parse a structured `{ value, reason }` back.
//
// The default parser reads a JSON object, else an explicit rating in the
// text ("85/100", "8 out of 10", "85%", or one lone number in 0..1). A
// reply it cannot read unambiguously throws `JudgeParseError`, which the
// runner records as a failed score with the reason — it never guesses a
// scale.
// ---------------------------------------------------------------------------

import type { LLMProvider, Message } from "@promin/agent";
import type { Scorer, ScorerInput } from "../types.ts";

/** A prompt for the judge — a bare user string, or split system / user. */
export type JudgePrompt = string | { readonly system?: string; readonly user: string };

/** What a reply parser extracts. `value` must be in 0..1. */
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
   * Parse the judge's reply; throw when it carries no usable score.
   * Default: `parseJudgement`.
   */
  readonly parse?: (reply: string) => ParsedJudgement;
}

/** The judge's reply carried no score `parseJudgement` could read unambiguously. */
export class JudgeParseError extends Error {
  readonly _tag = "JudgeParseError";
  readonly reply: string;

  constructor(params: { readonly reply: string; readonly detail: string }) {
    super(`unparsable judge reply (${params.detail}): ${excerpt(params.reply)}`);
    this.name = "JudgeParseError";
    this.reply = params.reply;
  }
}

/** Build an LLM-as-judge `Scorer`. */
export function createLLMScorer(config: LLMScorerConfig): Scorer {
  const parse = config.parse ?? parseJudgement;
  return {
    id: config.id,
    threshold: config.threshold ?? 0.5,
    ...(config.required !== undefined && { required: config.required }),
    async score(input) {
      const response = await config.judge.chat({ messages: toMessages(config.prompt(input)) });
      const reply = response.content ?? "";
      const parsed = parse(reply);
      if (!Number.isFinite(parsed.value) || parsed.value < 0 || parsed.value > 1) {
        throw new JudgeParseError({ reply, detail: `value ${parsed.value} is outside 0..1` });
      }
      return {
        scorerId: config.id,
        value: parsed.value,
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

const NUMBER = String.raw`(\d+(?:\.\d+)?)`;
/** "85/100", "8 / 10". */
const FRACTION = new RegExp(String.raw`${NUMBER}\s*/\s*${NUMBER}`);
/** "8 out of 10". */
const OUT_OF = new RegExp(String.raw`${NUMBER}\s+out\s+of\s+${NUMBER}`, "i");
/** "85%". */
const PERCENT = new RegExp(String.raw`${NUMBER}\s*%`);
const ANY_NUMBER = new RegExp(NUMBER, "g");

/**
 * Read a judge reply. Accepted, in order:
 *
 *   - a JSON object `{ "value" | "score": n, "reason"?, "label"? }` with n
 *     in 0..1, or with `"outOf": m` / `"max": m` scaling n into 0..1;
 *   - a rating with its scale in the text: `85/100`, `8 out of 10`, `85%`;
 *   - a single number in 0..1, when it is the only number in the reply.
 *
 * Anything else throws `JudgeParseError`: a bare "7" has no scale, so it
 * is never read as 7/10.
 */
export function parseJudgement(reply: string): ParsedJudgement {
  const jsonSlice = reply.match(/\{[\s\S]*\}/);
  if (jsonSlice) {
    const fromJson = parseJsonJudgement({ reply, json: jsonSlice[0] });
    if (fromJson !== undefined) return fromJson;
  }
  const reason = "parsed from a non-JSON judge reply";
  for (const pattern of [FRACTION, OUT_OF]) {
    const match = reply.match(pattern);
    if (match)
      return { value: scaled({ reply, raw: Number(match[1]), max: Number(match[2]) }), reason };
  }
  const percent = reply.match(PERCENT);
  if (percent) return { value: scaled({ reply, raw: Number(percent[1]), max: 100 }), reason };
  const numbers = [...reply.matchAll(ANY_NUMBER)].map((m) => Number(m[1]));
  if (numbers.length === 1 && numbers[0]! >= 0 && numbers[0]! <= 1) {
    return { value: numbers[0]!, reason };
  }
  throw new JudgeParseError({
    reply,
    detail:
      numbers.length === 0
        ? "no score in the reply"
        : "no 0..1 value, JSON object or rating with its scale",
  });
}

/** A JSON judgement, or undefined when the slice is not JSON with a score. */
function parseJsonJudgement(params: { reply: string; json: string }): ParsedJudgement | undefined {
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(params.json) as Record<string, unknown>;
  } catch {
    return undefined; // not valid JSON — try the text forms
  }
  const raw = typeof obj.value === "number" ? obj.value : obj.score;
  if (typeof raw !== "number") return undefined;
  const scale = typeof obj.outOf === "number" ? obj.outOf : obj.max;
  const value = typeof scale === "number" ? scaled({ reply: params.reply, raw, max: scale }) : raw;
  if (value < 0 || value > 1) {
    throw new JudgeParseError({
      reply: params.reply,
      detail: `value ${raw} is outside 0..1 and the reply names no scale`,
    });
  }
  return {
    value,
    ...(typeof obj.reason === "string" && { reason: obj.reason }),
    ...(typeof obj.label === "string" && { label: obj.label }),
  };
}

/** `raw / max`, refusing a rating above its own scale. */
function scaled(params: { reply: string; raw: number; max: number }): number {
  const { raw, max } = params;
  if (!(max > 0) || raw < 0 || raw > max) {
    throw new JudgeParseError({ reply: params.reply, detail: `rating ${raw}/${max}` });
  }
  return raw / max;
}

function excerpt(reply: string): string {
  const flat = reply.replace(/\s+/g, " ").trim();
  return flat.length > 200 ? `${flat.slice(0, 200)}…` : flat;
}
