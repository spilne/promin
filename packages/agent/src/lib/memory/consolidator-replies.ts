// ---------------------------------------------------------------------------
// Parsing of the consolidation LLM's replies. Model output is untrusted and
// often slightly off-format, so every field is read defensively: a missing
// or malformed one counts as empty, and nothing here throws on bad JSON.
// ---------------------------------------------------------------------------

import { MAX_ENTITY_CHARS, MAX_PREDICATE_CHARS, normalizeEntityName } from "./relations.ts";
import { clamp01 } from "./scoring.ts";
import { MAX_FACT_CHARS } from "./types.ts";

/** Relations taken from one distill reply; the rest are dropped. */
export const MAX_RELATIONS_PER_DISTILL = 8;
/** Corrections taken from one distill reply; the rest are dropped. */
export const MAX_CORRECTIONS_PER_DISTILL = 5;

export interface ParsedFact {
  readonly text: string;
  readonly confidence?: number;
}

export interface ParsedRelation {
  readonly subject: string;
  readonly predicate: string;
  readonly object: string;
  readonly confidence?: number;
}

export interface ParsedDistillReply {
  readonly summary: string;
  readonly outcome: string | null;
  readonly salience: number;
  readonly facts: ParsedFact[];
  readonly relations: ParsedRelation[];
  readonly corrections: string[];
}

/** `raw` without a surrounding ```json fence or outer whitespace. */
export function stripJsonFence(raw: string): string {
  return raw
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "");
}

/**
 * The distill reply. Text that is not a JSON object becomes a low-salience
 * stub, so distillation still records something rather than failing.
 */
export function parseDistillReply(raw: string): ParsedDistillReply {
  const text = stripJsonFence(raw);
  const json = parseJson(text);
  if (!isRecord(json)) {
    return {
      summary: text.slice(0, 500) || "(no summary)",
      outcome: null,
      salience: 0.1,
      facts: [],
      relations: [],
      corrections: [],
    };
  }
  const salience = json["salience"];
  return {
    summary: readString(json, "summary") ?? "(no summary)",
    outcome: readString(json, "outcome") ?? null,
    salience: typeof salience === "number" ? clamp01(salience) : 0.5,
    facts: parseFacts(json["facts"]),
    relations: parseRelations(json["relations"]),
    corrections: parseStringList({
      raw: json["corrections"],
      maxChars: MAX_FACT_CHARS,
      maxItems: MAX_CORRECTIONS_PER_DISTILL,
    }),
  };
}

/**
 * The reconcile reply `{"replaces":[{"new":n,"old":m}]}` as a map from
 * 1-based new-fact number to 1-based saved-fact number. Pairs naming a
 * number outside the lists shown, or reusing a saved or new fact, are
 * dropped, so the model cannot retire what it was not shown. An unparseable
 * reply gives an empty map.
 */
export function parseReconcileReply(params: {
  readonly raw: string;
  readonly savedCount: number;
  readonly newCount: number;
}): Map<number, number> {
  const out = new Map<number, number>();
  const json = parseJson(stripJsonFence(params.raw));
  const pairs = isRecord(json) ? json["replaces"] : undefined;
  if (!Array.isArray(pairs)) return out;
  const usedSaved = new Set<number>();
  for (const pair of pairs) {
    if (!isRecord(pair)) continue;
    const next = pair["new"];
    const old = pair["old"];
    if (typeof next !== "number" || typeof old !== "number") continue;
    if (!Number.isInteger(next) || !Number.isInteger(old)) continue;
    if (next < 1 || next > params.newCount || old < 1 || old > params.savedCount) continue;
    if (usedSaved.has(old) || out.has(next)) continue;
    usedSaved.add(old);
    out.set(next, old);
  }
  return out;
}

/** A compaction reply as plain text; a JSON envelope's `summary` when the model sent one. */
export function parseCompactionReply(raw: string): string {
  const text = stripJsonFence(raw);
  if (!text.startsWith("{")) return text;
  const summary = readString(parseJson(text), "summary");
  return summary !== undefined ? summary.trim() : text;
}

/** Facts as plain strings or `{text, confidence}` objects; anything else is dropped. */
function parseFacts(raw: unknown): ParsedFact[] {
  if (!Array.isArray(raw)) return [];
  const out: ParsedFact[] = [];
  for (const item of raw) {
    const text = (typeof item === "string" ? item : readString(item, "text"))?.trim() ?? "";
    if (text.length === 0 || text.length > MAX_FACT_CHARS) continue;
    const confidence = isRecord(item) ? parseConfidence(item["confidence"]) : undefined;
    out.push({ text, ...(confidence !== undefined && { confidence }) });
  }
  return out;
}

/** Relations with usable names, at most `MAX_RELATIONS_PER_DISTILL`. */
function parseRelations(raw: unknown): ParsedRelation[] {
  if (!Array.isArray(raw)) return [];
  const out: ParsedRelation[] = [];
  for (const item of raw) {
    const subject = readString(item, "subject")?.trim();
    const predicate = readString(item, "predicate")?.trim();
    const object = readString(item, "object")?.trim();
    if (!subject || !predicate || !object) continue;
    if (subject.length > MAX_ENTITY_CHARS || object.length > MAX_ENTITY_CHARS) continue;
    if (predicate.length > MAX_PREDICATE_CHARS) continue;
    if (normalizeEntityName(subject) === normalizeEntityName(object)) continue;
    const confidence = isRecord(item) ? parseConfidence(item["confidence"]) : undefined;
    out.push({ subject, predicate, object, ...(confidence !== undefined && { confidence }) });
    if (out.length >= MAX_RELATIONS_PER_DISTILL) break;
  }
  return out;
}

/** Non-blank strings of `raw`, trimmed, no longer than `maxChars`, at most `maxItems`. */
function parseStringList(params: {
  readonly raw: unknown;
  readonly maxChars: number;
  readonly maxItems: number;
}): string[] {
  if (!Array.isArray(params.raw)) return [];
  return params.raw
    .filter((s): s is string => typeof s === "string")
    .map((s) => s.trim())
    .filter((s) => s.length > 0 && s.length <= params.maxChars)
    .slice(0, params.maxItems);
}

function parseConfidence(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? clamp01(value) : undefined;
}

/** `JSON.parse` that returns undefined instead of throwing. */
function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

/** `record[key]` when `record` is an object and the value is a string. */
function readString(record: unknown, key: string): string | undefined {
  if (!isRecord(record)) return undefined;
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
