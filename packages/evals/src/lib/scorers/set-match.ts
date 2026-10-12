// ---------------------------------------------------------------------------
// setMatch — compare a list of strings the target produced against the
// case's expected list: precision, recall or F1.
//
// Items are compared normalized (case, surrounding whitespace and trailing
// punctuation ignored). Precision is per produced item: an item that is
// not expected, or repeats one already matched, is a miss — a stored
// duplicate fact is noise too. Recall is over the distinct expected items.
// Typical use: facts a memory pipeline distilled, tags a classifier emitted.
// ---------------------------------------------------------------------------

import type { Scorer } from "../types.ts";

export type SetMatchMetric = "precision" | "recall" | "f1";

export interface SetMatchConfig {
  /** Default `"f1"`. */
  readonly metric?: SetMatchMetric;
  /** Scorer id — default `"setMatch"`. */
  readonly id?: string;
  /** Pass cutoff on the metric. Default 1. */
  readonly threshold?: number;
  readonly required?: boolean;
}

/** Build a scorer comparing `output.output` with `expected`, both lists of strings. */
export function setMatch(config: SetMatchConfig = {}): Scorer {
  const id = config.id ?? "setMatch";
  const metric = config.metric ?? "f1";
  return {
    id,
    threshold: config.threshold ?? 1,
    ...(config.required !== undefined && { required: config.required }),
    async score({ output, expected }) {
      const actual = stringList(output.output);
      const wantedList = stringList(expected);
      if (actual === undefined) {
        throw new Error(`${id}: the output must be an array of strings`);
      }
      if (wantedList === undefined) {
        throw new Error(`${id}: the case's expected value must be an array of strings`);
      }
      const wanted = new Set(wantedList);
      const matched = new Set<string>();
      const extra: string[] = [];
      for (const item of actual) {
        if (wanted.has(item) && !matched.has(item)) matched.add(item);
        else extra.push(item);
      }
      // An empty side is perfect only when the other side is empty too.
      const precision =
        actual.length === 0 ? (wanted.size === 0 ? 1 : 0) : matched.size / actual.length;
      const recall = wanted.size === 0 ? (actual.length === 0 ? 1 : 0) : matched.size / wanted.size;
      const value =
        metric === "precision"
          ? precision
          : metric === "recall"
            ? recall
            : precision + recall === 0
              ? 0
              : (2 * precision * recall) / (precision + recall);
      const missing = [...wanted].filter((item) => !matched.has(item));
      return {
        scorerId: id,
        value,
        ...(value < 1 && {
          reason: [
            extra.length > 0 ? `unexpected [${extra.join("; ")}]` : undefined,
            missing.length > 0 ? `missing [${missing.join("; ")}]` : undefined,
          ]
            .filter((part): part is string => part !== undefined)
            .join(", "),
        }),
      };
    },
  };
}

/** Normalize one item: lowercase, trimmed, trailing punctuation dropped. */
export function normalizeSetItem(item: string): string {
  return item
    .trim()
    .toLowerCase()
    .replace(/[\s.!?;,]+$/u, "");
}

function stringList(value: unknown): string[] | undefined {
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) return undefined;
  return (value as string[]).map(normalizeSetItem).filter((item) => item.length > 0);
}
