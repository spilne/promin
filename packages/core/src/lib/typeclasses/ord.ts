// ---------------------------------------------------------------------------
// Ord<T> — Orderable typeclass
// "I can compare two T values for ordering"
// ---------------------------------------------------------------------------

import type { Eq } from "./eq.ts";

export interface Ord<T> extends Eq<T> {
  compare(a: T, b: T): -1 | 0 | 1;
}

/** Ord for numbers. */
export const numberOrd: Ord<number> = {
  equals: (a, b) => a === b,
  compare: (a, b) => (a < b ? -1 : a > b ? 1 : 0),
};

/** Ord for strings (lexicographic). */
export const stringOrd: Ord<string> = {
  equals: (a, b) => a === b,
  compare: (a, b) => (a < b ? -1 : a > b ? 1 : 0),
};

/** Derive Ord from a key extractor function. */
export function ordBy<T>(toNumber: (value: T) => number): Ord<T> {
  return {
    equals: (a, b) => toNumber(a) === toNumber(b),
    compare: (a, b) => {
      const na = toNumber(a);
      const nb = toNumber(b);
      return na < nb ? -1 : na > nb ? 1 : 0;
    },
  };
}
