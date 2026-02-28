// ---------------------------------------------------------------------------
// Eq<T> — Equality typeclass
// "I can compare two T values"
// ---------------------------------------------------------------------------

import type { Codec } from "./codec.ts";

export interface Eq<T> {
  equals(a: T, b: T): boolean;
}

/** Default: JSON deep equality via stringify comparison. */
export const JsonEq: Eq<unknown> = {
  equals: (a, b) => JSON.stringify(a) === JSON.stringify(b),
};

/** Derive Eq from a Codec (serialize both, compare serialized forms). */
export function eqFromCodec<T>(codec: Codec<T>): Eq<T> {
  return {
    equals: (a, b) => JSON.stringify(codec.encode(a)) === JSON.stringify(codec.encode(b)),
  };
}
