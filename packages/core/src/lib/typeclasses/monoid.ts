// ---------------------------------------------------------------------------
// Monoid<T> — Combinable typeclass
// "I can combine two T values and I have an identity element"
// ---------------------------------------------------------------------------

export interface Monoid<T> {
  readonly empty: T;
  concat(a: T, b: T): T;
}

/** Monoid for arrays — empty is [], concat is concatenation. */
export function arrayMonoid<T>(): Monoid<T[]> {
  return { empty: [], concat: (a, b) => [...a, ...b] };
}

/** Monoid for numbers under addition. */
export const sumMonoid: Monoid<number> = {
  empty: 0,
  concat: (a, b) => a + b,
};

/** Monoid for strings under concatenation. */
export const stringMonoid: Monoid<string> = {
  empty: "",
  concat: (a, b) => a + b,
};
