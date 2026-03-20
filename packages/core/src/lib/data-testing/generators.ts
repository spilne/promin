// ---------------------------------------------------------------------------
// Test data generators — realistic fake data for pipeline testing
// ---------------------------------------------------------------------------

/** Seeded random number generator (deterministic). */
function seededRandom(seed: number): () => number {
  let s = seed;
  return () => {
    s = (s * 1664525 + 1013904223) % 4294967296;
    return s / 4294967296;
  };
}

export interface GeneratorContext {
  index: number;
  random: () => number;
}

export type Generator<T> = (ctx: GeneratorContext) => T;

export const generators = {
  /** Sequential integer starting from `start`. */
  sequence:
    (start = 1): Generator<number> =>
    (ctx) =>
      start + ctx.index,

  /** Random UUID. */
  uuid: (): Generator<string> => (ctx) => {
    const r = ctx.random;
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const v = c === "x" ? (r() * 16) | 0 : ((r() * 4) | 0) + 8;
      return v.toString(16);
    });
  },

  /** Pick randomly from a list of values. */
  pick:
    <T>(...values: T[]): Generator<T> =>
    (ctx) =>
      values[Math.floor(ctx.random() * values.length)]!,

  /** Random integer between min and max (inclusive). */
  int:
    (min: number, max: number): Generator<number> =>
    (ctx) =>
      Math.floor(ctx.random() * (max - min + 1)) + min,

  /** Random float between min and max. */
  float:
    (min: number, max: number): Generator<number> =>
    (ctx) =>
      Math.round((ctx.random() * (max - min) + min) * 100) / 100,

  /** Random boolean with given probability of true. */
  bool:
    (truePct = 0.5): Generator<boolean> =>
    (ctx) =>
      ctx.random() < truePct,

  /** Random date between from and to. */
  date:
    (from: Date, to: Date): Generator<string> =>
    (ctx) => {
      const ts = from.getTime() + ctx.random() * (to.getTime() - from.getTime());
      return new Date(ts).toISOString();
    },

  /** Random string of given length. */
  string:
    (length: number): Generator<string> =>
    (ctx) => {
      const chars = "abcdefghijklmnopqrstuvwxyz";
      let s = "";
      for (let i = 0; i < length; i++) {
        s += chars[Math.floor(ctx.random() * chars.length)];
      }
      return s;
    },

  /** Random email. */
  email: (): Generator<string> => (ctx) => {
    const name = generators.string(8)(ctx);
    const domain = generators.pick("gmail.com", "yahoo.com", "company.com")(ctx);
    return `${name}@${domain}`;
  },

  /** Null with given probability. Wraps another generator. */
  nullable:
    <T>(gen: Generator<T>, nullPct = 0.1): Generator<T | null> =>
    (ctx) =>
      ctx.random() < nullPct ? null : gen(ctx),

  /** Constant value. */
  constant:
    <T>(value: T): Generator<T> =>
    () =>
      value,
};

/** Generate N rows from a generator map. */
export function generateRows<T extends Record<string, unknown>>(
  gens: Record<string, Generator<unknown>>,
  count: number,
  seed = 42,
): T[] {
  const random = seededRandom(seed);
  const rows: T[] = [];

  for (let i = 0; i < count; i++) {
    const ctx: GeneratorContext = { index: i, random };
    const row: Record<string, unknown> = {};
    for (const [col, gen] of Object.entries(gens)) {
      row[col] = gen(ctx);
    }
    rows.push(row as T);
  }

  return rows;
}
