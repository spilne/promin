// ---------------------------------------------------------------------------
// `CompactionStrategy` — fluent builder for `AutoCompactConfig.when` predicates.
//
// `AutoCompactConfig.when` already accepts a raw closure over `AutoCompactSignals`,
// which is fine for one-liners. Once you start composing rules ("messages over 30
// OR tokens over 8k OR last compact older than an hour"), inline boolean ladders
// get noisy. This module is a thin DSL on top of the same predicate signature —
// purely additive, no behaviour changes inside `LocalAgent`.
//
// Usage:
//
//     import { CompactionStrategy, gt, olderThan, hours } from "@promin/agent";
//
//     // Equivalent ways to express "compact when uncompacted message count > 7":
//     // 1. Numeric struct fields on AutoCompactConfig:
//     autoCompact: { messageThreshold: 7 };
//     // 2. Raw predicate:
//     autoCompact: { when: ({ uncompactedCount }) => uncompactedCount > 7 };
//     // 3. Builder:
//     autoCompact: {
//       when: CompactionStrategy.messages(gt(7)).build(),
//     };
//
//     // Compound rule — fire on any of:
//     //   uncompacted messages > 7
//     //   uncompacted tokens > 7000
//     //   last compact older than 1 hour (or never compacted)
//     const strategy = CompactionStrategy.messages(gt(7))
//       .or(CompactionStrategy.tokens(gt(7_000)))
//       .or(CompactionStrategy.lastCompactedAt(olderThan(hours(1))));
//
//     autoCompact: { when: strategy.build() };
//
// Operators are pure value-level — `gt(n)` returns `(v) => v > n`, `olderThan(ms)`
// returns `(t) => Date.now() - t > ms` (treating 0 / never-compacted as "older
// than anything").
// ---------------------------------------------------------------------------

import type { AutoCompactSignals } from "../agent/local-agent.ts";

// --- Comparators -----------------------------------------------------------

/** Compares a numeric field of `AutoCompactSignals` against a fixed bound. */
export type NumberComparator = (value: number) => boolean;

/** Compares a `lastCompactedAt` timestamp against `Date.now()`. */
export type TimeComparator = (lastCompactedAt: number) => boolean;

/** `value > n` */
export const gt =
  (n: number): NumberComparator =>
  (value) =>
    value > n;
/** `value < n` */
export const lt =
  (n: number): NumberComparator =>
  (value) =>
    value < n;
/** `value >= n` */
export const gte =
  (n: number): NumberComparator =>
  (value) =>
    value >= n;
/** `value <= n` */
export const lte =
  (n: number): NumberComparator =>
  (value) =>
    value <= n;
/** `value === n` */
export const eq =
  (n: number): NumberComparator =>
  (value) =>
    value === n;
/** `lo <= value <= hi` (inclusive) */
export const between =
  (lo: number, hi: number): NumberComparator =>
  (value) =>
    value >= lo && value <= hi;

/**
 * Fires when `Date.now() - lastCompactedAt > ms`. A `lastCompactedAt` of 0
 * means "never compacted" and is treated as older than any window — useful
 * as the leading clause in an OR chain so brand-new threads still trigger.
 */
export const olderThan =
  (ms: number): TimeComparator =>
  (lastCompactedAt) => {
    if (lastCompactedAt === 0) return true;
    return Date.now() - lastCompactedAt > ms;
  };

/**
 * Fires when `Date.now() - lastCompactedAt <= ms`. A `lastCompactedAt` of 0
 * (never compacted) returns `false` — there's no "recent" compact to be
 * inside the window of.
 */
export const within =
  (ms: number): TimeComparator =>
  (lastCompactedAt) => {
    if (lastCompactedAt === 0) return false;
    return Date.now() - lastCompactedAt <= ms;
  };

// --- Duration helpers ------------------------------------------------------
//
// Plain ms math. Named for readability at the call site:
//   olderThan(hours(1))  reads as "older than one hour"
// rather than `olderThan(60 * 60 * 1000)`.

export const seconds = (n: number): number => n * 1_000;
export const minutes = (n: number): number => n * 60_000;
export const hours = (n: number): number => n * 3_600_000;
export const days = (n: number): number => n * 86_400_000;

// --- Strategy builder ------------------------------------------------------

type Predicate = (signals: AutoCompactSignals) => boolean;

/**
 * Immutable, chainable builder for `AutoCompactConfig.when` predicates.
 *
 * Each static factory returns a fresh strategy gated on a single signal field.
 * `or` / `and` / `not` return new strategies — never mutate the receiver.
 * `build()` is the terminal: it returns the closure to drop into `autoCompact.when`.
 */
export class CompactionStrategy {
  private constructor(private readonly predicate: Predicate) {}

  // --- Static factories ----------------------------------------------------

  /** Gate on `uncompactedCount` — messages since the last compact episode. */
  static messages(cmp: NumberComparator): CompactionStrategy {
    return new CompactionStrategy((s) => cmp(s.uncompactedCount));
  }

  /** Gate on `uncompactedTokens` — tokens since the last compact episode. */
  static tokens(cmp: NumberComparator): CompactionStrategy {
    return new CompactionStrategy((s) => cmp(s.uncompactedTokens));
  }

  /** Gate on `totalCount` — every persisted message in the thread. */
  static totalMessages(cmp: NumberComparator): CompactionStrategy {
    return new CompactionStrategy((s) => cmp(s.totalCount));
  }

  /** Gate on `totalTokens` — every persisted message in the thread. */
  static totalTokens(cmp: NumberComparator): CompactionStrategy {
    return new CompactionStrategy((s) => cmp(s.totalTokens));
  }

  /**
   * Gate on `lastCompactedAt` — the createdAt ms of the most recent compact
   * episode (0 when none exists). Pair with `olderThan` / `within`.
   */
  static lastCompactedAt(cmp: TimeComparator): CompactionStrategy {
    return new CompactionStrategy((s) => cmp(s.lastCompactedAt));
  }

  /**
   * Sugar for `tokens(gt(limit * fraction))`. Mirrors the same-named knob on
   * `AutoCompactConfig` (`contextLimit` + `compressAt`) so callers can pick
   * either spelling. `fraction` defaults to 0.7 — leaves the model 30%
   * headroom for the assistant's reply.
   */
  static contextLimit(limit: number, fraction = 0.7): CompactionStrategy {
    const threshold = limit * fraction;
    return CompactionStrategy.tokens(gt(threshold));
  }

  // --- Combinators ---------------------------------------------------------

  /** Logical OR. Short-circuits — `other` is not consulted when `this` fires. */
  or(other: CompactionStrategy): CompactionStrategy {
    const left = this.predicate;
    const right = other.predicate;
    return new CompactionStrategy((s) => left(s) || right(s));
  }

  /** Logical AND. Short-circuits — `other` is not consulted when `this` is false. */
  and(other: CompactionStrategy): CompactionStrategy {
    const left = this.predicate;
    const right = other.predicate;
    return new CompactionStrategy((s) => left(s) && right(s));
  }

  /** Logical NOT. */
  not(): CompactionStrategy {
    const inner = this.predicate;
    return new CompactionStrategy((s) => !inner(s));
  }

  // --- Terminal ------------------------------------------------------------

  /** Returns the predicate closure for `AutoCompactConfig.when`. */
  build(): Predicate {
    return this.predicate;
  }
}
