// ---------------------------------------------------------------------------
// CompactionStrategy — fluent builder for AutoCompactConfig.when predicates.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  CompactionStrategy,
  between,
  days,
  eq,
  gt,
  gte,
  hours,
  lt,
  lte,
  minutes,
  olderThan,
  seconds,
  within,
} from "../compaction-strategy.ts";
import type { AutoCompactSignals } from "../../agent/local-agent.ts";

function signals(overrides: Partial<AutoCompactSignals> = {}): AutoCompactSignals {
  return {
    totalCount: 0,
    uncompactedCount: 0,
    totalTokens: 0,
    uncompactedTokens: 0,
    lastCompactedAt: 0,
    threadKey: { namespaceId: "ns", resourceId: "res", threadId: "t1" },
    ...overrides,
  };
}

describe("CompactionStrategy", () => {
  describe("numeric gates", () => {
    it("messages(gt(5)) fires when uncompactedCount > 5, not when <=", () => {
      const when = CompactionStrategy.messages(gt(5)).build();
      expect(when(signals({ uncompactedCount: 6 }))).toBe(true);
      expect(when(signals({ uncompactedCount: 5 }))).toBe(false);
      expect(when(signals({ uncompactedCount: 0 }))).toBe(false);
    });

    it("tokens(gt(1000)) fires on uncompactedTokens > 1000", () => {
      const when = CompactionStrategy.tokens(gt(1000)).build();
      expect(when(signals({ uncompactedTokens: 1001 }))).toBe(true);
      expect(when(signals({ uncompactedTokens: 1000 }))).toBe(false);
    });

    it("totalMessages and totalTokens read the right fields", () => {
      const wMsgs = CompactionStrategy.totalMessages(gte(10)).build();
      const wToks = CompactionStrategy.totalTokens(gte(500)).build();
      expect(wMsgs(signals({ totalCount: 10, uncompactedCount: 0 }))).toBe(true);
      expect(wMsgs(signals({ totalCount: 9 }))).toBe(false);
      expect(wToks(signals({ totalTokens: 500, uncompactedTokens: 0 }))).toBe(true);
      expect(wToks(signals({ totalTokens: 499 }))).toBe(false);
    });

    it("between(10, 30) is inclusive on both ends", () => {
      const when = CompactionStrategy.messages(between(10, 30)).build();
      expect(when(signals({ uncompactedCount: 9 }))).toBe(false);
      expect(when(signals({ uncompactedCount: 10 }))).toBe(true);
      expect(when(signals({ uncompactedCount: 20 }))).toBe(true);
      expect(when(signals({ uncompactedCount: 30 }))).toBe(true);
      expect(when(signals({ uncompactedCount: 31 }))).toBe(false);
    });

    it("lt / lte / eq comparators behave as documented", () => {
      const wLt = CompactionStrategy.messages(lt(5)).build();
      const wLte = CompactionStrategy.messages(lte(5)).build();
      const wEq = CompactionStrategy.messages(eq(5)).build();
      expect(wLt(signals({ uncompactedCount: 4 }))).toBe(true);
      expect(wLt(signals({ uncompactedCount: 5 }))).toBe(false);
      expect(wLte(signals({ uncompactedCount: 5 }))).toBe(true);
      expect(wLte(signals({ uncompactedCount: 6 }))).toBe(false);
      expect(wEq(signals({ uncompactedCount: 5 }))).toBe(true);
      expect(wEq(signals({ uncompactedCount: 4 }))).toBe(false);
    });
  });

  describe("combinators", () => {
    it("or short-circuits — left fires, right is not consulted", () => {
      let rightCalls = 0;
      // Hand-roll a CompactionStrategy whose predicate increments a counter
      // when invoked. Easiest way is to build via a static factory and chain
      // through .or — to count right-hand calls we use a comparator that
      // increments on call.
      const right = CompactionStrategy.messages((value) => {
        rightCalls += 1;
        return value > 0;
      });
      const when = CompactionStrategy.messages(gt(5)).or(right).build();

      expect(when(signals({ uncompactedCount: 100 }))).toBe(true);
      expect(rightCalls).toBe(0);

      expect(when(signals({ uncompactedCount: 0 }))).toBe(false);
      expect(rightCalls).toBe(1); // right was consulted exactly once
    });

    it("and requires both", () => {
      const when = CompactionStrategy.messages(gt(5))
        .and(CompactionStrategy.tokens(gt(100)))
        .build();
      expect(when(signals({ uncompactedCount: 6, uncompactedTokens: 101 }))).toBe(true);
      expect(when(signals({ uncompactedCount: 6, uncompactedTokens: 100 }))).toBe(false);
      expect(when(signals({ uncompactedCount: 5, uncompactedTokens: 101 }))).toBe(false);
    });

    it("and short-circuits — left false skips right", () => {
      let rightCalls = 0;
      const right = CompactionStrategy.tokens((value) => {
        rightCalls += 1;
        return value > 0;
      });
      const when = CompactionStrategy.messages(gt(5)).and(right).build();
      expect(when(signals({ uncompactedCount: 0, uncompactedTokens: 100 }))).toBe(false);
      expect(rightCalls).toBe(0);
    });

    it("not() inverts", () => {
      const when = CompactionStrategy.messages(gt(5)).not().build();
      expect(when(signals({ uncompactedCount: 6 }))).toBe(false);
      expect(when(signals({ uncompactedCount: 5 }))).toBe(true);
    });

    it("chained .or(...).or(...) works left-to-right", () => {
      const when = CompactionStrategy.messages(gt(7))
        .or(CompactionStrategy.tokens(gt(7_000)))
        .or(CompactionStrategy.totalMessages(gt(50)))
        .build();

      // None of the three trigger.
      expect(when(signals({ uncompactedCount: 1, uncompactedTokens: 10, totalCount: 5 }))).toBe(
        false,
      );
      // Only the first triggers.
      expect(when(signals({ uncompactedCount: 8 }))).toBe(true);
      // Only the second triggers.
      expect(when(signals({ uncompactedTokens: 7_001 }))).toBe(true);
      // Only the third triggers.
      expect(when(signals({ totalCount: 51 }))).toBe(true);
    });

    it("strategies are immutable — combinators return new instances", () => {
      const base = CompactionStrategy.messages(gt(5));
      const combined = base.or(CompactionStrategy.tokens(gt(100)));
      // The base predicate must still behave like it did before .or.
      expect(base.build()(signals({ uncompactedCount: 6 }))).toBe(true);
      expect(base.build()(signals({ uncompactedCount: 0, uncompactedTokens: 200 }))).toBe(false);
      // The combined one ORs in the token rule.
      expect(combined.build()(signals({ uncompactedCount: 0, uncompactedTokens: 200 }))).toBe(true);
    });
  });

  describe("time gates", () => {
    it("lastCompactedAt(olderThan(hours(1))) fires past the window", () => {
      const when = CompactionStrategy.lastCompactedAt(olderThan(hours(1))).build();
      const now = Date.now();
      // 2 hours ago — older than the window.
      expect(when(signals({ lastCompactedAt: now - hours(2) }))).toBe(true);
      // 30 minutes ago — inside the window.
      expect(when(signals({ lastCompactedAt: now - minutes(30) }))).toBe(false);
    });

    it("lastCompactedAt(olderThan(...)) returns true when lastCompactedAt === 0 (never compacted)", () => {
      const when = CompactionStrategy.lastCompactedAt(olderThan(hours(1))).build();
      expect(when(signals({ lastCompactedAt: 0 }))).toBe(true);
    });

    it("lastCompactedAt(within(...)) returns false when never compacted", () => {
      const when = CompactionStrategy.lastCompactedAt(within(hours(1))).build();
      expect(when(signals({ lastCompactedAt: 0 }))).toBe(false);
      // Recently compacted — inside the window.
      expect(when(signals({ lastCompactedAt: Date.now() - minutes(10) }))).toBe(true);
      // Long ago — outside the window.
      expect(when(signals({ lastCompactedAt: Date.now() - hours(5) }))).toBe(false);
    });
  });

  describe("contextLimit sugar", () => {
    it("contextLimit(200_000, 0.7) is equivalent to tokens(gt(140_000))", () => {
      const a = CompactionStrategy.contextLimit(200_000, 0.7).build();
      const b = CompactionStrategy.tokens(gt(140_000)).build();
      for (const value of [0, 139_999, 140_000, 140_001, 200_000]) {
        const s = signals({ uncompactedTokens: value });
        expect(a(s)).toBe(b(s));
      }
    });

    it("contextLimit defaults fraction to 0.7", () => {
      const def = CompactionStrategy.contextLimit(100_000).build();
      const explicit = CompactionStrategy.contextLimit(100_000, 0.7).build();
      for (const value of [69_999, 70_000, 70_001, 100_000]) {
        const s = signals({ uncompactedTokens: value });
        expect(def(s)).toBe(explicit(s));
      }
    });
  });

  describe("duration helpers", () => {
    it("seconds / minutes / hours / days return ms", () => {
      expect(seconds(1)).toBe(1_000);
      expect(seconds(30)).toBe(30_000);
      expect(minutes(1)).toBe(60_000);
      expect(minutes(5)).toBe(300_000);
      expect(hours(1)).toBe(3_600_000);
      expect(hours(2)).toBe(7_200_000);
      expect(days(1)).toBe(86_400_000);
      expect(days(7)).toBe(604_800_000);
    });
  });
});
