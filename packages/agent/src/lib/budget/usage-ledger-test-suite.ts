// ---------------------------------------------------------------------------
// Portable `UsageLedger` conformance suite. Every implementation must pass.
//
//   import { usageLedgerTestSuite } from "@promin/agent/testing";
//   usageLedgerTestSuite(() => {
//     const clock = FakeWallClock.create(0);
//     return { ledger: new InMemoryUsageLedger({ clock }), age: async (ms) => clock.advance(ms) };
//   });
//
// `age(ms)` makes every recorded row `ms` older relative to the store's
// clock — advance a fake clock, or shift stored timestamps back on a store
// that stamps rows with the database clock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { EMPTY_USAGE, type BudgetUsage } from "./budget.ts";
import type { UsageLedger } from "./usage-ledger.ts";

export interface UsageLedgerHarness {
  readonly ledger: UsageLedger;
  /** Make every recorded row `ms` older on the store's clock. */
  age(ms: number): Promise<void>;
}

export type UsageLedgerFactory = () => UsageLedgerHarness | Promise<UsageLedgerHarness>;

function spend(partial: Partial<BudgetUsage>): BudgetUsage {
  return { ...EMPTY_USAGE, ...partial };
}

export function usageLedgerTestSuite(factory: UsageLedgerFactory): void {
  describe("UsageLedger conformance", () => {
    it("sums a namespace's turns and a thread's turns", async () => {
      const { ledger } = await factory();
      await ledger.record({
        turnId: "t1",
        namespaceId: "ns",
        threadId: "a",
        agentId: "bot",
        usage: spend({ inputTokens: 100, outputTokens: 10, costUsd: 0.5, llmCalls: 2 }),
      });
      await ledger.record({
        turnId: "t2",
        namespaceId: "ns",
        threadId: "b",
        usage: spend({ inputTokens: 50, cacheReadTokens: 7, toolCalls: 3, llmCalls: 1 }),
      });
      await ledger.record({
        turnId: "t3",
        namespaceId: "other",
        threadId: "a",
        usage: spend({ inputTokens: 999 }),
      });

      expect(await ledger.total({ namespaceId: "ns" })).toEqual(
        spend({
          inputTokens: 150,
          outputTokens: 10,
          cacheReadTokens: 7,
          costUsd: 0.5,
          llmCalls: 3,
          toolCalls: 3,
        }),
      );
      expect(await ledger.total({ namespaceId: "ns", threadId: "a" })).toEqual(
        spend({ inputTokens: 100, outputTokens: 10, costUsd: 0.5, llmCalls: 2 }),
      );
      expect(await ledger.total({ namespaceId: "empty" })).toEqual(EMPTY_USAGE);
    });

    it("records a turn once: a retried record changes nothing", async () => {
      const { ledger } = await factory();
      const entry = { turnId: "t1", namespaceId: "ns", usage: spend({ outputTokens: 40 }) };
      expect(await ledger.record(entry)).toBe(true);
      expect(await ledger.record(entry)).toBe(false);
      expect((await ledger.total({ namespaceId: "ns" })).outputTokens).toBe(40);
    });

    it("keeps the larger spend of a turn recorded again (failed, then run on)", async () => {
      const { ledger } = await factory();
      await ledger.record({
        turnId: "t1",
        namespaceId: "ns",
        usage: spend({ inputTokens: 100, outputTokens: 40, llmCalls: 1, costUsd: 0.5 }),
      });
      // A late, smaller record lowers nothing.
      await ledger.record({
        turnId: "t1",
        namespaceId: "ns",
        usage: spend({ inputTokens: 50, outputTokens: 10, llmCalls: 1 }),
      });
      expect(
        await ledger.record({
          turnId: "t1",
          namespaceId: "ns",
          usage: spend({ inputTokens: 300, outputTokens: 40, llmCalls: 3, costUsd: 0.75 }),
        }),
      ).toBe(false);
      expect(await ledger.total({ namespaceId: "ns" })).toEqual(
        spend({ inputTokens: 300, outputTokens: 40, llmCalls: 3, costUsd: 0.75 }),
      );
    });

    it("keys turns per namespace", async () => {
      const { ledger } = await factory();
      await ledger.record({ turnId: "t1", namespaceId: "a", usage: spend({ outputTokens: 1 }) });
      expect(
        await ledger.record({ turnId: "t1", namespaceId: "b", usage: spend({ outputTokens: 2 }) }),
      ).toBe(true);
      expect((await ledger.total({ namespaceId: "b" })).outputTokens).toBe(2);
    });

    it("counts every concurrent record exactly once", async () => {
      const { ledger } = await factory();
      const turns = Array.from({ length: 20 }, (_, i) => `t${i}`);
      const results = await Promise.all([
        ...turns.map((turnId) =>
          ledger.record({
            turnId,
            namespaceId: "ns",
            usage: spend({ outputTokens: 5, llmCalls: 1 }),
          }),
        ),
        // The same turns again, racing their first record.
        ...turns.map((turnId) =>
          ledger.record({
            turnId,
            namespaceId: "ns",
            usage: spend({ outputTokens: 5, llmCalls: 1 }),
          }),
        ),
      ]);
      expect(results.filter(Boolean)).toHaveLength(20);
      const total = await ledger.total({ namespaceId: "ns" });
      expect(total.outputTokens).toBe(100);
      expect(total.llmCalls).toBe(20);
    });

    it("sums only the window's turns", async () => {
      const { ledger, age } = await factory();
      await ledger.record({ turnId: "old", namespaceId: "ns", usage: spend({ outputTokens: 7 }) });
      await age(60_000);
      await ledger.record({ turnId: "new", namespaceId: "ns", usage: spend({ outputTokens: 3 }) });

      expect((await ledger.total({ namespaceId: "ns", windowMs: 30_000 })).outputTokens).toBe(3);
      expect((await ledger.total({ namespaceId: "ns", windowMs: 120_000 })).outputTokens).toBe(10);
      expect((await ledger.total({ namespaceId: "ns" })).outputTokens).toBe(10);
    });

    it("keeps fractional costs", async () => {
      const { ledger } = await factory();
      await ledger.record({ turnId: "a", namespaceId: "ns", usage: spend({ costUsd: 0.000123 }) });
      await ledger.record({ turnId: "b", namespaceId: "ns", usage: spend({ costUsd: 0.25 }) });
      expect((await ledger.total({ namespaceId: "ns" })).costUsd).toBeCloseTo(0.250123, 9);
    });
  });
}
