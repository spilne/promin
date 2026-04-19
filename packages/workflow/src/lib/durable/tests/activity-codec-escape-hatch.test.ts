// ---------------------------------------------------------------------------
// Per-activity codec escape hatch — class instances & custom shapes.
//
// LosslessJsonCodec handles Date/BigInt/Map/Set/Error natively, but can't
// preserve user-defined class prototypes (new User(...), new Money(...)).
// ActivityOptions.codec — added in promin-sd5k — is the escape hatch: callers
// provide { encode, decode } to round-trip any value shape they need, with
// inheritance from the step codec / pipeline default when omitted.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { Codec } from "@promin/core";
import { runJournaledStep } from "../journaled-step.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import type { ActivityJournalStorage, JournalEntry } from "../activity-journal.ts";

// ---------------------------------------------------------------------------
// A user-defined class — the kind of thing LosslessJsonCodec can't revive
// ---------------------------------------------------------------------------

class Money {
  constructor(
    readonly cents: number,
    readonly currency: string,
  ) {}

  format(): string {
    return `${this.currency} ${(this.cents / 100).toFixed(2)}`;
  }
}

const moneyCodec: Codec<Money> = {
  encode: (m) => ({ cents: m.cents, currency: m.currency }),
  decode: (raw) => {
    const r = raw as { cents: number; currency: string };
    return new Money(r.cents, r.currency);
  },
};

/**
 * JSON-round-trip storage — mirrors what Postgres / Redis do to the journal.
 * Ensures the test exercises the encoded-then-parsed path, not the pure
 * in-memory one.
 */
class JsonJournalStorage extends InMemoryWorkflowStorage {
  async appendEntry(entry: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    activityName: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const cloned = JSON.parse(JSON.stringify(entry));
    await super.appendEntry(cloned);
  }
  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const cloned = JSON.parse(JSON.stringify(params));
    await super.completePendingEntry(cloned);
  }
}

describe("activity codec escape hatch — class instances", () => {
  it("custom codec preserves the Money prototype through fresh run + replay", async () => {
    const storage = new JsonJournalStorage();
    let capturedFresh: Money | undefined;
    let capturedReplay: Money | undefined;

    const body = function* (ctx: any) {
      const m = yield* ctx.activity("price", async () => new Money(1299, "USD"), {
        codec: moneyCodec,
      });
      return m;
    };

    capturedFresh = await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-money",
      stepName: "s",
      storage,
      body,
    });
    capturedReplay = await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-money",
      stepName: "s",
      storage,
      body,
    });

    expect(capturedFresh).toBeInstanceOf(Money);
    expect(capturedReplay).toBeInstanceOf(Money);
    expect(capturedFresh!.format()).toBe("USD 12.99");
    expect(capturedReplay!.format()).toBe("USD 12.99");
  });

  it("per-activity codec overrides the step-level codec", async () => {
    const storage = new JsonJournalStorage();
    // Step-level codec: loses the Money prototype (encode identity but decode
    // stays plain object).
    const lossyStep: Codec<unknown> = {
      encode: (v) => v,
      decode: (raw) => raw,
    };

    const body = function* (ctx: any) {
      const m = yield* ctx.activity(
        "price",
        async () => new Money(500, "EUR"),
        { codec: moneyCodec }, // per-activity override wins
      );
      return m;
    };

    const result = await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-override",
      stepName: "s",
      storage,
      codec: lossyStep,
      body,
    });

    expect(result).toBeInstanceOf(Money);
    expect(result.format()).toBe("EUR 5.00");
  });

  it("activity without its own codec inherits the step codec", async () => {
    const storage = new JsonJournalStorage();

    const body = function* (ctx: any) {
      // NO per-activity codec — inherit from step.
      return yield* ctx.activity("price", async () => new Money(7500, "GBP"));
    };

    const result = await runJournaledStep({
      input: undefined,
      prev: undefined,
      workflowId: "wf-inherit",
      stepName: "s",
      storage,
      codec: moneyCodec as Codec<unknown>, // step-level
      body,
    });

    expect(result).toBeInstanceOf(Money);
    expect(result.format()).toBe("GBP 75.00");
  });
});
