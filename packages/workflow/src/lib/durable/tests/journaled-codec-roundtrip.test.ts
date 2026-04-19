// ---------------------------------------------------------------------------
// Activity-level codec round-trip tests for journaled steps.
//
// Mirrors the step-boundary fix at the activity boundary: a `ctx.activity()`
// that returns a Date / BigInt / Map / Set / Error must produce the same
// shape on replay as on fresh run, even when the journal is JSON-round-tripped
// by a real storage backend in between.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { JsonCodec } from "@promin/core";
import type { ActivityJournalStorage, JournalEntry } from "../activity-journal.ts";
import { runJournaledStep } from "../journaled-step.ts";

/**
 * A minimal in-memory journal storage whose `appendEntry` puts the value
 * through JSON.stringify + JSON.parse before storing — exactly what Redis /
 * Postgres journals do. If the codec is pulling its weight, the replay path
 * hydrates values back to their original types.
 */
class JsonJournalStorage implements ActivityJournalStorage {
  private readonly entries = new Map<string, JournalEntry[]>();

  private key(workflowId: string, stepName: string): string {
    return `${workflowId}\x00${stepName}`;
  }

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    return this.entries.get(this.key(workflowId, stepName)) ?? [];
  }

  async appendEntry(entry: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = entry.branchPath ?? "";
    const normalized = { ...entry, branchPath };
    const roundTripped: JournalEntry = JSON.parse(JSON.stringify(normalized));
    roundTripped.branchPath = normalized.branchPath;
    const key = this.key(entry.workflowId, entry.stepName);
    const list = this.entries.get(key) ?? [];
    list.push(roundTripped);
    this.entries.set(key, list);
  }
}

describe("journaled activity — codec round-trip on replay", () => {
  it("Date returned by an activity survives replay", async () => {
    const storage = new JsonJournalStorage();

    const run = () =>
      runJournaledStep<{ ignored: boolean }, unknown, Date>({
        input: { ignored: true },
        prev: undefined,
        workflowId: "wf-date",
        stepName: "step",
        storage,
        body: function* (ctx) {
          const d = yield* ctx.activity("fetch", async () => new Date("2026-04-16T00:00:00Z"));
          return d;
        },
      });

    const fresh = await run();
    const replay = await run();

    expect(fresh).toBeInstanceOf(Date);
    expect(replay).toBeInstanceOf(Date);
    expect(fresh.getTime()).toBe(replay.getTime());
  });

  it("BigInt survives replay", async () => {
    const storage = new JsonJournalStorage();
    const big = 99999999999999999n;

    const run = () =>
      runJournaledStep<unknown, unknown, bigint>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-bi",
        stepName: "step",
        storage,
        body: function* (ctx) {
          const v = yield* ctx.activity("fetch", async () => big);
          return v;
        },
      });

    const fresh = await run();
    const replay = await run();

    expect(typeof fresh).toBe("bigint");
    expect(typeof replay).toBe("bigint");
    expect(fresh).toBe(big);
    expect(replay).toBe(big);
  });

  it("Map with Date keys survives replay", async () => {
    const storage = new JsonJournalStorage();

    const run = () =>
      runJournaledStep<unknown, unknown, Map<Date, string>>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-map",
        stepName: "step",
        storage,
        body: function* (ctx) {
          const m = yield* ctx.activity(
            "fetch",
            async () =>
              new Map<Date, string>([
                [new Date("2026-01-01Z"), "alpha"],
                [new Date("2026-06-01Z"), "beta"],
              ]),
          );
          return m;
        },
      });

    const fresh = await run();
    const replay = await run();

    expect(fresh).toBeInstanceOf(Map);
    expect(replay).toBeInstanceOf(Map);
    const freshEntries = [...fresh.entries()];
    const replayEntries = [...replay.entries()];
    expect(replayEntries.length).toBe(2);
    expect(replayEntries[0]![0]).toBeInstanceOf(Date);
    expect(replayEntries[0]![0].getTime()).toBe(freshEntries[0]![0].getTime());
  });

  it("per-activity codec override takes precedence over the step default", async () => {
    const storage = new JsonJournalStorage();

    const run = () =>
      runJournaledStep<unknown, unknown, { raw: unknown; decoded: Date }>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-override",
        stepName: "step",
        storage,
        body: function* (ctx) {
          // Explicit JsonCodec — opt out of lossless round-trip.
          const raw = yield* ctx.activity(
            "raw-date",
            async () => new Date("2026-04-16T00:00:00Z"),
            { codec: JsonCodec },
          );
          // Default (inherits step codec = LosslessJsonCodec) — lossless.
          const decoded = yield* ctx.activity(
            "decoded-date",
            async () => new Date("2026-04-16T00:00:00Z"),
          );
          return { raw, decoded };
        },
      });

    await run(); // first run writes the journal
    const replay = await run(); // second run reads it

    // `raw-date` went through identity codec; JSON storage stringified the
    // Date, JsonCodec.decode didn't revive it → string.
    expect(typeof replay.raw).toBe("string");
    // `decoded-date` used LosslessJsonCodec → round-tripped back to Date.
    expect(replay.decoded).toBeInstanceOf(Date);
  });

  it("activity inherits step codec when no override is passed", async () => {
    const storage = new JsonJournalStorage();

    // Step codec is explicit JsonCodec (identity) → activities inherit it →
    // replay cannot revive the Date (storage already stringified it).
    const fresh = await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-inherit-1",
      stepName: "step",
      storage,
      codec: JsonCodec,
      body: function* (ctx) {
        const v = yield* ctx.activity("a", async () => new Date("2026-04-16Z"));
        return v;
      },
    });
    // Fresh run: `encoded` is the in-memory Date (identity), and decode
    // returns it unchanged — body saw a real Date.
    expect(fresh).toBeInstanceOf(Date);

    const replay = await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-inherit-1",
      stepName: "step",
      storage,
      codec: JsonCodec,
      body: function* (ctx) {
        const v = yield* ctx.activity("a", async () => new Date("2026-04-16Z"));
        return v;
      },
    });
    // Replay reads the JSON-round-tripped string from storage; identity
    // codec doesn't hydrate it, so we see the asymmetry JsonCodec causes —
    // confirms the inheritance wired through correctly.
    expect(typeof replay).toBe("string");
  });

  it("fresh-run activity result is round-tripped through the codec", async () => {
    const storage = new JsonJournalStorage();
    let sawDate = false;

    await runJournaledStep<unknown, unknown, unknown>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-fresh",
      stepName: "step",
      storage,
      body: function* (ctx) {
        const d = yield* ctx.activity("produce", async () => new Date("2026-04-16Z"));
        sawDate = d instanceof Date;
        return undefined;
      },
    });

    // Critical: on fresh run the activity return value is the decoded form,
    // so body code doesn't have to branch on whether it's a fresh run vs
    // replay. The symmetry the codec is there to guarantee.
    expect(sawDate).toBe(true);
  });
});
