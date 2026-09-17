// ---------------------------------------------------------------------------
// RateLimitedConsolidator — caps distill cost per (namespace, resource).
// Pinned cases:
//   1. under-cap → delegates straight through
//   2. at-cap → throws ConsolidatorRateLimitError; inner not invoked
//   3. window slides — old episodes age out, new fires allowed
//   4. compactThread + distillResource always delegate (no rate limit on those)
//   5. no-resourceId calls bypass the limit entirely (delegate, surface inner error)
//   6. force: true does NOT bypass the rate limit (limit is cost-based, not dedup-based)
//   7. counts only kind==="distill" episodes, ignoring other resource episodes
//   8. per-namespace cap throttles tenant-wide spend even when every
//      individual resource is under its own per-resource cap
//   9. per-namespace cap allows distill while the namespace total is under it
//  10. per-resource cap is checked first when both caps would trip
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeClock } from "@promin/core";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import {
  RateLimitedConsolidator,
  ConsolidatorRateLimitError,
} from "../rate-limited-consolidator.ts";
import type { Consolidator, DistillThreadOptions } from "../consolidator.ts";
import type { EpisodicRecord, ScopedKey, ThreadKey } from "../types.ts";

function makeStubConsolidator() {
  let distillCalls = 0;
  let compactCalls = 0;
  let distillResourceCalls = 0;
  const inner: Consolidator = {
    async compactThread(_key: ThreadKey): Promise<EpisodicRecord> {
      compactCalls += 1;
      return {} as EpisodicRecord;
    },
    async distillResource(_key: ScopedKey): Promise<EpisodicRecord[]> {
      distillResourceCalls += 1;
      return [];
    },
    async distillThread(_key: ThreadKey, _opts?: DistillThreadOptions) {
      distillCalls += 1;
      return {} as EpisodicRecord;
    },
  };
  return {
    inner,
    counts: () => ({
      distill: distillCalls,
      compact: compactCalls,
      distillResource: distillResourceCalls,
    }),
  };
}

async function seedDistillEpisode(
  memory: InMemoryMemoryStore,
  scope: { namespaceId: string; resourceId: string },
  threadId: string,
) {
  return memory.appendResourceEpisode(scope, {
    summary: "seed",
    sourceThreadId: threadId,
    salience: 0.5,
    facts: [],
    metadata: { kind: "distill" },
  });
}

const KEY: ThreadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t-1" };
const SCOPE = { namespaceId: "acme", resourceId: "alice" };

describe("RateLimitedConsolidator", () => {
  it("delegates when under the cap", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 3,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "old-t");
    await rl.distillThread(KEY);
    expect(counts().distill).toBe(1);
  });

  it("throws ConsolidatorRateLimitError at the cap; inner not invoked", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 2,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "t-a");
    clock.advance(10_000);
    await seedDistillEpisode(memory, SCOPE, "t-b");

    let caught: ConsolidatorRateLimitError | undefined;
    try {
      await rl.distillThread(KEY);
    } catch (err) {
      caught = err as ConsolidatorRateLimitError;
    }
    expect(caught).toBeInstanceOf(ConsolidatorRateLimitError);
    expect(caught!.scope).toEqual({ kind: "resource", ...SCOPE });
    expect(caught!.max).toBe(2);
    expect(caught!.seen).toBe(2);
    // Oldest in-window episode was the seed at t=1_000_000.
    expect(caught!.oldestInWindowAt).toBe(1_000_000);
    // Retry-After: oldest ages out at 1_000_000 + 60_000 = 1_060_000.
    // Now is 1_010_000 → 50s until retry. Ceiling, min 1.
    expect(caught!.retryAfterSeconds(clock.currentTimeMs())).toBe(50);
    expect(counts().distill).toBe(0);
  });

  it("window slides — old episodes age out and re-allow distill", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 1,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "old"); // createdAt = 1_000_000
    await expect(rl.distillThread(KEY)).rejects.toBeInstanceOf(ConsolidatorRateLimitError);

    // Advance past window — old episode (createdAt 1_000_000) is now
    // outside [now-60s, now] (now=1_000_000+61_000=1_061_000, since=1_001_000).
    clock.advance(61_000);
    await rl.distillThread(KEY);
    expect(counts().distill).toBe(1);
  });

  it("compactThread + distillResource always delegate (no rate limit on those)", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 0, // even at zero — these paths are unaffected
      clock,
    });

    await rl.compactThread(KEY);
    await rl.distillResource(SCOPE);
    expect(counts().compact).toBe(1);
    expect(counts().distillResource).toBe(1);
  });

  it("no-resourceId calls bypass the limit (delegate to inner)", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 0,
      clock,
    });

    const noRes: ThreadKey = { namespaceId: "acme", threadId: "t-no-res" };
    await rl.distillThread(noRes);
    expect(counts().distill).toBe(1);
  });

  it("force: true does NOT bypass the rate limit (limit is cost-based, not dedup-based)", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 1,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "t");
    await expect(rl.distillThread(KEY, { force: true })).rejects.toBeInstanceOf(
      ConsolidatorRateLimitError,
    );
    expect(counts().distill).toBe(0);
  });

  it("counts only kind=='distill' episodes, ignoring other resource episodes", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 1,
      clock,
    });

    // Seed two NON-distill episodes (e.g. operator-uploaded docs, manual rollups).
    // They should not count toward the distill cap.
    await memory.appendResourceEpisode(SCOPE, {
      summary: "manual upload",
      sourceThreadId: null,
      salience: 0.5,
      facts: [],
      metadata: { kind: "manual" },
    });
    await memory.appendResourceEpisode(SCOPE, {
      summary: "no kind",
      sourceThreadId: null,
      salience: 0.5,
      facts: [],
    });

    await rl.distillThread(KEY); // first distill ever for resource → allowed
    expect(counts().distill).toBe(1);
  });

  it("per-namespace cap throttles tenant-wide spend under generous per-resource caps", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 5, // generous per-resource cap
      maxDistillsPerNamespacePerWindow: 2,
      clock,
    });

    // Two different resources in the same namespace, one distill each.
    // Each is well under the per-resource cap of 5, but the namespace
    // total is at the per-namespace cap of 2.
    await seedDistillEpisode(memory, SCOPE, "t-alice");
    clock.advance(10_000);
    await seedDistillEpisode(memory, { namespaceId: "acme", resourceId: "bob" }, "t-bob");

    let caught: ConsolidatorRateLimitError | undefined;
    try {
      await rl.distillThread(KEY);
    } catch (err) {
      caught = err as ConsolidatorRateLimitError;
    }
    expect(caught).toBeInstanceOf(ConsolidatorRateLimitError);
    expect(caught!.scope).toEqual({ kind: "namespace", namespaceId: "acme" });
    expect(caught!.max).toBe(2);
    expect(caught!.seen).toBe(2);
    // Oldest in-window distill across the namespace was the alice seed.
    expect(caught!.oldestInWindowAt).toBe(1_000_000);
    expect(counts().distill).toBe(0);
  });

  it("per-namespace cap allows distill while the namespace total is under it", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, counts } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 5,
      maxDistillsPerNamespacePerWindow: 3,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "t-alice");
    await seedDistillEpisode(memory, { namespaceId: "acme", resourceId: "bob" }, "t-bob");
    // Namespace total = 2, cap 3 → allowed.
    await rl.distillThread(KEY);
    expect(counts().distill).toBe(1);
  });

  it("checks the per-resource cap first when both caps would trip", async () => {
    const clock = FakeClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner } = makeStubConsolidator();
    const rl = new RateLimitedConsolidator(inner, memory, {
      windowMs: 60_000,
      maxDistillsPerWindow: 1,
      maxDistillsPerNamespacePerWindow: 1,
      clock,
    });

    await seedDistillEpisode(memory, SCOPE, "t");
    let caught: ConsolidatorRateLimitError | undefined;
    try {
      await rl.distillThread(KEY);
    } catch (err) {
      caught = err as ConsolidatorRateLimitError;
    }
    // Both caps sit at 1 and both are hit — the resource check runs first.
    expect(caught!.scope).toEqual({ kind: "resource", ...SCOPE });
  });
});
