// ---------------------------------------------------------------------------
// RateLimitedConsolidator — caps distill cost per (namespace, resource) and
// per namespace, counted atomically in the memory store.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import {
  RateLimitedConsolidator,
  ConsolidatorRateLimitError,
  type ConsolidatorRateLimitConfig,
} from "../rate-limited-consolidator.ts";
import type { Consolidator, DistillThreadOptions } from "../consolidator.ts";
import type { EpisodicRecord, ScopedKey, ThreadKey } from "../types.ts";

const KEY: ThreadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t-1" };
const SCOPE = { namespaceId: "acme", resourceId: "alice" };

/**
 * An inner consolidator that writes a distill episode per call (like the
 * real one) after an optional gate, or throws when `fail` is set.
 */
function makeInner(params: { memory: InMemoryMemoryStore; gate?: Promise<void>; fail?: boolean }) {
  const calls = { distill: 0, compact: 0, distillResource: 0 };
  const inner: Consolidator = {
    async compactThread(_key: ThreadKey): Promise<EpisodicRecord> {
      calls.compact += 1;
      return params.memory.appendThreadEpisode(KEY, { summary: "c" });
    },
    async distillResource(_key: ScopedKey): Promise<EpisodicRecord[]> {
      calls.distillResource += 1;
      return [];
    },
    async distillThread(key: ThreadKey, _opts?: DistillThreadOptions) {
      calls.distill += 1;
      await params.gate;
      if (params.fail) throw new Error("llm down");
      return params.memory.appendResourceEpisode(
        { namespaceId: key.namespaceId, resourceId: key.resourceId! },
        { summary: "d", sourceThreadId: key.threadId, metadata: { kind: "distill" } },
      );
    },
  };
  return { inner, calls };
}

function limited(params: {
  memory: InMemoryMemoryStore;
  inner: Consolidator;
  limits: ConsolidatorRateLimitConfig;
}): RateLimitedConsolidator {
  return new RateLimitedConsolidator(params);
}

describe("RateLimitedConsolidator", () => {
  it("delegates when under the cap", async () => {
    const memory = new InMemoryMemoryStore();
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 2 } });
    await rl.distillThread(KEY, { force: true });
    expect(calls.distill).toBe(1);
  });

  it("throws at the cap without invoking inner, with the store's retry-after", async () => {
    const clock = FakeWallClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 2 } });
    await rl.distillThread(KEY, { force: true });
    clock.advance(10_000);
    await rl.distillThread({ ...KEY, threadId: "t-2" });
    const err = await rl.distillThread({ ...KEY, threadId: "t-3" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ConsolidatorRateLimitError);
    const limit = err as ConsolidatorRateLimitError;
    expect(limit.scope).toEqual({ kind: "resource", ...SCOPE });
    expect(limit.seen).toBe(2);
    expect(limit.retryAfterMs).toBe(50_000);
    expect(limit.retryAfterSeconds()).toBe(50);
    expect(calls.distill).toBe(2);
  });

  it("concurrent distills never overshoot the cap", async () => {
    const memory = new InMemoryMemoryStore();
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    const { inner, calls } = makeInner({ memory, gate });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 2 } });
    // All checks run before any inner call has written its episode: a
    // check-then-act limiter would let all five through.
    const runs = Array.from({ length: 5 }, (_, i) =>
      rl.distillThread({ ...KEY, threadId: `t-${i}` }).then(
        () => "ok",
        (e: unknown) => (e instanceof ConsolidatorRateLimitError ? "limited" : "error"),
      ),
    );
    await new Promise((r) => setTimeout(r, 0));
    open();
    const outcomes = await Promise.all(runs);
    expect(outcomes.filter((o) => o === "ok")).toHaveLength(2);
    expect(outcomes.filter((o) => o === "limited")).toHaveLength(3);
    expect(calls.distill).toBe(2);
  });

  it("the window slides: old distills age out", async () => {
    const clock = FakeWallClock.create(1_000_000);
    const memory = new InMemoryMemoryStore({ clock });
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 1 } });
    await rl.distillThread(KEY);
    await expect(rl.distillThread({ ...KEY, threadId: "t-2" })).rejects.toBeInstanceOf(
      ConsolidatorRateLimitError,
    );
    clock.advance(60_001);
    await rl.distillThread({ ...KEY, threadId: "t-2" });
    expect(calls.distill).toBe(2);
  });

  it("a failed distill gives its slot back", async () => {
    const memory = new InMemoryMemoryStore();
    const failing = makeInner({ memory, fail: true });
    const rl = limited({
      memory,
      inner: failing.inner,
      limits: { windowMs: 60_000, maxDistillsPerWindow: 1 },
    });
    await expect(rl.distillThread(KEY)).rejects.toThrow("llm down");
    const ok = makeInner({ memory });
    const rl2 = limited({
      memory,
      inner: ok.inner,
      limits: { windowMs: 60_000, maxDistillsPerWindow: 1 },
    });
    await rl2.distillThread(KEY);
    expect(ok.calls.distill).toBe(1);
  });

  it("an already-distilled thread (no force) takes no slot", async () => {
    const memory = new InMemoryMemoryStore();
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 1 } });
    await rl.distillThread(KEY);
    // Re-fires on the same thread (autoDistill after every turn) are no-ops.
    await rl.distillThread(KEY);
    await rl.distillThread(KEY);
    expect(calls.distill).toBe(3);
    // force re-distills cost a slot: the one slot is taken.
    await expect(rl.distillThread(KEY, { force: true })).rejects.toBeInstanceOf(
      ConsolidatorRateLimitError,
    );
  });

  it("compactThread delegates without a limit", async () => {
    const memory = new InMemoryMemoryStore();
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 0 } });
    await rl.compactThread(KEY);
    expect(calls.compact).toBe(1);
  });

  it("distillResource counts every thread it distills and stops at the cap", async () => {
    const memory = new InMemoryMemoryStore();
    for (const threadId of ["t-1", "t-2", "t-3"]) {
      await memory.appendMessages({ ...KEY, threadId }, [
        { role: "user", content: "hi" },
        { role: "assistant", content: "hello" },
      ]);
    }
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 2 } });
    const episodes = await rl.distillResource(SCOPE);
    expect(episodes).toHaveLength(2);
    expect(calls.distill).toBe(2);
    // The inner sweep (uncounted) was never used.
    expect(calls.distillResource).toBe(0);
  });

  it("no-resourceId calls delegate to inner", async () => {
    const memory = new InMemoryMemoryStore();
    const { inner, calls } = makeInner({ memory });
    const rl = limited({ memory, inner, limits: { windowMs: 60_000, maxDistillsPerWindow: 0 } });
    const noRes: ThreadKey = { namespaceId: "acme", threadId: "t-1" };
    await rl.distillThread(noRes).catch(() => undefined);
    expect(calls.distill).toBe(1);
  });

  it("the per-namespace cap throttles tenant-wide spend", async () => {
    const memory = new InMemoryMemoryStore();
    const { inner, calls } = makeInner({ memory });
    const rl = limited({
      memory,
      inner,
      limits: { windowMs: 60_000, maxDistillsPerWindow: 10, maxDistillsPerNamespacePerWindow: 2 },
    });
    await rl.distillThread({ ...KEY, resourceId: "r1" });
    await rl.distillThread({ ...KEY, resourceId: "r2" });
    const err = await rl
      .distillThread({ ...KEY, resourceId: "r3" })
      .catch((e: unknown) => e as ConsolidatorRateLimitError);
    expect(err).toBeInstanceOf(ConsolidatorRateLimitError);
    expect((err as ConsolidatorRateLimitError).scope).toEqual({
      kind: "namespace",
      namespaceId: "acme",
    });
    expect(calls.distill).toBe(2);
  });
});
