// ---------------------------------------------------------------------------
// Memory scoring: decay ages salience without a floor of zero, and hybrid
// ranking gates on relevance, then orders by importance and recency.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { decayedSalience, memoizeEmbedder, rankCandidates } from "../scoring.ts";

const DAY = 24 * 60 * 60 * 1000;
const NOW = 1_800_000_000_000;

describe("decayedSalience", () => {
  it("halves per half-life, floors at 20%, and is a no-op without a half-life", () => {
    const at = (days: number) =>
      decayedSalience({
        salience: 1,
        occurredAt: NOW - days * DAY,
        now: NOW,
        halfLifeMs: 30 * DAY,
      });
    expect(at(0)).toBe(1);
    expect(at(30)).toBeCloseTo(0.5);
    expect(at(10_000)).toBeCloseTo(0.2);
    expect(decayedSalience({ salience: 0.7, occurredAt: 0, now: NOW })).toBe(0.7);
  });
});

describe("rankCandidates", () => {
  const rank = (
    query: string,
    candidates: Array<{ text: string; importance: number; ageDays: number }>,
    embedder?: { embed(text: string): Promise<number[]> },
  ) =>
    rankCandidates({
      query,
      candidates: candidates.map((c) => ({
        text: c.text,
        importance: c.importance,
        at: NOW - c.ageDays * DAY,
      })),
      now: NOW,
      recencyHalfLifeMs: 90 * DAY,
      ...(embedder && { embedder }),
    });

  it("drops irrelevant candidates however important, and prefers rarer terms", async () => {
    const ranked = await rank("postgres migration", [
      { text: "user loves hiking in the alps", importance: 1, ageDays: 0 },
      { text: "the migration touched many tables", importance: 0.5, ageDays: 5 },
      { text: "postgres migration plan approved", importance: 0.5, ageDays: 5 },
    ]);
    expect(ranked.map((r) => r.index)).toEqual([2, 1]);
  });

  it("breaks relevance ties by recency and importance", async () => {
    const ranked = await rank("deploy", [
      { text: "deploy failed", importance: 0.5, ageDays: 400 },
      { text: "deploy failed", importance: 0.5, ageDays: 1 },
    ]);
    expect(ranked.map((r) => r.index)).toEqual([1, 0]);
  });

  it("matches on embedding similarity alone, and falls back to lexical when the embedder throws", async () => {
    const vectors: Record<string, number[]> = {
      "where do I live": [1, 0],
      "resides in Lisbon": [0.9, 0.1],
      "likes tea": [0, 1],
    };
    const embedder = { embed: async (t: string) => vectors[t] ?? [0, 0] };
    const candidates = [
      { text: "likes tea", importance: 0.5, ageDays: 1 },
      { text: "resides in Lisbon", importance: 0.5, ageDays: 1 },
    ];
    expect((await rank("where do I live", candidates, embedder)).map((r) => r.index)).toEqual([1]);

    const broken = {
      embed: async (): Promise<number[]> => {
        throw new Error("embedding service down");
      },
    };
    expect(await rank("where do I live", candidates, broken)).toEqual([]);
    expect((await rank("tea", candidates, broken)).map((r) => r.index)).toEqual([0]);
  });
});

describe("embedding calls", () => {
  const candidates = Array.from({ length: 30 }, (_, i) => ({
    text: `unrelated note ${i}`,
    importance: 0.5,
    at: NOW,
  }));

  it("bounds calls in flight and reports a failing embedder", async () => {
    let inFlight = 0;
    let peak = 0;
    const slow = {
      embed: async () => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise((r) => setTimeout(r, 1));
        inFlight--;
        return [1, 0];
      },
    };
    await rankCandidates({
      query: "anything",
      candidates,
      now: NOW,
      recencyHalfLifeMs: 30 * DAY,
      embedder: slow,
    });
    expect(peak).toBeGreaterThan(1);
    expect(peak).toBeLessThanOrEqual(8);

    const errors: unknown[] = [];
    const ranked = await rankCandidates({
      query: "unrelated note",
      candidates,
      now: NOW,
      recencyHalfLifeMs: 30 * DAY,
      embedder: {
        embed: async () => {
          throw new Error("down");
        },
      },
      onEmbedderError: (e) => errors.push(e),
    });
    expect(errors).toHaveLength(1);
    expect(ranked.length).toBeGreaterThan(0);
  });

  it("memoizeEmbedder embeds each text once, evicts the oldest, and does not cache failures", async () => {
    const calls: string[] = [];
    let fail = true;
    const base = {
      embed: async (text: string) => {
        calls.push(text);
        if (text === "flaky" && fail) throw new Error("boom");
        return [text.length];
      },
    };
    const cached = memoizeEmbedder({ embedder: base, maxEntries: 2 });
    await cached.embed("a");
    await cached.embed("a");
    expect(calls).toEqual(["a"]);

    await cached.embed("bb");
    await cached.embed("ccc"); // evicts "a"
    await cached.embed("a");
    expect(calls.filter((c) => c === "a")).toHaveLength(2);

    await expect(cached.embed("flaky")).rejects.toThrow("boom");
    fail = false;
    expect(await cached.embed("flaky")).toEqual([5]);
  });
});

describe("semantic candidate selection", () => {
  it("still embeds a candidate BM25 missed when many lexical hits compete for the slots", async () => {
    const lexicalHits = Array.from({ length: 80 }, (_, i) => ({
      text: `tea note ${i}`,
      importance: 0.5,
      at: NOW,
    }));
    const semanticOnly = { text: "oolong infusion", importance: 0.5, at: NOW - 10 * DAY };
    const vectors = new Map<string, number[]>([
      ["tea", [1, 0]],
      [semanticOnly.text, [1, 0]],
    ]);
    const ranked = await rankCandidates({
      query: "tea",
      candidates: [...lexicalHits, semanticOnly],
      now: NOW,
      recencyHalfLifeMs: 30 * DAY,
      embedder: { embed: async (t) => vectors.get(t) ?? [0, 1] },
      maxSemanticCandidates: 64,
    });
    expect(ranked.map((r) => r.index)).toContain(lexicalHits.length);
  });
});
