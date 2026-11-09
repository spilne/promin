// ---------------------------------------------------------------------------
// Records every MemoryStore builds from caller input: a fresh id, timestamps
// and defaults. Shared so the in-memory, SQLite and Postgres stores cannot
// drift apart on what a new fact or episode looks like.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { resolveFactConfidence } from "./fact-confidence.ts";
import { clamp01 } from "./scoring.ts";
import type { EpisodeInput, EpisodicRecord, Fact, FactImport, FactInput } from "./types.ts";

const DEFAULT_EPISODE_SALIENCE = 0.5;

/** A new, active fact written at `now` (epoch ms). */
export function newFact(params: { readonly input: FactInput; readonly now: number }): Fact {
  const { input, now } = params;
  return {
    id: randomUUID(),
    text: input.text,
    source: input.source,
    createdAt: now,
    updatedAt: now,
    retiredAt: null,
    supersededBy: null,
    confidence: resolveFactConfidence(input),
  };
}

/**
 * Stored facts for an import batch: fresh ids, the given record otherwise,
 * `supersededByRef` resolved to the new id. Throws on a repeated ref.
 */
export function importedFacts(facts: ReadonlyArray<FactImport>): Fact[] {
  const ids = new Map<string, string>();
  for (const f of facts) {
    if (ids.has(f.ref)) throw new Error(`importThreadFacts: duplicate ref ${f.ref}`);
    ids.set(f.ref, randomUUID());
  }
  return facts.map((f) => ({
    id: ids.get(f.ref)!,
    text: f.text,
    source: f.source,
    createdAt: f.createdAt,
    updatedAt: f.updatedAt,
    retiredAt: f.retiredAt,
    supersededBy: f.supersededByRef === null ? null : (ids.get(f.supersededByRef) ?? null),
    confidence: clamp01(f.confidence),
  }));
}

/** A new episode written at `now`: salience defaults to 0.5 (also for NaN) and is clamped to 0..1. */
export function newEpisode(params: {
  readonly input: EpisodeInput;
  readonly now: number;
}): EpisodicRecord {
  const { input, now } = params;
  const salience = input.salience ?? DEFAULT_EPISODE_SALIENCE;
  return {
    id: randomUUID(),
    summary: input.summary,
    outcome: input.outcome ?? null,
    salience: clamp01(Number.isNaN(salience) ? DEFAULT_EPISODE_SALIENCE : salience),
    sourceThreadId: input.sourceThreadId ?? null,
    sourceMessageRange: input.sourceMessageRange ?? null,
    occurredAt: input.occurredAt ?? now,
    createdAt: now,
    metadata: input.metadata ?? {},
  };
}
