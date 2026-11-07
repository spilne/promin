// ---------------------------------------------------------------------------
// Records every MemoryStore builds from caller input: a fresh id, timestamps
// and defaults. Shared so the in-memory, SQLite and Postgres stores cannot
// drift apart on what a new fact or episode looks like.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { resolveFactConfidence } from "./fact-confidence.ts";
import { clamp01 } from "./scoring.ts";
import type { EpisodeInput, EpisodicRecord, Fact, FactInput } from "./types.ts";

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
