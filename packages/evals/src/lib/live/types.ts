// ---------------------------------------------------------------------------
// Live scoring — types.
//
// Live scoring runs the Scorer seam against an agent's OWN production runs,
// sampled and off-thread. It reuses Scorer / Score / toEvalOutput unchanged;
// only the trigger (a real run, not a dataset case) and the sink differ.
// ---------------------------------------------------------------------------

import type { Clock } from "@promin/core";
import type { Score, Scorer } from "../types.ts";

/** One run's live-scoring result. */
export interface LiveScore {
  readonly runId: string;
  readonly agentId: string;
  readonly namespaceId?: string;
  readonly resourceId?: string;
  readonly scoredAt: number;
  readonly scores: ReadonlyArray<Score>;
}

/** Where live scores go. */
export interface LiveScoreSink {
  record(score: LiveScore): void | Promise<void>;
}

export interface LiveScoringConfig {
  /** Scorers to run on each sampled production run — should be reference-free. */
  readonly scorers: ReadonlyArray<Scorer>;
  readonly sink: LiveScoreSink;
  /** Fraction of runs to score, 0..1. Default 1 (every run). */
  readonly sampling?: { readonly rate: number };
  /** Deterministic sampling override — when set, replaces `sampling.rate`. */
  readonly decide?: (runId: string) => boolean;
  /** Label attached to every `LiveScore`. Default `"agent"`. */
  readonly agentId?: string;
  readonly clock?: Clock;
  /** Sink for scorer / scoring faults — they never reach the production run. */
  readonly onError?: (err: unknown) => void;
}
