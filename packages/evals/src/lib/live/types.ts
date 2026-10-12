// ---------------------------------------------------------------------------
// Live scoring — types.
//
// Live scoring runs the Scorer seam against an agent's OWN production runs,
// sampled and off-thread. It reuses Scorer / Score / toEvalOutput unchanged;
// only the trigger (a real run, not a dataset case) and the sink differ.
// ---------------------------------------------------------------------------

import type { WallClock } from "@promin/workflow";
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

/** Default `LiveScoringConfig.maxConcurrent`. */
export const DEFAULT_LIVE_MAX_CONCURRENT = 4;

export interface LiveScoringConfig {
  /** Scorers to run on each sampled production run — should be reference-free. */
  readonly scorers: ReadonlyArray<Scorer>;
  readonly sink: LiveScoreSink;
  /** Fraction of runs to score, 0..1 (`RangeError` otherwise). Default 1 (every run). */
  readonly sampling?: { readonly rate: number };
  /** Deterministic sampling override — when set, replaces `sampling.rate`. */
  readonly decide?: (runId: string) => boolean;
  /**
   * Most sampled runs scored at once. A run sampled while this many are
   * being scored is dropped (`onDropped`), so a traffic spike never piles
   * up scorer calls behind production. Default `DEFAULT_LIVE_MAX_CONCURRENT`.
   */
  readonly maxConcurrent?: number;
  /** Called with the run id of a sampled run dropped at the concurrency cap. */
  readonly onDropped?: (runId: string) => void;
  /** Label attached to every `LiveScore`. Default `"agent"`. */
  readonly agentId?: string;
  readonly clock?: WallClock;
  /** Sink for scorer / scoring faults — they never reach the production run. */
  readonly onError?: (err: unknown) => void;
}
