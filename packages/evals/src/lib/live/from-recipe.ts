// ---------------------------------------------------------------------------
// liveScoredFromRecipe — bridge a declarative config into the decorator.
//
// A `RegisteredAgent` recipe carries JSON, not live objects. This resolver
// reads a declarative `{ scorerRefs, sampling, maxConcurrent }` block, looks
// the scorer names up in a catalog, and returns a `liveScored` agent — the
// "scoring field wraps into the decorator" step, done host-side.
// ---------------------------------------------------------------------------

import type { Agent } from "@promin/agent";
import type { WallClock } from "@promin/workflow";
import type { Scorer } from "../types.ts";
import { liveScored } from "./live-scored.ts";
import type { LiveScoreSink } from "./types.ts";

/** Declarative live-scoring config — pure JSON, suitable for a recipe field. */
export interface LiveScoringRecipeConfig {
  /** Scorer ids to resolve against the catalog. */
  readonly scorerRefs: ReadonlyArray<string>;
  /** Fraction of runs to score, 0..1. */
  readonly sampling?: { readonly rate: number };
  /** Most sampled runs scored at once; see `LiveScoringConfig.maxConcurrent`. */
  readonly maxConcurrent?: number;
}

export interface LiveScoredFromRecipeParams<Input, Output> {
  readonly agent: Agent<Input, Output>;
  readonly config: LiveScoringRecipeConfig;
  /** Scorer id to implementation — the host's resolvable scorer set. */
  readonly scorerCatalog: Readonly<Record<string, Scorer>>;
  readonly sink: LiveScoreSink;
  readonly agentId?: string;
  readonly clock?: WallClock;
  readonly onError?: (err: unknown) => void;
  readonly onDropped?: (runId: string) => void;
}

/**
 * Resolve a declarative live-scoring config into a `liveScored` agent.
 * Throws when a referenced scorer is absent from the catalog, and
 * `RangeError` for out-of-range sampling or concurrency.
 */
export function liveScoredFromRecipe<Input, Output>(
  params: LiveScoredFromRecipeParams<Input, Output>,
): Agent<Input, Output> {
  const { config } = params;
  const scorers = config.scorerRefs.map((ref) => {
    const scorer = params.scorerCatalog[ref];
    if (scorer === undefined) {
      throw new Error(`liveScoredFromRecipe: scorer "${ref}" not found in the catalog`);
    }
    return scorer;
  });
  return liveScored({
    agent: params.agent,
    scorers,
    sink: params.sink,
    ...(config.sampling !== undefined && { sampling: config.sampling }),
    ...(config.maxConcurrent !== undefined && { maxConcurrent: config.maxConcurrent }),
    ...(params.agentId !== undefined && { agentId: params.agentId }),
    ...(params.clock !== undefined && { clock: params.clock }),
    ...(params.onError !== undefined && { onError: params.onError }),
    ...(params.onDropped !== undefined && { onDropped: params.onDropped }),
  });
}
