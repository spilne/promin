// ---------------------------------------------------------------------------
// liveScoredFromRecipe — bridge a declarative config into the decorator.
//
// A `RegisteredAgent` recipe carries JSON, not live objects. This resolver
// reads a declarative `{ scorerRefs, sampling }` block, looks the scorer
// names up in a catalog, and returns a `liveScored` agent — the "scoring
// field wraps into the decorator" step, done host-side.
// ---------------------------------------------------------------------------

import type { Agent } from "@promin/agent";
import type { Clock } from "@promin/core";
import type { Scorer } from "../types.ts";
import { liveScored } from "./live-scored.ts";
import type { LiveScoreSink } from "./types.ts";

/** Declarative live-scoring config — pure JSON, suitable for a recipe field. */
export interface LiveScoringRecipeConfig {
  /** Scorer ids to resolve against the catalog. */
  readonly scorerRefs: ReadonlyArray<string>;
  readonly sampling?: { readonly rate: number };
}

export interface LiveScoredFromRecipeDeps {
  /** Scorer id to implementation — the host's resolvable scorer set. */
  readonly scorerCatalog: Readonly<Record<string, Scorer>>;
  readonly sink: LiveScoreSink;
  readonly agentId?: string;
  readonly clock?: Clock;
  readonly onError?: (err: unknown) => void;
}

/**
 * Resolve a declarative live-scoring config into a `liveScored` agent.
 * Throws when a referenced scorer is absent from the catalog.
 */
export function liveScoredFromRecipe<Input, Output>(
  agent: Agent<Input, Output>,
  config: LiveScoringRecipeConfig,
  deps: LiveScoredFromRecipeDeps,
): Agent<Input, Output> {
  const scorers = config.scorerRefs.map((ref) => {
    const scorer = deps.scorerCatalog[ref];
    if (scorer === undefined) {
      throw new Error(`liveScoredFromRecipe: scorer "${ref}" not found in the catalog`);
    }
    return scorer;
  });
  return liveScored(agent, {
    scorers,
    sink: deps.sink,
    ...(config.sampling !== undefined && { sampling: config.sampling }),
    ...(deps.agentId !== undefined && { agentId: deps.agentId }),
    ...(deps.clock !== undefined && { clock: deps.clock }),
    ...(deps.onError !== undefined && { onError: deps.onError }),
  });
}
