import type { FactSource } from "./types.ts";

/**
 * Confidence a fact gets when its writer gives none: people's own words are
 * taken as certain, the model's notes less so.
 */
export const DEFAULT_FACT_CONFIDENCE: Readonly<Record<FactSource, number>> = {
  operator: 1,
  user: 1,
  model: 0.6,
};

/**
 * Ceiling for a `model` fact's own claim, so the agent cannot rate its notes
 * as certain as something a person said.
 */
export const MAX_MODEL_FACT_CONFIDENCE = 0.9;

/**
 * The confidence to store: the input's, clamped to 0..1 (`model` facts to
 * `MAX_MODEL_FACT_CONFIDENCE`), else the source default.
 */
export function resolveFactConfidence(params: {
  readonly source: FactSource;
  readonly confidence?: number | undefined;
}): number {
  const { source, confidence } = params;
  if (confidence === undefined || Number.isNaN(confidence)) return DEFAULT_FACT_CONFIDENCE[source];
  const ceiling = source === "model" ? MAX_MODEL_FACT_CONFIDENCE : 1;
  return Math.min(ceiling, Math.max(0, confidence));
}
