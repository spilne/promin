// ---------------------------------------------------------------------------
// Step-level cache wrapper — runs before the step body, falls through to a
// cache miss on any cache error so storage hiccups never fail the workflow.
// ---------------------------------------------------------------------------

import { succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../shared/eff.ts";
import type { TaggedError } from "../shared/tagged-error.ts";
import type { StepCacheOption, StepContext, StepEff } from "./step-definition.ts";

/** Sentinel for "no usable cache entry" (absent, unreadable or undecodable). */
const CACHE_MISS: unique symbol = Symbol("cache-miss");

/**
 * Build the store key for a cached step result. The physical step name is
 * always part of the key, so two steps (or two `parallelSteps` branches)
 * that share one cache config and see the same context never read each
 * other's results.
 */
export function stepCacheKey(params: {
  readonly namespace: string;
  readonly stepName: string;
  readonly key: string;
}): string {
  return `${params.namespace}:${params.stepName}:${params.key}`;
}

/**
 * Run `runBody` through the step cache when the step has a `cache` option,
 * else run it directly. `namespace` is the default (the workflow name); the
 * cache option's own `namespace` wins.
 */
export function withOptionalStepCache(params: {
  readonly cache: StepCacheOption | undefined;
  readonly ctx: StepContext<unknown, unknown>;
  readonly runBody: () => StepEff<unknown, TaggedError>;
  readonly stepName: string;
  readonly namespace: string;
  readonly codec: Codec<unknown>;
}): StepEff<unknown, TaggedError> {
  const { cache } = params;
  if (!cache) return params.runBody();
  return wrapWithStepCache({ ...params, cache, namespace: cache.namespace ?? params.namespace });
}

function wrapWithStepCache(params: {
  readonly cache: StepCacheOption;
  readonly ctx: StepContext<unknown, unknown>;
  readonly runBody: () => StepEff<unknown, TaggedError>;
  /** Physical step name (for `parallelSteps` branches: `block.branch`). */
  readonly stepName: string;
  readonly namespace: string;
  /** The step's codec. Values are stored encoded and decoded on a hit. */
  readonly codec: Codec<unknown>;
}): StepEff<unknown, TaggedError> {
  const { cache, codec } = params;
  const cacheKey = stepCacheKey({
    namespace: params.namespace,
    stepName: params.stepName,
    key: cache.key(params.ctx),
  });

  // Lookup is expressed as an Eff so we can keep everything inside the
  // caller's error channel. Values are stored codec-encoded, so a hit has
  // the same shape as a fresh run's result (Dates, BigInts, etc. survive a
  // serializing store). `undefined` from the store means "no entry"; a step
  // result of `undefined` is still cacheable because its encoded form is
  // not `undefined` for the default codec. Read and decode failures fall
  // through to a miss.
  const lookup = promiseOrDie(async (): Promise<unknown> => {
    try {
      const hit = await cache.store.get(cacheKey);
      return hit === undefined ? CACHE_MISS : codec.decode(hit);
    } catch {
      return CACHE_MISS;
    }
  });

  return lookup.flatMap((value): StepEff<unknown, TaggedError> => {
    if (value !== CACHE_MISS) return succeed(value);
    // Miss — run the body, then write to cache on success. The write is
    // awaited (so tests see cache state deterministically), and write
    // errors are swallowed so cache backends can never fail a step.
    return params.runBody().tap((result) =>
      promiseOrDie(async () => {
        try {
          await cache.store.set(cacheKey, codec.encode(result), cache.ttlMs);
        } catch {
          /* a cache write failure must never fail the step */
        }
      }),
    );
  });
}
