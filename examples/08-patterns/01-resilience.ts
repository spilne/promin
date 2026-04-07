/**
 * Resilience patterns — retry, circuit breaker, race, hedge
 *
 * Shows how to compose Pipeline primitives for fault-tolerant systems:
 * - Fan-out with per-branch resilience
 * - Competitive redundancy (fastest provider wins)
 * - Hedged multi-region requests
 */

import { Pipeline, CircuitBreaker, PipelineCache } from "@promin/core";

// ---------------------------------------------------------------------------
// Fan-out with per-branch resilience
// ---------------------------------------------------------------------------
// Fetch user, then in parallel: videos (retry 3x), analytics (fallback),
// subscription (cached). Any branch can fail independently.

async function fanOutWithResilience(
  api: any,
  UserSchema: any,
  VideosSchema: any,
  AnalyticsSchema: any,
  SubSchema: any,
) {
  const subCache = new PipelineCache<unknown>(5 * 60 * 1000);

  return api
    .get("/users/1", UserSchema)
    .flatMap((user: any) =>
      Pipeline.all(
        api.get(`/users/${user.id}/videos`, VideosSchema).retry(3),
        api.get(`/users/${user.id}/analytics`, AnalyticsSchema).orElse({ views: 0, subs: 0 }),
        api.get(`/users/${user.id}/subscription`, SubSchema).cached(subCache),
      ).map(([videos, analytics, subscription]: any[]) => ({
        user,
        videos,
        analytics,
        subscription,
      })),
    )
    .timeout(10_000)
    .runPromise();
}

// ---------------------------------------------------------------------------
// Competitive redundancy — fastest AI provider wins
// ---------------------------------------------------------------------------
// 3 providers race. Each has a circuit breaker. Fallback to cheap model.

async function competitiveRedundancy(api: any, Schema: any, prompt: unknown) {
  const openaiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
  const anthropicBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });
  const geminiBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 60_000 });

  return Pipeline.race(
    api
      .post("/ai/openai", Schema, { json: prompt })
      .withCircuitBreaker(openaiBreaker)
      .timeout(10_000),
    api
      .post("/ai/anthropic", Schema, { json: prompt })
      .withCircuitBreaker(anthropicBreaker)
      .timeout(10_000),
    api
      .post("/ai/gemini", Schema, { json: prompt })
      .withCircuitBreaker(geminiBreaker)
      .timeout(10_000),
  )
    .orElsePipeline(() => api.post("/ai/cheap-model", Schema, { json: prompt }).retry(2))
    .runPromise();
}

// ---------------------------------------------------------------------------
// Hedged multi-region requests
// ---------------------------------------------------------------------------
// Primary fires immediately. Backup fires after delay if primary is slow.
// First response wins, others are cancelled.

async function hedgedMultiRegion(api: any, Schema: any) {
  const usBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 15_000 });
  const euBreaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 15_000 });

  return Pipeline.race(
    Pipeline.hedged(api.get("/data", Schema).withCircuitBreaker(usBreaker), { hedgeDelayMs: 200 }),
    api.get("/data", Schema).withCircuitBreaker(euBreaker),
  )
    .timeout(5_000)
    .runPromise();
}

export { fanOutWithResilience, competitiveRedundancy, hedgedMultiRegion };
