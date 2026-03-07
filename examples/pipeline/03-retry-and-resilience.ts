/**
 * Retry, circuit breakers, caching, semaphores
 *
 * Resilience patterns for unreliable external services.
 */

import { Data } from "effect";
import { Pipeline, PipelineSemaphore, PipelineCache, CircuitBreaker } from "@promin/core";

class ApiError extends Data.TaggedError("ApiError")<{
  readonly status: number;
  readonly message: string;
}> {}

// Basic retry with exponential backoff
async function basicRetry() {
  let attempts = 0;

  const result = await Pipeline.fromPromise(async () => {
    attempts++;
    if (attempts < 3) throw new Error("transient");
    return "success";
  })
    .retry({ maxRetries: 5, baseDelayMs: 100, jitter: true })
    .runPromise();

  console.log(result, `(${attempts} attempts)`); // "success (3 attempts)"
}

// Selective retry — only retry certain errors
async function selectiveRetry() {
  const callApi = (): Pipeline<string, ApiError> =>
    Pipeline.fail(new ApiError({ status: 400, message: "bad request" }));

  const { error } = await callApi()
    .retry({
      maxRetries: 3,
      when: (err) => err.status >= 500, // only retry 5xx
    })
    .runSafe();

  // 400 is not retried — fails immediately
  console.log("Failed:", (error as ApiError)?.status); // 400
}

// Circuit breaker — stop calling a failing service
async function circuitBreaker() {
  const breaker = new CircuitBreaker({
    failureThreshold: 3,
    resetTimeoutMs: 5_000,
  });

  const callApi = () =>
    Pipeline.fail(new ApiError({ status: 503, message: "down" }))
      .withCircuitBreaker(breaker);

  // First 3 calls: actually attempted, all fail
  for (let i = 0; i < 3; i++) {
    await callApi().runSafe();
  }

  // 4th call: circuit is open, fails instantly without calling the API
  const { error } = await callApi().runSafe();
  console.log("Circuit open:", (error as any)?._tag); // "CircuitOpenError"
}

// Semaphore — limit concurrent access
async function semaphore() {
  const limit = PipelineSemaphore.make(3); // max 3 concurrent
  let concurrent = 0;
  let maxConcurrent = 0;

  const tasks = Array.from({ length: 10 }, (_, i) =>
    Pipeline.fromPromise(async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 50));
      concurrent--;
      return i;
    }).withPermit(limit),
  );

  await Pipeline.all(...tasks).runPromise();
  console.log("Max concurrent:", maxConcurrent); // 3
}

// Cache — avoid redundant calls
async function caching() {
  const cache = new PipelineCache<string>(5_000); // 5s TTL
  let apiCalls = 0;

  const fetchUser = () =>
    Pipeline.fromPromise(async () => {
      apiCalls++;
      return `user-data-${apiCalls}`;
    }).cached(cache);

  const first = await fetchUser().runPromise();
  const second = await fetchUser().runPromise(); // cache hit

  console.log(first, second); // "user-data-1" "user-data-1"
  console.log("API calls:", apiCalls); // 1
}

// Competitive redundancy — race multiple providers, first wins
async function competitiveRedundancy() {
  const result = await Pipeline.race(
    Pipeline.fromPromise(async () => {
      await new Promise((r) => setTimeout(r, 200));
      return "provider-A";
    }),
    Pipeline.fromPromise(async () => {
      await new Promise((r) => setTimeout(r, 50));
      return "provider-B";
    }),
    Pipeline.fromPromise(async () => {
      await new Promise((r) => setTimeout(r, 150));
      return "provider-C";
    }),
  ).runPromise();

  console.log("Winner:", result); // "provider-B"
}

// Fan-out/fan-in with per-branch resilience
async function fanOutFanIn() {
  const cache = new PipelineCache<{ tier: string }>(60_000);

  const dashboard = await Pipeline.succeed({ id: "u_1" })
    .flatMap((user) =>
      Pipeline.all(
        // Videos: retry 3x
        Pipeline.fromPromise(async () => [{ title: "Video 1" }]).retry(3),
        // Analytics: fallback to zeros
        Pipeline.fromPromise(async () => ({ views: 100 })).orElse({ views: 0 }),
        // Subscription: cached
        Pipeline.fromPromise(async () => ({ tier: "pro" })).cached(cache),
      ).map(([videos, analytics, subscription]) => ({
        user,
        videos,
        analytics,
        subscription,
      })),
    )
    .timeout(10_000)
    .runPromise();

  console.log(dashboard);
}

export {
  basicRetry,
  selectiveRetry,
  circuitBreaker,
  semaphore,
  caching,
  competitiveRedundancy,
  fanOutFanIn,
};
