// Core pipeline
export { Pipeline, type TaggedError, type PipelineDefaults } from "./lib/pipeline.ts";
export { StreamPipeline } from "./lib/stream-pipeline.ts";

// Errors
export { TimeoutError, PollTimeoutError, CircuitOpenError } from "./lib/pipeline-error.ts";

// Retry
export { PipelineResult, type RetryPolicy, type RetryAllPolicy } from "./lib/retry.ts";
export { withRetry, withRetryAll } from "./lib/retry.ts";

// Combinators
export {
  parallel,
  allSettled,
  validate,
  race,
  fallbackChain,
  hedged,
  poll,
  pollWithBackoff,
  withTimeout,
  forEach,
  type PollOptions,
} from "./lib/combinators.ts";

// Primitives
export { PipelineSemaphore } from "./lib/semaphore.ts";
export { CircuitBreaker, type CircuitBreakerConfig } from "./lib/circuit-breaker.ts";
export { PipelineCache } from "./lib/cache.ts";
export { PipelineQueue } from "./lib/queue.ts";
export { PipelineRef } from "./lib/ref.ts";
export { PipelineDeferred } from "./lib/deferred.ts";
export { PipelineChannel } from "./lib/channel.ts";
export { PipelineSignal } from "./lib/signal.ts";
export { PipelinePubSub } from "./lib/pubsub.ts";
export { PipelinePool } from "./lib/pool.ts";
