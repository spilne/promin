// Core pipeline
export { Pipeline, type TaggedError, type PipelineDefaults } from "./lib/pipeline.ts";
export { StreamPipeline } from "./lib/stream-pipeline.ts";
export { OptimizedStreamPipeline } from "./lib/optimized-stream-pipeline.ts";
export { RawStream } from "./lib/raw-stream.ts";
export { type FusibleOp, SKIP, compileFused, hasFilterOps, fuseOpsToStream } from "./lib/fusion.ts";

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

// Clock
export { type Clock, SystemClock, FakeClock } from "./lib/clock.ts";

// Duration
export { Duration, type DurationInput, resolveMs } from "./lib/duration.ts";

// Primitives
export { PipelineSemaphore, type Semaphore } from "./lib/semaphore.ts";
export { CircuitBreaker, type CircuitBreakerConfig } from "./lib/circuit-breaker.ts";
export { PipelineCache } from "./lib/cache.ts";
export {
  type CacheStore,
  MemoryCache,
  type MemoryCacheConfig,
  LayeredCache,
  layered,
} from "./lib/cache-store.ts";
export { PipelineQueue, type AsyncQueue } from "./lib/queue.ts";
export { PipelineRef, type AtomicRef } from "./lib/ref.ts";
export { PipelineDeferred, type DeferredValue } from "./lib/deferred.ts";
export { PipelineChannel, type Channel } from "./lib/channel.ts";
export { PipelineSignal, type Signal } from "./lib/signal.ts";
export { PipelinePubSub, type PubSubBroadcast } from "./lib/pubsub.ts";
export { PipelinePool, type ResourcePool } from "./lib/pool.ts";
export { PipelineSingleflight, type Singleflight } from "./lib/singleflight.ts";
export { PipelineThrottle, type Throttle } from "./lib/throttle.ts";
export { PipelineRateLimiter, RateLimitExceeded, type RateLimiter } from "./lib/rate-limiter.ts";
export { PipelineLatch, type Latch } from "./lib/latch.ts";
export { PipelineBarrier, type Barrier } from "./lib/barrier.ts";

// Typeclasses — data
export {
  type Codec,
  JsonCodec,
  codecFromSchema,
  codecTuple,
  codecRecord,
  codecArray,
  type Eq,
  JsonEq,
  eqFromCodec,
  type Show,
  JsonShow,
  type Monoid,
  arrayMonoid,
  sumMonoid,
  stringMonoid,
  type Ord,
  numberOrd,
  stringOrd,
  ordBy,
} from "./lib/typeclasses/index.ts";

// Typeclasses — streaming
export {
  type Streamable,
  isStreamable,
  type Sinkable,
  isSinkable,
  type KeyedSinkable,
  isKeyedSinkable,
  type Partitionable,
  isPartitionable,
  type Replayable,
  isReplayable,
  type Offset,
  type Acknowledgeable,
  isAcknowledgeable,
  type Envelope,
  type Checkpointable,
  isCheckpointable,
} from "./lib/typeclasses/index.ts";

// Typeclasses — DataFrame
export {
  type Frameable,
  isFrameable,
  type FrameSchema,
  type PushdownFilterable,
  isPushdownFilterable,
  type Predicate,
  type ColumnSelectable,
  isColumnSelectable,
  type SourceSortable,
  isSourceSortable,
} from "./lib/typeclasses/index.ts";

// Typeclasses — state
export { type StateBackend } from "./lib/typeclasses/index.ts";

// In-memory adapters
export { MemoryStream } from "./lib/adapters/memory/index.ts";
export { IterableSource, fromIterable } from "./lib/adapters/memory/index.ts";
export { InMemoryState } from "./lib/adapters/memory/index.ts";

// NOTE: Durable execution, DataFrame, and Stream topology have been moved to
// dedicated packages. Import from:
//   @promin/workflow  — workflows, state machines, distributed workers, scheduler, sql-models
//   @promin/data      — DataFrame, data quality, profiling, diff, contracts
//   @promin/topology  — stream topology builder and runners

// Stream pipes — reusable through() transformations
export {
  utf8Decode,
  lines,
  csv,
  tsv,
  ssv,
  fixedWidth,
  regex,
  xml,
  jsonl,
  jsonlAs,
  parseAs,
  parseAsLenient,
  binaryDecode,
  lengthPrefixed,
  base64Encode,
  base64Decode,
  type CsvOptions,
  type FixedWidthColumn,
  type XmlEvent,
} from "./lib/stream-pipes.ts";
