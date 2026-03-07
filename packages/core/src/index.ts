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

// Durable execution
export {
  workflow,
  flow,
  WorkflowBuilder,
  type WorkflowDefinition,
  type WorkflowHooks,
  type WorkflowDAG,
  dagToMermaid,
  dagToDot,
  type StepContext,
  type DagStepContext,
  type MapStepContext,
  type StepOptions,
  type StepFailureStrategy,
  type WorkflowStorage,
  type WorkflowState,
  type StepState,
  type StepTaskState,
  type SignalState,
  type WorkflowStatus,
  type StepStatus,
  type StepType,
  InMemoryWorkflowStorage,
  WorkflowError,
  StepError,
  StorageError,
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowTimeoutError,
  topologicalSort,
  computeReadySet,
  type DagNode,
  trigger,
  WorkflowResult,
  type CompensateConfig,
  type CompensationStatus,
  type StepAttemptRecord,
  type StepAttemptType,
  type StepAttemptStorage,
  isStepAttemptStorage,
  type FailedWorkflowRecord,
  type WorkflowSchema,
  type StepSchema,
  type SingleStepSchema,
  type MapStepSchema,
  type JsonSchema,
  type NodeUiMeta,
  WorkflowSchemaZ,
  validateWorkflowSchema,
  validateWorkflowSchemaSafe,
  type ActivityRegistry,
  type ActivityFactory,
  type ActivityContext,
  MapActivityRegistry,
  compileWorkflow,
  WorkflowCompilationError,
} from "./lib/durable/index.ts";

// Scheduler
export {
  type ScheduleConfig,
  type ScheduleTick,
  type Scheduler,
  InMemoryScheduler,
  createScheduler,
} from "./lib/scheduler/index.ts";
