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
export {
  type CacheStore,
  MemoryCache,
  type MemoryCacheConfig,
  LayeredCache,
  layered,
} from "./lib/cache-store.ts";
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
  type DispatchConfig,
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

// DataFrame
export {
  DataFrame,
  GroupedDataFrame,
  ArrayExecutor,
  StringAccessor,
  DateAccessor,
  type DataFrameExecutor,
  type ExecutionCost,
  type LogicalPlan,
  type AggFn,
  type WindowFn,
  type RollingFn,
  type CumulativeFn,
} from "./lib/dataframe/index.ts";

// Data profiling
export {
  type ProfileReport,
  type ColumnProfile,
  type NumericProfile,
  type StringProfile,
  type BooleanProfile,
  type DateProfile,
  type CorrelationPair,
  type ProfileWarning,
  profileData,
  type ProfileOptions,
} from "./lib/data-profiler/index.ts";

// Data diff
export {
  dataDiff,
  schemaDiff,
  type DataDiffResult,
  type DiffOptions,
  type Modification,
  type SchemaDiffResult,
} from "./lib/data-diff/index.ts";

// Data quality
export {
  type Expectation,
  type ExpectationResult,
  type ValidationResult,
  ExpectationSuite,
} from "./lib/data-quality/index.ts";

// Data contracts
export {
  type DataContract,
  type ContractValidationResult,
  type ValidatableContract,
  defineContract,
} from "./lib/data-contracts/index.ts";

// Data testing
export {
  generators,
  generateRows,
  type Generator,
  dataframeProperties,
  streamProperties,
} from "./lib/data-testing/index.ts";

// SQL Models (dbt-style)
export {
  type SqlModel,
  type SqlProject,
  type SqlProjectResult,
  type Materialization,
  type ExpectationDef as SqlExpectationDef,
  compileSqlProject,
  type SqlCompilerConfig,
} from "./lib/sql-models/index.ts";

// Distributed workflow execution
export {
  type StepRegistry,
  type StepHandler,
  type StepRegistration,
  type WorkerStepOptions,
  MapStepRegistry,
  type StepQueue,
  type StepTask,
  InMemoryStepQueue,
  type WorkflowCoordinator,
  type CoordinatorConfig,
  DefaultCoordinator,
  createCoordinator,
  type WorkflowWorker,
  type WorkerConfig,
  type WorkerInfo,
  type WorkerRegistry,
  InMemoryWorkerRegistry,
  type LeaderElection,
  SingleLeader,
  type SleepScanner,
  type SleepScannerConfig,
  createSleepScanner,
  DefaultWorker,
  createWorker,
  type WorkerHooks,
  type WorkerMiddleware,
  type NextFn,
  timeoutMiddleware,
  retryMiddleware,
  loggingMiddleware,
  metricsMiddleware,
} from "./lib/distributed/index.ts";

// Stream topology — distributed stream processing
export {
  StreamTopology,
  KeyedTopology,
  WindowedTopology,
  BuiltTopology,
  TopologyRunner,
  WindowManager,
  JoinBuffer,
  type JoinedPair,
  type TimeWindow,
  type WindowType,
  type AggregateSpec,
  type ProcessSpec,
  type JoinConfig,
  type TopologyConfig,
  type TopologyHandle,
  type TopologyMetrics,
  type BackpressureStats,
  type CompiledTopology,
} from "./lib/stream-topology/index.ts";

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
  type CsvOptions,
  type FixedWidthColumn,
  type XmlEvent,
} from "./lib/stream-pipes.ts";
