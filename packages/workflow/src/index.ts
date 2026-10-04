// ---------------------------------------------------------------------------
// @promin/workflow — build and run durable workflows.
//
// The root entry covers authoring (workflow, flow, steps, signals, streams,
// state machines), running (createWorkflowRunner, recovery, triggers), the
// storage contract, errors and the in-memory implementations. It loads in
// any JavaScript runtime: nothing reachable from here imports a Node
// built-in or `bun:test`.
//
// Everything else lives behind a subpath:
//   @promin/workflow/distributed  workers, step queue, distributed runner
//   @promin/workflow/scheduler    cron / rrule / interval schedulers
//   @promin/workflow/discovery    filesystem scanners (Node-compatible runtime)
//   @promin/workflow/sql-models   dbt-style SQL model DAGs
//   @promin/workflow/storage-kit  helpers for storage backend authors
//   @promin/workflow/testing      conformance suites (bun:test)
//   @promin/workflow/dev          non-determinism instrumentation
// ---------------------------------------------------------------------------

// Authoring
export { workflow, flow, type WorkflowBuilder } from "./lib/durable/workflow-builder.ts";
export type {
  Workflow,
  WorkflowErrorOf,
  IdempotencyConfig,
  WorkflowHandle,
  WorkflowStatusInfo,
  WorkflowHooks,
  CompensateConfig,
} from "./lib/durable/workflow-types.ts";
export { type WorkflowDAG, dagToMermaid, dagToDot } from "./lib/durable/workflow-dag-viz.ts";
export type {
  StepContext,
  DagStepContext,
  MapStepContext,
  StepOptions,
  StepFailureStrategy,
  MapOverOptions,
  MapElementOptions,
  ParallelStepsOptions,
  JournaledStepOptions,
  SubworkflowOptions,
  TripwireOptions,
  StepQueueOption,
  StepQueueContext,
  StepRuntime,
  WorkflowMetadataRef,
  RunChildWorkflow,
} from "./lib/durable/step-definition.ts";
export type { LoopOptions } from "./lib/durable/steps/loop-step.ts";
export { MatchError, type MatchParams } from "./lib/durable/steps/match-step.ts";

// Storage contract
export {
  type WorkflowStorage,
  type WorkflowRunStore,
  type WorkflowLockStore,
  type WorkflowQueryStore,
  type WorkflowScannerStore,
  type SignalStore,
  type SignalTokenStore,
  type StreamStore,
  type RunEventStore,
  type StepAttemptStore,
  type StepCheckpointStore,
  type CompensationLedgerStore,
  type JournalStore,
  type FenceGuard,
  type FenceToken,
  type FencedWrite,
  type CreateWorkflowParams,
  type CreateWorkflowResult,
  type FindWorkflowByIdempotencyKeyParams,
  type StepResultRecord,
  type SaveStepResultParams,
  type BatchSaveStepResultsParams,
  type SaveStepFailureParams,
  type SaveTaskResultParams,
  type SaveTaskFailureParams,
  type CompleteWorkflowParams,
  type FailWorkflowParams,
  type TripwireWorkflowParams,
  type CancelWorkflowParams,
  type SuspendWorkflowParams,
  type SetWorkflowMetadataParams,
  type StartFreshRunParams,
  type ResetStepsParams,
  type TryLockParams,
  type TryLockResult,
  type TryLockAndLoadResult,
  type ReleaseLockParams,
  type HeartbeatParams,
  type WorkflowOrderBy,
  type WorkflowListFilter,
  type ListWorkflowsParams,
  type LoadRunHistoryParams,
  type PurgeCompletedParams,
  type CancelStaleWorkflowsParams,
  type WorkflowWakeup,
  type OrphanedRun,
  type ListDueTimersParams,
  type ListSignalWakeupsParams,
  type ListOrphanedRunsParams,
  type DeliverSignalParams,
  type SignalTokenRecord,
  type CreateSignalTokenParams,
  type MarkSignalTokenCompletedParams,
  type MarkSignalTokenCompletedResult,
  type StreamChunk,
  type AppendStreamChunkParams,
  type ReadStreamChunksParams,
  type NotifyStepStartedParams,
  type SubscribeToWorkflowParams,
  type SaveStepAttemptParams,
  type LoadStepAttemptsParams,
  type StepCheckpoint,
  type CheckpointStepParams,
  type StepCompensationOutcome,
  type BeginCompensationParams,
  type SaveStepCompensationParams,
  type LoadJournalParams,
  type AppendEntryParams,
  type AppendPendingEntryParams,
  type CompletePendingEntryParams,
  type DiscardJournalEntriesParams,
  type FindDueSleepsParams,
  type FindPendingSignalParams,
  type DueSleep,
  type StorageCapability,
  type StorageCapabilityMap,
  type WorkflowStorageCapabilities,
  hasCapability,
  storageCapabilities,
  STORAGE_CAPABILITIES,
} from "./lib/durable/workflow-storage.ts";
export {
  type WorkflowState,
  type WorkflowSummary,
  type WorkflowRunSummary,
  type StepState,
  type StepTaskState,
  type SignalState,
  type WorkflowStatus,
  type WorkflowStatusSnapshot,
  type RunSource,
  type StepStatus,
  type StepType,
  type CompensationStatus,
  type StepAttemptRecord,
  type StepAttemptType,
  type FailedWorkflowRecord,
  type WorkflowRunEvent,
  WORKFLOW_STATUSES,
  TERMINAL_WORKFLOW_STATUSES,
  isTerminalWorkflowStatus,
  isCancelledRun,
} from "./lib/durable/workflow-state.ts";
export { InMemoryWorkflowStorage } from "./lib/durable/in-memory-storage.ts";

// Streams
export {
  type StreamDescriptor,
  type StreamKind,
  type DefineStreamOptions,
  defineStream,
  defineInputStream,
  appendStreamChunk,
  appendExternalStreamChunk,
  peekStreamChunk,
} from "./lib/durable/streams.ts";

// Versioning
export {
  type WorkflowVersionRegistry,
  InMemoryWorkflowVersionRegistry,
  ScopedWorkflowVersionRegistry,
  type WorkflowVersionRegistryConfig,
  type VersionRecord,
  type VersionRunCounts,
  type VersionStatus,
} from "./lib/durable/workflow-version-registry.ts";

// Errors
export {
  WorkflowError,
  StepError,
  WorkflowFailedError,
  WorkflowCancelledError,
  CheckpointError,
  WorkflowLockLostError,
  StorageError,
  WorkflowLockError,
  WorkflowSuspendedError,
  WorkflowContinueAsNewError,
  WorkflowTimeoutError,
  StepTimeoutError,
  WorkflowDeadlineError,
  WorkflowVersionMismatchError,
  FenceTokenMismatchError,
  GuardError,
  WorkflowTripwireError,
  TripwireStorageMissingError,
  LoopLimitExceededError,
} from "./lib/durable/durable-pipeline-error.ts";

// Running
export {
  InProcessStepExecutor,
  RoutingStepExecutor,
  createWorkflowRunner,
  RecoveryStrategy,
  RecoveryStrategyBuilder,
  recoverWorkflows,
  type WorkflowRunner,
  type WorkflowRunnerConfig,
  type WorkflowRunnerRunParams,
  type WorkflowRunnerStartParams,
  type WorkflowRunSafeError,
  type WorkflowRunError,
  type WorkflowRunSafeResult,
  type WorkflowRunParamsFor,
  type WorkflowSubscribeParams,
  type WorkflowGetStatusParams,
  type StepExecutor,
  type StepExecutionRequest,
  type StepExecutionResult,
  type StepAttemptFailure,
  type RecoveryResult,
  type StaleTerminationAction,
} from "./lib/durable/workflow-runner.ts";
export {
  trigger,
  WorkflowResult,
  type TriggerParams,
  type TriggerDuplicatePolicy,
} from "./lib/durable/workflow-trigger.ts";
export {
  webhookTrigger,
  type WebhookTriggerConfig,
  type WebhookRequest,
  type WebhookHandler,
  type WebhookHmacConfig,
} from "./lib/durable/webhook-trigger.ts";

// Journaled steps
export {
  type JournalEntry,
  type JournalExit,
  type JournalFailureExit,
  type JournalSlot,
  type CompletePendingResult,
  type JournalStepType,
  type JournalPhase,
} from "./lib/durable/activity-journal.ts";
export {
  runJournaledStep,
  completeSignal,
  completeDueSleeps,
  JournalNonDeterminismError,
  JournalStorageMissingError,
  type JournaledContext,
  type JournaledStepBody,
  type ActivityYield,
  type ActivityOptions,
} from "./lib/durable/journaled-step.ts";
export { wakeParentOfEndedRun } from "./lib/durable/child-wake.ts";
export {
  invokeQueryHandler,
  hasQueryHandlers,
  listQueryHandlers,
  configureQueryRegistry,
  DEFAULT_SUSPENDED_QUERY_TTL_MS,
} from "./lib/durable/query-registry.ts";

// Serializable workflow schema (visual editors) and its compiler
export {
  type WorkflowSchema,
  type StepSchema,
  type SingleStepSchema,
  type MapStepSchema,
  type StepSchemaOptions,
  type MapStepSchemaOptions,
  type NodeUiMeta,
  type JsonSchema,
} from "./lib/durable/workflow-schema.ts";
export {
  WorkflowSchemaZ,
  validateWorkflowSchema,
  validateWorkflowSchemaSafe,
} from "./lib/durable/workflow-schema-validator.ts";
export {
  type ActivityRegistry,
  type ActivityFactory,
  type ActivityContext,
  MapActivityRegistry,
} from "./lib/durable/activity-registry.ts";
export { compileWorkflow, WorkflowCompilationError } from "./lib/durable/workflow-compiler.ts";

// State machines
export {
  stateMachine,
  machine,
  pureStateMachine,
  type StateMachineBuilder,
  type StateMachineInstance,
  type QuickMachineBuilder,
  type MachineHandle,
  type PureStateMachineBuilder,
  EventDataValidationError,
  StateMachineVersionMismatchError,
  TIMEOUT_EVENT,
  type MachineLimits,
  type MachineMiddleware,
  type TransitionContext,
  composeMachineMiddleware,
  retryMiddleware as machineRetryMiddleware,
} from "./lib/durable/state-machine.ts";
export {
  type StateMachineStorage,
  type StateMachineLockToken,
  InMemoryStateMachineStorage,
} from "./lib/durable/state-machine-storage.ts";
export {
  type MachineSnapshot,
  type MachineState,
  type TransitionEvent,
  type ContextOf,
  type TransitionsOf,
  type EventsOf,
  type TerminalStates,
  type TransitionTo,
  type EventsMap,
  type EventData,
  type EventName,
  type SendParams,
  type StrictSchemas,
  transitionTo,
} from "./lib/durable/state-machine-types.ts";

// Zero-dep JSON-Schema-native schema builder + validator. Used by
// `defineSignal` to type signal payloads and snapshot validation rules
// onto suspended steps. The builder's `JsonSchema` is exported as
// `SignalPayloadSchema` so it doesn't collide with the workflow-schema
// `JsonSchema` above.
export {
  s,
  type Schema,
  type OptionalSchema,
  type JsonSchema as SignalPayloadSchema,
  type Infer,
} from "./lib/schema/builder.ts";
export { validate, type ValidationError, type ValidationResult } from "./lib/schema/validator.ts";

// First-class signal types
export {
  defineSignal,
  approvalSignal,
  ApprovalSchema,
  APPROVAL_NAME_PREFIX,
  type SignalType,
  type SignalPayload,
  type ApprovalDecision,
} from "./lib/signals/define-signal.ts";

// Injectable wall-clock time source + timer scheduler. `FakeWallClock`
// drives it in tests.
export {
  type WallClock,
  type TimerHandle,
  SystemWallClock,
  FakeWallClock,
} from "./lib/shared/wall-clock.ts";

// Plain-data contracts shared by the engine and its storage/transport
// packages: persisted retry shape, typed-error constraint, step cache and
// stream source/sink contracts.
export {
  type RetryPolicy,
  type WorkflowRetryPolicy,
  RETRY_POLICY_DEFAULTS,
} from "./lib/shared/retry-policy.ts";
export { type TaggedError } from "./lib/shared/tagged-error.ts";
export { type CacheStore, MemoryCache, type MemoryCacheConfig } from "./lib/shared/cache-store.ts";
export { type Streamable, type Sinkable } from "./lib/shared/streamable.ts";
