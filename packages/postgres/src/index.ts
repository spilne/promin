// Drizzle DB type
export type { DrizzleDb } from "./lib/drizzle-db.ts";

// Storage
export { PostgresWorkflowStorage } from "./lib/postgres-workflow-storage.ts";
export { PostgresWorkflowVersionRegistry } from "./lib/postgres-workflow-version-registry.ts";
export { PostgresWorkerRegistry } from "./lib/postgres-worker-registry.ts";
export { type PostgresStorageConfig } from "./lib/config.ts";
export { migrate, type MigrateOptions } from "./lib/migrate.ts";

// Lookup utilities
export { defineLookup, type Lookup, type LookupEntry } from "./lib/lookup.ts";
export {
  createLookupTable,
  type LookupTable,
  type LookupBinding,
  seedLookupEnums,
  validateLookupEnums,
} from "./lib/lookup-table.ts";

// Workflow-specific lookups (core enums → integer IDs)
export {
  WorkflowStatusIds,
  StepStatusIds,
  StepTypeIds,
  AttemptTypeIds,
} from "./lib/workflow-lookups.ts";

// Durable scheduler
export {
  DurableScheduler,
  createDurableScheduler,
  type DurableScheduleConfig,
  type DurableSchedulerConfig,
} from "./lib/durable-scheduler.ts";
export { durableSchedules, durableScheduleTicks } from "./lib/scheduler-schema.ts";

// SKIP LOCKED queue (no extension required)
export { PgQueue, type PgQueueConfig } from "./lib/pg-queue.ts";

// LISTEN/NOTIFY change stream (CDC)
export { PgChangeStream, type PgChangeStreamConfig } from "./lib/pg-change-stream.ts";

// Distributed step queue (SKIP LOCKED)
export { PgStepQueue, type PgStepQueueConfig } from "./lib/pg-step-queue.ts";

// Workflow advertisements + start queue — Postgres-backed for multi-replica
// coordination. Same `WorkflowAdvertisementRegistry` / `WorkflowStartQueue`
// interfaces the in-memory + sqlite backends implement.
export {
  PgWorkflowAdvertisementRegistry,
  type PgWorkflowAdvertisementRegistryConfig,
} from "./lib/pg-workflow-advertisements.ts";
export {
  PgWorkflowStartQueue,
  type PgWorkflowStartQueueConfig,
} from "./lib/pg-workflow-start-queue.ts";

// Leader election (advisory lock)
export { PgLeaderElection, type PgLeaderElectionConfig } from "./lib/pg-leader-election.ts";

// State backend (topology checkpoints)
export { PgStateBackend, type PgStateBackendConfig } from "./lib/pg-state-backend.ts";

// State machine storage
export { PgStateMachineStorage } from "./lib/pg-state-machine-storage.ts";

// Schema exports for custom migrations
export {
  workflows,
  workflowRuns,
  workflowSteps,
  workflowStepTasks,
  workflowSignals,
  workflowLocks,
  workflowStatusTable,
  stepStatusTable,
  stepTypeTable,
  stepQueue,
  stepAttempts,
  attemptTypeTable,
  workerRegistry,
  machines,
  machineEvents,
  LOOKUP_BINDINGS,
} from "./lib/schema.ts";
export { createQueueTable, type QueueTable } from "./lib/pg-queue-schema.ts";
export { createTopologyStateTable, topologyState } from "./lib/pg-state-schema.ts";

// Schema utilities
export { ensureTable } from "./lib/schema-utils.ts";

// Concurrency primitives
export { PgRateLimiter, type PgRateLimiterConfig } from "./lib/pg-rate-limiter.ts";
export { PgThrottle, type PgThrottleConfig } from "./lib/pg-throttle.ts";
export { PgSingleflight, type PgSingleflightConfig } from "./lib/pg-singleflight.ts";
export { PgRef, type PgRefConfig } from "./lib/pg-ref.ts";

// Metrics
export {
  PgWorkflowMetrics,
  type WorkflowMetrics,
  type MetricsQuery,
  type WorkflowMetricsSummary,
} from "./lib/pg-workflow-metrics.ts";
