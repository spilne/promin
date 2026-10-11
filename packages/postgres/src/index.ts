// Storage
export { PostgresWorkflowStorage } from "./lib/postgres-workflow-storage.ts";
export { PostgresWorkflowVersionRegistry } from "./lib/postgres-workflow-version-registry.ts";
export {
  PostgresWorkflowSchemaStore,
  type PostgresWorkflowSchemaStoreConfig,
} from "./lib/postgres-workflow-schema-store.ts";
export { PostgresWorkerRegistry } from "./lib/postgres-worker-registry.ts";
export {
  PostgresNamespaceRegistry,
  type PostgresNamespaceRegistryConfig,
} from "./lib/postgres-namespace-registry.ts";
export {
  PostgresAgentRegistry,
  type PostgresAgentRegistryConfig,
} from "./lib/agent/agent-registry.ts";
export {
  PostgresRoleRegistry,
  type PostgresRoleRegistryConfig,
} from "./lib/agent/role-registry.ts";
export {
  PostgresSkillRegistry,
  type PostgresSkillRegistryConfig,
} from "./lib/agent/skill-registry.ts";
export {
  PostgresFragmentStore,
  type PostgresFragmentStoreConfig,
} from "./lib/agent/fragment-store.ts";
export {
  PostgresKnowledgeBaseStore,
  type PostgresKnowledgeBaseStoreConfig,
} from "./lib/agent/knowledge-base-store.ts";
export { PostgresMemoryStore, type PostgresMemoryStoreConfig } from "./lib/agent/memory-store.ts";
export { PostgresLeaseStore, type PostgresLeaseStoreConfig } from "./lib/agent/lease-store.ts";
export {
  PostgresPendingApprovalStore,
  type PostgresPendingApprovalStoreConfig,
} from "./lib/agent/pending-approval-store.ts";
export {
  PostgresSecretsStorage,
  type PostgresSecretsStorageConfig,
} from "./lib/agent/secrets-storage.ts";
export {
  PostgresToolAuditLogger,
  type PostgresToolAuditLoggerConfig,
  type ToolAuditLogQuery,
} from "./lib/agent/tool-audit-logger.ts";
export {
  PostgresToolHistoryStore,
  type PostgresToolHistoryStoreConfig,
} from "./lib/agent/tool-history-store.ts";
export {
  PostgresAgentInstanceRegistry,
  type PostgresAgentInstanceRegistryConfig,
} from "./lib/agent/agent-instance-registry.ts";
export { PostgresDagRegistry, type PostgresDagRegistryConfig } from "./lib/agent/dag-registry.ts";
export {
  PgVectorRetriever,
  type PgVectorEnsureSchemaOptions,
  type PgVectorRetrieverConfig,
} from "./lib/agent/pgvector-retriever.ts";
export {
  PostgresEvalRunStore,
  type PostgresEvalRunStoreConfig,
} from "./lib/evals/eval-run-store.ts";
export {
  PostgresEvalDatasetStore,
  type PostgresEvalDatasetStoreConfig,
} from "./lib/evals/eval-dataset-store.ts";
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
// Scheduler storage on its own — for hosts that drive schedules with their
// own loop (e.g. Zorya's `ZoryaScheduler`) instead of `DurableScheduler`.
export { PgSchedulerStorage, type PgSchedulerStorageConfig } from "./lib/pg-scheduler-storage.ts";
export { durableSchedules, durableScheduleTicks, leaderLeases } from "./lib/scheduler-schema.ts";

// Leader leases (fenced, server-clock TTL) — reusable beyond the scheduler
export {
  PgLeaderLeaseStore,
  type PgLeaderLeaseStoreConfig,
  assertPgLeaseCurrent,
} from "./lib/pg-leader-lease-store.ts";

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
  namespaceRegistry,
  machines,
  machineEvents,
  LOOKUP_BINDINGS,
} from "./lib/schema.ts";

// Metrics
export {
  PgWorkflowMetrics,
  type WorkflowMetrics,
  type MetricsQuery,
  type WorkflowMetricsSummary,
} from "./lib/pg-workflow-metrics.ts";
