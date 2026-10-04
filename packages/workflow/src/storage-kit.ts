// ---------------------------------------------------------------------------
// @promin/workflow/storage-kit — building blocks for storage backends.
//
// The helpers every bundled backend (in-memory, Postgres, Redis, SQLite)
// shares, for anyone writing another one: the status, run-source and
// journal codecs, `setWorkflowMetadata` / `listWorkflows` semantics,
// capability detection, fallbacks for the round-trip-saving methods, the
// child-wake signal name, schedule metadata filters and the step-queue
// dead-letter / percentile helpers. The contracts themselves come from
// `@promin/workflow` (`WorkflowStorage`), `@promin/workflow/scheduler`
// (`SchedulerStorage`) and `@promin/workflow/distributed` (`StepQueue`).
// ---------------------------------------------------------------------------

export {
  WORKFLOW_STATUSES,
  TERMINAL_WORKFLOW_STATUSES,
  isTerminalWorkflowStatus,
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  withoutCompensationLedger,
  encodeRunSource,
  decodeRunSource,
  RUN_SOURCE_CODES,
} from "./lib/durable/workflow-state.ts";
export { JOURNAL_STEP_TYPES } from "./lib/durable/activity-journal.ts";
export { workflowMetadataMatches, applyMetadataPatch } from "./lib/durable/storage/metadata.ts";
export {
  sortWorkflowRows,
  workflowSortKey,
  type WorkflowSortFields,
} from "./lib/durable/storage/ordering.ts";
export {
  tryLockAndLoadDefault,
  batchSaveStepResultsDefault,
} from "./lib/durable/storage/defaults.ts";
export {
  hasCapability,
  storageCapabilities,
  STORAGE_CAPABILITIES,
  type StorageCapability,
  type StorageCapabilityMap,
  type WorkflowStorageCapabilities,
} from "./lib/durable/storage/capabilities.ts";
export { FenceTokenMismatchError } from "./lib/durable/durable-pipeline-error.ts";
export { CHILD_ENDED_SIGNAL_PREFIX, childEndedSignalName } from "./lib/durable/child-wake.ts";
export { scheduleMetadataContains, flattenLeafPaths } from "./lib/scheduler/metadata-filter.ts";
export {
  DEFAULT_MAX_DELIVERIES,
  deadLetterError,
  percentileCont,
} from "./lib/distributed/step-queue.ts";
