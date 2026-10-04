// ---------------------------------------------------------------------------
// @promin/workflow/storage-kit — building blocks for WorkflowStorage backends.
//
// The helpers every bundled backend (in-memory, Postgres, Redis, SQLite)
// shares, for anyone writing another one: the status and journal enums,
// `setWorkflowMetadata` / `listWorkflows` semantics, capability detection,
// and fallbacks for the round-trip-saving methods. The contract itself
// (`WorkflowStorage` and its stores) is exported from `@promin/workflow`.
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
