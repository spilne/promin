// ---------------------------------------------------------------------------
// WorkflowStorage — pluggable persistence interface.
//
// The contract is split into cohesive stores (see `./storage/`):
//
//   WorkflowRunStore         run row, step / task rows, lifecycle transitions
//   WorkflowLockStore        run lock (lease) and its fence token
//   WorkflowQueryStore       listing, counts, run history, retention
//   WorkflowScannerStore     indexed scanner / recovery queries (optional)
//   SignalStore              signal delivery
//   SignalTokenStore         public-bearer signal tokens
//   StreamStore              per-workflow streams
//   RunEventStore            live run events (optional)
//
// `WorkflowStorage` is their composition and what everything accepts. The
// optional capabilities — the members marked optional above plus the
// `JournalStore`, `StepAttemptStore`, `StepCheckpointStore` and
// `CompensationLedgerStore` extensions — are named in
// `StorageCapabilityMap`; `hasCapability()` / `storageCapabilities()` detect
// them.
//
// Every method takes at most one argument; a method with more than one input
// takes a params object, and a fenced write carries its fence in that
// object's `guard` field (`FencedWrite`).
// ---------------------------------------------------------------------------

import type { WorkflowLockStore } from "./storage/lock-store.ts";
import type { WorkflowQueryStore } from "./storage/query-store.ts";
import type { RunEventStore } from "./storage/run-event-store.ts";
import type { WorkflowRunStore } from "./storage/run-store.ts";
import type { WorkflowScannerStore } from "./storage/scanner-store.ts";
import type { SignalStore, SignalTokenStore } from "./storage/signal-store.ts";
import type { StreamStore } from "./storage/stream-store.ts";

export type { FenceToken, FenceGuard, FencedWrite } from "./storage/fencing.ts";
export type {
  WorkflowRunStore,
  CreateWorkflowParams,
  CreateWorkflowResult,
  FindWorkflowByIdempotencyKeyParams,
  StepResultRecord,
  SaveStepResultParams,
  BatchSaveStepResultsParams,
  SaveStepFailureParams,
  SaveTaskResultParams,
  SaveTaskFailureParams,
  CompleteWorkflowParams,
  FailWorkflowParams,
  TripwireWorkflowParams,
  CancelWorkflowParams,
  SuspendWorkflowParams,
  SetWorkflowMetadataParams,
  StartFreshRunParams,
  ResetStepsParams,
} from "./storage/run-store.ts";
export type {
  WorkflowLockStore,
  TryLockParams,
  TryLockResult,
  TryLockAndLoadResult,
  ReleaseLockParams,
  HeartbeatParams,
} from "./storage/lock-store.ts";
export type {
  WorkflowQueryStore,
  WorkflowOrderBy,
  WorkflowListFilter,
  ListWorkflowsParams,
  LoadRunHistoryParams,
  PurgeCompletedParams,
  CancelStaleWorkflowsParams,
} from "./storage/query-store.ts";
export type {
  WorkflowScannerStore,
  WorkflowWakeup,
  OrphanedRun,
  ListDueTimersParams,
  ListSignalWakeupsParams,
  ListOrphanedRunsParams,
} from "./storage/scanner-store.ts";
export type {
  SignalStore,
  DeliverSignalParams,
  SignalTokenStore,
  SignalTokenRecord,
  CreateSignalTokenParams,
  MarkSignalTokenCompletedParams,
  MarkSignalTokenCompletedResult,
} from "./storage/signal-store.ts";
export type {
  StreamStore,
  StreamChunk,
  AppendStreamChunkParams,
  ReadStreamChunksParams,
} from "./storage/stream-store.ts";
export type {
  RunEventStore,
  NotifyStepStartedParams,
  SubscribeToWorkflowParams,
} from "./storage/run-event-store.ts";
export type {
  StepAttemptStore,
  SaveStepAttemptParams,
  LoadStepAttemptsParams,
  StepCheckpoint,
  StepCheckpointStore,
  CheckpointStepParams,
} from "./storage/step-attempt-store.ts";
export type {
  CompensationLedgerStore,
  StepCompensationOutcome,
  BeginCompensationParams,
  SaveStepCompensationParams,
} from "./storage/compensation-ledger-store.ts";
export type {
  JournalStore,
  LoadJournalParams,
  AppendEntryParams,
  AppendPendingEntryParams,
  CompletePendingEntryParams,
  DiscardJournalEntriesParams,
  FindDueSleepsParams,
  FindPendingSignalParams,
  DueSleep,
} from "./storage/journal-store.ts";
export {
  hasCapability,
  storageCapabilities,
  STORAGE_CAPABILITIES,
  type StorageCapability,
  type StorageCapabilityMap,
  type WorkflowStorageCapabilities,
} from "./storage/capabilities.ts";
export { workflowMetadataMatches } from "./storage/metadata.ts";

/**
 * The workflow storage contract: every store a runner, scanner, coordinator
 * or dashboard needs, composed. Optional members are capabilities (see
 * `StorageCapabilityMap`).
 */
export interface WorkflowStorage
  extends
    WorkflowRunStore,
    WorkflowLockStore,
    WorkflowQueryStore,
    WorkflowScannerStore,
    SignalStore,
    SignalTokenStore,
    StreamStore,
    RunEventStore {}
