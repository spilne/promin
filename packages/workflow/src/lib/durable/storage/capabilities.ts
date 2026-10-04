// ---------------------------------------------------------------------------
// Storage capabilities — the optional parts of the storage contract, named.
//
// Every capability is a set of methods a storage either implements in full
// or leaves out. `storageCapabilities()` answers all of them at once and
// `hasCapability()` narrows a storage to the interface a capability brings,
// both from one table, so no caller probes methods ad hoc.
// ---------------------------------------------------------------------------

import type { CompensationLedgerStore } from "./compensation-ledger-store.ts";
import type { JournalStore } from "./journal-store.ts";
import type { WorkflowQueryStore } from "./query-store.ts";
import type { RunEventStore } from "./run-event-store.ts";
import type { WorkflowRunStore } from "./run-store.ts";
import type { WorkflowScannerStore } from "./scanner-store.ts";
import type { StepAttemptStore, StepCheckpointStore } from "./step-attempt-store.ts";

/** The interface each capability adds to a storage. */
export interface StorageCapabilityMap {
  /** `.journaled()` steps (`JournalStore`). */
  journal: JournalStore;
  /** `discardJournalEntries`: a step retry re-executes recorded failures. */
  journalDiscard: Required<Pick<JournalStore, "discardJournalEntries">>;
  /** Step attempt history (`StepAttemptStore`). */
  stepAttempts: StepAttemptStore;
  /** One-call step checkpoint (`StepCheckpointStore`). */
  stepCheckpoint: StepCheckpointStore;
  /** Durable saga rollback (`CompensationLedgerStore`). */
  compensationLedger: CompensationLedgerStore;
  /** `tripwireWorkflow`: the `.tripwire()` builder primitive. */
  tripwire: Required<Pick<WorkflowRunStore, "tripwireWorkflow">>;
  /** `resetSteps`: `WorkflowRunner.resume(workflowId, fromStep)`. */
  resetSteps: Required<Pick<WorkflowRunStore, "resetSteps">>;
  /** `subscribeToWorkflow`: live per-run event streams. */
  runEvents: Required<Pick<RunEventStore, "subscribeToWorkflow">>;
  /** `notifyStepStarted`: `step-started` events. */
  stepStartedEvents: Required<Pick<RunEventStore, "notifyStepStarted">>;
  /** `listWorkflowSummaries`: lean list rows. */
  summaries: Required<Pick<WorkflowQueryStore, "listWorkflowSummaries">>;
  /** `countWorkflows`: counts without loading rows. */
  countWorkflows: Required<Pick<WorkflowQueryStore, "countWorkflows">>;
  /** `cancelStaleWorkflows`: one-statement stale-run termination. */
  cancelStale: Required<Pick<WorkflowQueryStore, "cancelStaleWorkflows">>;
  /** `listDueTimers`: the indexed sleep / signal-timeout scan. */
  dueTimers: Required<Pick<WorkflowScannerStore, "listDueTimers">>;
  /** `listSignalWakeups`: the indexed delivered-signal scan. */
  signalWakeups: Required<Pick<WorkflowScannerStore, "listSignalWakeups">>;
  /** `listOrphanedRuns`: the indexed recovery scan. */
  orphanedRuns: Required<Pick<WorkflowScannerStore, "listOrphanedRuns">>;
}

/** Name of one optional storage capability. */
export type StorageCapability = keyof StorageCapabilityMap;

/** Which optional capabilities a storage has, one flag each. */
export type WorkflowStorageCapabilities = { readonly [K in StorageCapability]: boolean };

/** The methods that make up each capability. */
const CAPABILITY_METHODS: { readonly [K in StorageCapability]: readonly string[] } = {
  journal: [
    "loadJournal",
    "appendEntry",
    "appendPendingEntry",
    "completePendingEntry",
    "findDueSleeps",
    "findPendingSignal",
  ],
  journalDiscard: ["discardJournalEntries"],
  stepAttempts: ["saveStepAttempt", "loadStepAttempts"],
  stepCheckpoint: ["checkpointStep"],
  compensationLedger: ["beginCompensation", "saveStepCompensation"],
  tripwire: ["tripwireWorkflow"],
  resetSteps: ["resetSteps"],
  runEvents: ["subscribeToWorkflow"],
  stepStartedEvents: ["notifyStepStarted"],
  summaries: ["listWorkflowSummaries"],
  countWorkflows: ["countWorkflows"],
  cancelStale: ["cancelStaleWorkflows"],
  dueTimers: ["listDueTimers"],
  signalWakeups: ["listSignalWakeups"],
  orphanedRuns: ["listOrphanedRuns"],
};

/** Every capability name, in declaration order. */
export const STORAGE_CAPABILITIES = Object.keys(CAPABILITY_METHODS) as readonly StorageCapability[];

/**
 * True when `storage` implements every method of `capability`, narrowing
 * it to the interface the capability adds.
 *
 * ```ts
 * if (hasCapability(storage, "journal")) await storage.loadJournal({ workflowId, stepName });
 * ```
 */
export function hasCapability<S extends object, K extends StorageCapability>(
  storage: S,
  capability: K,
): storage is S & StorageCapabilityMap[K] {
  const record = storage as Record<string, unknown>;
  return CAPABILITY_METHODS[capability].every((m) => typeof record[m] === "function");
}

/** Every optional capability of `storage`, as flags. */
export function storageCapabilities(storage: object): WorkflowStorageCapabilities {
  const out = {} as Record<StorageCapability, boolean>;
  for (const capability of STORAGE_CAPABILITIES) {
    out[capability] = hasCapability(storage, capability);
  }
  return out;
}
