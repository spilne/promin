// ---------------------------------------------------------------------------
// Fallback implementations of the round-trip-saving storage methods, for a
// custom storage that has no better way to do them.
// ---------------------------------------------------------------------------

import type { TryLockAndLoadResult, TryLockParams, WorkflowLockStore } from "./lock-store.ts";
import type { BatchSaveStepResultsParams, WorkflowRunStore } from "./run-store.ts";

/**
 * `tryLockAndLoad` as `tryLock` then `loadWorkflow`. Atomic across the pair
 * only if the underlying storage serializes both calls (in-process
 * backends are fine); a distributed storage with no coupling between the
 * two may see another writer commit between them. Backends that can do
 * better should.
 */
export async function tryLockAndLoadDefault(
  params: TryLockParams & {
    readonly storage: Pick<WorkflowLockStore, "tryLock"> & Pick<WorkflowRunStore, "loadWorkflow">;
  },
): Promise<TryLockAndLoadResult> {
  const { storage, workflowId, lockDurationMs } = params;
  const { acquired, token } = await storage.tryLock({ workflowId, lockDurationMs });
  const state = await storage.loadWorkflow(workflowId);
  return { locked: acquired, token, state };
}

/**
 * `batchSaveStepResults` as one `saveStepResult` per record, in order.
 * Intentionally not concurrent — callers rely on the batch staying ordered
 * so that step rows created later in the batch sort after earlier ones.
 */
export async function batchSaveStepResultsDefault(
  params: BatchSaveStepResultsParams & {
    readonly storage: Pick<WorkflowRunStore, "saveStepResult">;
  },
): Promise<void> {
  const { storage, records, guard } = params;
  for (const r of records) {
    await storage.saveStepResult({ ...r, guard });
  }
}
