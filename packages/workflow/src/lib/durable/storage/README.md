# Workflow storage

`WorkflowStorage` is everything the engine persists: run rows, step and task
rows, locks, signals, the activity journal and more. It is the composition of
small stores, plus optional parts that are named **capabilities**. The
bundled backends are `InMemoryWorkflowStorage` (`@promin/workflow`),
`PostgresWorkflowStorage` (`@promin/postgres`), `RedisWorkflowStorage`
(`@promin/redis`), `SqliteWorkflowStorage` (`@promin/sqlite`) and
`RemoteWorkflowStorage` (`@promin/workflow-remote`, over HTTP to any of the
others).

## The stores

| Store                  | Methods                                                                                                                                                                                                                                                                                                                                       |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WorkflowRunStore`     | `loadWorkflow`, `loadWorkflowStatus`, `createWorkflow`, `findWorkflowByIdempotencyKey`, `saveStepResult`, `batchSaveStepResults`, `saveStepFailure`, `saveTaskResult`, `saveTaskFailure`, `completeWorkflow`, `failWorkflow`, `cancelWorkflow`, `suspendWorkflow`, `setWorkflowMetadata`, `startFreshRun`, `tripwireWorkflow?`, `resetSteps?` |
| `WorkflowLockStore`    | `tryLock`, `tryLockAndLoad`, `releaseLock`, `heartbeat`                                                                                                                                                                                                                                                                                       |
| `WorkflowQueryStore`   | `listWorkflows`, `distinctWorkflowNames`, `distinctWorkflowTypes`, `distinctNamespaces`, `loadRunHistory`, `purgeCompleted`, `listWorkflowSummaries?`, `countWorkflows?`, `cancelStaleWorkflows?`                                                                                                                                             |
| `WorkflowScannerStore` | `listDueTimers?`, `listSignalWakeups?`, `listOrphanedRuns?`                                                                                                                                                                                                                                                                                   |
| `SignalStore`          | `deliverSignal`, `loadSignals`                                                                                                                                                                                                                                                                                                                |
| `SignalTokenStore`     | `createSignalToken`, `findSignalTokenById`, `markSignalTokenCompleted`, `listSignalTokensForWorkflow`                                                                                                                                                                                                                                         |
| `StreamStore`          | `appendStreamChunk`, `readStreamChunks`                                                                                                                                                                                                                                                                                                       |
| `RunEventStore`        | `notifyStepStarted?`, `subscribeToWorkflow?`                                                                                                                                                                                                                                                                                                  |

Optional extensions: `JournalStore` (`loadJournal`, `appendEntry`,
`appendPendingEntry`, `completePendingEntry`, `discardJournalEntries`,
`findDueSleeps`, `findPendingSignal`), `StepAttemptStore`
(`saveStepAttempt`, `loadStepAttempts`), `StepCheckpointStore`
(`checkpointStep`) and `CompensationLedgerStore` (`beginCompensation`,
`saveStepCompensation`).

## Params objects

Every storage method takes at most one argument, a params object. A fenced
write carries the lock holder's fence in that object's `guard` field:

```typescript
import { InMemoryWorkflowStorage } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
await storage.createWorkflow({ workflowId: "wf-1", workflowName: "demo", input: {} });

const lock = await storage.tryLock({ workflowId: "wf-1", lockDurationMs: 30_000 });
if (lock.acquired) {
  const guard = { fenceToken: lock.token };
  await storage.saveStepResult({
    workflowId: "wf-1",
    stepName: "a",
    result: 1,
    durationMs: 3,
    startedAt: new Date(),
    guard,
  });
  await storage.completeWorkflow({ workflowId: "wf-1", result: 1, guard });
  await storage.releaseLock({ workflowId: "wf-1", guard });
}
```

## Capabilities

The optional parts of the contract are named capabilities.
`hasCapability(storage, name)` is true when the storage implements every
method of that capability, and narrows it to the interface the capability
adds; `storageCapabilities(storage)` returns all the flags. The engine uses
them instead of probing methods.

```typescript
import { hasCapability, InMemoryWorkflowStorage, storageCapabilities } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
if (hasCapability(storage, "journal")) {
  const entries = await storage.loadJournal({ workflowId: "wf-1", stepName: "checkout" });
  console.log(entries.length);
}
console.log(storageCapabilities(storage)); // { journal: true, stepAttempts: true, ... }
```

| Capability           | What it enables                                                      | In-memory | Postgres | Redis | SQLite | Remote |
| -------------------- | -------------------------------------------------------------------- | :-------: | :------: | :---: | :----: | :----: |
| `journal`            | `.journaled()` steps (required by them)                              |    yes    |   yes    |  yes  |  yes   |  yes   |
| `stepAttempts`       | per-attempt history rows                                             |    yes    |   yes    |  yes  |  yes   |  yes   |
| `stepCheckpoint`     | one fenced call per settled step (row + attempts + run status)       |    yes    |   yes    |  yes  |  yes   |   –    |
| `compensationLedger` | durable saga rollback (`compensating` status, per-step ledger)       |    yes    |   yes    |  yes  |  yes   |  yes   |
| `tripwire`           | the `.tripwire()` step                                               |    yes    |   yes    |  yes  |   –    |  yes   |
| `resetSteps`         | `runner.resume({ fromStep })`                                        |    yes    |   yes    |  yes  |  yes   |  yes   |
| `runEvents`          | push `runner.subscribe` (otherwise polling)                          |    yes    |    –     |   –   |   –    |   –    |
| `stepStartedEvents`  | `step-started` events                                                |    yes    |    –     |   –   |   –    |   –    |
| `summaries`          | `listWorkflowSummaries` (no input/result blobs)                      |    yes    |   yes    |  yes  |  yes   |   –    |
| `countWorkflows`     | counts without loading rows                                          |    yes    |   yes    |  yes  |  yes   |   –    |
| `cancelStale`        | one-statement stale-run termination for `recover()`                  |     –     |    –     |   –   |  yes   |   –    |
| `dueTimers`          | indexed sleep / signal-timeout scan for the sleep scanner            |    yes    |   yes    |  yes  |  yes   |  yes   |
| `signalWakeups`      | indexed delivered-signal scan for the signal scanner                 |    yes    |   yes    |  yes  |  yes   |  yes   |
| `orphanedRuns`       | keyset recovery scan for `recover()` and the distributed coordinator |    yes    |   yes    |  yes  |  yes   |  yes   |

`PostgresWorkflowStorage` writes attempt rows only with `recordAttempts: true`.
Without a scanner capability the scanners and recovery fall back to paging
`listWorkflows`; without `countWorkflows` callers count listed rows.

## Fencing

A run is driven under a lease lock. `tryLock` / `tryLockAndLoad` return a
fence token (an opaque, monotonically increasing string), the driver
heartbeats the lock, and every write it makes carries the token in `guard`.

- **Fenced methods**: `saveStepResult`, `batchSaveStepResults`,
  `saveStepFailure`, `saveTaskResult`, `saveTaskFailure`, `checkpointStep`,
  `saveStepAttempt`, `completeWorkflow`, `failWorkflow`, `tripwireWorkflow`,
  `cancelWorkflow`, `suspendWorkflow`, `setWorkflowMetadata`,
  `startFreshRun`, `appendStreamChunk`, `createWorkflow` (a child, fenced on
  the parent's lock), `beginCompensation`, `saveStepCompensation`,
  `heartbeat`, `releaseLock`, and the journal writes `appendEntry`,
  `appendPendingEntry`, `completePendingEntry`, `discardJournalEntries`.
- **Atomic check.** The token is checked in the same step as the write: one
  SQL statement or transaction holding the lock row `FOR SHARE` (Postgres,
  against the server clock `NOW()`), one Lua script (Redis), one transaction
  (SQLite), one synchronous step (in-memory), forwarded with the RPC
  (remote). A takeover cannot land between the check and the write.
- **Mismatch.** A fenced write whose token is not the current one rejects
  with `FenceTokenMismatchError` and writes nothing.
- **Expired locks fence nothing.** A lock past its expiry rejects every
  fenced write carrying its token, `heartbeat` included, whether or not
  another driver has taken it over yet. A stale `releaseLock` never frees a
  newer holder's lock. So a driver that stalled past its lease learns it lost
  the run at its next write, and the runner stops with `WorkflowLockLostError`.
- **Unfenced writes.** A write without a token is accepted. Operator actions,
  `deliverSignal`, the scanners and the distributed worker's step-result
  writes are unfenced (workers are fenced by their queue claim instead; see
  [Distributed](../../distributed/README.md)).

Fencing limits what a stale driver can **write**; it cannot stop a stale
driver's step body from running. Anything a step does outside storage can
happen twice.

## Lifecycle semantics

- `completeWorkflow`, `failWorkflow`, `tripwireWorkflow` and `cancelWorkflow`
  only move a run out of a non-terminal status; on a `completed`, `failed`
  (cancelled included) or `tripwire` run they are no-ops. Only
  `startFreshRun` and `resetSteps` leave a terminal status.
- `cancelWorkflow` stores the failure with `errorTag: "WorkflowCancelledError"`;
  `{ cascade: true }` cancels descendants too.
- `startFreshRun` bumps the run number and, atomically with it, drops the
  journal, the delivered signals and the compensation ledger, so the new run
  never replays the previous one.
- `createWorkflow` is create-if-absent and reports `{ created }`.
- An idempotency key past its expiry is released when a new run claims it.

## Signals

`deliverSignal({ workflowId, signalName, payload })` stores a named value on
the current run. A second delivery under the same name replaces the first
(last delivery wins); `loadSignals` never consumes them; `startFreshRun`
clears them; `resetSteps` keeps them. Delivery is unfenced and does not
resume the run — the signal scanner does (or the next `runner.run` of that
`workflowId`).

## Journal

`completePendingEntry` reports `{ completed, exit }`: whether this call
completed the entry and the exit stored on it. A signal delivery and a signal
timeout race on that call; both sides continue with the stored winner.
Signal exits are stored tagged (`{ $signal: "delivered", value }` /
`{ $signal: "timeout" }`). `discardJournalEntries` removes the failed attempt's
entries so the next attempt re-runs them.

## Writing a backend

1. Implement `WorkflowStorage` (plus the capabilities you want) with the
   params shapes exported from `@promin/workflow` (`SaveStepResultParams`,
   `CompleteWorkflowParams`, `FencedWrite`, …). Check every fenced write's
   `guard.fenceToken` atomically with the write.
2. Reuse `@promin/workflow/storage-kit`: metadata patch semantics
   (`applyMetadataPatch`), list ordering (`sortWorkflowRows`,
   `workflowSortKey`), `tryLockAndLoadDefault`, `batchSaveStepResultsDefault`,
   status and journal enums, run-source codecs and the child-wake signal name.
3. Run the conformance suites from `@promin/workflow/testing` under
   `bun:test`: `storageTestSuite` (CRUD, locks, fencing matrix, lifecycle,
   signals, journal, capabilities), `journalReplayTestSuite`,
   `zombieWorkerTestSuite` (a stalled runner loses its lock and every later
   write fails), and, for the other stores, `stepQueueTestSuite`,
   `schedulerStorageTestSuite`, `stateMachineStorageTestSuite`,
   `versionRegistryTestSuite` / `versionDrainTestSuite`,
   `workerRegistryConformance`, `workflowStartQueueTestSuite` and
   `workflowAdvertisementRegistryTestSuite`.

```typescript
import { describe } from "bun:test";
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { storageTestSuite } from "@promin/workflow/testing";

describe("my storage", () => {
  storageTestSuite(() => new InMemoryWorkflowStorage());
});
```
