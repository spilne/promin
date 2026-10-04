// ---------------------------------------------------------------------------
// RemoteWorkflowStorage — client-side WorkflowStorage backed by HTTP.
//
// Implements the full WorkflowStorage interface by forwarding each call to
// a server running `createWorkflowStorageHandler`. Passes the portable
// conformance suite.
//
// Accepts any `fetch`-compatible function — lets tests swap a direct
// in-process handler in place of real HTTP without spinning up a server.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  WorkflowState,
  WorkflowStatusSnapshot,
  WorkflowRunSummary,
  SignalState,
  JournalStore,
  JournalEntry,
  CompletePendingResult,
  StepAttemptStore,
  StepAttemptRecord,
  CompensationLedgerStore,
  SignalTokenRecord,
  StreamChunk,
  WorkflowWakeup,
  OrphanedRun,
  AppendEntryParams,
  AppendPendingEntryParams,
  AppendStreamChunkParams,
  BatchSaveStepResultsParams,
  BeginCompensationParams,
  CancelWorkflowParams,
  CompletePendingEntryParams,
  CompleteWorkflowParams,
  CreateSignalTokenParams,
  CreateWorkflowParams,
  CreateWorkflowResult,
  DeliverSignalParams,
  DiscardJournalEntriesParams,
  DueSleep,
  FailWorkflowParams,
  FindDueSleepsParams,
  FindPendingSignalParams,
  FindWorkflowByIdempotencyKeyParams,
  HeartbeatParams,
  ListDueTimersParams,
  ListOrphanedRunsParams,
  ListSignalWakeupsParams,
  ListWorkflowsParams,
  LoadJournalParams,
  LoadRunHistoryParams,
  LoadStepAttemptsParams,
  MarkSignalTokenCompletedParams,
  MarkSignalTokenCompletedResult,
  PurgeCompletedParams,
  ReadStreamChunksParams,
  ReleaseLockParams,
  ResetStepsParams,
  SaveStepAttemptParams,
  SaveStepCompensationParams,
  SaveStepFailureParams,
  SaveStepResultParams,
  SaveTaskFailureParams,
  SaveTaskResultParams,
  SetWorkflowMetadataParams,
  StartFreshRunParams,
  SuspendWorkflowParams,
  TripwireWorkflowParams,
  TryLockAndLoadResult,
  TryLockParams,
  TryLockResult,
} from "@promin/workflow";
import { WIRE_CODEC, type RpcResponse, type StorageMethod } from "./wire.ts";

export type FetchLike = (req: Request) => Promise<Response>;

export interface RemoteWorkflowStorageConfig {
  /**
   * URL the client POSTs to. Path doesn't matter — the server handler
   * dispatches by request body, not path.
   */
  readonly url: string;
  /**
   * Fetch implementation. Defaults to the global `fetch`. Tests (and
   * in-process adapters) pass a handler-bound fetch that skips the network.
   */
  readonly fetch?: FetchLike;
  /**
   * Extra headers applied to every request (auth tokens, tracing, etc.).
   */
  readonly headers?: Record<string, string>;
}

export class RemoteWorkflowStorage
  implements WorkflowStorage, JournalStore, StepAttemptStore, CompensationLedgerStore
{
  private readonly url: string;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;

  constructor(config: RemoteWorkflowStorageConfig) {
    this.url = config.url;
    this.fetch = config.fetch ?? ((req) => globalThis.fetch(req));
    this.headers = { "content-type": "application/json", ...(config.headers ?? {}) };
  }

  /**
   * Core RPC primitive. Encodes params via LosslessJsonCodec, POSTs, decodes
   * the response. Surfaces server-side errors as thrown JS errors so callers
   * see the same semantics they'd get from an in-process storage.
   */
  private async call<T>(method: StorageMethod, params: unknown): Promise<T> {
    const body = JSON.stringify({ method, params: WIRE_CODEC.encode(params) });
    const req = new Request(this.url, { method: "POST", headers: this.headers, body });
    const res = await this.fetch(req);

    let envelope: RpcResponse;
    try {
      envelope = (await res.json()) as RpcResponse;
    } catch (err) {
      throw new Error(
        `RemoteWorkflowStorage: invalid response from ${this.url} (status ${res.status}): ${
          (err as Error).message
        }`,
      );
    }

    if (!envelope.ok) {
      // Rehydrate tagged errors — the server packs `_tag` + public
      // fields into the envelope so `.toMatchObject({ _tag: "..." })` and
      // downstream `err._tag === "FenceTokenMismatchError"` branches work
      // the same way they would for an in-process storage.
      if (envelope.errorTag) {
        const decoded = (envelope.errorFields ? WIRE_CODEC.decode(envelope.errorFields) : {}) as
          | Record<string, unknown>
          | undefined;
        const tagged = Object.assign(
          new Error(envelope.error),
          { _tag: envelope.errorTag },
          decoded ?? {},
        );
        throw tagged;
      }
      throw new Error(`RemoteWorkflowStorage.${method}: ${envelope.error}`);
    }
    return WIRE_CODEC.decode(envelope.result) as T;
  }

  // -------------------------------------------------------------------------
  // Thin delegates. Each method sends its params object as the RPC params,
  // `guard` included, so the server's dispatcher hands it straight to the
  // backing storage.
  // -------------------------------------------------------------------------

  loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    return this.call("loadWorkflow", { workflowId });
  }

  loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    return this.call("loadWorkflowStatus", { workflowId });
  }

  listWorkflows(params?: ListWorkflowsParams): Promise<WorkflowState[]> {
    return this.call("listWorkflows", params ?? {});
  }

  distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    return this.call("distinctWorkflowNames", params ?? {});
  }

  distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    return this.call("distinctWorkflowTypes", params ?? {});
  }

  distinctNamespaces(): Promise<string[]> {
    return this.call("distinctNamespaces", {});
  }

  cancelWorkflow(params: CancelWorkflowParams): Promise<void> {
    return this.call("cancelWorkflow", params);
  }

  createWorkflow(params: CreateWorkflowParams): Promise<CreateWorkflowResult> {
    return this.call("createWorkflow", params);
  }

  findWorkflowByIdempotencyKey(
    params: FindWorkflowByIdempotencyKeyParams,
  ): Promise<{ workflowId: string } | null> {
    return this.call("findWorkflowByIdempotencyKey", params);
  }

  saveStepResult(params: SaveStepResultParams): Promise<void> {
    return this.call("saveStepResult", params);
  }

  batchSaveStepResults(params: BatchSaveStepResultsParams): Promise<void> {
    return this.call("batchSaveStepResults", params);
  }

  saveStepFailure(params: SaveStepFailureParams): Promise<void> {
    return this.call("saveStepFailure", params);
  }

  saveTaskResult(params: SaveTaskResultParams): Promise<void> {
    return this.call("saveTaskResult", params);
  }

  saveTaskFailure(params: SaveTaskFailureParams): Promise<void> {
    return this.call("saveTaskFailure", params);
  }

  completeWorkflow(params: CompleteWorkflowParams): Promise<void> {
    return this.call("completeWorkflow", params);
  }

  failWorkflow(params: FailWorkflowParams): Promise<void> {
    return this.call("failWorkflow", params);
  }

  tripwireWorkflow(params: TripwireWorkflowParams): Promise<void> {
    return this.call("tripwireWorkflow", params);
  }

  suspendWorkflow(params: SuspendWorkflowParams): Promise<void> {
    return this.call("suspendWorkflow", params);
  }

  deliverSignal(params: DeliverSignalParams): Promise<void> {
    return this.call("deliverSignal", params);
  }

  loadSignals(workflowId: string): Promise<SignalState[]> {
    return this.call("loadSignals", { workflowId });
  }

  setWorkflowMetadata(params: SetWorkflowMetadataParams): Promise<void> {
    return this.call("setWorkflowMetadata", params);
  }

  // ---------------------------------------------------------------------------
  // Signal tokens — public-bearer authz; remoted as plain RPC.
  // ---------------------------------------------------------------------------

  createSignalToken(
    params: CreateSignalTokenParams,
  ): Promise<{ record: SignalTokenRecord; isCached: boolean }> {
    return this.call("createSignalToken", params);
  }

  findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    return this.call("findSignalTokenById", { tokenId });
  }

  markSignalTokenCompleted(
    params: MarkSignalTokenCompletedParams,
  ): Promise<MarkSignalTokenCompletedResult> {
    return this.call("markSignalTokenCompleted", params);
  }

  listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    return this.call("listSignalTokensForWorkflow", { workflowId });
  }

  appendStreamChunk(params: AppendStreamChunkParams): Promise<{ chunkIndex: number }> {
    return this.call("appendStreamChunk", params);
  }

  readStreamChunks(params: ReadStreamChunksParams): Promise<ReadonlyArray<StreamChunk>> {
    return this.call("readStreamChunks", params);
  }

  tryLock(params: TryLockParams): Promise<TryLockResult> {
    return this.call("tryLock", params);
  }

  tryLockAndLoad(params: TryLockParams): Promise<TryLockAndLoadResult> {
    return this.call("tryLockAndLoad", params);
  }

  releaseLock(params: ReleaseLockParams): Promise<void> {
    return this.call("releaseLock", params);
  }

  heartbeat(params: HeartbeatParams): Promise<void> {
    return this.call("heartbeat", params);
  }

  startFreshRun(params: StartFreshRunParams): Promise<number> {
    return this.call("startFreshRun", params);
  }

  loadRunHistory(params: LoadRunHistoryParams): Promise<WorkflowRunSummary[]> {
    return this.call("loadRunHistory", params);
  }

  /**
   * Reset the listed steps so a resumed run re-executes them. The server
   * feature-detects on the backing storage and surfaces a clear error if
   * that storage doesn't implement `resetSteps`.
   */
  resetSteps(params: ResetStepsParams): Promise<void> {
    return this.call("resetSteps", params);
  }

  purgeCompleted(params: PurgeCompletedParams): Promise<number> {
    return this.call("purgeCompleted", params);
  }

  // -------------------------------------------------------------------------
  // Scanner / recovery queries. Forwarded over the wire so the sleep and
  // signal scanners and coordinator recovery run the backend's indexed
  // queries instead of paging `listWorkflows`. The server feature-detects on
  // the backing storage and surfaces a clear error if it lacks one (every
  // bundled backend implements all three).
  // -------------------------------------------------------------------------

  listDueTimers(params: ListDueTimersParams): Promise<WorkflowWakeup[]> {
    return this.call("listDueTimers", params);
  }

  listSignalWakeups(params: ListSignalWakeupsParams): Promise<WorkflowWakeup[]> {
    return this.call("listSignalWakeups", params);
  }

  listOrphanedRuns(params: ListOrphanedRunsParams): Promise<OrphanedRun[]> {
    return this.call("listOrphanedRuns", params);
  }

  // -------------------------------------------------------------------------
  // JournalStore. Forwarded over the wire so .journaled() workflows (with
  // ctx.activity / ctx.sleep / ctx.signal) can run against a remote storage.
  // -------------------------------------------------------------------------

  loadJournal(params: LoadJournalParams): Promise<JournalEntry[]> {
    return this.call("loadJournal", params);
  }

  appendEntry(params: AppendEntryParams): Promise<void> {
    return this.call("appendEntry", params);
  }

  appendPendingEntry(params: AppendPendingEntryParams): Promise<void> {
    return this.call("appendPendingEntry", params);
  }

  completePendingEntry(params: CompletePendingEntryParams): Promise<CompletePendingResult> {
    return this.call("completePendingEntry", params);
  }

  discardJournalEntries(params: DiscardJournalEntriesParams): Promise<void> {
    return this.call("discardJournalEntries", params);
  }

  findDueSleeps(params: FindDueSleepsParams): Promise<DueSleep[]> {
    return this.call("findDueSleeps", params);
  }

  findPendingSignal(params: FindPendingSignalParams): Promise<JournalEntry | null> {
    return this.call("findPendingSignal", params);
  }

  // -------------------------------------------------------------------------
  // StepAttemptStore. Remote workers (whose effective storage IS this
  // RemoteWorkflowStorage) get the audit trail written on the central
  // server's storage — surfacing workerId per attempt to the dashboard's
  // run-detail / graph views.
  // -------------------------------------------------------------------------

  saveStepAttempt(params: SaveStepAttemptParams): Promise<void> {
    return this.call("saveStepAttempt", params);
  }

  loadStepAttempts(params: LoadStepAttemptsParams): Promise<StepAttemptRecord[]> {
    return this.call("loadStepAttempts", params);
  }

  // -------------------------------------------------------------------------
  // CompensationLedgerStore — forwarded to the server's storage, which
  // must implement it (the handler rejects the call otherwise).
  // -------------------------------------------------------------------------

  beginCompensation(params: BeginCompensationParams): Promise<boolean> {
    return this.call("beginCompensation", params);
  }

  saveStepCompensation(params: SaveStepCompensationParams): Promise<void> {
    return this.call("saveStepCompensation", params);
  }
}
