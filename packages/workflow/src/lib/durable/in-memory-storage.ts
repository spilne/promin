// ---------------------------------------------------------------------------
// InMemoryWorkflowStorage — for testing and single-process use
// ---------------------------------------------------------------------------
//
// Single-process only. State lives in plain Maps on the JS heap, which is
// not shared across Bun workers or Node worker_threads (each gets its own
// isolate). For multi-worker deployments, use PostgresWorkflowStorage or
// another networked backend via the WorkflowStorage interface.
//
// The facade owns the run rows and composes one module per store:
//
//   in-memory/lease-table.ts    WorkflowLockStore + fencing
//   in-memory/journal.ts        JournalStore
//   in-memory/signal-tokens.ts  SignalTokenStore
//   in-memory/streams.ts        StreamStore
//   in-memory/run-events.ts     RunEventStore
//   in-memory/run-record.ts     the run row and the helpers over it
//
// Atomicity: every method checks its fence and writes with no `await` in
// between, so concurrent callers on the same event loop cannot interleave.
// ---------------------------------------------------------------------------

import type {
  AppendEntryParams,
  AppendPendingEntryParams,
  AppendStreamChunkParams,
  BatchSaveStepResultsParams,
  BeginCompensationParams,
  CancelWorkflowParams,
  CheckpointStepParams,
  CompensationLedgerStore,
  CompletePendingEntryParams,
  CompleteWorkflowParams,
  CreateSignalTokenParams,
  CreateWorkflowParams,
  CreateWorkflowResult,
  DeliverSignalParams,
  DiscardJournalEntriesParams,
  DueSleep,
  FailWorkflowParams,
  FenceGuard,
  FindDueSleepsParams,
  FindPendingSignalParams,
  FindWorkflowByIdempotencyKeyParams,
  HeartbeatParams,
  JournalStore,
  ListDueTimersParams,
  ListOrphanedRunsParams,
  ListSignalWakeupsParams,
  ListWorkflowsParams,
  LoadJournalParams,
  LoadRunHistoryParams,
  LoadStepAttemptsParams,
  MarkSignalTokenCompletedParams,
  MarkSignalTokenCompletedResult,
  NotifyStepStartedParams,
  OrphanedRun,
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
  SignalTokenRecord,
  StartFreshRunParams,
  StepAttemptStore,
  StepCheckpointStore,
  StepResultRecord,
  StreamChunk,
  SubscribeToWorkflowParams,
  SuspendWorkflowParams,
  TripwireWorkflowParams,
  TryLockAndLoadResult,
  TryLockParams,
  TryLockResult,
  WorkflowListFilter,
  WorkflowStorage,
  WorkflowWakeup,
} from "./workflow-storage.ts";
import { applyMetadataPatch } from "./storage/metadata.ts";
import { sortWorkflowRows } from "./storage/ordering.ts";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  isTerminalWorkflowStatus,
  withoutCompensationLedger as withoutLedger,
  type SignalState,
  type StepAttemptRecord,
  type StepState,
  type WorkflowRunEvent,
  type WorkflowRunSummary,
  type WorkflowState,
  type WorkflowStatusSnapshot,
} from "./workflow-state.ts";
import type { CompletePendingResult, JournalEntry } from "./activity-journal.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { InMemoryLeaseTable } from "./in-memory/lease-table.ts";
import { InMemoryJournal } from "./in-memory/journal.ts";
import { InMemorySignalTokens } from "./in-memory/signal-tokens.ts";
import { InMemoryStreams } from "./in-memory/streams.ts";
import { InMemoryRunEvents } from "./in-memory/run-events.ts";
import {
  currentRunSummary,
  idempotencyIndexKey,
  matchesListFilter,
  sortFieldsOf,
  statusSnapshot,
  stepsByName,
  toWorkflowState,
  writeTaskRow,
  type MutableWorkflow,
} from "./in-memory/run-record.ts";

export class InMemoryWorkflowStorage
  implements
    WorkflowStorage,
    StepAttemptStore,
    StepCheckpointStore,
    CompensationLedgerStore,
    JournalStore
{
  private workflows = new Map<string, MutableWorkflow>();
  private readonly leases: InMemoryLeaseTable;
  private readonly journal: InMemoryJournal;
  private readonly signalTokens: InMemorySignalTokens;
  private readonly streams: InMemoryStreams;
  private readonly events = new InMemoryRunEvents();
  private signals = new Map<string, SignalState[]>();
  private attempts = new Map<string, StepAttemptRecord[]>();
  private runHistory = new Map<string, WorkflowRunSummary[]>();
  /** Workflow id by `idempotencyIndexKey`, for the run that last claimed the key. */
  private idempotencyIndex = new Map<string, string>();
  /** Child workflow ids by parent workflow id, for cascade cancel. */
  private children = new Map<string, Set<string>>();
  private readonly namespace: string | null;
  /**
   * Time source. Every timestamp + lock-expiry check routes through here
   * — pass a `FakeWallClock` in tests to drive deterministic semantics
   * without real waits.
   */
  private readonly clock: WallClock;

  constructor(config?: { namespace?: string | null; instanceId?: string; clock?: WallClock }) {
    this.namespace = config?.namespace ?? null;
    this.clock = config?.clock ?? SystemWallClock;
    this.leases = new InMemoryLeaseTable({
      instanceId: config?.instanceId ?? crypto.randomUUID(),
      clock: this.clock,
    });
    this.journal = new InMemoryJournal(this.clock);
    this.signalTokens = new InMemorySignalTokens(this.clock);
    this.streams = new InMemoryStreams(this.clock);
  }

  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
  }

  private emit(params: { workflowId: string; event: WorkflowRunEvent; terminal: boolean }): void {
    this.events.emit(params);
  }

  private checkFence(params: { workflowId: string; guard?: FenceGuard }): void {
    this.leases.checkFence(params);
  }

  // -------------------------------------------------------------------------
  // WorkflowRunStore
  // -------------------------------------------------------------------------

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const wf = this.workflows.get(workflowId);
    return wf ? toWorkflowState(wf) : null;
  }

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    const wf = this.workflows.get(workflowId);
    return wf ? statusSnapshot(wf) : null;
  }

  async createWorkflow({ guard, ...params }: CreateWorkflowParams): Promise<CreateWorkflowResult> {
    // A child create is fenced on the parent's lock.
    if (guard?.fenceToken) {
      if (params.parentWorkflowId === undefined) {
        throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
      }
      this.checkFence({ workflowId: params.parentWorkflowId, guard });
    }
    // Idempotency-key path: if `(namespace, workflowName, idempotencyKey)` is already
    // claimed by an unexpired row, return that row instead. Mirrors the
    // partial-unique-index conflict resolution that postgres does
    // natively, which is what makes the redirect race-safe.
    if (params.idempotencyKey) {
      const claimed = this.idempotencyClaim({
        namespace: this.resolveNamespace(params.namespace),
        workflowName: params.workflowName,
        idempotencyKey: params.idempotencyKey,
        now: this.clock.now(),
      });
      if (claimed) return { created: false, existing: toWorkflowState(claimed) };
    }

    const existing = this.workflows.get(params.workflowId);
    if (existing) return { created: false, existing: toWorkflowState(existing) };

    const now = this.clock.now();
    const namespace = this.resolveNamespace(params.namespace);
    if (params.idempotencyKey) {
      const key = idempotencyIndexKey({
        namespace,
        workflowName: params.workflowName,
        idempotencyKey: params.idempotencyKey,
      });
      this.idempotencyIndex.set(key, params.workflowId);
    }
    if (params.parentWorkflowId !== undefined) {
      const siblings = this.children.get(params.parentWorkflowId) ?? new Set<string>();
      siblings.add(params.workflowId);
      this.children.set(params.parentWorkflowId, siblings);
    }
    this.workflows.set(params.workflowId, {
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      workflowType: params.workflowType,
      parentWorkflowId: params.parentWorkflowId,
      namespace,
      status: "pending",
      version: params.version,
      run: 1,
      input: params.input,
      metadata: params.metadata,
      runSource: params.runSource,
      runSourceId: params.runSourceId,
      idempotencyKey: params.idempotencyKey,
      idempotencyExpiresAt: params.idempotencyExpiresAt,
      steps: new Map(),
      ownedTasks: new Map(),
      handedOut: 0,
      createdAt: now,
      updatedAt: now,
    });
    return { created: true };
  }

  /**
   * The run holding an unexpired claim on `(namespace, workflowName,
   * idempotencyKey)`, if any. An index lookup; the row is re-checked, since
   * the indexed run may have been purged or its claim may have expired.
   */
  private idempotencyClaim(params: {
    namespace: string | undefined;
    workflowName: string;
    idempotencyKey: string;
    now: Date;
  }): MutableWorkflow | undefined {
    const id = this.idempotencyIndex.get(idempotencyIndexKey(params));
    const wf = id !== undefined ? this.workflows.get(id) : undefined;
    if (
      wf &&
      wf.namespace === params.namespace &&
      wf.workflowName === params.workflowName &&
      wf.idempotencyKey === params.idempotencyKey &&
      wf.idempotencyExpiresAt &&
      wf.idempotencyExpiresAt.getTime() > params.now.getTime()
    ) {
      return wf;
    }
    return undefined;
  }

  async findWorkflowByIdempotencyKey(
    params: FindWorkflowByIdempotencyKeyParams,
  ): Promise<{ workflowId: string } | null> {
    const wf = this.idempotencyClaim({
      namespace: this.resolveNamespace(params.namespace),
      workflowName: params.workflowName,
      idempotencyKey: params.idempotencyKey,
      now: params.now,
    });
    return wf ? { workflowId: wf.workflowId } : null;
  }

  /** Transition pending → running on first step activity. */
  private markRunning(wf: MutableWorkflow): void {
    if (wf.status === "pending") {
      wf.status = "running";
      wf.startedAt = this.clock.now();
    }
  }

  async saveStepResult({ guard, ...record }: SaveStepResultParams): Promise<void> {
    this.checkFence({ workflowId: record.workflowId, guard });
    this.writeStepResult(record);
  }

  /** The unfenced body of `saveStepResult`; synchronous so a batch is one step. */
  private writeStepResult(params: StepResultRecord): void {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;
    this.markRunning(wf);

    const existing = wf.steps.get(params.stepName);
    const now = this.clock.now();
    wf.steps.set(params.stepName, {
      stepName: params.stepName,
      run: wf.run,
      status: "completed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      result: params.result,
      metadata: params.metadata ?? existing?.metadata,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    });
    wf.updatedAt = now;
    this.emit({
      workflowId: params.workflowId,
      event: {
        type: "step-completed",
        stepName: params.stepName,
        result: params.result,
        durationMs: params.durationMs,
        at: now,
      },
      terminal: false,
    });
  }

  async batchSaveStepResults({ records, guard }: BatchSaveStepResultsParams): Promise<void> {
    // Every fence is checked before the first write and nothing awaits in
    // between, so the batch lands whole or not at all.
    for (const workflowId of new Set(records.map((r) => r.workflowId))) {
      this.checkFence({ workflowId, guard });
    }
    for (const r of records) this.writeStepResult(r);
  }

  async saveStepFailure({ guard, ...params }: SaveStepFailureParams): Promise<void> {
    this.checkFence({ workflowId: params.workflowId, guard });
    this.writeStepFailure(params);
  }

  /** The unfenced body of `saveStepFailure`. */
  private writeStepFailure(params: Omit<SaveStepFailureParams, "guard">): void {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;
    this.markRunning(wf);

    const existing = wf.steps.get(params.stepName);
    const now = this.clock.now();
    wf.steps.set(params.stepName, {
      stepName: params.stepName,
      run: wf.run,
      status: "failed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      error: params.error,
      ...(params.errorTag !== undefined && { errorTag: params.errorTag }),
      metadata: params.metadata ?? existing?.metadata,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    });
    this.emit({
      workflowId: params.workflowId,
      event: { type: "step-failed", stepName: params.stepName, error: params.error, at: now },
      terminal: false,
    });
    wf.updatedAt = now;
  }

  async saveTaskResult({ result, ...params }: SaveTaskResultParams): Promise<void> {
    this.saveTask({ ...params, outcome: { status: "completed", result } });
  }

  async saveTaskFailure({ error, ...params }: SaveTaskFailureParams): Promise<void> {
    this.saveTask({ ...params, outcome: { status: "failed", error } });
  }

  /** `saveTaskResult` / `saveTaskFailure`: fence, then upsert the task row. */
  private saveTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    guard?: FenceGuard;
    outcome:
      | { readonly status: "completed"; readonly result: unknown }
      | { readonly status: "failed"; readonly error: string };
  }): void {
    this.checkFence(params);
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;
    writeTaskRow({
      wf,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: params.outcome,
      now: this.clock.now(),
    });
  }

  async completeWorkflow({ workflowId, result, guard }: CompleteWorkflowParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf || isTerminalWorkflowStatus(wf.status)) return;
    const now = this.clock.now();
    wf.status = "completed";
    wf.result = result;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emit({
      workflowId,
      event: { type: "workflow-completed", result, at: now },
      terminal: true,
    });
  }

  async failWorkflow({ workflowId, error, errorTag, guard }: FailWorkflowParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf || isTerminalWorkflowStatus(wf.status)) return;
    const now = this.clock.now();
    wf.status = "failed";
    wf.error = error;
    wf.errorTag = errorTag;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emit({ workflowId, event: { type: "workflow-failed", error, at: now }, terminal: true });
  }

  async tripwireWorkflow({ workflowId, reason, guard }: TripwireWorkflowParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf || isTerminalWorkflowStatus(wf.status)) return;
    const now = this.clock.now();
    wf.status = "tripwire";
    wf.tripwire = reason;
    wf.completedAt = now;
    wf.updatedAt = now;
    // Locate the firing step (runner writes tripwireFired metadata on it).
    const firedStep = [...wf.steps.values()].find(
      (s) => (s.metadata as { tripwireFired?: boolean } | undefined)?.tripwireFired === true,
    );
    this.emit({
      workflowId,
      event: {
        type: "workflow-tripwire",
        stepName: firedStep?.stepName ?? "unknown",
        reason,
        at: now,
      },
      terminal: true,
    });
  }

  async cancelWorkflow({ workflowId, cascade, guard }: CancelWorkflowParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    if (wf.status !== "pending" && wf.status !== "running" && wf.status !== "suspended") return;

    const now = this.clock.now();
    wf.status = "failed";
    wf.error = CANCELLED_ERROR;
    wf.errorTag = CANCELLED_ERROR_TAG;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emit({
      workflowId,
      event: { type: "workflow-failed", error: CANCELLED_ERROR, at: now },
      terminal: true,
    });

    if (cascade) {
      for (const childId of this.children.get(workflowId) ?? []) {
        await this.cancelWorkflow({ workflowId: childId, cascade: true });
      }
    }
  }

  async suspendWorkflow({
    workflowId,
    stepName,
    stepUpdate,
    guard,
  }: SuspendWorkflowParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf) return;

    const existing = wf.steps.get(stepName);
    const now = this.clock.now();
    wf.steps.set(stepName, {
      stepName,
      run: wf.run,
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      attempt: existing?.attempt ?? 1,
      startedAt: existing?.startedAt ?? now,
      ...stepUpdate,
    } as StepState);
    wf.status = "suspended";
    wf.updatedAt = now;
  }

  async setWorkflowMetadata({
    workflowId,
    patch,
    guard,
  }: SetWorkflowMetadataParams): Promise<void> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf) return; // silent no-op on missing workflow — scrubs don't need to fail
    wf.metadata = applyMetadataPatch({ current: wf.metadata, patch });
    wf.updatedAt = this.clock.now();
  }

  async startFreshRun({ workflowId, guard }: StartFreshRunParams): Promise<number> {
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf) throw new Error(`Workflow ${workflowId} not found`);

    // Archive the current run.
    const runs = this.runHistory.get(workflowId) ?? [];
    runs.push(currentRunSummary(wf));
    this.runHistory.set(workflowId, runs);

    wf.run++;
    wf.status = "pending";
    wf.result = undefined;
    wf.error = undefined;
    wf.errorTag = undefined;
    wf.tripwire = undefined;
    wf.startedAt = undefined;
    wf.completedAt = undefined;
    wf.steps = new Map();
    wf.ownedTasks = new Map();
    wf.updatedAt = this.clock.now();
    // A fresh run re-executes every activity from scratch: replaying the
    // prior run's journal would never re-fire the side effects.
    this.journal.deleteWorkflow(workflowId);
    // Signals are run-scoped: a delivery meant for the previous run must
    // not satisfy a wait in the new one.
    this.signals.delete(workflowId);
    return wf.run;
  }

  async resetSteps({ workflowId, stepNames }: ResetStepsParams): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) throw new Error(`Workflow ${workflowId} not found`);
    if (stepNames.length === 0) return;

    // Drop the listed steps from the workflow's step map. The DAG executor
    // re-creates them on the next run() — we don't keep "pending" stubs
    // because the running flag is implicit (a step is pending iff it's
    // not in `wf.steps`). This matches how brand-new workflows look.
    for (const stepName of stepNames) {
      wf.steps.delete(stepName);
      this.journal.deleteStep({ workflowId, stepName });
    }
    // The kept steps start a fresh compensation ledger.
    for (const [name, step] of wf.steps) {
      if (step.compensationStatus !== undefined) wf.steps.set(name, withoutLedger(step));
    }

    // Flip the workflow back into a runnable state. Terminal statuses
    // (completed / failed / tripwire) become "running" so the runner
    // picks it up; suspended / running stay as-is.
    if (wf.status === "completed" || wf.status === "failed" || wf.status === "tripwire") {
      wf.status = "running";
      wf.result = undefined;
      wf.error = undefined;
      wf.errorTag = undefined;
      wf.tripwire = undefined;
      wf.completedAt = undefined;
    }
    wf.updatedAt = this.clock.now();
  }

  // -------------------------------------------------------------------------
  // WorkflowLockStore
  // -------------------------------------------------------------------------

  async tryLock(params: TryLockParams): Promise<TryLockResult> {
    return this.leases.tryAcquire(params);
  }

  async tryLockAndLoad(params: TryLockParams): Promise<TryLockAndLoadResult> {
    // Single-process storage — both operations run against the same Maps,
    // so composing them is already atomic. No transaction or Lua needed.
    const { acquired, token } = await this.tryLock(params);
    const state = await this.loadWorkflow(params.workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(params: ReleaseLockParams): Promise<void> {
    this.leases.release(params);
  }

  async heartbeat(params: HeartbeatParams): Promise<void> {
    this.leases.extend(params);
  }

  // -------------------------------------------------------------------------
  // WorkflowQueryStore
  // -------------------------------------------------------------------------

  async listWorkflows(params?: ListWorkflowsParams): Promise<WorkflowState[]> {
    const namespace = params?.namespace ?? this.namespace;
    // Filter pass first — the sort needs the full filtered set before
    // offset/limit apply.
    const filtered: MutableWorkflow[] = [];
    for (const wf of this.workflows.values()) {
      if (matchesListFilter({ wf, filter: params, namespace })) filtered.push(wf);
    }
    sortWorkflowRows({
      rows: filtered,
      orderBy: params?.orderBy ?? "startedAt",
      orderDir: params?.orderDir ?? "desc",
      fields: sortFieldsOf,
    });
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? Infinity;
    return filtered.slice(offset, offset + limit).map(toWorkflowState);
  }

  // In-memory: WorkflowState already satisfies WorkflowSummary — delegate.
  listWorkflowSummaries: InMemoryWorkflowStorage["listWorkflows"] = this.listWorkflows.bind(this);

  async countWorkflows(params?: WorkflowListFilter): Promise<number> {
    const namespace = params?.namespace ?? this.namespace;
    let count = 0;
    for (const wf of this.workflows.values()) {
      if (matchesListFilter({ wf, filter: params, namespace })) count++;
    }
    return count;
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    return this.distinct({ namespace: params?.namespace, pick: (wf) => wf.workflowName });
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    return this.distinct({ namespace: params?.namespace, pick: (wf) => wf.workflowType });
  }

  async distinctNamespaces(): Promise<string[]> {
    const seen = new Set<string>();
    for (const wf of this.workflows.values()) {
      if (wf.namespace) seen.add(wf.namespace);
    }
    return [...seen].sort();
  }

  /** Distinct non-empty `pick` values in a namespace, sorted. */
  private distinct(params: {
    namespace: string | undefined;
    pick: (wf: MutableWorkflow) => string | undefined;
  }): string[] {
    const ns = params.namespace ?? this.namespace;
    const seen = new Set<string>();
    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      const value = params.pick(wf);
      if (value) seen.add(value);
    }
    return [...seen].sort();
  }

  async loadRunHistory({
    workflowId,
    limit,
    offset,
  }: LoadRunHistoryParams): Promise<WorkflowRunSummary[]> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return [];
    const archived = this.runHistory.get(workflowId) ?? [];
    const runs = [currentRunSummary(wf), ...archived];
    runs.sort((a, b) => b.run - a.run);
    const from = offset ?? 0;
    return runs.slice(from, from + (limit ?? runs.length));
  }

  async purgeCompleted(params: PurgeCompletedParams): Promise<number> {
    let fromMs: number;
    let toMs: number;
    if ("olderThanMs" in params) {
      fromMs = 0;
      toMs = this.clock.currentTimeMs() - params.olderThanMs;
    } else {
      fromMs = params.from.getTime();
      toMs = params.to.getTime();
    }

    let deleted = 0;
    for (const [id, wf] of this.workflows) {
      if (deleted >= params.limit) break;
      if (!isTerminalWorkflowStatus(wf.status)) continue;
      if (!wf.completedAt) continue;
      const t = wf.completedAt.getTime();
      if (t < fromMs || t >= toMs) continue;

      this.workflows.delete(id);
      this.forgetIndexes(wf);
      this.leases.forget(id);
      this.signals.delete(id);
      this.attempts.delete(id);
      this.runHistory.delete(id);
      this.events.forget(id);
      this.journal.deleteWorkflow(id);
      this.signalTokens.deleteWorkflow(id);
      this.streams.deleteWorkflow(id);
      deleted++;
    }
    return deleted;
  }

  /** Drop a deleted run from the idempotency and parent indexes. */
  private forgetIndexes(wf: MutableWorkflow): void {
    if (wf.idempotencyKey !== undefined) {
      const key = idempotencyIndexKey({
        namespace: wf.namespace,
        workflowName: wf.workflowName,
        idempotencyKey: wf.idempotencyKey,
      });
      if (this.idempotencyIndex.get(key) === wf.workflowId) this.idempotencyIndex.delete(key);
    }
    if (wf.parentWorkflowId !== undefined) {
      const siblings = this.children.get(wf.parentWorkflowId);
      siblings?.delete(wf.workflowId);
      if (siblings?.size === 0) this.children.delete(wf.parentWorkflowId);
    }
  }

  // -------------------------------------------------------------------------
  // WorkflowScannerStore
  // -------------------------------------------------------------------------

  /**
   * Workflows after the keyset cursor, in `workflowId` order, that pass
   * `pick`. Scoped to the configured namespace like `listWorkflows`.
   * Stops as soon as `limit` rows are collected.
   */
  private scanWorkflows<T>(params: {
    limit: number;
    afterWorkflowId?: string;
    pick: (wf: MutableWorkflow) => T | undefined;
  }): T[] {
    const after = params.afterWorkflowId;
    const ns = this.namespace;
    const ids = [...this.workflows.keys()]
      .filter((id) => after === undefined || id > after)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const out: T[] = [];
    for (const id of ids) {
      if (out.length >= params.limit) break;
      const wf = this.workflows.get(id)!;
      if (ns && wf.namespace !== ns) continue;
      const row = params.pick(wf);
      if (row !== undefined) out.push(row);
    }
    return out;
  }

  async listDueTimers(params: ListDueTimersParams): Promise<WorkflowWakeup[]> {
    const nowMs = params.now.getTime();
    const due = (at: Date | string | undefined): boolean =>
      at !== undefined && at !== null && new Date(at).getTime() <= nowMs;
    return this.scanWorkflows<WorkflowWakeup>({
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: (wf) => {
        if (wf.status !== "suspended") return undefined;
        for (const step of stepsByName(wf)) {
          if (step.status === "sleeping" && due(step.wakeAt)) {
            return toWakeup({ wf, stepName: step.stepName, reason: "sleep" });
          }
          if (step.status === "waiting_for_signal" && due(step.signalTimeoutAt)) {
            return toWakeup({
              wf,
              stepName: step.stepName,
              reason: "signal-timeout",
              signalName: step.signalName,
            });
          }
        }
        return undefined;
      },
    });
  }

  async listSignalWakeups(params: ListSignalWakeupsParams): Promise<WorkflowWakeup[]> {
    return this.scanWorkflows<WorkflowWakeup>({
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: (wf) => {
        if (wf.status !== "suspended") return undefined;
        const delivered = this.signals.get(wf.workflowId);
        if (!delivered || delivered.length === 0) return undefined;
        for (const step of stepsByName(wf)) {
          if (step.status !== "waiting_for_signal" || step.signalName === undefined) continue;
          const signal = delivered.find((s) => s.signalName === step.signalName);
          if (!signal) continue;
          return toWakeup({
            wf,
            stepName: step.stepName,
            reason: "signal",
            signalName: step.signalName,
            signalPayload: signal.payload,
          });
        }
        return undefined;
      },
    });
  }

  async listOrphanedRuns(params: ListOrphanedRunsParams): Promise<OrphanedRun[]> {
    const nowMs = params.now.getTime();
    const beforeMs = params.updatedBefore.getTime();
    return this.scanWorkflows<OrphanedRun>({
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: (wf) => {
        if (wf.status !== "pending" && wf.status !== "running" && wf.status !== "compensating") {
          return undefined;
        }
        if (wf.updatedAt.getTime() >= beforeMs) return undefined;
        if (this.leases.isHeld({ workflowId: wf.workflowId, nowMs })) return undefined;
        return {
          workflowId: wf.workflowId,
          workflowName: wf.workflowName,
          ...(wf.version !== undefined ? { version: wf.version } : {}),
          status: wf.status,
          input: wf.input,
          ...(wf.metadata !== undefined ? { metadata: wf.metadata } : {}),
        };
      },
    });
  }

  // -------------------------------------------------------------------------
  // SignalStore
  // -------------------------------------------------------------------------

  async deliverSignal({ workflowId, signalName, payload }: DeliverSignalParams): Promise<void> {
    // Last delivery per name wins — same shape as the keyed rows the
    // networked backends keep.
    const others = (this.signals.get(workflowId) ?? []).filter((s) => s.signalName !== signalName);
    others.push({ signalName, payload, deliveredAt: this.clock.now() });
    this.signals.set(workflowId, others);
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    return [...(this.signals.get(workflowId) ?? [])];
  }

  // -------------------------------------------------------------------------
  // SignalTokenStore
  // -------------------------------------------------------------------------

  async createSignalToken(
    params: CreateSignalTokenParams,
  ): Promise<{ record: SignalTokenRecord; isCached: boolean }> {
    return this.signalTokens.create(params);
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    return this.signalTokens.findById(tokenId);
  }

  async markSignalTokenCompleted(
    params: MarkSignalTokenCompletedParams,
  ): Promise<MarkSignalTokenCompletedResult> {
    return this.signalTokens.markCompleted(params);
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    return this.signalTokens.listForWorkflow(workflowId);
  }

  // -------------------------------------------------------------------------
  // StreamStore
  // -------------------------------------------------------------------------

  async appendStreamChunk({
    guard,
    ...params
  }: AppendStreamChunkParams): Promise<{ chunkIndex: number }> {
    this.checkFence({ workflowId: params.workflowId, guard });
    return this.streams.append(params);
  }

  async readStreamChunks(params: ReadStreamChunksParams): Promise<ReadonlyArray<StreamChunk>> {
    return this.streams.read(params);
  }

  // -------------------------------------------------------------------------
  // RunEventStore
  // -------------------------------------------------------------------------

  notifyStepStarted({ workflowId, stepName }: NotifyStepStartedParams): void {
    // Event-bus only — no persistence.
    this.emit({
      workflowId,
      event: { type: "step-started", stepName, at: this.clock.now() },
      terminal: false,
    });
  }

  subscribeToWorkflow(params: SubscribeToWorkflowParams): AsyncIterable<WorkflowRunEvent> {
    return this.events.subscribe(params);
  }

  // -------------------------------------------------------------------------
  // StepAttemptStore / StepCheckpointStore
  // -------------------------------------------------------------------------

  async saveStepAttempt({ record, guard }: SaveStepAttemptParams): Promise<void> {
    this.checkFence({ workflowId: record.workflowId, guard });
    this.appendAttempt(record);
  }

  private appendAttempt(record: StepAttemptRecord): void {
    const existing = this.attempts.get(record.workflowId) ?? [];
    existing.push(record);
    this.attempts.set(record.workflowId, existing);
  }

  async loadStepAttempts({
    workflowId,
    stepName,
  }: LoadStepAttemptsParams): Promise<StepAttemptRecord[]> {
    const all = this.attempts.get(workflowId) ?? [];
    return stepName ? all.filter((a) => a.stepName === stepName) : all;
  }

  async checkpointStep({
    guard,
    ...checkpoint
  }: CheckpointStepParams): Promise<WorkflowStatusSnapshot | null> {
    const { workflowId, stepName, outcome } = checkpoint;
    // Fence check and every write in one synchronous step.
    this.checkFence({ workflowId, guard });
    const wf = this.workflows.get(workflowId);
    if (!wf) return null;
    for (const attempt of checkpoint.attempts) this.appendAttempt(attempt);
    const row = {
      workflowId,
      stepName,
      durationMs: outcome.durationMs,
      startedAt: outcome.startedAt,
      ...(outcome.metadata !== undefined && { metadata: outcome.metadata }),
    };
    if (outcome.kind === "completed") this.writeStepResult({ ...row, result: outcome.result });
    else {
      this.writeStepFailure({
        ...row,
        error: outcome.error,
        ...(outcome.errorTag !== undefined && { errorTag: outcome.errorTag }),
      });
    }
    return statusSnapshot(wf);
  }

  // -------------------------------------------------------------------------
  // CompensationLedgerStore
  // -------------------------------------------------------------------------

  async beginCompensation({ guard, ...params }: BeginCompensationParams): Promise<boolean> {
    this.checkFence({ workflowId: params.workflowId, guard });
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return false;
    if (wf.status === "compensating") return true;
    if (wf.status !== "pending" && wf.status !== "running" && wf.status !== "suspended") {
      return false;
    }
    wf.status = "compensating";
    wf.error = params.error;
    wf.errorTag = params.errorTag;
    wf.updatedAt = this.clock.now();
    return true;
  }

  async saveStepCompensation({ guard, ...params }: SaveStepCompensationParams): Promise<void> {
    this.checkFence({ workflowId: params.workflowId, guard });
    const wf = this.workflows.get(params.workflowId);
    const step = wf?.steps.get(params.stepName);
    if (!wf || !step) return;
    const now = this.clock.now();
    wf.steps.set(params.stepName, {
      ...withoutLedger(step),
      compensationStatus: params.status,
      ...(params.error !== undefined && { compensationError: params.error }),
      compensatedAt: now,
    });
    wf.updatedAt = now;
  }

  // -------------------------------------------------------------------------
  // JournalStore
  // -------------------------------------------------------------------------

  async loadJournal(params: LoadJournalParams): Promise<JournalEntry[]> {
    return this.journal.load(params);
  }

  async appendEntry({ guard, ...params }: AppendEntryParams): Promise<void> {
    this.checkFence({ workflowId: params.workflowId, guard });
    this.journal.append(params);
  }

  async appendPendingEntry({ guard, ...params }: AppendPendingEntryParams): Promise<void> {
    this.checkFence({ workflowId: params.workflowId, guard });
    this.journal.appendPending(params);
  }

  async completePendingEntry({
    guard,
    ...params
  }: CompletePendingEntryParams): Promise<CompletePendingResult> {
    this.checkFence({ workflowId: params.workflowId, guard });
    return this.journal.completePending(params);
  }

  async discardJournalEntries({ guard, ...params }: DiscardJournalEntriesParams): Promise<void> {
    this.checkFence({ workflowId: params.workflowId, guard });
    this.journal.discard(params);
  }

  async findDueSleeps(params: FindDueSleepsParams): Promise<DueSleep[]> {
    return this.journal.findDueSleeps(params);
  }

  async findPendingSignal(params: FindPendingSignalParams): Promise<JournalEntry | null> {
    return this.journal.findPendingSignal(params);
  }

  // -------------------------------------------------------------------------
  // Test helpers
  // -------------------------------------------------------------------------

  /** Test helper: step rows of every archived run of a workflow. */
  getStepHistory(workflowId: string): StepState[] {
    const archived = this.runHistory.get(workflowId) ?? [];
    return archived.flatMap((r) => Object.values(r.steps));
  }

  /** Test helper: delete a journal entry (simulates crash-before-append). */
  deleteJournalEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
  }): void {
    this.journal.discardIndex(params);
  }

  /** Test helper: get the raw workflow state. */
  getWorkflow(workflowId: string): WorkflowState | undefined {
    const wf = this.workflows.get(workflowId);
    return wf ? toWorkflowState(wf) : undefined;
  }

  /** Test helper: clear all data. */
  clear(): void {
    this.workflows.clear();
    this.leases.clear();
    this.signals.clear();
    this.attempts.clear();
    this.runHistory.clear();
    this.journal.clear();
    this.idempotencyIndex.clear();
    this.children.clear();
    this.signalTokens.clear();
    this.streams.clear();
  }
}

/** A scanner row for a due step of `wf`. */
function toWakeup(params: {
  wf: MutableWorkflow;
  stepName: string;
  reason: WorkflowWakeup["reason"];
  signalName?: string;
  signalPayload?: unknown;
}): WorkflowWakeup {
  const { wf } = params;
  return {
    workflowId: wf.workflowId,
    workflowName: wf.workflowName,
    ...(wf.version !== undefined ? { version: wf.version } : {}),
    input: wf.input,
    stepName: params.stepName,
    reason: params.reason,
    ...(params.signalName !== undefined ? { signalName: params.signalName } : {}),
    ...(params.reason === "signal" ? { signalPayload: params.signalPayload } : {}),
  };
}
