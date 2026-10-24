// ---------------------------------------------------------------------------
// InMemoryWorkflowStorage — for testing and single-process use
// ---------------------------------------------------------------------------
//
// Single-process only. State lives in plain Maps on the JS heap, which is
// not shared across Bun workers or Node worker_threads (each gets its own
// isolate). For multi-worker deployments, use PostgresWorkflowStorage or
// another networked backend via the WorkflowStorage interface.
//
// Lock atomicity: tryLock checks + sets synchronously (no await between
// the read and write), so concurrent callers on the same event loop
// cannot interleave.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  StepAttemptStorage,
  StepCheckpoint,
  StepCheckpointStorage,
  CompensationLedgerStorage,
  StepCompensationOutcome,
  FenceGuard,
  FenceToken,
  WorkflowOrderBy,
  SignalTokenRecord,
  StreamChunk,
  WorkflowWakeup,
  OrphanedRun,
} from "./workflow-storage.ts";
import { workflowMetadataMatches } from "./workflow-storage.ts";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  isTerminalWorkflowStatus,
  withoutCompensationLedger as withoutLedger,
  type WorkflowStatusSnapshot,
} from "./workflow-state.ts";
import { createWorkflowEventStream } from "./workflow-event-stream.ts";
import type {
  ActivityJournalStorage,
  CompletePendingResult,
  JournalEntry,
  JournalExit,
  JournalSlot,
} from "./activity-journal.ts";
import type {
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  WorkflowRunEvent,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  RunSource,
} from "./workflow-state.ts";
import { FenceTokenMismatchError } from "./durable-pipeline-error.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

/**
 * Comparator factory for sortable `listWorkflows` columns. NULL/undefined
 * values always sort last (regardless of direction) so still-running rows
 * with no `startedAt` / no `completedAt` / no `duration` don't push real
 * data off the first page. Status sorts on the lookup name alphabetically
 * to match how Postgres exposes it.
 */
function makeWorkflowComparator(
  orderBy: WorkflowOrderBy,
  dir: "asc" | "desc",
): (a: MutableWorkflow, b: MutableWorkflow) => number {
  const sign = dir === "asc" ? 1 : -1;
  return (a, b) => {
    const av = workflowSortKey(a, orderBy);
    const bv = workflowSortKey(b, orderBy);
    if (av === undefined && bv === undefined) return 0;
    if (av === undefined) return 1;
    if (bv === undefined) return -1;
    if (av < bv) return -1 * sign;
    if (av > bv) return 1 * sign;
    return 0;
  };
}

function workflowSortKey(
  wf: MutableWorkflow,
  orderBy: WorkflowOrderBy,
): number | string | undefined {
  switch (orderBy) {
    case "createdAt":
      return wf.createdAt.getTime();
    case "startedAt":
      return wf.startedAt?.getTime();
    case "completedAt":
      return wf.completedAt?.getTime();
    case "duration":
      return wf.completedAt ? wf.completedAt.getTime() - wf.createdAt.getTime() : undefined;
    case "status":
      return wf.status;
    case "name":
      return wf.workflowName;
  }
}

/** Mutable internal workflow state — avoids spread-copy on every mutation. */
interface MutableWorkflow {
  workflowId: string;
  workflowName: string;
  workflowType?: string;
  parentWorkflowId?: string;
  namespace?: string;
  status: WorkflowStatus;
  version?: string;
  run: number;
  input: unknown;
  result?: unknown;
  error?: string;
  errorTag?: string;
  tripwire?: unknown;
  runSource?: RunSource;
  runSourceId?: string;
  metadata?: Record<string, unknown>;
  idempotencyKey?: string;
  idempotencyExpiresAt?: Date;
  steps: Map<string, StepState>;
  /**
   * Task arrays this storage may still append to in place, by step name
   * (see `writeTask`). An entry is writable only while it is the step row's
   * own `tasks` array and no row of the run was handed out since it was
   * built (`handedOut` still equals its `epoch`).
   */
  ownedTasks: Map<string, OwnedTasks>;
  /** Bumped every time the run's step rows are handed out to a caller. */
  handedOut: number;
  createdAt: Date;
  startedAt?: Date;
  updatedAt: Date;
  completedAt?: Date;
}

/**
 * A map step's task array, owned by the storage, with each task's position
 * by task index: a task write is a lookup plus an in-place set or push
 * instead of a copy and a linear search.
 */
interface OwnedTasks {
  readonly tasks: StepTaskState[];
  readonly position: Map<number, number>;
  readonly epoch: number;
}

/** One step's activity journal: entries in write order, positions by slot. */
interface JournalSlots {
  entries: JournalEntry[];
  /** Position in `entries` by `slotKey(activityIndex, branchPath)`. */
  position: Map<string, number>;
  /** `entries` sorted by slot, built on the first load after a write. */
  sorted?: JournalEntry[];
}

function slotKey(activityIndex: number, branchPath: string): string {
  return `${activityIndex}:${branchPath}`;
}

/** Key of the `(namespace, workflowName, idempotencyKey)` index. */
function idempotencyIndexKey(params: {
  namespace: string | undefined;
  workflowName: string;
  idempotencyKey: string;
}): string {
  return JSON.stringify([params.namespace ?? null, params.workflowName, params.idempotencyKey]);
}

export class InMemoryWorkflowStorage
  implements
    WorkflowStorage,
    StepAttemptStorage,
    StepCheckpointStorage,
    CompensationLedgerStorage,
    ActivityJournalStorage
{
  private workflows = new Map<string, MutableWorkflow>();
  /**
   * Workflow locks. `token` is the fence stamp handed back by `tryLock`
   * and required by every subsequent mutating call — guards against a
   * stale holder that woke up past lock expiry.
   */
  private locks = new Map<string, { expiresAt: number; lockedBy: string; token: FenceToken }>();
  /**
   * Monotonic fence-token counter. Each successful `tryLock` bumps it so
   * the token a new holder gets is strictly greater than any prior one,
   * even when a stale holder's entry was already cleared from `locks`.
   */
  private nextFenceToken = 1;
  private readonly instanceId: string;
  private signals = new Map<string, SignalState[]>();
  private attempts = new Map<string, StepAttemptRecord[]>();
  private runHistory = new Map<string, WorkflowRunSummary[]>();
  /** Activity journal keyed by `${workflowId}::${stepName}`. */
  private journal = new Map<string, JournalSlots>();
  /** Journal keys by workflow id, so a workflow's journal is dropped without a scan. */
  private journalKeys = new Map<string, Set<string>>();
  /** Journal keys that have (or had) a pending sleep entry: what `findDueSleeps` scans. */
  private sleepKeys = new Set<string>();
  /** Workflow id by `idempotencyIndexKey`, for the run that last claimed the key. */
  private idempotencyIndex = new Map<string, string>();
  /** Child workflow ids by parent workflow id, for cascade cancel. */
  private children = new Map<string, Set<string>>();
  /** Signal tokens keyed by tokenId — public-bearer auth for deliverSignal. */
  private signalTokens = new Map<string, MutableSignalToken>();
  /** Stream chunks keyed by `${workflowId}::${streamId}` → ordered by chunk_index. */
  private streamChunks = new Map<string, StreamChunk[]>();
  /**
   * Per-workflow event subscribers. Each active call to `subscribeToWorkflow`
   * registers a push function keyed by workflowId; the mutating storage
   * methods fan out to every registered sub. Passing `null` signals
   * terminal — the iterator resolves `{ done: true }` and the sub is
   * removed. Fresh map on every workflowId so one subscriber's terminal
   * doesn't starve another subscriber attached to a different run.
   */
  private subscribers = new Map<string, Set<(event: WorkflowRunEvent | null) => void>>();
  private readonly namespace: string | null;
  /**
   * Time source. Every timestamp + lock-expiry check routes through here
   * — pass a `FakeWallClock` in tests to drive deterministic semantics
   * without real waits.
   */
  private readonly clock: WallClock;

  constructor(config?: { namespace?: string | null; instanceId?: string; clock?: WallClock }) {
    this.namespace = config?.namespace ?? null;
    this.instanceId = config?.instanceId ?? crypto.randomUUID();
    this.clock = config?.clock ?? SystemWallClock;
  }

  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
  }

  /**
   * Fan an event out to every subscriber for this workflow. `terminal`
   * indicates the run is ending — after delivering the event, each sub is
   * signalled done (null) and the subscriber set is cleared. Called
   * synchronously from the save/fail/complete/tripwire methods so
   * subscribers observe events in the same order as the underlying state
   * transitions.
   */
  private emitEvent(workflowId: string, event: WorkflowRunEvent, terminal: boolean): void {
    const subs = this.subscribers.get(workflowId);
    if (!subs || subs.size === 0) return;
    for (const push of subs) {
      push(event);
      if (terminal) push(null);
    }
    if (terminal) this.subscribers.delete(workflowId);
  }

  private toState(wf: MutableWorkflow): WorkflowState {
    const steps = this.handOutSteps(wf);
    return {
      workflowId: wf.workflowId,
      workflowName: wf.workflowName,
      workflowType: wf.workflowType,
      parentWorkflowId: wf.parentWorkflowId,
      namespace: wf.namespace,
      status: wf.status,
      version: wf.version,
      run: wf.run,
      input: wf.input,
      result: wf.result,
      error: wf.error,
      errorTag: wf.errorTag,
      tripwire: wf.tripwire,
      runSource: wf.runSource,
      runSourceId: wf.runSourceId,
      metadata: wf.metadata,
      steps,
      createdAt: wf.createdAt,
      startedAt: wf.startedAt,
      updatedAt: wf.updatedAt,
      completedAt: wf.completedAt,
    };
  }

  /**
   * The run's step rows as a record for a caller. Their `tasks` arrays are
   * now shared with the caller, so the next task write copies them first.
   */
  private handOutSteps(wf: MutableWorkflow): Record<string, StepState> {
    wf.handedOut++;
    const steps: Record<string, StepState> = {};
    for (const [k, v] of wf.steps) steps[k] = v;
    return steps;
  }

  private statusOf(wf: MutableWorkflow): WorkflowStatusSnapshot {
    return {
      status: wf.status,
      ...(wf.error !== undefined && { error: wf.error }),
      ...(wf.errorTag !== undefined && { errorTag: wf.errorTag }),
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const wf = this.workflows.get(workflowId);
    return wf ? this.toState(wf) : null;
  }

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    const wf = this.workflows.get(workflowId);
    return wf ? this.statusOf(wf) : null;
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    orderBy?: WorkflowOrderBy;
    orderDir?: "asc" | "desc";
  }): Promise<WorkflowState[]> {
    const ns = params?.namespace ?? this.namespace;
    const metadataFilter = params?.metadata;

    // Filter pass first — sort needs the full filtered set before we can
    // apply offset/limit, so we can't short-circuit inside the loop the way
    // unordered scans did.
    const filtered: MutableWorkflow[] = [];
    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      if (params?.status && wf.status !== params.status) continue;
      if (params?.name && wf.workflowName !== params.name) continue;
      if (params?.version !== undefined && wf.version !== params.version) continue;
      if (params?.type && wf.workflowType !== params.type) continue;
      if (params?.parentId && wf.parentWorkflowId !== params.parentId) continue;
      if (params?.runSource !== undefined && wf.runSource !== params.runSource) continue;
      if (params?.runSourceId !== undefined && wf.runSourceId !== params.runSourceId) continue;
      if (metadataFilter && !workflowMetadataMatches(wf.metadata, metadataFilter)) continue;
      filtered.push(wf);
    }

    const orderBy = params?.orderBy ?? "startedAt";
    const orderDir = params?.orderDir ?? "desc";
    filtered.sort(makeWorkflowComparator(orderBy, orderDir));

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? Infinity;
    const page = filtered.slice(offset, offset + limit);
    return page.map((wf) => this.toState(wf));
  }

  // In-memory: WorkflowState already satisfies WorkflowSummary — delegate.
  listWorkflowSummaries: InMemoryWorkflowStorage["listWorkflows"] = this.listWorkflows.bind(this);

  async countWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    const ns = params?.namespace ?? this.namespace;
    let count = 0;
    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      if (params?.status && wf.status !== params.status) continue;
      if (params?.name && wf.workflowName !== params.name) continue;
      if (params?.version !== undefined && wf.version !== params.version) continue;
      if (params?.type && wf.workflowType !== params.type) continue;
      if (params?.parentId && wf.parentWorkflowId !== params.parentId) continue;
      if (params?.runSource !== undefined && wf.runSource !== params.runSource) continue;
      if (params?.runSourceId !== undefined && wf.runSourceId !== params.runSourceId) continue;
      if (params?.metadata && !workflowMetadataMatches(wf.metadata, params.metadata)) continue;
      count++;
    }
    return count;
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const seen = new Set<string>();
    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      seen.add(wf.workflowName);
    }
    return [...seen].sort();
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const seen = new Set<string>();
    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      if (wf.workflowType) seen.add(wf.workflowType);
    }
    return [...seen].sort();
  }

  async distinctNamespaces(): Promise<string[]> {
    const seen = new Set<string>();
    for (const wf of this.workflows.values()) {
      if (wf.namespace) seen.add(wf.namespace);
    }
    return [...seen].sort();
  }

  async cancelWorkflow(
    workflowId: string,
    options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    if (wf.status !== "pending" && wf.status !== "running" && wf.status !== "suspended") return;

    const now = this.clock.now();
    wf.status = "failed";
    wf.error = CANCELLED_ERROR;
    wf.errorTag = CANCELLED_ERROR_TAG;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emitEvent(workflowId, { type: "workflow-failed", error: CANCELLED_ERROR, at: now }, true);

    if (options?.cascade) {
      for (const childId of this.children.get(workflowId) ?? []) {
        await this.cancelWorkflow(childId, { cascade: true });
      }
    }
  }

  async createWorkflow(
    params: {
      workflowId: string;
      workflowName: string;
      input: unknown;
      workflowType?: string;
      parentWorkflowId?: string;
      namespace?: string;
      metadata?: Record<string, unknown>;
      version?: string;
      runSource?: RunSource;
      runSourceId?: string;
      idempotencyKey?: string;
      idempotencyExpiresAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    this.checkParentFence(params.parentWorkflowId, guard);
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
      if (claimed) return { created: false, existing: this.toState(claimed) };
    }

    const existing = this.workflows.get(params.workflowId);
    if (existing) return { created: false, existing: this.toState(existing) };

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

  async findWorkflowByIdempotencyKey(params: {
    workflowName: string;
    namespace?: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
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

  async saveStepResult(
    params: {
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    this.writeStepResult(params);
  }

  /** The unfenced body of `saveStepResult`; synchronous so a batch is one step. */
  private writeStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
  }): void {
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
    this.emitEvent(
      params.workflowId,
      {
        type: "step-completed",
        stepName: params.stepName,
        result: params.result,
        durationMs: params.durationMs,
        at: now,
      },
      false,
    );
  }

  async batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
    guard?: FenceGuard,
  ): Promise<void> {
    // Every fence is checked before the first write and nothing awaits in
    // between, so the batch lands whole or not at all.
    for (const id of new Set(records.map((r) => r.workflowId))) this.checkFence(id, guard);
    for (const r of records) this.writeStepResult(r);
  }

  async saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      errorTag?: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    this.writeStepFailure(params);
  }

  /** The unfenced body of `saveStepFailure`. */
  private writeStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    errorTag?: string;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
  }): void {
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
    this.emitEvent(
      params.workflowId,
      {
        type: "step-failed",
        stepName: params.stepName,
        error: params.error,
        at: now,
      },
      false,
    );
    wf.updatedAt = now;
  }

  async saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    this.writeTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "completed", result: params.result },
    });
  }

  async saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    this.writeTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "failed", error: params.error },
    });
  }

  /**
   * Upsert one task row of a map step (creating a `running` map step row
   * when the step has none). Amortised O(1): the step's task array is
   * appended to or updated in place while the storage owns it, and copied
   * once after the run's rows were handed out to a caller, so a caller's
   * snapshot never changes under it.
   */
  private writeTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    outcome:
      | { readonly status: "completed"; readonly result: unknown }
      | { readonly status: "failed"; readonly error: string };
  }): void {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;
    const { stepName, taskIndex, outcome } = params;
    const existing = wf.steps.get(stepName);
    let owned = wf.ownedTasks.get(stepName);
    if (owned === undefined || owned.epoch !== wf.handedOut || existing?.tasks !== owned.tasks) {
      const tasks = existing?.tasks ? [...existing.tasks] : [];
      const position = new Map<number, number>();
      for (let i = 0; i < tasks.length; i++) position.set(tasks[i]!.taskIndex, i);
      owned = { tasks, position, epoch: wf.handedOut };
      wf.ownedTasks.set(stepName, owned);
    }

    const now = this.clock.now();
    const at = owned.position.get(taskIndex);
    const prev = at !== undefined ? owned.tasks[at] : undefined;
    const task: StepTaskState = {
      taskIndex,
      status: outcome.status,
      ...(outcome.status === "completed" ? { result: outcome.result } : { error: outcome.error }),
      startedAt: prev?.startedAt ?? now,
      completedAt: now,
      attempt: (prev?.attempt ?? 0) + 1,
    };
    if (at !== undefined) owned.tasks[at] = task;
    else {
      owned.position.set(taskIndex, owned.tasks.length);
      owned.tasks.push(task);
    }

    wf.steps.set(stepName, {
      ...(existing ?? {
        stepName,
        run: wf.run,
        status: "running" as const,
        dependsOn: [],
        stepType: "map" as const,
        attempt: 1,
      }),
      tasks: owned.tasks,
    });
    wf.updatedAt = now;
  }

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf || isTerminalWorkflowStatus(wf.status)) return;
    const now = this.clock.now();
    wf.status = "completed";
    wf.result = result;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emitEvent(workflowId, { type: "workflow-completed", result, at: now }, true);
  }

  async failWorkflow(
    workflowId: string,
    error: string,
    guard?: FenceGuard,
    details?: { readonly errorTag?: string },
  ): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf || isTerminalWorkflowStatus(wf.status)) return;
    const now = this.clock.now();
    wf.status = "failed";
    wf.error = error;
    wf.errorTag = details?.errorTag;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emitEvent(workflowId, { type: "workflow-failed", error, at: now }, true);
  }

  async tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    this.checkFence(workflowId, guard);
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
    this.emitEvent(
      workflowId,
      {
        type: "workflow-tripwire",
        stepName: firedStep?.stepName ?? "unknown",
        reason,
        at: now,
      },
      true,
    );
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(workflowId, guard);
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

  notifyStepStarted(workflowId: string, stepName: string): void {
    // Event-bus only — no persistence. The runner calls this before each
    // local step body runs; here we fan out `step-started` to any active
    // subscribers for this workflowId. Storages without subscribers or
    // without this method entirely silently drop the signal.
    this.emitEvent(workflowId, { type: "step-started", stepName, at: this.clock.now() }, false);
  }

  /**
   * Stream step/workflow-lifecycle events for a single run. Returns an
   * async iterable closed by any terminal event or by the supplied
   * `AbortSignal`. Each call registers its own push function — multiple
   * concurrent subscribers to the same workflowId each see every event.
   * Subscribers started after the run has already terminated receive
   * immediate end-of-stream.
   *
   * Implementation uses a single-producer, single-consumer queue per
   * subscriber: events arrive synchronously from storage mutators and the
   * iterator drains them asynchronously. Overlap between production and
   * consumption is handled by a waiter slot — if the consumer is idle when
   * a new event arrives, the resolver fires directly; otherwise the event
   * sits in the queue until the next `next()`.
   */
  subscribeToWorkflow(
    workflowId: string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<WorkflowRunEvent> {
    return createWorkflowEventStream((producer) => {
      // Adapter: storage's internal subscriber set uses `(event|null) =>
      // void` so `null` signals terminal. The shared stream's producer has
      // separate `push` / `end` methods — adapt both shapes here.
      const adapt = (event: WorkflowRunEvent | null): void => {
        if (event === null) producer.end();
        else producer.push(event);
      };
      const subs = this.subscribers.get(workflowId) ?? new Set();
      subs.add(adapt);
      this.subscribers.set(workflowId, subs);

      const onAbort = (): void => producer.end();
      options?.signal?.addEventListener("abort", onAbort, { once: true });

      return () => {
        const cur = this.subscribers.get(workflowId);
        if (cur) {
          cur.delete(adapt);
          if (cur.size === 0) this.subscribers.delete(workflowId);
        }
        options?.signal?.removeEventListener("abort", onAbort);
      };
    });
  }

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    // Last delivery per name wins — same shape as the keyed rows the
    // networked backends keep.
    const others = (this.signals.get(workflowId) ?? []).filter((s) => s.signalName !== signalName);
    others.push({ signalName, payload, deliveredAt: this.clock.now() });
    this.signals.set(workflowId, others);
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    return [...(this.signals.get(workflowId) ?? [])];
  }

  async setWorkflowMetadata(
    workflowId: string,
    patch: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) return; // silent no-op on missing workflow — scrubs don't need to fail
    const current = wf.metadata ?? {};
    const merged: Record<string, unknown> = { ...current };
    for (const [k, v] of Object.entries(patch)) {
      if (v === null) delete merged[k];
      else merged[k] = v;
    }
    wf.metadata = merged;
    wf.updatedAt = this.clock.now();
  }

  async tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: FenceToken }> {
    const lock = this.locks.get(workflowId);
    const now = this.clock.currentTimeMs();
    if (lock !== undefined && lock.expiresAt > now) return { acquired: false };
    const token = String(this.nextFenceToken++);
    this.locks.set(workflowId, {
      expiresAt: now + lockDurationMs,
      lockedBy: this.instanceId,
      token,
    });
    return { acquired: true, token };
  }

  async tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: FenceToken; state: WorkflowState | null }> {
    // Single-process storage — both operations run against the same Map,
    // so composing them is already atomic. No transaction or Lua needed.
    const { acquired, token } = await this.tryLock(workflowId, lockDurationMs);
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    const lock = this.locks.get(workflowId);
    if (!lock) return;
    // Token check when the caller holds one — silent no-op on mismatch so
    // a stale worker's late `releaseLock` doesn't rip away a fresh holder's
    // lease. Callers that don't pass a token fall back to the instanceId
    // check.
    if (guard?.fenceToken) {
      if (lock.token !== guard.fenceToken) return;
    } else if (lock.lockedBy !== this.instanceId) {
      return;
    }
    this.locks.delete(workflowId);
  }

  async heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    // A token holder whose lock is gone or re-taken learns it lost the
    // run (`checkFence` rejects); without a token, a lock this instance
    // doesn't hold is left alone.
    this.checkFence(workflowId, guard);
    const lock = this.locks.get(workflowId);
    if (!lock) return;
    if (!guard?.fenceToken && lock.lockedBy !== this.instanceId) return;
    this.locks.set(workflowId, {
      expiresAt: this.clock.currentTimeMs() + lockDurationMs,
      lockedBy: lock.lockedBy,
      token: lock.token,
    });
  }

  /**
   * Reject a fenced write unless `guard.fenceToken` is the workflow's
   * current, unexpired lock token. Every caller runs it before its first
   * mutation with no `await` in between, so the check and the write are one
   * synchronous step. Without a token the write is unfenced.
   */
  private checkFence(workflowId: string, guard?: FenceGuard): void {
    if (!guard?.fenceToken) return;
    const lock = this.locks.get(workflowId);
    // No lock at all — the new holder already released, or never held.
    // Either way, the stale write must be rejected.
    if (!lock) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(no lock)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — no active lock`,
      });
    }
    if (lock.token !== guard.fenceToken) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: lock.token,
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — token mismatch (expected "${lock.token}", got "${guard.fenceToken}")`,
      });
    }
    if (lock.expiresAt <= this.clock.currentTimeMs()) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: "(expired)",
        provided: guard.fenceToken,
        message: `Fenced write for "${workflowId}" rejected — the lock for token "${guard.fenceToken}" expired`,
      });
    }
  }

  /** `checkFence` on the parent's lock — the fence of a child create. */
  private checkParentFence(parentWorkflowId: string | undefined, guard?: FenceGuard): void {
    if (!guard?.fenceToken) return;
    if (parentWorkflowId === undefined) {
      throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
    }
    this.checkFence(parentWorkflowId, guard);
  }

  async startFreshRun(workflowId: string, guard?: FenceGuard): Promise<number> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) throw new Error(`Workflow ${workflowId} not found`);

    // Archive current run
    const steps = this.handOutSteps(wf);

    const runs = this.runHistory.get(workflowId) ?? [];
    runs.push({
      run: wf.run,
      version: wf.version,
      status: wf.status,
      result: wf.result,
      error: wf.error,
      tripwire: wf.tripwire,
      steps,
      createdAt: wf.createdAt,
      startedAt: wf.startedAt,
      completedAt: wf.completedAt,
    });
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
    // Clear activity journal entries — a fresh run must re-execute all
    // activities from scratch, otherwise replay reads stale entries from
    // the prior run and never re-fires the side effects. Required for
    // continue-as-new and any other rerun path that should re-execute
    // from zero.
    this.deleteJournal(workflowId);
    // Signals are run-scoped: a delivery meant for the previous run must
    // not satisfy a wait in the new one.
    this.signals.delete(workflowId);
    return wf.run;
  }

  async resetSteps(workflowId: string, stepNames: readonly string[]): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) throw new Error(`Workflow ${workflowId} not found`);
    if (stepNames.length === 0) return;

    // Drop the listed steps from the workflow's step map. The DAG executor
    // re-creates them on the next run() — we don't keep "pending" stubs
    // because the running flag is implicit (a step is pending iff it's
    // not in `wf.steps`). This matches how brand-new workflows look.
    for (const name of stepNames) {
      wf.steps.delete(name);
      // Clear journal entries for the step. Journaled bodies key by
      // (workflowId, stepName) so we delete the matching journal Map slot.
      this.journal.delete(this.journalKey(workflowId, name));
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

  /** Steps of a workflow sorted by name — the scanners pick the smallest due one. */
  private sortedSteps(wf: MutableWorkflow): StepState[] {
    return [...wf.steps.values()].sort((a, b) =>
      a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0,
    );
  }

  async listDueTimers(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const nowMs = params.now.getTime();
    const due = (at: Date | string | undefined): boolean =>
      at !== undefined && at !== null && new Date(at).getTime() <= nowMs;
    return this.scanWorkflows<WorkflowWakeup>({
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: (wf) => {
        if (wf.status !== "suspended") return undefined;
        for (const step of this.sortedSteps(wf)) {
          if (step.status === "sleeping" && due(step.wakeAt)) {
            return this.toWakeup({ wf, stepName: step.stepName, reason: "sleep" });
          }
          if (step.status === "waiting_for_signal" && due(step.signalTimeoutAt)) {
            return this.toWakeup({
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

  async listSignalWakeups(params: {
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    return this.scanWorkflows<WorkflowWakeup>({
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: (wf) => {
        if (wf.status !== "suspended") return undefined;
        const delivered = this.signals.get(wf.workflowId);
        if (!delivered || delivered.length === 0) return undefined;
        for (const step of this.sortedSteps(wf)) {
          if (step.status !== "waiting_for_signal" || step.signalName === undefined) continue;
          const signal = delivered.find((s) => s.signalName === step.signalName);
          if (!signal) continue;
          return this.toWakeup({
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

  async listOrphanedRuns(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]> {
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
        const lock = this.locks.get(wf.workflowId);
        if (lock !== undefined && lock.expiresAt > nowMs) return undefined;
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

  private toWakeup(params: {
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

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return [];

    const archived = this.runHistory.get(workflowId) ?? [];
    const currentSteps = this.handOutSteps(wf);

    const runs: WorkflowRunSummary[] = [
      {
        run: wf.run,
        version: wf.version,
        status: wf.status,
        result: wf.result,
        error: wf.error,
        tripwire: wf.tripwire,
        steps: currentSteps,
        createdAt: wf.createdAt,
        startedAt: wf.startedAt,
        completedAt: wf.completedAt,
      },
      ...archived,
    ];
    runs.sort((a, b) => b.run - a.run);

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? runs.length;
    return runs.slice(offset, offset + limit);
  }

  async purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
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
      this.locks.delete(id);
      this.signals.delete(id);
      this.attempts.delete(id);
      this.runHistory.delete(id);
      this.subscribers.delete(id);
      this.deleteJournal(id);
      for (const [tokenId, token] of this.signalTokens) {
        if (token.workflowId === id) this.signalTokens.delete(tokenId);
      }
      const streamPrefix = `${id}::`;
      for (const key of this.streamChunks.keys()) {
        if (key.startsWith(streamPrefix)) this.streamChunks.delete(key);
      }
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

  /** Drop every journal entry of one workflow, across all steps. */
  private deleteJournal(workflowId: string): void {
    for (const key of this.journalKeys.get(workflowId) ?? []) this.journal.delete(key);
    this.journalKeys.delete(workflowId);
  }

  /** Get step history across all runs for a workflow. */
  getStepHistory(workflowId: string): StepState[] {
    const archived = this.runHistory.get(workflowId) ?? [];
    return archived.flatMap((r) => Object.values(r.steps));
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStorage
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord, guard?: FenceGuard): Promise<void> {
    this.checkFence(record.workflowId, guard);
    this.appendAttempt(record);
  }

  private appendAttempt(record: StepAttemptRecord): void {
    const existing = this.attempts.get(record.workflowId) ?? [];
    existing.push(record);
    this.attempts.set(record.workflowId, existing);
  }

  // ---------------------------------------------------------------------------
  // StepCheckpointStorage
  // ---------------------------------------------------------------------------

  async checkpointStep(
    checkpoint: StepCheckpoint,
    guard?: FenceGuard,
  ): Promise<WorkflowStatusSnapshot | null> {
    const { workflowId, stepName, outcome } = checkpoint;
    // Fence check and every write in one synchronous step.
    this.checkFence(workflowId, guard);
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
    return this.statusOf(wf);
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const all = this.attempts.get(workflowId) ?? [];
    return stepName ? all.filter((a) => a.stepName === stepName) : all;
  }

  // ---------------------------------------------------------------------------
  // CompensationLedgerStorage
  // ---------------------------------------------------------------------------

  async beginCompensation(
    params: { readonly workflowId: string; readonly error: string; readonly errorTag?: string },
    guard?: FenceGuard,
  ): Promise<boolean> {
    this.checkFence(params.workflowId, guard);
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

  async saveStepCompensation(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly status: StepCompensationOutcome;
      readonly error?: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
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

  // ---------------------------------------------------------------------------
  // ActivityJournalStorage — .journaled() step support
  // ---------------------------------------------------------------------------

  private journalKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  /** The step's journal, if it has one. */
  private journalOf(workflowId: string, stepName: string): JournalSlots | undefined {
    return this.journal.get(this.journalKey(workflowId, stepName));
  }

  /** The step's journal, created empty when it has none. */
  private openJournal(workflowId: string, stepName: string): JournalSlots {
    const key = this.journalKey(workflowId, stepName);
    let slots = this.journal.get(key);
    if (slots === undefined) {
      slots = { entries: [], position: new Map() };
      this.journal.set(key, slots);
      const keys = this.journalKeys.get(workflowId) ?? new Set<string>();
      keys.add(key);
      this.journalKeys.set(workflowId, keys);
    }
    return slots;
  }

  /** Put `entry` at its slot: replaces the entry there, or appends. */
  private putEntry(slots: JournalSlots, entry: JournalEntry): void {
    const key = slotKey(entry.activityIndex, entry.branchPath);
    const at = slots.position.get(key);
    if (at !== undefined) slots.entries[at] = entry;
    else {
      slots.position.set(key, slots.entries.length);
      slots.entries.push(entry);
    }
    slots.sorted = undefined;
  }

  /** Keep only the entries `keep` accepts, re-indexing the rest. */
  private filterEntries(slots: JournalSlots, keep: (e: JournalEntry) => boolean): void {
    slots.entries = slots.entries.filter(keep);
    slots.position = new Map();
    for (let i = 0; i < slots.entries.length; i++) {
      const e = slots.entries[i]!;
      slots.position.set(slotKey(e.activityIndex, e.branchPath), i);
    }
    slots.sorted = undefined;
  }

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const slots = this.journalOf(workflowId, stepName);
    if (!slots) return [];
    // Defensive copy + stable sort: by activityIndex primarily, then by
    // branchPath so `ctx.parallel` branches have a deterministic replay
    // order when a consumer iterates the journal directly. The sorted
    // order is kept until the next write.
    slots.sorted ??= [...slots.entries].sort((a, b) => {
      if (a.activityIndex !== b.activityIndex) return a.activityIndex - b.activityIndex;
      return a.branchPath.localeCompare(b.branchPath);
    });
    return [...slots.sorted];
  }

  async appendEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      activityName: string;
      payloadHash?: string;
      exit: NonNullable<JournalEntry["exit"]>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    const branchPath = params.branchPath ?? "";
    const slots = this.openJournal(params.workflowId, params.stepName);
    // Idempotent: skip if the same (index, branchPath) is already recorded and completed.
    const at = slots.position.get(slotKey(params.activityIndex, branchPath));
    const existing = at !== undefined ? slots.entries[at] : undefined;
    if (existing !== undefined && existing.phase !== "pending") return;
    // Preserve payloadHash from the prior pending row if the completer didn't
    // pass one — pending→completed transition shouldn't drop the fingerprint.
    this.putEntry(slots, {
      activityIndex: params.activityIndex,
      branchPath,
      activityName: params.activityName,
      stepType: "activity",
      phase: "completed",
      payloadHash: params.payloadHash ?? existing?.payloadHash,
      exit: params.exit,
      createdAt: this.clock.now(),
    });
  }

  async appendPendingEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      activityName: string;
      payloadHash?: string;
      stepType: "sleep" | "signal" | "activity" | "compensation" | "child";
      wakeAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    const branchPath = params.branchPath ?? "";
    const slots = this.openJournal(params.workflowId, params.stepName);
    // Idempotent: if an entry at this (index, branchPath) already exists, leave it alone.
    if (slots.position.has(slotKey(params.activityIndex, branchPath))) return;
    this.putEntry(slots, {
      activityIndex: params.activityIndex,
      branchPath,
      activityName: params.activityName,
      stepType: params.stepType,
      phase: "pending",
      payloadHash: params.payloadHash,
      wakeAt: params.wakeAt,
      createdAt: this.clock.now(),
    });
    if (params.stepType === "sleep") {
      this.sleepKeys.add(this.journalKey(params.workflowId, params.stepName));
    }
  }

  async completePendingEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      exit: JournalExit;
    },
    guard?: FenceGuard,
  ): Promise<CompletePendingResult> {
    this.checkFence(params.workflowId, guard);
    const branchPath = params.branchPath ?? "";
    const slots = this.journalOf(params.workflowId, params.stepName);
    const at = slots?.position.get(slotKey(params.activityIndex, branchPath));
    if (slots === undefined || at === undefined) return { completed: false, exit: undefined };
    const existing = slots.entries[at]!;
    // First writer wins — report the stored exit to the loser.
    if (existing.phase !== "pending") return { completed: false, exit: existing.exit };
    this.putEntry(slots, { ...existing, phase: "completed", exit: params.exit });
    return { completed: true, exit: params.exit };
  }

  async discardJournalEntries(
    params: {
      workflowId: string;
      stepName: string;
      slots: readonly JournalSlot[];
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
    const slots = this.journalOf(params.workflowId, params.stepName);
    if (!slots) return;
    const drop = new Set(params.slots.map((s) => slotKey(s.activityIndex, s.branchPath)));
    this.filterEntries(slots, (e) => !drop.has(slotKey(e.activityIndex, e.branchPath)));
  }

  async findDueSleeps(params: { now: Date; limit: number }): Promise<
    Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }>
  > {
    const due: Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }> = [];
    // Only journals that ever held a pending sleep; one found without any
    // is dropped from the scan set.
    for (const key of this.sleepKeys) {
      const slots = this.journal.get(key);
      const [workflowId, stepName] = key.split("::") as [string, string];
      let pendingSleeps = 0;
      for (const e of slots?.entries ?? []) {
        if (e.stepType !== "sleep" || e.phase !== "pending") continue;
        pendingSleeps++;
        if (e.wakeAt && e.wakeAt.getTime() <= params.now.getTime()) {
          due.push({
            workflowId,
            stepName,
            activityIndex: e.activityIndex,
            branchPath: e.branchPath,
            wakeAt: e.wakeAt,
          });
          if (due.length >= params.limit) return due;
        }
      }
      if (pendingSleeps === 0) this.sleepKeys.delete(key);
    }
    return due;
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const entries = this.journal.get(this.journalKey(params.workflowId, params.stepName))?.entries;
    if (!entries) return null;
    const hit = entries.find(
      (e) =>
        e.stepType === "signal" && e.phase === "pending" && e.activityName === params.signalName,
    );
    return hit ?? null;
  }

  // ---------------------------------------------------------------------------
  // Signal tokens — public-bearer authorization for deliverSignal
  // ---------------------------------------------------------------------------

  async createSignalToken(params: {
    tokenId: string;
    workflowId: string;
    signalName: string;
    bearer: string;
    tags: ReadonlyArray<string>;
    idempotencyKey?: string | null;
    expiresAt: Date;
  }): Promise<{ record: SignalTokenRecord; isCached: boolean }> {
    if (params.idempotencyKey) {
      for (const t of this.signalTokens.values()) {
        if (t.workflowId === params.workflowId && t.idempotencyKey === params.idempotencyKey) {
          return { record: snapshotSignalToken(t), isCached: true };
        }
      }
    }
    const record: MutableSignalToken = {
      tokenId: params.tokenId,
      workflowId: params.workflowId,
      signalName: params.signalName,
      bearer: params.bearer,
      tags: [...params.tags],
      idempotencyKey: params.idempotencyKey ?? null,
      expiresAt: params.expiresAt,
      completedAt: null,
      completedValue: null,
      createdAt: this.clock.now(),
    };
    this.signalTokens.set(params.tokenId, record);
    return { record: snapshotSignalToken(record), isCached: false };
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    const t = this.signalTokens.get(tokenId);
    return t ? snapshotSignalToken(t) : null;
  }

  async markSignalTokenCompleted(params: {
    tokenId: string;
    value: unknown;
    now: Date;
  }): Promise<
    | { outcome: "delivered"; record: SignalTokenRecord }
    | { outcome: "already_completed"; record: SignalTokenRecord }
  > {
    const t = this.signalTokens.get(params.tokenId);
    if (!t) {
      throw new Error(`signal token ${params.tokenId} not found`);
    }
    if (t.completedAt !== null) {
      return { outcome: "already_completed", record: snapshotSignalToken(t) };
    }
    t.completedAt = params.now;
    t.completedValue = params.value;
    return { outcome: "delivered", record: snapshotSignalToken(t) };
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    const out: MutableSignalToken[] = [];
    for (const t of this.signalTokens.values()) {
      if (t.workflowId === workflowId) out.push(t);
    }
    out.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
    return out.map(snapshotSignalToken);
  }

  // ---------------------------------------------------------------------------
  // Streams — append-only chunks per (workflow, stream).
  // ---------------------------------------------------------------------------

  async appendStreamChunk(
    params: {
      workflowId: string;
      streamId: string;
      payload: unknown;
      appendedBy: "workflow" | "external";
    },
    guard?: FenceGuard,
  ): Promise<{ chunkIndex: number }> {
    this.checkFence(params.workflowId, guard);
    const key = `${params.workflowId}::${params.streamId}`;
    const existing = this.streamChunks.get(key) ?? [];
    const chunkIndex = existing.length;
    const chunk: StreamChunk = {
      chunkIndex,
      payload: params.payload,
      appendedBy: params.appendedBy,
      appendedAt: this.clock.now(),
    };
    existing.push(chunk);
    this.streamChunks.set(key, existing);
    return { chunkIndex };
  }

  async readStreamChunks(params: {
    workflowId: string;
    streamId: string;
    since?: number;
    limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>> {
    const key = `${params.workflowId}::${params.streamId}`;
    const all = this.streamChunks.get(key) ?? [];
    const filtered =
      params.since !== undefined ? all.filter((c) => c.chunkIndex > params.since!) : all;
    return params.limit !== undefined ? filtered.slice(0, params.limit) : filtered;
  }

  /** Test helper: delete a specific journal entry (simulates crash-before-append). */
  deleteJournalEntry(workflowId: string, stepName: string, activityIndex: number): void {
    const slots = this.journal.get(this.journalKey(workflowId, stepName));
    if (!slots) return;
    this.filterEntries(slots, (e) => e.activityIndex !== activityIndex);
  }

  /** Test helper: get the raw workflow state. */
  getWorkflow(workflowId: string): WorkflowState | undefined {
    const wf = this.workflows.get(workflowId);
    return wf ? this.toState(wf) : undefined;
  }

  /** Test helper: clear all data. */
  clear(): void {
    this.workflows.clear();
    this.locks.clear();
    this.signals.clear();
    this.attempts.clear();
    this.runHistory.clear();
    this.journal.clear();
    this.journalKeys.clear();
    this.sleepKeys.clear();
    this.idempotencyIndex.clear();
    this.children.clear();
    this.signalTokens.clear();
    this.streamChunks.clear();
  }
}

interface MutableSignalToken {
  tokenId: string;
  workflowId: string;
  signalName: string;
  bearer: string;
  tags: string[];
  idempotencyKey: string | null;
  expiresAt: Date;
  completedAt: Date | null;
  completedValue: unknown;
  createdAt: Date;
}

function snapshotSignalToken(t: MutableSignalToken): SignalTokenRecord {
  return {
    tokenId: t.tokenId,
    workflowId: t.workflowId,
    signalName: t.signalName,
    bearer: t.bearer,
    tags: [...t.tags],
    idempotencyKey: t.idempotencyKey,
    expiresAt: new Date(t.expiresAt.getTime()),
    completedAt: t.completedAt ? new Date(t.completedAt.getTime()) : null,
    completedValue: t.completedValue,
    createdAt: new Date(t.createdAt.getTime()),
  };
}
