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
  FenceGuard,
  FenceToken,
  WorkflowOrderBy,
} from "./workflow-storage.ts";
import { workflowMetadataMatches } from "./workflow-storage.ts";
import { createWorkflowEventStream } from "./workflow-event-stream.ts";
import type {
  ActivityJournalStorage,
  JournaledSuspendStorage,
  JournalEntry,
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
} from "./workflow-state.ts";
import { FenceTokenMismatchError } from "./durable-pipeline-error.ts";
import { SystemClock, type Clock } from "@promin/core";

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
  tripwire?: unknown;
  metadata?: Record<string, unknown>;
  steps: Map<string, StepState>;
  createdAt: Date;
  startedAt?: Date;
  updatedAt: Date;
  completedAt?: Date;
}

export class InMemoryWorkflowStorage
  implements WorkflowStorage, StepAttemptStorage, ActivityJournalStorage, JournaledSuspendStorage
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
  /** Activity journal keyed by `${workflowId}::${stepName}` → ordered entries. */
  private journal = new Map<string, JournalEntry[]>();
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
   * — pass a `FakeClock` in tests to drive deterministic semantics
   * without real waits.
   */
  private readonly clock: Clock;

  constructor(config?: { namespace?: string | null; instanceId?: string; clock?: Clock }) {
    this.namespace = config?.namespace ?? null;
    this.instanceId = config?.instanceId ?? crypto.randomUUID();
    this.clock = config?.clock ?? SystemClock;
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
    const steps: Record<string, StepState> = {};
    for (const [k, v] of wf.steps) steps[k] = v;
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
      tripwire: wf.tripwire,
      metadata: wf.metadata,
      steps,
      createdAt: wf.createdAt,
      startedAt: wf.startedAt,
      updatedAt: wf.updatedAt,
      completedAt: wf.completedAt,
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const wf = this.workflows.get(workflowId);
    return wf ? this.toState(wf) : null;
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
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
      if (params?.type && wf.workflowType !== params.type) continue;
      if (params?.parentId && wf.parentWorkflowId !== params.parentId) continue;
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
    wf.error = "Cancelled";
    wf.completedAt = now;
    wf.updatedAt = now;

    if (options?.cascade) {
      for (const [childId, child] of this.workflows) {
        if (child.parentWorkflowId === workflowId) {
          await this.cancelWorkflow(childId, { cascade: true });
        }
      }
    }
  }

  async createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    parentWorkflowId?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    const existing = this.workflows.get(params.workflowId);
    if (existing) return { created: false, existing: this.toState(existing) };

    const now = this.clock.now();
    this.workflows.set(params.workflowId, {
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      workflowType: params.workflowType,
      parentWorkflowId: params.parentWorkflowId,
      namespace: this.resolveNamespace(params.namespace),
      status: "pending",
      version: params.version,
      run: 1,
      input: params.input,
      metadata: params.metadata,
      steps: new Map(),
      createdAt: now,
      updatedAt: now,
    });
    return { created: true };
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
    // In-memory doesn't have a "batch" primitive to exploit — the loop-over-
    // single-writes form is already O(n) with no round-trip amplification.
    // Kept explicit (rather than delegating to the default helper) so the
    // conformance suite's batch tests cover the actual method body here.
    for (const r of records) await this.saveStepResult(r, guard);
  }

  async saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    this.checkFence(params.workflowId, guard);
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
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const tasks = existing?.tasks ? [...existing.tasks] : [];
    const now = this.clock.now();

    const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
    const prev = idx >= 0 ? tasks[idx] : undefined;
    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "completed",
      result: params.result,
      startedAt: prev?.startedAt ?? now,
      completedAt: now,
      attempt: (prev?.attempt ?? 0) + 1,
    };
    if (idx >= 0) tasks[idx] = task;
    else tasks.push(task);

    wf.steps.set(params.stepName, {
      ...(existing ?? {
        stepName: params.stepName,
        run: wf.run,
        status: "running" as const,
        dependsOn: [],
        stepType: "map" as const,
        attempt: 1,
      }),
      tasks,
    });
    wf.updatedAt = now;
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
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const tasks = existing?.tasks ? [...existing.tasks] : [];
    const now = this.clock.now();

    const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
    const prev = idx >= 0 ? tasks[idx] : undefined;
    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "failed",
      error: params.error,
      startedAt: prev?.startedAt ?? now,
      completedAt: now,
      attempt: (prev?.attempt ?? 0) + 1,
    };
    if (idx >= 0) tasks[idx] = task;
    else tasks.push(task);

    wf.steps.set(params.stepName, {
      ...(existing ?? {
        stepName: params.stepName,
        run: wf.run,
        status: "running" as const,
        dependsOn: [],
        stepType: "map" as const,
        attempt: 1,
      }),
      tasks,
    });
    wf.updatedAt = now;
  }

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    const now = this.clock.now();
    wf.status = "completed";
    wf.result = result;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emitEvent(workflowId, { type: "workflow-completed", result, at: now }, true);
  }

  async failWorkflow(workflowId: string, error: string, guard?: FenceGuard): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    const now = this.clock.now();
    wf.status = "failed";
    wf.error = error;
    wf.completedAt = now;
    wf.updatedAt = now;
    this.emitEvent(workflowId, { type: "workflow-failed", error, at: now }, true);
  }

  async tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    this.checkFence(workflowId, guard);
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
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
    const existing = this.signals.get(workflowId) ?? [];
    existing.push({ signalName, payload, deliveredAt: this.clock.now() });
    this.signals.set(workflowId, existing);
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    return this.signals.get(workflowId) ?? [];
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
    const lock = this.locks.get(workflowId);
    if (!lock) return;
    // Same reasoning as releaseLock — silent no-op when a stale holder
    // tries to extend. The real holder keeps ticking.
    if (guard?.fenceToken) {
      if (lock.token !== guard.fenceToken) return;
    } else if (lock.lockedBy !== this.instanceId) {
      return;
    }
    this.locks.set(workflowId, {
      expiresAt: this.clock.currentTimeMs() + lockDurationMs,
      lockedBy: lock.lockedBy,
      token: lock.token,
    });
  }

  /**
   * Reject a mutating call when the caller's fence token doesn't match the
   * current lock. `guard` is optional — legacy call sites that don't pass
   * a token still succeed (fencing is additive during migration). Pass a
   * token and back it up with a lock, or don't pass one at all.
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
  }

  async startFreshRun(workflowId: string): Promise<number> {
    const wf = this.workflows.get(workflowId);
    if (!wf) throw new Error(`Workflow ${workflowId} not found`);

    // Archive current run
    const steps: Record<string, StepState> = {};
    for (const [k, v] of wf.steps) steps[k] = v;

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
    wf.tripwire = undefined;
    wf.startedAt = undefined;
    wf.completedAt = undefined;
    wf.steps = new Map();
    wf.updatedAt = this.clock.now();
    // Clear activity journal entries — a fresh run must re-execute all
    // activities from scratch, otherwise replay reads stale entries from
    // the prior run and never re-fires the side effects. Required for
    // continue-as-new and any other rerun path that should re-execute
    // from zero.
    const journalPrefix = `${workflowId}::`;
    for (const key of this.journal.keys()) {
      if (key.startsWith(journalPrefix)) this.journal.delete(key);
    }
    return wf.run;
  }

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return [];

    const archived = this.runHistory.get(workflowId) ?? [];
    const currentSteps: Record<string, StepState> = {};
    for (const [k, v] of wf.steps) currentSteps[k] = v;

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
      if (wf.status !== "completed" && wf.status !== "failed") continue;
      if (!wf.completedAt) continue;
      const t = wf.completedAt.getTime();
      if (t < fromMs || t >= toMs) continue;

      this.workflows.delete(id);
      this.locks.delete(id);
      this.signals.delete(id);
      this.attempts.delete(id);
      this.runHistory.delete(id);
      deleted++;
    }

    return deleted;
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
    const existing = this.attempts.get(record.workflowId) ?? [];
    existing.push(record);
    this.attempts.set(record.workflowId, existing);
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const all = this.attempts.get(workflowId) ?? [];
    return stepName ? all.filter((a) => a.stepName === stepName) : all;
  }

  // ---------------------------------------------------------------------------
  // ActivityJournalStorage — .journaled() step support
  // ---------------------------------------------------------------------------

  private journalKey(workflowId: string, stepName: string): string {
    return `${workflowId}::${stepName}`;
  }

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const entries = this.journal.get(this.journalKey(workflowId, stepName)) ?? [];
    // Defensive copy + stable sort: by activityIndex primarily, then by
    // branchPath so `ctx.parallel` branches have a deterministic replay
    // order when a consumer iterates the journal directly.
    return [...entries].sort((a, b) => {
      if (a.activityIndex !== b.activityIndex) return a.activityIndex - b.activityIndex;
      return a.branchPath.localeCompare(b.branchPath);
    });
  }

  /** Locate an entry by its composite (activityIndex, branchPath) key. */
  private findEntryIndex(
    entries: JournalEntry[],
    activityIndex: number,
    branchPath: string,
  ): number {
    return entries.findIndex(
      (e) => e.activityIndex === activityIndex && e.branchPath === branchPath,
    );
  }

  async appendEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const key = this.journalKey(params.workflowId, params.stepName);
    const entries = this.journal.get(key) ?? [];
    // Idempotent: skip if the same (index, branchPath) is already recorded and completed.
    const existing = this.findEntryIndex(entries, params.activityIndex, branchPath);
    if (existing !== -1 && entries[existing]!.phase !== "pending") return;
    // Preserve payloadHash from the prior pending row if the completer didn't
    // pass one — pending→completed transition shouldn't drop the fingerprint.
    const priorHash = existing !== -1 ? entries[existing]!.payloadHash : undefined;
    const entry: JournalEntry = {
      activityIndex: params.activityIndex,
      branchPath,
      activityName: params.activityName,
      stepType: "activity",
      phase: "completed",
      payloadHash: params.payloadHash ?? priorHash,
      exit: params.exit,
      createdAt: this.clock.now(),
    };
    if (existing !== -1) entries[existing] = entry;
    else entries.push(entry);
    this.journal.set(key, entries);
  }

  async appendPendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    stepType: "sleep" | "signal" | "activity" | "compensation";
    wakeAt?: Date;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const key = this.journalKey(params.workflowId, params.stepName);
    const entries = this.journal.get(key) ?? [];
    // Idempotent: if an entry at this (index, branchPath) already exists, leave it alone.
    if (this.findEntryIndex(entries, params.activityIndex, branchPath) !== -1) return;
    entries.push({
      activityIndex: params.activityIndex,
      branchPath,
      activityName: params.activityName,
      stepType: params.stepType,
      phase: "pending",
      payloadHash: params.payloadHash,
      wakeAt: params.wakeAt,
      createdAt: this.clock.now(),
    });
    this.journal.set(key, entries);
  }

  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const key = this.journalKey(params.workflowId, params.stepName);
    const entries = this.journal.get(key);
    if (!entries) return;
    const idx = this.findEntryIndex(entries, params.activityIndex, branchPath);
    if (idx === -1) return;
    const existing = entries[idx]!;
    // Idempotent on repeated delivery — ignore if already completed.
    if (existing.phase === "completed") return;
    entries[idx] = {
      ...existing,
      phase: "completed",
      exit: params.exit,
    };
    this.journal.set(key, entries);
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
    for (const [key, entries] of this.journal) {
      const [workflowId, stepName] = key.split("::") as [string, string];
      for (const e of entries) {
        if (
          e.stepType === "sleep" &&
          e.phase === "pending" &&
          e.wakeAt &&
          e.wakeAt.getTime() <= params.now.getTime()
        ) {
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
    }
    return due;
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const entries = this.journal.get(this.journalKey(params.workflowId, params.stepName));
    if (!entries) return null;
    const hit = entries.find(
      (e) =>
        e.stepType === "signal" && e.phase === "pending" && e.activityName === params.signalName,
    );
    return hit ?? null;
  }

  /** Test helper: delete a specific journal entry (simulates crash-before-append). */
  deleteJournalEntry(workflowId: string, stepName: string, activityIndex: number): void {
    const key = this.journalKey(workflowId, stepName);
    const entries = this.journal.get(key);
    if (!entries) return;
    this.journal.set(
      key,
      entries.filter((e) => e.activityIndex !== activityIndex),
    );
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
  }
}
