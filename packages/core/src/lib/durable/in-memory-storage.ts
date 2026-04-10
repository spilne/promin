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

import type { WorkflowStorage, StepAttemptStorage } from "./workflow-storage.ts";
import type {
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
} from "./workflow-state.ts";

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
  metadata?: Record<string, unknown>;
  steps: Map<string, StepState>;
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
}

export class InMemoryWorkflowStorage implements WorkflowStorage, StepAttemptStorage {
  private workflows = new Map<string, MutableWorkflow>();
  private locks = new Map<string, { expiresAt: number; lockedBy: string }>(); // workflowId → lock info
  private readonly instanceId: string;
  private signals = new Map<string, SignalState[]>();
  private attempts = new Map<string, StepAttemptRecord[]>();
  private runHistory = new Map<string, WorkflowRunSummary[]>();
  private readonly namespace: string | null;

  constructor(config?: { namespace?: string | null; instanceId?: string }) {
    this.namespace = config?.namespace ?? null;
    this.instanceId = config?.instanceId ?? crypto.randomUUID();
  }

  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
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
      metadata: wf.metadata,
      steps,
      createdAt: wf.createdAt,
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
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]> {
    const ns = params?.namespace ?? this.namespace;
    const results: WorkflowState[] = [];
    let skipped = 0;
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? Infinity;

    for (const wf of this.workflows.values()) {
      if (ns && wf.namespace !== ns) continue;
      if (params?.status && wf.status !== params.status) continue;
      if (params?.name && wf.workflowName !== params.name) continue;
      if (params?.type && wf.workflowType !== params.type) continue;
      if (params?.parentId && wf.parentWorkflowId !== params.parentId) continue;
      if (skipped < offset) {
        skipped++;
        continue;
      }
      if (results.length >= limit) break;
      results.push(this.toState(wf));
    }
    return results;
  }

  async cancelWorkflow(workflowId: string, options?: { cascade?: boolean }): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    if (wf.status !== "running" && wf.status !== "suspended") return;

    const now = new Date();
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
  }): Promise<void> {
    const now = new Date();
    this.workflows.set(params.workflowId, {
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      workflowType: params.workflowType,
      parentWorkflowId: params.parentWorkflowId,
      namespace: this.resolveNamespace(params.namespace),
      status: "running",
      version: params.version,
      run: 1,
      input: params.input,
      metadata: params.metadata,
      steps: new Map(),
      createdAt: now,
      updatedAt: now,
    });
  }

  async saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const now = new Date();
    wf.steps.set(params.stepName, {
      stepName: params.stepName,
      run: wf.run,
      status: "completed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      result: params.result,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    });
    wf.updatedAt = now;
  }

  async saveStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    durationMs: number;
    startedAt: Date;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const now = new Date();
    wf.steps.set(params.stepName, {
      stepName: params.stepName,
      run: wf.run,
      status: "failed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      error: params.error,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    });
    wf.updatedAt = now;
  }

  async saveTaskResult(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    result: unknown;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const tasks = existing?.tasks ? [...existing.tasks] : [];
    const now = new Date();

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

  async saveTaskFailure(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    error: string;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps.get(params.stepName);
    const tasks = existing?.tasks ? [...existing.tasks] : [];
    const now = new Date();

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

  async completeWorkflow(workflowId: string, result: unknown): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    const now = new Date();
    wf.status = "completed";
    wf.result = result;
    wf.completedAt = now;
    wf.updatedAt = now;
  }

  async failWorkflow(workflowId: string, error: string): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    const now = new Date();
    wf.status = "failed";
    wf.error = error;
    wf.completedAt = now;
    wf.updatedAt = now;
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
  ): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;

    const existing = wf.steps.get(stepName);
    const now = new Date();
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

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    const existing = this.signals.get(workflowId) ?? [];
    existing.push({ signalName, payload, deliveredAt: new Date() });
    this.signals.set(workflowId, existing);
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    return this.signals.get(workflowId) ?? [];
  }

  async tryLock(workflowId: string, lockDurationMs: number): Promise<boolean> {
    const lock = this.locks.get(workflowId);
    const now = Date.now();
    if (lock !== undefined && lock.expiresAt > now) return false;
    this.locks.set(workflowId, { expiresAt: now + lockDurationMs, lockedBy: this.instanceId });
    return true;
  }

  async releaseLock(workflowId: string): Promise<void> {
    const lock = this.locks.get(workflowId);
    // Only release if we own the lock (or lock doesn't exist)
    if (lock && lock.lockedBy !== this.instanceId) return;
    this.locks.delete(workflowId);
  }

  async heartbeat(workflowId: string, lockDurationMs: number): Promise<void> {
    const lock = this.locks.get(workflowId);
    // Only extend if we own the lock
    if (lock && lock.lockedBy !== this.instanceId) return;
    this.locks.set(workflowId, {
      expiresAt: Date.now() + lockDurationMs,
      lockedBy: this.instanceId,
    });
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
      steps,
      createdAt: wf.createdAt,
      completedAt: wf.completedAt,
    });
    this.runHistory.set(workflowId, runs);

    wf.run++;
    wf.status = "running";
    wf.result = undefined;
    wf.error = undefined;
    wf.completedAt = undefined;
    wf.steps = new Map();
    wf.updatedAt = new Date();
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
        steps: currentSteps,
        createdAt: wf.createdAt,
        completedAt: wf.completedAt,
      },
      ...archived,
    ];
    runs.sort((a, b) => b.run - a.run);

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? runs.length;
    return runs.slice(offset, offset + limit);
  }

  /** Get step history across all runs for a workflow. */
  getStepHistory(workflowId: string): StepState[] {
    const archived = this.runHistory.get(workflowId) ?? [];
    return archived.flatMap((r) => Object.values(r.steps));
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStorage
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord): Promise<void> {
    const existing = this.attempts.get(record.workflowId) ?? [];
    existing.push(record);
    this.attempts.set(record.workflowId, existing);
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const all = this.attempts.get(workflowId) ?? [];
    return stepName ? all.filter((a) => a.stepName === stepName) : all;
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
  }
}
