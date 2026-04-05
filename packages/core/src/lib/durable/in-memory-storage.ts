// ---------------------------------------------------------------------------
// InMemoryWorkflowStorage — for testing
// ---------------------------------------------------------------------------

import type { WorkflowStorage, StepAttemptStorage } from "./workflow-storage.ts";
import type {
  WorkflowState,
  WorkflowStatus,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
} from "./workflow-state.ts";

export class InMemoryWorkflowStorage implements WorkflowStorage, StepAttemptStorage {
  private workflows = new Map<string, WorkflowState>();
  private locks = new Map<string, { expiresAt: number }>();
  private signals = new Map<string, SignalState[]>();
  private attempts = new Map<string, StepAttemptRecord[]>();
  private readonly namespace: string | null;

  constructor(config?: { namespace?: string | null }) {
    this.namespace = config?.namespace ?? null;
  }

  /** Resolve namespace: workflow-level → constructor default → null. */
  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    return this.workflows.get(workflowId) ?? null;
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
    let results = [...this.workflows.values()];
    // Scope to constructor namespace if set and no explicit namespace filter
    const ns = params?.namespace ?? this.namespace;
    if (ns) {
      results = results.filter((w) => w.namespace === ns);
    }
    if (params?.status) {
      results = results.filter((w) => w.status === params.status);
    }
    if (params?.name) {
      results = results.filter((w) => w.workflowName === params.name);
    }
    if (params?.type) {
      results = results.filter((w) => w.workflowType === params.type);
    }
    if (params?.parentId) {
      results = results.filter((w) => w.parentWorkflowId === params.parentId);
    }
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? results.length;
    return results.slice(offset, offset + limit);
  }

  async cancelWorkflow(workflowId: string, options?: { cascade?: boolean }): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;
    if (wf.status !== "running" && wf.status !== "suspended") return;

    const now = new Date();
    this.workflows.set(workflowId, {
      ...wf,
      status: "failed",
      error: "Cancelled",
      completedAt: now,
      updatedAt: now,
    });

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
  }): Promise<void> {
    const now = new Date();
    this.workflows.set(params.workflowId, {
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      workflowType: params.workflowType,
      parentWorkflowId: params.parentWorkflowId,
      namespace: this.resolveNamespace(params.namespace),
      status: "running",
      input: params.input,
      metadata: params.metadata,
      steps: {},
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

    const existing = wf.steps[params.stepName];
    const now = new Date();
    const step: StepState = {
      stepName: params.stepName,
      status: "completed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      result: params.result,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    };

    this.workflows.set(params.workflowId, {
      ...wf,
      steps: { ...wf.steps, [params.stepName]: step },
      updatedAt: now,
    });
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

    const existing = wf.steps[params.stepName];
    const now = new Date();
    const step: StepState = {
      stepName: params.stepName,
      status: "failed",
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      error: params.error,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: (existing?.attempt ?? 0) + 1,
      tasks: existing?.tasks,
    };

    this.workflows.set(params.workflowId, {
      ...wf,
      steps: { ...wf.steps, [params.stepName]: step },
      updatedAt: now,
    });
  }

  async saveTaskResult(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    result: unknown;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps[params.stepName];
    const tasks = [...(existing?.tasks ?? [])];
    const now = new Date();

    const existingTask = tasks.find((t) => t.taskIndex === params.taskIndex);
    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "completed",
      result: params.result,
      startedAt: existingTask?.startedAt ?? now,
      completedAt: now,
      attempt: (existingTask?.attempt ?? 0) + 1,
    };

    const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
    if (idx >= 0) {
      tasks[idx] = task;
    } else {
      tasks.push(task);
    }

    const step: StepState = {
      ...(existing ?? {
        stepName: params.stepName,
        status: "running",
        dependsOn: [],
        stepType: "map" as const,
        attempt: 1,
      }),
      tasks,
    };

    this.workflows.set(params.workflowId, {
      ...wf,
      steps: { ...wf.steps, [params.stepName]: step },
      updatedAt: now,
    });
  }

  async saveTaskFailure(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    error: string;
  }): Promise<void> {
    const wf = this.workflows.get(params.workflowId);
    if (!wf) return;

    const existing = wf.steps[params.stepName];
    const tasks = [...(existing?.tasks ?? [])];
    const now = new Date();

    const existingTask = tasks.find((t) => t.taskIndex === params.taskIndex);
    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "failed",
      error: params.error,
      startedAt: existingTask?.startedAt ?? now,
      completedAt: now,
      attempt: (existingTask?.attempt ?? 0) + 1,
    };

    const idx = tasks.findIndex((t) => t.taskIndex === params.taskIndex);
    if (idx >= 0) {
      tasks[idx] = task;
    } else {
      tasks.push(task);
    }

    const step: StepState = {
      ...(existing ?? {
        stepName: params.stepName,
        status: "running",
        dependsOn: [],
        stepType: "map" as const,
        attempt: 1,
      }),
      tasks,
    };

    this.workflows.set(params.workflowId, {
      ...wf,
      steps: { ...wf.steps, [params.stepName]: step },
      updatedAt: now,
    });
  }

  async completeWorkflow(workflowId: string, result: unknown): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;

    const now = new Date();
    this.workflows.set(workflowId, {
      ...wf,
      status: "completed",
      result,
      completedAt: now,
      updatedAt: now,
    });
  }

  async failWorkflow(workflowId: string, error: string): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;

    const now = new Date();
    this.workflows.set(workflowId, {
      ...wf,
      status: "failed",
      error,
      completedAt: now,
      updatedAt: now,
    });
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
  ): Promise<void> {
    const wf = this.workflows.get(workflowId);
    if (!wf) return;

    const existing = wf.steps[stepName];
    const now = new Date();

    const step: StepState = {
      stepName,
      dependsOn: existing?.dependsOn ?? [],
      stepType: existing?.stepType ?? "single",
      attempt: existing?.attempt ?? 1,
      startedAt: existing?.startedAt ?? now,
      ...stepUpdate,
    } as StepState;

    this.workflows.set(workflowId, {
      ...wf,
      status: "suspended",
      steps: { ...wf.steps, [stepName]: step },
      updatedAt: now,
    });
  }

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    const existing = this.signals.get(workflowId) ?? [];
    existing.push({
      signalName,
      payload,
      deliveredAt: new Date(),
    });
    this.signals.set(workflowId, existing);
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    return this.signals.get(workflowId) ?? [];
  }

  async tryLock(workflowId: string, lockDurationMs: number): Promise<boolean> {
    const existing = this.locks.get(workflowId);
    const now = Date.now();

    if (existing && existing.expiresAt > now) {
      return false;
    }

    this.locks.set(workflowId, { expiresAt: now + lockDurationMs });
    return true;
  }

  async releaseLock(workflowId: string): Promise<void> {
    this.locks.delete(workflowId);
  }

  async heartbeat(workflowId: string, lockDurationMs: number): Promise<void> {
    this.locks.set(workflowId, { expiresAt: Date.now() + lockDurationMs });
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStorage — attempt history
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord): Promise<void> {
    const key = record.workflowId;
    const existing = this.attempts.get(key) ?? [];
    existing.push(record);
    this.attempts.set(key, existing);
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const all = this.attempts.get(workflowId) ?? [];
    if (stepName) {
      return all.filter((a) => a.stepName === stepName);
    }
    return all;
  }

  /** Test helper: get the raw workflow state. */
  getWorkflow(workflowId: string): WorkflowState | undefined {
    return this.workflows.get(workflowId);
  }

  /** Test helper: clear all data. */
  clear(): void {
    this.workflows.clear();
    this.locks.clear();
    this.signals.clear();
    this.attempts.clear();
  }
}
