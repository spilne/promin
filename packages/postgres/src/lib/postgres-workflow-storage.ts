// ---------------------------------------------------------------------------
// PostgresWorkflowStorage — production-grade WorkflowStorage backed by Postgres
// ---------------------------------------------------------------------------

import { eq, and, sql, desc } from "drizzle-orm";
import type {
  WorkflowStorage,
  StepAttemptStorage,
  WorkflowState,
  WorkflowStatus,
  StepStatus,
  StepType,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
} from "@ts-backend/core";
import {
  workflows,
  workflowSteps,
  workflowStepTasks,
  workflowSignals,
  workflowLocks,
  stepAttempts,
  LOOKUP_BINDINGS,
} from "./schema.ts";
import {
  WorkflowStatusIds,
  StepStatusIds,
  StepTypeIds,
  AttemptTypeIds,
} from "./workflow-lookups.ts";
import { seedLookupEnums, validateLookupEnums } from "./lookup-table.ts";
import type { PostgresStorageConfig } from "./config.ts";
import { resolveConfig } from "./config.ts";
import { execRaw } from "./drizzle-db.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function hashToInt32(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0;
  }
  return hash;
}

// ---------------------------------------------------------------------------
// PostgresWorkflowStorage
// ---------------------------------------------------------------------------

export class PostgresWorkflowStorage implements WorkflowStorage, StepAttemptStorage {
  private readonly config: Required<PostgresStorageConfig>;

  private constructor(config: Required<PostgresStorageConfig>) {
    this.config = config;
  }

  /**
   * Create and initialize a PostgresWorkflowStorage.
   * Seeds lookup tables and validates they match the code definitions.
   */
  static async create(config: PostgresStorageConfig): Promise<PostgresWorkflowStorage> {
    const resolved = resolveConfig(config);
    const storage = new PostgresWorkflowStorage(resolved);

    if (resolved.autoSeedLookups) {
      await seedLookupEnums(resolved.db, LOOKUP_BINDINGS);
      await validateLookupEnums(resolved.db, LOOKUP_BINDINGS);
    }

    return storage;
  }

  private get db() {
    return this.config.db;
  }

  // ---------------------------------------------------------------------------
  // Row → domain mapping
  // ---------------------------------------------------------------------------

  private rowToWorkflowState(row: any, steps: StepState[]): WorkflowState {
    const stepMap: Record<string, StepState> = {};
    for (const s of steps) {
      stepMap[s.stepName] = s;
    }
    return {
      workflowId: row.workflowId,
      workflowName: row.workflowName,
      workflowType: row.workflowType ?? undefined,
      status: WorkflowStatusIds.toName(row.statusId),
      input: row.input,
      result: row.result ?? undefined,
      error: row.error ?? undefined,
      metadata: row.metadata ?? undefined,
      steps: stepMap,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      completedAt: row.completedAt ?? undefined,
    };
  }

  private rowToStepState(row: any, tasks?: StepTaskState[]): StepState {
    return {
      stepName: row.stepName,
      status: StepStatusIds.toName(row.statusId),
      dependsOn: row.dependsOn ?? [],
      stepType: StepTypeIds.toName(row.stepTypeId),
      result: row.result ?? undefined,
      error: row.error ?? undefined,
      startedAt: row.startedAt ?? undefined,
      completedAt: row.completedAt ?? undefined,
      durationMs: row.durationMs ?? undefined,
      attempt: row.attempt,
      tasks: tasks?.length ? tasks : undefined,
      wakeAt: row.wakeAt ?? undefined,
      signalName: row.signalName ?? undefined,
      signalTimeoutAt: row.signalTimeoutAt ?? undefined,
    };
  }

  private rowToTaskState(row: any): StepTaskState {
    return {
      taskIndex: row.taskIndex,
      status: StepStatusIds.toName(row.statusId),
      input: row.input ?? undefined,
      result: row.result ?? undefined,
      error: row.error ?? undefined,
      startedAt: row.startedAt ?? undefined,
      completedAt: row.completedAt ?? undefined,
      attempt: row.attempt,
    };
  }

  // ---------------------------------------------------------------------------
  // WorkflowStorage implementation
  // ---------------------------------------------------------------------------

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const [wfRow] = await this.db
      .select()
      .from(workflows)
      .where(eq(workflows.workflowId, workflowId));
    if (!wfRow) return null;

    const stepRows = await this.db
      .select()
      .from(workflowSteps)
      .where(eq(workflowSteps.workflowId, workflowId));
    const taskRows = await this.db
      .select()
      .from(workflowStepTasks)
      .where(eq(workflowStepTasks.workflowId, workflowId));

    const tasksByStep = new Map<string, StepTaskState[]>();
    for (const tr of taskRows) {
      if (!tasksByStep.has(tr.stepName)) tasksByStep.set(tr.stepName, []);
      tasksByStep.get(tr.stepName)!.push(this.rowToTaskState(tr));
    }

    const steps = stepRows.map((sr: any) => this.rowToStepState(sr, tasksByStep.get(sr.stepName)));
    return this.rowToWorkflowState(wfRow, steps);
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    type?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]> {
    const conditions = [];
    if (params?.status)
      conditions.push(eq(workflows.statusId, WorkflowStatusIds.toId(params.status)));
    if (params?.name) conditions.push(eq(workflows.workflowName, params.name));
    if (params?.type) conditions.push(eq(workflows.workflowType, params.type));

    const query = this.db.select().from(workflows).$dynamic();
    if (conditions.length > 0)
      query.where(conditions.length === 1 ? conditions[0] : and(...conditions));
    query.orderBy(desc(workflows.createdAt));
    if (params?.limit) query.limit(params.limit);
    if (params?.offset) query.offset(params.offset);

    const rows = await query;
    return rows.map((row: any) => this.rowToWorkflowState(row, []));
  }

  async cancelWorkflow(workflowId: string): Promise<void> {
    const now = new Date();
    await this.db
      .update(workflows)
      .set({
        statusId: WorkflowStatusIds.id.failed,
        error: "Cancelled",
        completedAt: now,
        updatedAt: now,
      })
      .where(
        and(
          eq(workflows.workflowId, workflowId),
          sql`${workflows.statusId} IN (${WorkflowStatusIds.id.running}, ${WorkflowStatusIds.id.suspended})`,
        ),
      );
  }

  async createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    await this.db.insert(workflows).values({
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      workflowType: params.workflowType,
      statusId: WorkflowStatusIds.id.running,
      input: params.input,
      metadata: params.metadata,
    });
  }

  async saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
  }): Promise<void> {
    const now = new Date();
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        statusId: StepStatusIds.id.completed,
        result: params.result,
        startedAt: params.startedAt,
        completedAt: now,
        durationMs: params.durationMs,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [workflowSteps.workflowId, workflowSteps.stepName],
        set: {
          statusId: StepStatusIds.id.completed,
          result: params.result,
          completedAt: now,
          durationMs: params.durationMs,
          attempt: sql`${workflowSteps.attempt} + 1`,
        },
      });
    await this.db
      .update(workflows)
      .set({ updatedAt: now })
      .where(eq(workflows.workflowId, params.workflowId));
  }

  async saveStepFailure(params: {
    workflowId: string;
    stepName: string;
    error: string;
    durationMs: number;
    startedAt: Date;
  }): Promise<void> {
    const now = new Date();
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        statusId: StepStatusIds.id.failed,
        error: params.error,
        startedAt: params.startedAt,
        completedAt: now,
        durationMs: params.durationMs,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [workflowSteps.workflowId, workflowSteps.stepName],
        set: {
          statusId: StepStatusIds.id.failed,
          error: params.error,
          completedAt: now,
          durationMs: params.durationMs,
          attempt: sql`${workflowSteps.attempt} + 1`,
        },
      });
    await this.db
      .update(workflows)
      .set({ updatedAt: now })
      .where(eq(workflows.workflowId, params.workflowId));
  }

  async saveTaskResult(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    result: unknown;
  }): Promise<void> {
    const now = new Date();
    // Ensure parent step row exists
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        statusId: StepStatusIds.id.running,
        stepTypeId: StepTypeIds.id.map,
        attempt: 1,
        startedAt: now,
      })
      .onConflictDoNothing();
    await this.db
      .insert(workflowStepTasks)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        taskIndex: params.taskIndex,
        statusId: StepStatusIds.id.completed,
        result: params.result,
        startedAt: now,
        completedAt: now,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [
          workflowStepTasks.workflowId,
          workflowStepTasks.stepName,
          workflowStepTasks.taskIndex,
        ],
        set: {
          statusId: StepStatusIds.id.completed,
          result: params.result,
          completedAt: now,
          attempt: sql`${workflowStepTasks.attempt} + 1`,
        },
      });
  }

  async saveTaskFailure(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    error: string;
  }): Promise<void> {
    const now = new Date();
    // Ensure parent step row exists
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        statusId: StepStatusIds.id.running,
        stepTypeId: StepTypeIds.id.map,
        attempt: 1,
        startedAt: now,
      })
      .onConflictDoNothing();
    await this.db
      .insert(workflowStepTasks)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        taskIndex: params.taskIndex,
        statusId: StepStatusIds.id.failed,
        error: params.error,
        startedAt: now,
        completedAt: now,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [
          workflowStepTasks.workflowId,
          workflowStepTasks.stepName,
          workflowStepTasks.taskIndex,
        ],
        set: {
          statusId: StepStatusIds.id.failed,
          error: params.error,
          completedAt: now,
          attempt: sql`${workflowStepTasks.attempt} + 1`,
        },
      });
  }

  async completeWorkflow(workflowId: string, result: unknown): Promise<void> {
    const now = new Date();
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.completed, result, completedAt: now, updatedAt: now })
      .where(eq(workflows.workflowId, workflowId));
  }

  async failWorkflow(workflowId: string, error: string): Promise<void> {
    const now = new Date();
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.failed, error, completedAt: now, updatedAt: now })
      .where(eq(workflows.workflowId, workflowId));
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
  ): Promise<void> {
    const now = new Date();
    const stepValues = {
      workflowId,
      stepName,
      attempt: 1,
      startedAt: now,
      statusId: stepUpdate.status ? StepStatusIds.toId(stepUpdate.status as StepStatus) : undefined,
      stepTypeId: stepUpdate.stepType
        ? StepTypeIds.toId(stepUpdate.stepType as StepType)
        : undefined,
      wakeAt: (stepUpdate.wakeAt as Date) ?? undefined,
      signalName: (stepUpdate.signalName as string) ?? undefined,
      signalTimeoutAt: (stepUpdate.signalTimeoutAt as Date) ?? undefined,
    };

    await this.db
      .insert(workflowSteps)
      .values(stepValues)
      .onConflictDoUpdate({
        target: [workflowSteps.workflowId, workflowSteps.stepName],
        set: stepValues,
      });
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.suspended, updatedAt: now })
      .where(eq(workflows.workflowId, workflowId));
  }

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    await this.db
      .insert(workflowSignals)
      .values({ workflowId, signalName, payload })
      .onConflictDoUpdate({
        target: [workflowSignals.workflowId, workflowSignals.signalName],
        set: { payload, deliveredAt: new Date() },
      });
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    const rows = await this.db
      .select()
      .from(workflowSignals)
      .where(eq(workflowSignals.workflowId, workflowId));
    return rows.map((r: any) => ({
      signalName: r.signalName,
      payload: r.payload,
      deliveredAt: r.deliveredAt,
    }));
  }

  async tryLock(workflowId: string, lockDurationMs: number): Promise<boolean> {
    if (this.config.useAdvisoryLocks) return this.tryAdvisoryLock(workflowId);
    return this.tryRowLock(workflowId, lockDurationMs);
  }

  async releaseLock(workflowId: string): Promise<void> {
    if (this.config.useAdvisoryLocks) {
      await this.releaseAdvisoryLock(workflowId);
      return;
    }
    await this.db.delete(workflowLocks).where(eq(workflowLocks.workflowId, workflowId));
  }

  async heartbeat(workflowId: string, lockDurationMs: number): Promise<void> {
    if (this.config.useAdvisoryLocks) return;
    await this.db
      .update(workflowLocks)
      .set({ expiresAt: new Date(Date.now() + lockDurationMs) })
      .where(eq(workflowLocks.workflowId, workflowId));
  }

  private async tryAdvisoryLock(workflowId: string): Promise<boolean> {
    const [result] = await execRaw(
      this.db,
      sql`SELECT pg_try_advisory_lock(${hashToInt32(workflowId)}) as acquired`,
    );
    return result?.acquired === true;
  }

  private async releaseAdvisoryLock(workflowId: string): Promise<void> {
    await execRaw(this.db, sql`SELECT pg_advisory_unlock(${hashToInt32(workflowId)})`);
  }

  private async tryRowLock(workflowId: string, lockDurationMs: number): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + lockDurationMs);
    const [result] = await execRaw(
      this.db,
      sql`
      INSERT INTO wf_workflow_locks (workflow_id, locked_at, expires_at, locked_by)
      VALUES (${workflowId}, ${now}, ${expiresAt}, ${this.config.instanceId})
      ON CONFLICT (workflow_id) DO UPDATE
        SET locked_at = ${now}, expires_at = ${expiresAt}, locked_by = ${this.config.instanceId}
        WHERE wf_workflow_locks.expires_at < ${now}
      RETURNING workflow_id
    `,
    );
    return !!result;
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStorage — attempt history (opt-in via recordAttempts config)
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord): Promise<void> {
    if (!this.config.recordAttempts) return;

    await this.db.insert(stepAttempts).values({
      workflowId: record.workflowId,
      stepName: record.stepName,
      attempt: record.attempt,
      attemptTypeId: AttemptTypeIds.toId(record.type),
      statusId: StepStatusIds.toId(record.status === "completed" ? "completed" : "failed"),
      result: record.result,
      error: record.error,
      durationMs: record.durationMs,
      startedAt: record.startedAt,
      completedAt: record.completedAt,
    });
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const query = this.db.select().from(stepAttempts).$dynamic();
    if (stepName) {
      query.where(
        and(eq(stepAttempts.workflowId, workflowId), eq(stepAttempts.stepName, stepName)),
      );
    } else {
      query.where(eq(stepAttempts.workflowId, workflowId));
    }
    query.orderBy(stepAttempts.stepName, stepAttempts.attempt);

    const rows = await query;
    return rows.map((r: any) => ({
      workflowId: r.workflowId,
      stepName: r.stepName,
      attempt: r.attempt,
      type: AttemptTypeIds.toName(r.attemptTypeId),
      status:
        StepStatusIds.toName(r.statusId) === "completed"
          ? ("completed" as const)
          : ("failed" as const),
      result: r.result ?? undefined,
      error: r.error ?? undefined,
      durationMs: Number(r.durationMs ?? 0),
      startedAt: r.startedAt,
      completedAt: r.completedAt,
    }));
  }
}
