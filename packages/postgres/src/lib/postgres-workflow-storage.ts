// ---------------------------------------------------------------------------
// PostgresWorkflowStorage — production-grade WorkflowStorage backed by Postgres
// ---------------------------------------------------------------------------

import { eq, and, sql, desc, inArray, gte, lt } from "drizzle-orm";
import type {
  WorkflowStorage,
  StepAttemptStorage,
  WorkflowState,
  WorkflowRunSummary,
  WorkflowStatus,
  StepStatus,
  StepType,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  ActivityJournalStorage,
  JournaledSuspendStorage,
  JournalEntry,
} from "@promin/workflow";
import {
  workflows,
  workflowRuns,
  workflowSteps,
  workflowStepTasks,
  workflowSignals,
  workflowLocks,
  stepAttempts,
  stepQueue,
  activityJournal,
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

export class PostgresWorkflowStorage
  implements WorkflowStorage, StepAttemptStorage, ActivityJournalStorage, JournaledSuspendStorage
{
  /**
   * Drizzle schemas for all workflow tables.
   * Use these to include workflow tables in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export const {
   *   workflows, workflowSteps, workflowStepTasks,
   *   workflowSignals, workflowLocks, stepAttempts,
   * } = PostgresWorkflowStorage.schema;
   * ```
   */
  static readonly schema = {
    workflows,
    workflowRuns,
    workflowSteps,
    workflowStepTasks,
    workflowSignals,
    workflowLocks,
    stepAttempts,
  };

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

  /** Resolve namespace: workflow-level → constructor default → null. */
  private resolveNamespace(workflowNamespace?: string): string | null {
    return workflowNamespace ?? this.config.namespace ?? null;
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
      namespace: row.namespace ?? undefined,
      version: row.version ?? undefined,
      run: row.run ?? 1,
      status: WorkflowStatusIds.toName(row.statusId),
      input: row.input,
      result: row.result ?? undefined,
      error: row.error ?? undefined,
      metadata: row.metadata ?? undefined,
      steps: stepMap,
      createdAt: row.createdAt,
      startedAt: row.startedAt ?? undefined,
      updatedAt: row.updatedAt,
      completedAt: row.completedAt ?? undefined,
    };
  }

  private rowToStepState(row: any, tasks?: StepTaskState[]): StepState {
    return {
      stepName: row.stepName,
      run: row.run ?? 1,
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

    const currentRun = wfRow.run ?? 1;
    const stepRows = await this.db
      .select()
      .from(workflowSteps)
      .where(and(eq(workflowSteps.workflowId, workflowId), eq(workflowSteps.run, currentRun)));
    const taskRows = await this.db
      .select()
      .from(workflowStepTasks)
      .where(
        and(eq(workflowStepTasks.workflowId, workflowId), eq(workflowStepTasks.run, currentRun)),
      );

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
    namespace?: string;
    limit?: number;
    offset?: number;
  }): Promise<WorkflowState[]> {
    const conditions = [];
    // Scope to constructor namespace if set and no explicit namespace filter
    const ns = params?.namespace ?? this.config.namespace;
    if (ns) conditions.push(eq(workflows.namespace, ns));
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
          sql`${workflows.statusId} IN (${WorkflowStatusIds.id.pending}, ${WorkflowStatusIds.id.running}, ${WorkflowStatusIds.id.suspended})`,
        ),
      );
  }

  async createWorkflow(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    workflowType?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    const ns = this.resolveNamespace(params.namespace);
    const [inserted] = await this.db
      .insert(workflows)
      .values({
        workflowId: params.workflowId,
        workflowName: params.workflowName,
        workflowType: params.workflowType,
        namespace: ns,
        version: params.version,
        statusId: WorkflowStatusIds.id.pending,
        input: params.input,
        metadata: params.metadata,
      })
      .onConflictDoNothing()
      .returning({ workflowId: workflows.workflowId });

    if (!inserted) {
      const existing = await this.loadWorkflow(params.workflowId);
      return { created: false, existing: existing! };
    }
    return { created: true };
  }

  /** Transition pending → running on first step activity. */
  private async markRunning(workflowId: string): Promise<void> {
    const now = new Date();
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.running, startedAt: now, updatedAt: now })
      .where(
        and(
          eq(workflows.workflowId, workflowId),
          eq(workflows.statusId, WorkflowStatusIds.id.pending),
        ),
      );
  }

  private async getCurrentRun(workflowId: string): Promise<number> {
    const [row] = await this.db
      .select({ run: workflows.run })
      .from(workflows)
      .where(eq(workflows.workflowId, workflowId));
    return row?.run ?? 1;
  }

  async saveStepResult(params: {
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
  }): Promise<void> {
    await this.markRunning(params.workflowId);
    const now = new Date();
    const run = await this.getCurrentRun(params.workflowId);
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        run,
        statusId: StepStatusIds.id.completed,
        result: params.result,
        startedAt: params.startedAt,
        completedAt: now,
        durationMs: params.durationMs,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [workflowSteps.workflowId, workflowSteps.stepName, workflowSteps.run],
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
    await this.markRunning(params.workflowId);
    const now = new Date();
    const run = await this.getCurrentRun(params.workflowId);
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        run,
        statusId: StepStatusIds.id.failed,
        error: params.error,
        startedAt: params.startedAt,
        completedAt: now,
        durationMs: params.durationMs,
        attempt: 1,
      })
      .onConflictDoUpdate({
        target: [workflowSteps.workflowId, workflowSteps.stepName, workflowSteps.run],
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
    const run = await this.getCurrentRun(params.workflowId);
    // Ensure parent step row exists
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        run,
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
        run,
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
          workflowStepTasks.run,
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
    const run = await this.getCurrentRun(params.workflowId);
    // Ensure parent step row exists
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        run,
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
        run,
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
          workflowStepTasks.run,
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
    const run = await this.getCurrentRun(workflowId);
    const stepValues = {
      workflowId,
      stepName,
      run,
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
        target: [workflowSteps.workflowId, workflowSteps.stepName, workflowSteps.run],
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
    await this.db
      .delete(workflowLocks)
      .where(
        and(
          eq(workflowLocks.workflowId, workflowId),
          eq(workflowLocks.lockedBy, this.config.instanceId),
        ),
      );
  }

  async heartbeat(workflowId: string, lockDurationMs: number): Promise<void> {
    if (this.config.useAdvisoryLocks) return;
    await this.db
      .update(workflowLocks)
      .set({ expiresAt: new Date(Date.now() + lockDurationMs) })
      .where(
        and(
          eq(workflowLocks.workflowId, workflowId),
          eq(workflowLocks.lockedBy, this.config.instanceId),
        ),
      );
  }

  async startFreshRun(workflowId: string): Promise<number> {
    const now = new Date();

    // Archive current run before resetting
    const [current] = await this.db
      .select()
      .from(workflows)
      .where(eq(workflows.workflowId, workflowId));

    if (current) {
      await this.db
        .insert(workflowRuns)
        .values({
          workflowId,
          run: current.run,
          statusId: current.statusId,
          result: current.result,
          error: current.error,
          createdAt: current.createdAt,
          startedAt: current.startedAt,
          completedAt: current.completedAt,
        })
        .onConflictDoNothing();
    }

    const [row] = await this.db
      .update(workflows)
      .set({
        run: sql`${workflows.run} + 1`,
        statusId: WorkflowStatusIds.id.pending,
        result: null,
        error: null,
        startedAt: null,
        completedAt: null,
        updatedAt: now,
      })
      .where(eq(workflows.workflowId, workflowId))
      .returning({ run: workflows.run });
    return row?.run ?? 1;
  }

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const [wfRow] = await this.db
      .select()
      .from(workflows)
      .where(eq(workflows.workflowId, workflowId));
    if (!wfRow) return [];

    // If offset=0, current run is the first entry
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? wfRow.run;
    const includeCurrentRun = offset === 0;

    // The current run lives in wf_workflows (not wf_workflow_runs), so when
    // paginating past it we need to adjust the SQL offset by -1.
    const archivedOffset = includeCurrentRun ? 0 : offset - 1;
    const archivedRows = await this.db
      .select()
      .from(workflowRuns)
      .where(eq(workflowRuns.workflowId, workflowId))
      .orderBy(desc(workflowRuns.run))
      .limit(params?.limit ?? 2147483647)
      .offset(archivedOffset);

    type RunMeta = {
      run: number;
      version?: string;
      statusId: number;
      result: unknown;
      error: string | null;
      createdAt: Date;
      startedAt: Date | null;
      completedAt: Date | null;
    };
    const runMetas: RunMeta[] = [];

    if (includeCurrentRun) {
      runMetas.push({
        run: wfRow.run,
        version: wfRow.version ?? undefined,
        statusId: wfRow.statusId,
        result: wfRow.result,
        error: wfRow.error,
        createdAt: wfRow.createdAt,
        startedAt: wfRow.startedAt,
        completedAt: wfRow.completedAt,
      });
    }

    for (const ar of archivedRows) {
      runMetas.push({
        run: ar.run,
        statusId: ar.statusId,
        result: ar.result,
        error: ar.error,
        createdAt: ar.createdAt,
        startedAt: ar.startedAt,
        completedAt: ar.completedAt,
      });
    }

    const page = includeCurrentRun ? runMetas.slice(0, limit) : runMetas;
    if (page.length === 0) return [];

    // Fetch steps for the runs in this page
    const runNumbers = page.map((r) => r.run);
    const stepRows = await this.db
      .select()
      .from(workflowSteps)
      .where(and(eq(workflowSteps.workflowId, workflowId), inArray(workflowSteps.run, runNumbers)));

    const stepsByRun = new Map<number, Record<string, StepState>>();
    for (const row of stepRows) {
      const run = row.run ?? 1;
      if (!stepsByRun.has(run)) stepsByRun.set(run, {});
      stepsByRun.get(run)![row.stepName] = this.rowToStepState(row);
    }

    return page.map((meta) => ({
      run: meta.run,
      version: meta.version,
      status: WorkflowStatusIds.toName(meta.statusId),
      result: meta.result ?? undefined,
      error: meta.error ?? undefined,
      steps: stepsByRun.get(meta.run) ?? {},
      createdAt: meta.createdAt,
      startedAt: meta.startedAt ?? undefined,
      completedAt: meta.completedAt ?? undefined,
    }));
  }

  async purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
    let from: Date;
    let to: Date;

    if ("olderThanMs" in params) {
      from = new Date(0);
      to = new Date(Date.now() - params.olderThanMs);
    } else {
      from = params.from;
      to = params.to;
    }

    // Find expired workflow IDs in a single query
    const expired = await this.db
      .select({ workflowId: workflows.workflowId })
      .from(workflows)
      .where(
        and(
          sql`${workflows.statusId} IN (${WorkflowStatusIds.id.completed}, ${WorkflowStatusIds.id.failed})`,
          gte(workflows.completedAt, from),
          lt(workflows.completedAt, to),
        ),
      )
      .limit(params.limit);

    if (expired.length === 0) return 0;

    const ids = expired.map((r) => r.workflowId);

    // Delete child tables first (FK order), then the workflow itself
    await this.db.delete(workflowSignals).where(inArray(workflowSignals.workflowId, ids));
    await this.db.delete(workflowStepTasks).where(inArray(workflowStepTasks.workflowId, ids));
    await this.db.delete(workflowSteps).where(inArray(workflowSteps.workflowId, ids));
    await this.db.delete(stepAttempts).where(inArray(stepAttempts.workflowId, ids));
    await this.db.delete(workflowRuns).where(inArray(workflowRuns.workflowId, ids));
    await this.db
      .delete(stepQueue)
      .where(
        and(
          inArray(stepQueue.workflowId, ids),
          sql`${stepQueue.status} IN ('completed', 'failed')`,
        ),
      );
    await this.db.delete(workflowLocks).where(inArray(workflowLocks.workflowId, ids));
    await this.db.delete(workflows).where(inArray(workflows.workflowId, ids));

    return ids.length;
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

  // ---------------------------------------------------------------------------
  // ActivityJournalStorage — .journaled() step support
  // ---------------------------------------------------------------------------

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const rows = await this.db
      .select()
      .from(activityJournal)
      .where(
        and(eq(activityJournal.workflowId, workflowId), eq(activityJournal.stepName, stepName)),
      )
      .orderBy(activityJournal.activityIndex, activityJournal.branchPath);
    return rows.map(rowToJournalEntry);
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
    // Idempotent append — PK conflict on
    // (workflow_id, step_name, activity_index, branch_path) is silently
    // dropped. Storage-level dedup: the engine may re-call append during a
    // retry that crashes after a successful INSERT but before the caller
    // observes completion.
    await this.db
      .insert(activityJournal)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        activityIndex: params.activityIndex,
        branchPath: params.branchPath ?? "",
        activityName: params.activityName,
        stepType: "activity",
        phase: "completed",
        payloadHash: params.payloadHash,
        exit: params.exit,
      })
      .onConflictDoNothing({
        target: [
          activityJournal.workflowId,
          activityJournal.stepName,
          activityJournal.activityIndex,
          activityJournal.branchPath,
        ],
      });
  }

  // ---------------------------------------------------------------------------
  // JournaledSuspendStorage — ctx.sleep / ctx.signal
  // ---------------------------------------------------------------------------

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
    await this.db
      .insert(activityJournal)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        activityIndex: params.activityIndex,
        branchPath: params.branchPath ?? "",
        activityName: params.activityName,
        stepType: params.stepType,
        phase: "pending",
        payloadHash: params.payloadHash,
        wakeAt: params.wakeAt,
        exit: null,
      })
      .onConflictDoNothing({
        target: [
          activityJournal.workflowId,
          activityJournal.stepName,
          activityJournal.activityIndex,
          activityJournal.branchPath,
        ],
      });
  }

  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    // Idempotent on repeated delivery: the WHERE clause restricts to still-
    // pending rows, so a second call on an already-completed row no-ops.
    await this.db
      .update(activityJournal)
      .set({ phase: "completed", exit: params.exit })
      .where(
        and(
          eq(activityJournal.workflowId, params.workflowId),
          eq(activityJournal.stepName, params.stepName),
          eq(activityJournal.activityIndex, params.activityIndex),
          eq(activityJournal.branchPath, params.branchPath ?? ""),
          eq(activityJournal.phase, "pending"),
        ),
      );
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
    const rows = await this.db
      .select({
        workflowId: activityJournal.workflowId,
        stepName: activityJournal.stepName,
        activityIndex: activityJournal.activityIndex,
        branchPath: activityJournal.branchPath,
        wakeAt: activityJournal.wakeAt,
      })
      .from(activityJournal)
      .where(
        and(
          eq(activityJournal.stepType, "sleep"),
          eq(activityJournal.phase, "pending"),
          sql`${activityJournal.wakeAt} <= ${params.now.toISOString()}::timestamptz`,
        ),
      )
      .orderBy(activityJournal.wakeAt)
      .limit(params.limit);
    return rows.map((r) => ({
      workflowId: r.workflowId,
      stepName: r.stepName,
      activityIndex: r.activityIndex,
      branchPath: r.branchPath,
      // WHERE guarantees non-null wakeAt here.
      wakeAt: r.wakeAt!,
    }));
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const [row] = await this.db
      .select()
      .from(activityJournal)
      .where(
        and(
          eq(activityJournal.workflowId, params.workflowId),
          eq(activityJournal.stepName, params.stepName),
          eq(activityJournal.activityName, params.signalName),
          eq(activityJournal.stepType, "signal"),
          eq(activityJournal.phase, "pending"),
        ),
      )
      .limit(1);
    return row ? rowToJournalEntry(row) : null;
  }
}

function rowToJournalEntry(row: {
  activityIndex: number;
  branchPath: string;
  activityName: string;
  stepType: string;
  phase: string;
  payloadHash: string | null;
  wakeAt: Date | null;
  exit: unknown;
  createdAt: Date;
}): JournalEntry {
  return {
    activityIndex: row.activityIndex,
    branchPath: row.branchPath,
    activityName: row.activityName,
    stepType: row.stepType as JournalEntry["stepType"],
    phase: row.phase as JournalEntry["phase"],
    payloadHash: row.payloadHash ?? undefined,
    wakeAt: row.wakeAt ?? undefined,
    exit: (row.exit ?? undefined) as JournalEntry["exit"],
    createdAt: row.createdAt,
  };
}
