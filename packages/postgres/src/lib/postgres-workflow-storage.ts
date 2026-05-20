// ---------------------------------------------------------------------------
// PostgresWorkflowStorage — production-grade WorkflowStorage backed by Postgres
// ---------------------------------------------------------------------------

import { eq, and, sql, desc, asc, inArray, gte, lt } from "drizzle-orm";
import type {
  WorkflowStorage,
  StepAttemptStorage,
  WorkflowState,
  WorkflowRunSummary,
  WorkflowStatus,
  WorkflowOrderBy,
  StepStatus,
  StepType,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  ActivityJournalStorage,
  JournaledSuspendStorage,
  JournalEntry,
  FenceGuard,
  SignalTokenRecord,
  StreamChunk,
} from "@promin/workflow";
import { FenceTokenMismatchError } from "@promin/workflow";
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
  signalTokens,
  workflowStreams,
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
      tripwire: row.tripwire ?? undefined,
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
      signalJsonSchema: row.signalJsonSchema ?? undefined,
      metadata: (row.metadata as Record<string, unknown> | null) ?? undefined,
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
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    orderBy?: WorkflowOrderBy;
    orderDir?: "asc" | "desc";
  }): Promise<WorkflowState[]> {
    const conditions = [];
    // Scope to constructor namespace if set and no explicit namespace filter
    const ns = params?.namespace ?? this.config.namespace;
    if (ns) conditions.push(eq(workflows.namespace, ns));
    if (params?.status)
      conditions.push(eq(workflows.statusId, WorkflowStatusIds.toId(params.status)));
    if (params?.name) conditions.push(eq(workflows.workflowName, params.name));
    if (params?.type) conditions.push(eq(workflows.workflowType, params.type));
    // jsonb `@>` containment: rows where `metadata` contains every supplied
    // key/value pair. A GIN index on `metadata` (`USING GIN (metadata)`) or
    // an expression index (`((metadata->>'<key>'))`) makes this index-driven
    // — the storage doesn't ship one by default; users opt in based on
    // their query patterns.
    if (params?.metadata && Object.keys(params.metadata).length > 0) {
      conditions.push(sql`${workflows.metadata} @> ${JSON.stringify(params.metadata)}::jsonb`);
    }

    const query = this.db.select().from(workflows).$dynamic();
    if (conditions.length > 0)
      query.where(conditions.length === 1 ? conditions[0] : and(...conditions));
    query.orderBy(postgresOrderByClause(params?.orderBy, params?.orderDir));
    if (params?.limit) query.limit(params.limit);
    if (params?.offset) query.offset(params.offset);

    const rows = await query;
    return rows.map((row: any) => this.rowToWorkflowState(row, []));
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.config.namespace;
    const query = this.db
      .selectDistinct({ workflowName: workflows.workflowName })
      .from(workflows)
      .$dynamic();
    if (ns) query.where(eq(workflows.namespace, ns));
    query.orderBy(workflows.workflowName);
    const rows = await query;
    return rows.map((r) => r.workflowName);
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.config.namespace;
    const query = this.db
      .selectDistinct({ workflowType: workflows.workflowType })
      .from(workflows)
      .$dynamic();
    if (ns) {
      query.where(and(eq(workflows.namespace, ns), sql`${workflows.workflowType} IS NOT NULL`));
    } else {
      query.where(sql`${workflows.workflowType} IS NOT NULL`);
    }
    query.orderBy(workflows.workflowType);
    const rows = await query;
    return rows.map((r) => r.workflowType!).filter((t): t is string => t != null);
  }

  async distinctNamespaces(): Promise<string[]> {
    const rows = await this.db
      .selectDistinct({ namespace: workflows.namespace })
      .from(workflows)
      .where(sql`${workflows.namespace} IS NOT NULL`)
      .orderBy(workflows.namespace);
    return rows.map((r) => r.namespace!).filter((n): n is string => n != null);
  }

  async cancelWorkflow(
    workflowId: string,
    _options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(workflowId, guard);
    const now = this.config.clock.now();
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
    idempotencyKey?: string;
    idempotencyExpiresAt?: Date;
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
        idempotencyKey: params.idempotencyKey,
        idempotencyExpiresAt: params.idempotencyExpiresAt,
      })
      .onConflictDoNothing()
      .returning({ workflowId: workflows.workflowId });

    if (!inserted) {
      // Conflict: either workflowId PK matched (caller's id was already
      // taken) or the partial-unique idempotency_key index matched
      // (another caller registered the key first). Resolve to whichever
      // row exists by id first, then by key.
      const existingById = await this.loadWorkflow(params.workflowId);
      if (existingById) return { created: false, existing: existingById };

      if (params.idempotencyKey) {
        const hit = await this.findWorkflowByIdempotencyKey({
          workflowName: params.workflowName,
          idempotencyKey: params.idempotencyKey,
          now: this.config.clock.now(),
        });
        if (hit) {
          const existing = await this.loadWorkflow(hit.workflowId);
          if (existing) return { created: false, existing };
        }
      }
      throw new Error(
        `createWorkflow: insert conflict for "${params.workflowId}" but neither workflow_id nor idempotency_key resolved.`,
      );
    }
    return { created: true };
  }

  async findWorkflowByIdempotencyKey(params: {
    workflowName: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
    const [row] = await this.db
      .select({ workflowId: workflows.workflowId })
      .from(workflows)
      .where(
        and(
          eq(workflows.workflowName, params.workflowName),
          eq(workflows.idempotencyKey, params.idempotencyKey),
          sql`${workflows.idempotencyExpiresAt} IS NOT NULL`,
          sql`${workflows.idempotencyExpiresAt} > ${params.now.toISOString()}::timestamptz`,
        ),
      )
      .limit(1);
    return row ? { workflowId: row.workflowId } : null;
  }

  /** Transition pending → running on first step activity. */
  private async markRunning(workflowId: string): Promise<void> {
    const now = this.config.clock.now();
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
    await this.batchSaveStepResults([params], guard);
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
    if (records.length === 0) return;
    // One fence check covers the whole batch — the engine already batches
    // per-workflow, so the unique set is tiny.
    const workflowIds = new Set(records.map((r) => r.workflowId));
    for (const id of workflowIds) await this.checkFence(id, guard);
    const now = this.config.clock.now();

    // Group by workflowId so we issue at most one markRunning + getCurrentRun
    // per workflow regardless of how many step records target it, then fold
    // everything into one multi-row INSERT inside a transaction. The 4n
    // round trips the single-row path costs collapse to O(workflows) reads
    // plus one bulk write.
    const byWf = new Map<string, Array<(typeof records)[number]>>();
    for (const r of records) {
      const bucket = byWf.get(r.workflowId);
      if (bucket) bucket.push(r);
      else byWf.set(r.workflowId, [r]);
    }

    await this.db.transaction(async (tx) => {
      const runsByWf = new Map<string, number>();
      for (const wfId of byWf.keys()) {
        // pending → running: record startedAt on the first transition.
        await tx
          .update(workflows)
          .set({ statusId: WorkflowStatusIds.id.running, startedAt: now, updatedAt: now })
          .where(
            and(
              eq(workflows.workflowId, wfId),
              eq(workflows.statusId, WorkflowStatusIds.id.pending),
            ),
          );
        // suspended → running: resume without overwriting startedAt.
        await tx
          .update(workflows)
          .set({ statusId: WorkflowStatusIds.id.running, updatedAt: now })
          .where(
            and(
              eq(workflows.workflowId, wfId),
              eq(workflows.statusId, WorkflowStatusIds.id.suspended),
            ),
          );
        const [row] = await tx
          .select({ run: workflows.run })
          .from(workflows)
          .where(eq(workflows.workflowId, wfId));
        runsByWf.set(wfId, row?.run ?? 1);
      }

      const values = records.map((r) => ({
        workflowId: r.workflowId,
        stepName: r.stepName,
        run: runsByWf.get(r.workflowId) ?? 1,
        statusId: StepStatusIds.id.completed,
        result: r.result,
        metadata: r.metadata,
        startedAt: r.startedAt,
        completedAt: now,
        durationMs: r.durationMs,
        attempt: 1,
      }));

      await tx
        .insert(workflowSteps)
        .values(values)
        .onConflictDoUpdate({
          target: [workflowSteps.workflowId, workflowSteps.stepName, workflowSteps.run],
          set: {
            statusId: StepStatusIds.id.completed,
            // EXCLUDED.* references the would-be-inserted row — lets us
            // apply per-row values on conflict without unrolling into N
            // statements. Same semantic as the single-row path.
            result: sql`EXCLUDED.result`,
            metadata: sql`COALESCE(EXCLUDED.metadata, ${workflowSteps.metadata})`,
            completedAt: sql`EXCLUDED.completed_at`,
            durationMs: sql`EXCLUDED.duration_ms`,
            attempt: sql`${workflowSteps.attempt} + 1`,
          },
        });

      for (const wfId of byWf.keys()) {
        await tx.update(workflows).set({ updatedAt: now }).where(eq(workflows.workflowId, wfId));
      }
    });
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
    await this.checkFence(params.workflowId, guard);
    await this.markRunning(params.workflowId);
    const now = this.config.clock.now();
    const run = await this.getCurrentRun(params.workflowId);
    await this.db
      .insert(workflowSteps)
      .values({
        workflowId: params.workflowId,
        stepName: params.stepName,
        run,
        statusId: StepStatusIds.id.failed,
        error: params.error,
        metadata: params.metadata,
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
          ...(params.metadata !== undefined ? { metadata: params.metadata } : {}),
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

  async saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(params.workflowId, guard);
    const now = this.config.clock.now();
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

  async saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(params.workflowId, guard);
    const now = this.config.clock.now();
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

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const now = this.config.clock.now();
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.completed, result, completedAt: now, updatedAt: now })
      .where(eq(workflows.workflowId, workflowId));
  }

  async failWorkflow(workflowId: string, error: string, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const now = this.config.clock.now();
    await this.db
      .update(workflows)
      .set({ statusId: WorkflowStatusIds.id.failed, error, completedAt: now, updatedAt: now })
      .where(eq(workflows.workflowId, workflowId));
  }

  async tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const now = this.config.clock.now();
    await this.db
      .update(workflows)
      .set({
        statusId: WorkflowStatusIds.id.tripwire,
        tripwire: reason,
        completedAt: now,
        updatedAt: now,
      })
      .where(eq(workflows.workflowId, workflowId));
  }

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(workflowId, guard);
    const now = this.config.clock.now();
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
      // Schema snapshot for `ctx.validatedSignal` / `ctx.approval` suspends.
      // Null for plain `ctx.signal()` — those keep the pass-through path.
      signalJsonSchema: stepUpdate.signalJsonSchema ?? undefined,
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
        set: { payload, deliveredAt: this.config.clock.now() },
      });
  }

  async setWorkflowMetadata(workflowId: string, patch: Record<string, unknown>): Promise<void> {
    // Postgres jsonb merge on the row's metadata column. `||` shallow-merges
    // top-level keys; null-valued entries in the patch are stripped via a
    // second `- text[]` op so callers can use `null` to remove a key.
    const removeKeys = Object.entries(patch)
      .filter(([, v]) => v === null)
      .map(([k]) => k);
    const writePatch: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(patch)) {
      if (v !== null) writePatch[k] = v;
    }
    await this.db
      .update(workflows)
      .set({
        metadata: sql`COALESCE(${workflows.metadata}, '{}'::jsonb) || ${JSON.stringify(writePatch)}::jsonb${
          removeKeys.length > 0
            ? sql` - ${sql.raw(`ARRAY[${removeKeys.map((k) => `'${k.replace(/'/g, "''")}'`).join(",")}]::text[]`)}`
            : sql``
        }`,
        updatedAt: this.config.clock.now(),
      })
      .where(eq(workflows.workflowId, workflowId));
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

  async tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: string }> {
    // Advisory locks live in pg_locks (not wf_workflow_locks) and have no
    // row to carry a fence token — same-session semantics are the guard
    // already. Row locks use the lock-table's bigserial fence_token.
    if (this.config.useAdvisoryLocks) {
      const acquired = await this.tryAdvisoryLock(workflowId);
      return { acquired };
    }
    return this.tryRowLock(workflowId, lockDurationMs);
  }

  async tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    // Sequences lock + load in the same connection — inexpensive locally,
    // collapses two HTTP round-trips when this storage is fronted by the
    // workflow-remote RPC. Wrapping in a transaction ensures the load sees
    // whatever the lock commits (advisory locks aren't row-scoped so the
    // transaction guarantee is weaker there, but state reads through
    // loadWorkflow go through the same connection and see a consistent
    // snapshot — good enough for the "are we joining an in-flight run?"
    // question the coordinator actually asks.
    const { acquired, token } = await this.tryLock(workflowId, lockDurationMs);
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    if (this.config.useAdvisoryLocks) {
      await this.releaseAdvisoryLock(workflowId);
      return;
    }
    // When a token is provided, only the current holder releases — a stale
    // holder whose lock already moved on silently no-ops (matches InMemory).
    if (guard?.fenceToken) {
      await this.db
        .delete(workflowLocks)
        .where(
          and(
            eq(workflowLocks.workflowId, workflowId),
            eq(workflowLocks.fenceToken, Number(guard.fenceToken)),
          ),
        );
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

  async heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    if (this.config.useAdvisoryLocks) return;
    if (guard?.fenceToken) {
      await this.db
        .update(workflowLocks)
        .set({ expiresAt: new Date(this.config.clock.currentTimeMs() + lockDurationMs) })
        .where(
          and(
            eq(workflowLocks.workflowId, workflowId),
            eq(workflowLocks.fenceToken, Number(guard.fenceToken)),
          ),
        );
      return;
    }
    await this.db
      .update(workflowLocks)
      .set({ expiresAt: new Date(this.config.clock.currentTimeMs() + lockDurationMs) })
      .where(
        and(
          eq(workflowLocks.workflowId, workflowId),
          eq(workflowLocks.lockedBy, this.config.instanceId),
        ),
      );
  }

  /**
   * Reject a mutating call when the caller's fence token doesn't match the
   * current row-lock. Advisory-lock mode and callers that don't pass a
   * token skip the check (fencing is additive — legacy call sites keep
   * working).
   */
  private async checkFence(workflowId: string, guard?: FenceGuard): Promise<void> {
    if (!guard?.fenceToken) return;
    if (this.config.useAdvisoryLocks) return; // no per-row fence in advisory mode
    const [row] = await this.db
      .select({ fenceToken: workflowLocks.fenceToken })
      .from(workflowLocks)
      .where(eq(workflowLocks.workflowId, workflowId));
    const current = row ? String(row.fenceToken) : undefined;
    if (current !== guard.fenceToken) {
      throw new FenceTokenMismatchError({
        workflowId,
        expected: current ?? "(no lock)",
        provided: guard.fenceToken,
        message:
          `Fenced write for "${workflowId}" rejected — ` +
          `token mismatch (expected "${current ?? "(no lock)"}", got "${guard.fenceToken}")`,
      });
    }
  }

  async startFreshRun(workflowId: string): Promise<number> {
    const now = this.config.clock.now();

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
          tripwire: current.tripwire,
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
        tripwire: null,
        startedAt: null,
        completedAt: null,
        updatedAt: now,
      })
      .where(eq(workflows.workflowId, workflowId))
      .returning({ run: workflows.run });
    return row?.run ?? 1;
  }

  async resetSteps(workflowId: string, stepNames: readonly string[]): Promise<void> {
    if (stepNames.length === 0) return;
    const names = [...stepNames];
    await this.db.transaction(async (tx) => {
      // Step / task rows are keyed per `run`; only the live run resets.
      // Unknown workflow → throw, matching InMemoryWorkflowStorage so
      // every backend shares one contract (the runner guards existence
      // before calling, so this is a defensive check).
      const [wf] = await tx
        .select({ run: workflows.run })
        .from(workflows)
        .where(eq(workflows.workflowId, workflowId));
      if (!wf) throw new Error(`Workflow ${workflowId} not found`);

      // Delete the listed steps + their map tasks — a deleted row reads
      // back as "never ran" (same shape as InMemoryWorkflowStorage,
      // which drops the entries from its step map). The DAG executor
      // re-creates them on the resumed run.
      await tx
        .delete(workflowSteps)
        .where(
          and(
            eq(workflowSteps.workflowId, workflowId),
            eq(workflowSteps.run, wf.run),
            inArray(workflowSteps.stepName, names),
          ),
        );
      await tx
        .delete(workflowStepTasks)
        .where(
          and(
            eq(workflowStepTasks.workflowId, workflowId),
            eq(workflowStepTasks.run, wf.run),
            inArray(workflowStepTasks.stepName, names),
          ),
        );
      // Clear journal entries so the activities re-fire on replay rather
      // than returning stale recorded values.
      await tx
        .delete(activityJournal)
        .where(
          and(eq(activityJournal.workflowId, workflowId), inArray(activityJournal.stepName, names)),
        );
      // Flip a terminal workflow back to running so the runner resumes
      // it; a still-running / suspended workflow is left as-is.
      await tx
        .update(workflows)
        .set({
          statusId: WorkflowStatusIds.id.running,
          result: null,
          error: null,
          tripwire: null,
          completedAt: null,
          updatedAt: this.config.clock.now(),
        })
        .where(
          and(
            eq(workflows.workflowId, workflowId),
            inArray(workflows.statusId, [
              WorkflowStatusIds.id.completed,
              WorkflowStatusIds.id.failed,
              WorkflowStatusIds.id.tripwire,
            ]),
          ),
        );
    });
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
      tripwire?: unknown;
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
        tripwire: wfRow.tripwire ?? undefined,
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
        tripwire: ar.tripwire ?? undefined,
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
      tripwire: meta.tripwire,
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
      to = new Date(this.config.clock.currentTimeMs() - params.olderThanMs);
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
          sql`${workflows.statusId} IN (${WorkflowStatusIds.id.completed}, ${WorkflowStatusIds.id.failed}, ${WorkflowStatusIds.id.tripwire})`,
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

  private async tryRowLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: string }> {
    const now = this.config.clock.now();
    const expiresAt = new Date(now.getTime() + lockDurationMs);
    // On fresh insert the bigserial column populates from its sequence; on
    // expired-lock takeover we force a new value via `nextval(...)` so the
    // token strictly increases across holders. The `WHERE expires_at < now`
    // clause on the UPDATE path guarantees we only rotate the token when
    // the previous holder's lease is dead.
    const [result] = await execRaw(
      this.db,
      sql`
      INSERT INTO wf_workflow_locks (workflow_id, locked_at, expires_at, locked_by)
      VALUES (${workflowId}, ${now}, ${expiresAt}, ${this.config.instanceId})
      ON CONFLICT (workflow_id) DO UPDATE
        SET locked_at = ${now},
            expires_at = ${expiresAt},
            locked_by = ${this.config.instanceId},
            fence_token = nextval(pg_get_serial_sequence('wf_workflow_locks', 'fence_token'))
        WHERE wf_workflow_locks.expires_at < ${now}
      RETURNING fence_token
    `,
    );
    if (!result) return { acquired: false };
    const token = String((result as { fence_token: number | string }).fence_token);
    return { acquired: true, token };
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStorage — attempt history (opt-in via recordAttempts config)
  // ---------------------------------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord, guard?: FenceGuard): Promise<void> {
    if (!this.config.recordAttempts) return;
    await this.checkFence(record.workflowId, guard);

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
      // Schema column is still named `worker_id` (drizzle field
      // `workerId`); the StepAttemptRecord interface renamed
      // `workerId` → `executorId` so non-worker executors (in-process
      // runner, embedded ZoryaWorkflows) read naturally too.
      workerId: record.executorId,
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
      executorId: r.workerId ?? undefined,
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
    stepType: "sleep" | "signal" | "activity" | "compensation" | "child" | "waitpoint";
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

  // ---------------------------------------------------------------------------
  // SignalToken — public-bearer authorization for storage.deliverSignal
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
      const [existing] = await this.db
        .select()
        .from(signalTokens)
        .where(
          and(
            eq(signalTokens.workflowId, params.workflowId),
            eq(signalTokens.idempotencyKey, params.idempotencyKey),
          ),
        )
        .limit(1);
      if (existing) {
        return { record: rowToSignalToken(existing), isCached: true };
      }
    }
    const [inserted] = await this.db
      .insert(signalTokens)
      .values({
        tokenId: params.tokenId,
        workflowId: params.workflowId,
        signalName: params.signalName,
        bearer: params.bearer,
        tags: [...params.tags],
        idempotencyKey: params.idempotencyKey ?? null,
        expiresAt: params.expiresAt,
      })
      .returning();
    return { record: rowToSignalToken(inserted!), isCached: false };
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    const [row] = await this.db
      .select()
      .from(signalTokens)
      .where(eq(signalTokens.tokenId, tokenId))
      .limit(1);
    return row ? rowToSignalToken(row) : null;
  }

  async markSignalTokenCompleted(params: {
    tokenId: string;
    value: unknown;
    now: Date;
  }): Promise<
    | { outcome: "delivered"; record: SignalTokenRecord }
    | { outcome: "already_completed"; record: SignalTokenRecord }
  > {
    // Atomic: only update rows still pending; UPDATE ... RETURNING tells us
    // whether we won the race or lost to a concurrent completer.
    const [won] = await this.db
      .update(signalTokens)
      .set({ completedAt: params.now, completedValue: params.value as never })
      .where(
        and(eq(signalTokens.tokenId, params.tokenId), sql`${signalTokens.completedAt} IS NULL`),
      )
      .returning();
    if (won) {
      return { outcome: "delivered", record: rowToSignalToken(won) };
    }
    const current = await this.findSignalTokenById(params.tokenId);
    if (!current) {
      throw new Error(`signal token ${params.tokenId} not found`);
    }
    return { outcome: "already_completed", record: current };
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    const rows = await this.db
      .select()
      .from(signalTokens)
      .where(eq(signalTokens.workflowId, workflowId))
      .orderBy(desc(signalTokens.createdAt));
    return rows.map(rowToSignalToken);
  }

  // ---------------------------------------------------------------------------
  // Streams — append-only chunks per (workflow, stream).
  // ---------------------------------------------------------------------------

  async appendStreamChunk(params: {
    workflowId: string;
    streamId: string;
    payload: unknown;
    appendedBy: "workflow" | "external";
  }): Promise<{ chunkIndex: number }> {
    // Compute next index in a single statement via subquery — atomic
    // against concurrent appends, no read-then-write race.
    const inserted = await this.db.execute(sql`
      INSERT INTO wf_streams (workflow_id, stream_id, chunk_index, payload, appended_by)
      VALUES (
        ${params.workflowId},
        ${params.streamId},
        COALESCE(
          (SELECT MAX(chunk_index) + 1 FROM wf_streams
           WHERE workflow_id = ${params.workflowId} AND stream_id = ${params.streamId}),
          0
        ),
        ${JSON.stringify(params.payload)}::jsonb,
        ${params.appendedBy}
      )
      RETURNING chunk_index
    `);
    const rows =
      (inserted as unknown as { rows?: Array<{ chunk_index: number }> }).rows ??
      (inserted as unknown as Array<{ chunk_index: number }>);
    const chunkIndex = Array.isArray(rows) ? rows[0]?.chunk_index : undefined;
    if (chunkIndex === undefined) {
      throw new Error("appendStreamChunk: no row returned from INSERT");
    }
    return { chunkIndex };
  }

  async readStreamChunks(params: {
    workflowId: string;
    streamId: string;
    since?: number;
    limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>> {
    const conditions = [
      eq(workflowStreams.workflowId, params.workflowId),
      eq(workflowStreams.streamId, params.streamId),
    ];
    if (params.since !== undefined) {
      conditions.push(sql`${workflowStreams.chunkIndex} > ${params.since}`);
    }
    const baseQuery = this.db
      .select()
      .from(workflowStreams)
      .where(and(...conditions))
      .orderBy(asc(workflowStreams.chunkIndex));
    const rows = await (params.limit !== undefined ? baseQuery.limit(params.limit) : baseQuery);
    return rows.map((r) => ({
      chunkIndex: r.chunkIndex,
      payload: r.payload,
      appendedBy: r.appendedBy as "workflow" | "external",
      appendedAt: r.appendedAt,
    }));
  }
}

/**
 * Build the ORDER BY expression for `listWorkflows`. NULL values always
 * sort last so still-running rows (no `started_at` / `completed_at` /
 * `duration`) don't push real data off the first page in either direction.
 * `status` orders by status_id (the integer enum) — alphabetizing requires
 * a join with the lookup table, and the cost isn't justified for a
 * dropdown-driven sort. Default: `started_at DESC NULLS LAST` so dashboards
 * lead with the most-recently-started run; pending rows that haven't
 * picked up a worker yet fall to the bottom.
 */
function postgresOrderByClause(orderBy?: WorkflowOrderBy, orderDir?: "asc" | "desc") {
  const direction = orderDir === "asc" ? sql.raw("ASC") : sql.raw("DESC");
  const nullsLast = sql.raw("NULLS LAST");
  switch (orderBy) {
    case "createdAt":
      return orderDir === "asc" ? asc(workflows.createdAt) : desc(workflows.createdAt);
    case "completedAt":
      return sql`${workflows.completedAt} ${direction} ${nullsLast}`;
    case "duration":
      return sql`(${workflows.completedAt} - ${workflows.createdAt}) ${direction} ${nullsLast}`;
    case "status":
      return orderDir === "asc" ? asc(workflows.statusId) : desc(workflows.statusId);
    case "name":
      return orderDir === "asc" ? asc(workflows.workflowName) : desc(workflows.workflowName);
    case "startedAt":
    default:
      return sql`${workflows.startedAt} ${direction} ${nullsLast}`;
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

function rowToSignalToken(row: {
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
}): SignalTokenRecord {
  return {
    tokenId: row.tokenId,
    workflowId: row.workflowId,
    signalName: row.signalName,
    bearer: row.bearer,
    tags: row.tags,
    idempotencyKey: row.idempotencyKey,
    expiresAt: row.expiresAt,
    completedAt: row.completedAt,
    completedValue: row.completedValue,
    createdAt: row.createdAt,
  };
}
