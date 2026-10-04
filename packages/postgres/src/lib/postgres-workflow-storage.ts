// ---------------------------------------------------------------------------
// PostgresWorkflowStorage — production-grade WorkflowStorage backed by Postgres
// ---------------------------------------------------------------------------

import { eq, and, or, sql, desc, asc, inArray, gte, lt, type SQL } from "drizzle-orm";
import type {
  WorkflowStorage,
  StepAttemptStore,
  StepCheckpoint,
  StepCheckpointStore,
  CompensationLedgerStore,
  StepCompensationOutcome,
  RunSource,
  WorkflowState,
  WorkflowStatusSnapshot,
  WorkflowSummary,
  WorkflowRunSummary,
  WorkflowStatus,
  WorkflowOrderBy,
  StepStatus,
  StepType,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  JournalStore,
  JournalEntry,
  JournalExit,
  CompletePendingResult,
  FenceGuard,
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
  CheckpointStepParams,
  CompletePendingEntryParams,
  CompleteWorkflowParams,
  CreateWorkflowParams,
  DeliverSignalParams,
  DiscardJournalEntriesParams,
  FailWorkflowParams,
  HeartbeatParams,
  LoadJournalParams,
  LoadRunHistoryParams,
  LoadStepAttemptsParams,
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
  TryLockParams,
} from "@promin/workflow";
import { FenceTokenMismatchError, type JournalStepType } from "@promin/workflow";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  encodeRunSource,
  decodeRunSource,
} from "@promin/workflow/storage-kit";
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
import { hashToInt32, type DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "./exec-raw.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Status ids a terminal transition (complete / fail / tripwire) may leave. */
const NON_TERMINAL_STATUS_IDS = [
  WorkflowStatusIds.id.pending,
  WorkflowStatusIds.id.running,
  WorkflowStatusIds.id.suspended,
  WorkflowStatusIds.id.compensating,
];

/** Status ids `cancelWorkflow` may leave. */
const CANCELLABLE_STATUS_IDS = [
  WorkflowStatusIds.id.pending,
  WorkflowStatusIds.id.running,
  WorkflowStatusIds.id.suspended,
];

/** `NOW() + <ms>` evaluated on the server clock. */
function serverNowPlusMs(ms: number) {
  return sql`NOW() + (${Math.max(0, Math.trunc(ms))}::double precision * INTERVAL '1 millisecond')`;
}

/** The error a rejected fenced write throws. */
function fenceMismatch(params: {
  workflowId: string;
  provided: string;
  current: string | undefined;
  expired: boolean;
}): FenceTokenMismatchError {
  const { workflowId, provided, current, expired } = params;
  const expected = current === undefined ? "(no lock)" : expired ? "(expired)" : current;
  const reason =
    current === undefined
      ? "no active lock"
      : expired && current === provided
        ? `the lock for token "${provided}" expired`
        : `token mismatch (expected "${current}", got "${provided}")`;
  return new FenceTokenMismatchError({
    workflowId,
    expected,
    provided,
    message: `Fenced write for "${workflowId}" rejected — ${reason}`,
  });
}

/** A jsonb bind parameter; `undefined` and `null` bind SQL NULL, like a drizzle insert. */
function jsonbParam(value: unknown): SQL {
  return value === undefined || value === null
    ? sql`NULL::jsonb`
    : sql`${JSON.stringify(value)}::jsonb`;
}

/** A timestamptz bind parameter (bound as ISO text, like the other raw queries here). */
function timestampParam(value: Date | undefined | null): SQL {
  return value == null ? sql`NULL::timestamptz` : sql`${value.toISOString()}::timestamptz`;
}

/**
 * A timestamptz column read through `execRaw`: drizzle's postgres-js
 * driver leaves timestamps as text, which its own column mapping parses.
 */
function timestampOf(value: Date | string | null | undefined): Date | null {
  if (value == null) return null;
  return value instanceof Date ? value : new Date(value);
}

/** Parse a jsonb column selected as `::text`; SQL NULL stays `null`. */
function parseJsonText(text: string | null | undefined): unknown {
  return text == null ? null : JSON.parse(text);
}

// ---------------------------------------------------------------------------
// PostgresWorkflowStorage
// ---------------------------------------------------------------------------

export class PostgresWorkflowStorage
  implements
    WorkflowStorage,
    StepAttemptStore,
    StepCheckpointStore,
    CompensationLedgerStore,
    JournalStore
{
  /**
   * Drizzle schemas for every table this storage reads and writes.
   * Use these to include workflow tables in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export const {
   *   workflows, workflowSteps, workflowStepTasks,
   *   workflowSignals, workflowLocks, stepAttempts,
   *   activityJournal, signalTokens, workflowStreams,
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
    activityJournal,
    signalTokens,
    workflowStreams,
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
      parentWorkflowId: row.parentWorkflowId ?? undefined,
      namespace: row.namespace ?? undefined,
      version: row.version ?? undefined,
      run: row.run ?? 1,
      runSource: decodeRunSource(row.runSource),
      runSourceId: row.runSourceId ?? undefined,
      status: WorkflowStatusIds.toName(row.statusId),
      input: row.input,
      result: row.result ?? undefined,
      error: row.error ?? undefined,
      errorTag: row.errorTag ?? undefined,
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
      errorTag: row.errorTag ?? undefined,
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
      ...(row.compensationStatus != null && {
        compensationStatus: row.compensationStatus as StepCompensationOutcome,
      }),
      ...(row.compensationError != null && { compensationError: row.compensationError }),
      ...(row.compensatedAt != null && { compensatedAt: row.compensatedAt }),
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

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    const [row] = await this.db
      .select({
        statusId: workflows.statusId,
        error: workflows.error,
        errorTag: workflows.errorTag,
      })
      .from(workflows)
      .where(eq(workflows.workflowId, workflowId));
    if (!row) return null;
    return {
      status: WorkflowStatusIds.toName(row.statusId),
      ...(row.error !== null && { error: row.error }),
      ...(row.errorTag !== null && { errorTag: row.errorTag }),
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    // One statement, so one snapshot: the workflow row and the step and
    // task rows of its current run can't come from different runs when a
    // `startFreshRun` commits mid-read. The three row kinds share one
    // column list (`kind` tells them apart; a kind's foreign columns are
    // NULL), so every row comes back as plain columns, without the
    // per-row cost of building json.
    const rows = await execRaw(
      this.db,
      sql`WITH w AS (SELECT * FROM wf_workflows WHERE workflow_id = ${workflowId})
        SELECT 0 AS kind, w.workflow_name AS name, w.workflow_type, w.namespace, w.version,
          w.parent_workflow_id, w.run_source, w.run_source_id, w.run, w.status_id, w.input,
          w.metadata, w.result, w.error, w.error_tag, w.tripwire, w.created_at, w.started_at,
          w.updated_at, w.completed_at,
          NULL::int AS step_type_id, NULL::jsonb AS depends_on, NULL::float8 AS duration_ms,
          NULL::int AS attempt, NULL::timestamptz AS wake_at, NULL::text AS signal_name,
          NULL::timestamptz AS signal_timeout_at, NULL::jsonb AS signal_json_schema,
          NULL::text AS compensation_status, NULL::text AS compensation_error,
          NULL::timestamptz AS compensated_at, NULL::int AS task_index
        FROM w
        UNION ALL
        SELECT 1, s.step_name, NULL, NULL, NULL, NULL, NULL, NULL, s.run, s.status_id, NULL,
          s.metadata, s.result, s.error, s.error_tag, NULL, NULL, s.started_at, NULL,
          s.completed_at, s.step_type_id, s.depends_on, s.duration_ms::float8, s.attempt,
          s.wake_at, s.signal_name, s.signal_timeout_at, s.signal_json_schema,
          s.compensation_status, s.compensation_error, s.compensated_at, NULL
        FROM wf_workflow_steps s JOIN w ON s.workflow_id = w.workflow_id AND s.run = w.run
        UNION ALL
        SELECT 2, t.step_name, NULL, NULL, NULL, NULL, NULL, NULL, t.run, t.status_id, t.input,
          NULL, t.result, t.error, NULL, NULL, NULL, t.started_at, NULL, t.completed_at, NULL,
          NULL, NULL, t.attempt, NULL, NULL, NULL, NULL, NULL, NULL, NULL, t.task_index
        FROM wf_workflow_step_tasks t JOIN w ON t.workflow_id = w.workflow_id AND t.run = w.run`,
    );

    let wfRow: Record<string, any> | undefined;
    const stepRows: Array<Record<string, any>> = [];
    const tasksByStep = new Map<string, StepTaskState[]>();
    for (const row of rows) {
      if (row.kind === 0) wfRow = row;
      else if (row.kind === 1) stepRows.push(row);
      else {
        let bucket = tasksByStep.get(row.name);
        if (!bucket) tasksByStep.set(row.name, (bucket = []));
        bucket.push(
          this.rowToTaskState({
            taskIndex: row.task_index,
            statusId: row.status_id,
            input: row.input,
            result: row.result,
            error: row.error,
            startedAt: timestampOf(row.started_at),
            completedAt: timestampOf(row.completed_at),
            attempt: row.attempt,
          }),
        );
      }
    }
    if (wfRow === undefined) return null;

    const steps = stepRows.map((row) =>
      this.rowToStepState(
        {
          stepName: row.name,
          run: row.run,
          statusId: row.status_id,
          stepTypeId: row.step_type_id,
          dependsOn: row.depends_on,
          result: row.result,
          error: row.error,
          errorTag: row.error_tag,
          startedAt: timestampOf(row.started_at),
          completedAt: timestampOf(row.completed_at),
          durationMs: row.duration_ms,
          attempt: row.attempt,
          wakeAt: timestampOf(row.wake_at),
          signalName: row.signal_name,
          signalTimeoutAt: timestampOf(row.signal_timeout_at),
          signalJsonSchema: row.signal_json_schema,
          metadata: row.metadata,
          compensationStatus: row.compensation_status,
          compensationError: row.compensation_error,
          compensatedAt: timestampOf(row.compensated_at),
        },
        tasksByStep.get(row.name),
      ),
    );
    return this.rowToWorkflowState(
      {
        workflowId,
        workflowName: wfRow.name,
        workflowType: wfRow.workflow_type,
        parentWorkflowId: wfRow.parent_workflow_id,
        namespace: wfRow.namespace,
        version: wfRow.version,
        run: wfRow.run,
        runSource: wfRow.run_source,
        runSourceId: wfRow.run_source_id,
        statusId: wfRow.status_id,
        input: wfRow.input,
        result: wfRow.result,
        error: wfRow.error,
        errorTag: wfRow.error_tag,
        tripwire: wfRow.tripwire,
        metadata: wfRow.metadata,
        createdAt: timestampOf(wfRow.created_at),
        startedAt: timestampOf(wfRow.started_at),
        updatedAt: timestampOf(wfRow.updated_at),
        completedAt: timestampOf(wfRow.completed_at),
      },
      steps,
    );
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
    const conditions = this.workflowFilterConditions(params);
    const query = this.db.select().from(workflows).$dynamic();
    if (conditions.length > 0)
      query.where(conditions.length === 1 ? conditions[0] : and(...conditions));
    query.orderBy(postgresOrderByClause(params?.orderBy, params?.orderDir));
    if (params?.limit) query.limit(params.limit);
    if (params?.offset) query.offset(params.offset);

    const rows = await query;
    return rows.map((row: any) => this.rowToWorkflowState(row, []));
  }

  /**
   * `listWorkflows` without the blob columns (`input`, `result`, `error`,
   * `tripwire`, step rows): what list views and the dashboard metrics
   * read, at the cost of the row headers only.
   */
  async listWorkflowSummaries(
    params?: Parameters<WorkflowStorage["listWorkflows"]>[0],
  ): Promise<WorkflowSummary[]> {
    const conditions = this.workflowFilterConditions(params);
    const query = this.db
      .select({
        workflowId: workflows.workflowId,
        workflowName: workflows.workflowName,
        workflowType: workflows.workflowType,
        namespace: workflows.namespace,
        statusId: workflows.statusId,
        version: workflows.version,
        run: workflows.run,
        runSource: workflows.runSource,
        runSourceId: workflows.runSourceId,
        metadata: workflows.metadata,
        createdAt: workflows.createdAt,
        startedAt: workflows.startedAt,
        updatedAt: workflows.updatedAt,
        completedAt: workflows.completedAt,
      })
      .from(workflows)
      .$dynamic();
    if (conditions.length > 0)
      query.where(conditions.length === 1 ? conditions[0] : and(...conditions));
    query.orderBy(postgresOrderByClause(params?.orderBy, params?.orderDir));
    if (params?.limit) query.limit(params.limit);
    if (params?.offset) query.offset(params.offset);

    const rows = await query;
    return rows.map((row) => ({
      workflowId: row.workflowId,
      workflowName: row.workflowName,
      workflowType: row.workflowType ?? undefined,
      namespace: row.namespace ?? undefined,
      status: WorkflowStatusIds.toName(row.statusId),
      version: row.version ?? undefined,
      run: row.run ?? 1,
      runSource: decodeRunSource(row.runSource),
      runSourceId: row.runSourceId ?? undefined,
      metadata: (row.metadata as Record<string, unknown> | null) ?? undefined,
      createdAt: row.createdAt,
      startedAt: row.startedAt ?? undefined,
      updatedAt: row.updatedAt,
      completedAt: row.completedAt ?? undefined,
    }));
  }

  /** `SELECT COUNT(*)` over the same filters as `listWorkflows`. */
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
    const conditions = this.workflowFilterConditions(params);
    const query = this.db
      .select({ n: sql<number>`count(*)::int` })
      .from(workflows)
      .$dynamic();
    if (conditions.length > 0)
      query.where(conditions.length === 1 ? conditions[0] : and(...conditions));
    const [row] = await query;
    return Number(row?.n ?? 0);
  }

  private workflowFilterConditions(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }) {
    const conditions = [];
    // Scope to constructor namespace if set and no explicit namespace filter
    const ns = params?.namespace ?? this.config.namespace;
    if (ns) conditions.push(eq(workflows.namespace, ns));
    if (params?.status)
      conditions.push(eq(workflows.statusId, WorkflowStatusIds.toId(params.status)));
    if (params?.name) conditions.push(eq(workflows.workflowName, params.name));
    if (params?.version !== undefined) conditions.push(eq(workflows.version, params.version));
    if (params?.type) conditions.push(eq(workflows.workflowType, params.type));
    if (params?.parentId) conditions.push(eq(workflows.parentWorkflowId, params.parentId));
    if (params?.runSource !== undefined) {
      conditions.push(eq(workflows.runSource, encodeRunSource(params.runSource)!));
    }
    if (params?.runSourceId !== undefined) {
      conditions.push(eq(workflows.runSourceId, params.runSourceId));
    }
    // jsonb `@>` containment: rows where `metadata` contains every supplied
    // key/value pair. A GIN index on `metadata` (`USING GIN (metadata)`) or
    // an expression index (`((metadata->>'<key>'))`) makes this index-driven
    // — the storage doesn't ship one by default; users opt in based on
    // their query patterns.
    if (params?.metadata && Object.keys(params.metadata).length > 0) {
      conditions.push(sql`${workflows.metadata} @> ${JSON.stringify(params.metadata)}::jsonb`);
    }
    return conditions;
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

  async cancelWorkflow({ workflowId, cascade, guard }: CancelWorkflowParams): Promise<void> {
    await this.fenced({
      workflowId,
      guard,
      write: (db) => this.cancelWithin({ db, workflowId, cascade: cascade === true }),
    });
  }

  private async cancelWithin(params: {
    db: DrizzleDb;
    workflowId: string;
    cascade: boolean;
  }): Promise<void> {
    const { db, workflowId } = params;
    const now = this.config.clock.now();
    const set = {
      statusId: WorkflowStatusIds.id.failed,
      error: CANCELLED_ERROR,
      errorTag: CANCELLED_ERROR_TAG,
      completedAt: now,
      updatedAt: now,
    };
    if (!params.cascade) {
      await db
        .update(workflows)
        .set(set)
        .where(
          and(
            eq(workflows.workflowId, workflowId),
            inArray(workflows.statusId, CANCELLABLE_STATUS_IDS),
          ),
        );
      return;
    }
    // Cascade: the run plus every descendant reachable through
    // parent_workflow_id, cancelled in one statement. UNION (not UNION ALL)
    // stops on a parent cycle.
    const family = sql`(
      WITH RECURSIVE family(workflow_id) AS (
        SELECT ${workflowId}::text
        UNION
        SELECT w.workflow_id FROM wf_workflows w
        JOIN family f ON w.parent_workflow_id = f.workflow_id
      )
      SELECT workflow_id FROM family
    )`;
    await db
      .update(workflows)
      .set(set)
      .where(
        and(
          sql`${workflows.workflowId} IN ${family}`,
          inArray(workflows.statusId, CANCELLABLE_STATUS_IDS),
        ),
      );
  }

  async createWorkflow({
    guard,
    ...params
  }: CreateWorkflowParams): Promise<
    { created: true } | { created: false; existing: WorkflowState }
  > {
    const ns = this.resolveNamespace(params.namespace);
    const parentToken = this.fenceTokenOf(guard);
    if (parentToken !== undefined && params.parentWorkflowId === undefined) {
      throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
    }
    const inserted = await this.db.transaction(async (tx) => {
      // A child create is fenced on the parent's lock, held for the insert.
      if (parentToken !== undefined) {
        await this.assertFence({
          db: tx as unknown as DrizzleDb,
          workflowId: params.parentWorkflowId!,
          token: parentToken,
        });
      }
      if (params.idempotencyKey) {
        // An expired key no longer owns its slot in the partial unique
        // index: release it in the same transaction so this insert can
        // claim the key. Expiry is judged on the same clock as
        // `findWorkflowByIdempotencyKey`, so both agree on "expired".
        await tx
          .update(workflows)
          .set({ idempotencyKey: null })
          .where(
            and(
              sql`COALESCE(${workflows.namespace}, '') = COALESCE(${ns}::text, '')`,
              eq(workflows.workflowName, params.workflowName),
              eq(workflows.idempotencyKey, params.idempotencyKey),
              sql`(${workflows.idempotencyExpiresAt} IS NULL OR ${workflows.idempotencyExpiresAt} <= ${this.config.clock.now().toISOString()}::timestamptz)`,
            ),
          );
      }
      const [row] = await tx
        .insert(workflows)
        .values({
          workflowId: params.workflowId,
          workflowName: params.workflowName,
          workflowType: params.workflowType,
          parentWorkflowId: params.parentWorkflowId,
          namespace: ns,
          version: params.version,
          runSource: encodeRunSource(params.runSource),
          runSourceId: params.runSourceId,
          statusId: WorkflowStatusIds.id.pending,
          input: params.input,
          metadata: params.metadata,
          idempotencyKey: params.idempotencyKey,
          idempotencyExpiresAt: params.idempotencyExpiresAt,
        })
        .onConflictDoNothing()
        .returning({ workflowId: workflows.workflowId });
      return row;
    });

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
          ...(params.namespace !== undefined && { namespace: params.namespace }),
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
    namespace?: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
    const ns = this.resolveNamespace(params.namespace);
    const [row] = await this.db
      .select({ workflowId: workflows.workflowId })
      .from(workflows)
      .where(
        and(
          ns === null ? sql`${workflows.namespace} IS NULL` : eq(workflows.namespace, ns),
          eq(workflows.workflowName, params.workflowName),
          eq(workflows.idempotencyKey, params.idempotencyKey),
          sql`${workflows.idempotencyExpiresAt} IS NOT NULL`,
          sql`${workflows.idempotencyExpiresAt} > ${params.now.toISOString()}::timestamptz`,
        ),
      )
      .limit(1);
    return row ? { workflowId: row.workflowId } : null;
  }

  async saveStepResult({ guard, ...params }: SaveStepResultParams): Promise<void> {
    const { workflowId, stepName, ...outcome } = params;
    await this.writeStep({
      checkpoint: {
        workflowId,
        stepName,
        outcome: { kind: "completed", ...outcome },
        attempts: [],
      },
      guard,
    });
  }

  /**
   * `saveStepAttempt` for every attempt plus `saveStepResult` /
   * `saveStepFailure`, as one statement: the fence check, the workflow
   * row's status move, the step row upsert and the attempt rows, with the
   * run's status read back from the same statement.
   */
  async checkpointStep({
    guard,
    ...checkpoint
  }: CheckpointStepParams): Promise<WorkflowStatusSnapshot | null> {
    return this.writeStep({ checkpoint, guard });
  }

  /**
   * Write a settled step's row (and its attempt rows) in one fenced
   * statement. `wf` moves the workflow row (pending → running, and for a
   * completed step suspended → running too) and returns its run and
   * status; the step upsert and the attempt rows read the run from it, so
   * nothing is written for a missing workflow. Resolves with the run's
   * status, or `null` when the workflow does not exist.
   */
  private async writeStep(params: {
    checkpoint: StepCheckpoint;
    guard?: FenceGuard;
  }): Promise<WorkflowStatusSnapshot | null> {
    const { workflowId, stepName, outcome } = params.checkpoint;
    const now = timestampParam(this.config.clock.now());
    const ids = WorkflowStatusIds.id;
    const completed = outcome.kind === "completed";
    const resumable = completed ? [ids.pending, ids.suspended] : [ids.pending];
    const stepStatus = completed ? StepStatusIds.id.completed : StepStatusIds.id.failed;
    const error = completed ? null : outcome.error;
    const errorTag = completed ? null : (outcome.errorTag ?? null);
    // A completed step keeps an earlier error text, a failed one overwrites
    // it, like the separate saveStepResult / saveStepFailure writes did.
    const onConflict = completed
      ? sql`result = EXCLUDED.result`
      : sql`error = EXCLUDED.error, error_tag = EXCLUDED.error_tag`;
    const attempts = this.config.recordAttempts ? params.checkpoint.attempts : [];
    const attemptRows =
      attempts.length === 0
        ? sql``
        : sql`, attempts AS (
            INSERT INTO wf_step_attempts (workflow_id, step_name, attempt, attempt_type_id,
              status_id, result, error, duration_ms, started_at, completed_at, worker_id)
            SELECT v.* FROM (VALUES ${sql.join(
              attempts.map(
                (a) => sql`(${a.workflowId}::text, ${a.stepName}::text, ${a.attempt}::int,
                  ${AttemptTypeIds.toId(a.type)}::int,
                  ${StepStatusIds.toId(a.status === "completed" ? "completed" : "failed")}::int,
                  ${jsonbParam(a.result)}, ${a.error ?? null}::text, ${a.durationMs}::bigint,
                  ${timestampParam(a.startedAt)}, ${timestampParam(a.completedAt)},
                  ${a.executorId ?? null}::text)`,
              ),
              sql`, `,
            )}) AS v
            WHERE EXISTS (SELECT 1 FROM wf)
            ON CONFLICT (workflow_id, step_name, run, attempt, attempt_type_id) DO UPDATE SET
              status_id = EXCLUDED.status_id, result = EXCLUDED.result, error = EXCLUDED.error,
              duration_ms = EXCLUDED.duration_ms, started_at = EXCLUDED.started_at,
              completed_at = EXCLUDED.completed_at, worker_id = EXCLUDED.worker_id
          )`;

    const row = await this.fencedStatement({
      workflowId,
      guard: params.guard,
      statement: (ok) => sql`
        wf AS (
          UPDATE wf_workflows SET
            status_id = CASE WHEN status_id IN (${sql.join(
              resumable.map((id) => sql`${id}::int`),
              sql`, `,
            )}) THEN ${ids.running}::int ELSE status_id END,
            started_at = CASE WHEN status_id = ${ids.pending}::int THEN ${now} ELSE started_at END,
            updated_at = ${now}
          WHERE workflow_id = ${workflowId} AND ${ok}
          RETURNING run, status_id, error, error_tag
        ),
        step AS (
          INSERT INTO wf_workflow_steps (workflow_id, step_name, run, status_id, result, error,
            error_tag, metadata, started_at, completed_at, duration_ms, attempt)
          SELECT ${workflowId}, ${stepName}, wf.run, ${stepStatus}::int,
            ${jsonbParam(completed ? outcome.result : undefined)}, ${error}::text,
            ${errorTag}::text, ${jsonbParam(outcome.metadata)},
            ${timestampParam(outcome.startedAt)}, ${now}, ${outcome.durationMs}::bigint, 1
          FROM wf
          ON CONFLICT (workflow_id, step_name, run) DO UPDATE SET
            status_id = EXCLUDED.status_id,
            ${onConflict},
            metadata = COALESCE(EXCLUDED.metadata, wf_workflow_steps.metadata),
            completed_at = EXCLUDED.completed_at,
            duration_ms = EXCLUDED.duration_ms,
            attempt = wf_workflow_steps.attempt + 1
          RETURNING 1
        )${attemptRows}`,
      select: sql`(SELECT status_id FROM wf) AS status_id,
        (SELECT error FROM wf) AS error,
        (SELECT error_tag FROM wf) AS error_tag`,
    });
    if (row.status_id == null) return null;
    return {
      status: WorkflowStatusIds.toName(Number(row.status_id)),
      ...(row.error != null && { error: row.error as string }),
      ...(row.error_tag != null && { errorTag: row.error_tag as string }),
    };
  }

  async batchSaveStepResults({ records, guard }: BatchSaveStepResultsParams): Promise<void> {
    if (records.length === 0) return;
    const token = this.fenceTokenOf(guard);
    const now = this.config.clock.now();

    // Group by workflowId so we issue at most one status move + run read
    // per workflow regardless of how many step records target it, then fold
    // everything into one multi-row INSERT inside a transaction: O(workflows)
    // statements plus one bulk write, where looping `saveStepResult` would
    // cost one statement per record.
    const byWf = new Map<string, Array<(typeof records)[number]>>();
    for (const r of records) {
      const bucket = byWf.get(r.workflowId);
      if (bucket) bucket.push(r);
      else byWf.set(r.workflowId, [r]);
    }

    await this.db.transaction(async (tx) => {
      // The fence of every workflow in the batch is held for the whole
      // transaction, so the batch lands whole or not at all.
      if (token !== undefined) {
        for (const wfId of byWf.keys()) {
          await this.assertFence({ db: tx as unknown as DrizzleDb, workflowId: wfId, token });
        }
      }
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

  async saveStepFailure({ guard, ...params }: SaveStepFailureParams): Promise<void> {
    const { workflowId, stepName, ...outcome } = params;
    await this.writeStep({
      checkpoint: { workflowId, stepName, outcome: { kind: "failed", ...outcome }, attempts: [] },
      guard,
    });
  }

  async saveTaskResult({ guard, ...params }: SaveTaskResultParams): Promise<void> {
    await this.writeTask({
      ...params,
      outcome: { statusId: StepStatusIds.id.completed, result: params.result },
      guard,
    });
  }

  async saveTaskFailure({ guard, ...params }: SaveTaskFailureParams): Promise<void> {
    await this.writeTask({
      ...params,
      outcome: { statusId: StepStatusIds.id.failed, error: params.error },
      guard,
    });
  }

  /**
   * Upsert one map task row (and its parent step row) with `outcome`, in
   * one fenced statement. Nothing is written for a missing workflow.
   */
  private async writeTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    outcome:
      | { statusId: number; result: unknown; error?: undefined }
      | { statusId: number; error: string; result?: undefined };
    guard?: FenceGuard;
  }): Promise<void> {
    const { workflowId, stepName, taskIndex, outcome } = params;
    const now = timestampParam(this.config.clock.now());
    // The outcome's column is set; the other keeps its stored value.
    const set =
      outcome.error !== undefined ? sql`error = EXCLUDED.error` : sql`result = EXCLUDED.result`;
    await this.fencedStatement({
      workflowId,
      guard: params.guard,
      statement: (ok) => sql`
        cur AS (
          SELECT run FROM wf_workflows WHERE workflow_id = ${workflowId} AND ${ok}
        ),
        parent AS (
          INSERT INTO wf_workflow_steps (workflow_id, step_name, run, status_id, step_type_id,
            attempt, started_at)
          SELECT ${workflowId}, ${stepName}, cur.run, ${StepStatusIds.id.running}::int,
            ${StepTypeIds.id.map}::int, 1, ${now}
          FROM cur
          ON CONFLICT DO NOTHING
          RETURNING 1
        ),
        task AS (
          INSERT INTO wf_workflow_step_tasks (workflow_id, step_name, run, task_index, status_id,
            result, error, started_at, completed_at, attempt)
          SELECT ${workflowId}, ${stepName}, cur.run, ${taskIndex}::int, ${outcome.statusId}::int,
            ${jsonbParam(outcome.result)}, ${outcome.error ?? null}::text, ${now}, ${now}, 1
          FROM cur
          ON CONFLICT (workflow_id, step_name, run, task_index) DO UPDATE SET
            status_id = EXCLUDED.status_id,
            ${set},
            completed_at = EXCLUDED.completed_at,
            attempt = wf_workflow_step_tasks.attempt + 1
          RETURNING 1
        )`,
      select: sql`1 AS one`,
    });
  }

  async completeWorkflow({ workflowId, result, guard }: CompleteWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      set: { statusId: WorkflowStatusIds.id.completed, result },
    });
  }

  async failWorkflow({ workflowId, error, errorTag, guard }: FailWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      set: { statusId: WorkflowStatusIds.id.failed, error, errorTag: errorTag ?? null },
    });
  }

  async tripwireWorkflow({ workflowId, reason, guard }: TripwireWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      set: { statusId: WorkflowStatusIds.id.tripwire, tripwire: reason },
    });
  }

  /** Fenced terminal transition; a no-op on a run that already ended. */
  private async finishWorkflow(params: {
    workflowId: string;
    guard?: FenceGuard;
    set: {
      statusId: number;
      result?: unknown;
      error?: string;
      errorTag?: string | null;
      tripwire?: unknown;
    };
  }): Promise<void> {
    const { workflowId, set } = params;
    const now = timestampParam(this.config.clock.now());
    // Only the columns the transition sets change; an `undefined` value
    // leaves its column alone, as in a drizzle `.set()`.
    const columns: SQL[] = [sql`status_id = ${set.statusId}::int`];
    if (set.result !== undefined) columns.push(sql`result = ${jsonbParam(set.result)}`);
    if (set.error !== undefined) columns.push(sql`error = ${set.error}::text`);
    if (set.errorTag !== undefined) columns.push(sql`error_tag = ${set.errorTag}::text`);
    if (set.tripwire !== undefined) columns.push(sql`tripwire = ${jsonbParam(set.tripwire)}`);
    await this.fencedStatement({
      workflowId,
      guard: params.guard,
      statement: (ok) => sql`
        ended AS (
          UPDATE wf_workflows SET ${sql.join(columns, sql`, `)}, completed_at = ${now},
            updated_at = ${now}
          WHERE workflow_id = ${workflowId} AND ${ok}
            AND status_id IN (${sql.join(
              NON_TERMINAL_STATUS_IDS.map((id) => sql`${id}::int`),
              sql`, `,
            )})
          RETURNING 1
        )`,
      select: sql`1 AS one`,
    });
  }

  async suspendWorkflow({
    workflowId,
    stepName,
    stepUpdate,
    guard,
  }: SuspendWorkflowParams): Promise<void> {
    // One fenced statement, so the step row and the workflow's `suspended`
    // status land together. Fields `stepUpdate` leaves out keep their
    // stored value on an existing row (and their default on a new one).
    const now = timestampParam(this.config.clock.now());
    const fields: Array<[string, SQL]> = [
      ["run", sql`cur.run`],
      ["attempt", sql`1`],
      ["started_at", now],
    ];
    if (stepUpdate.status) {
      fields.push(["status_id", sql`${StepStatusIds.toId(stepUpdate.status as StepStatus)}::int`]);
    }
    if (stepUpdate.stepType) {
      fields.push(["step_type_id", sql`${StepTypeIds.toId(stepUpdate.stepType as StepType)}::int`]);
    }
    if (stepUpdate.wakeAt != null)
      fields.push(["wake_at", timestampParam(stepUpdate.wakeAt as Date)]);
    if (stepUpdate.signalName != null) {
      fields.push(["signal_name", sql`${stepUpdate.signalName as string}::text`]);
    }
    if (stepUpdate.signalTimeoutAt != null) {
      fields.push(["signal_timeout_at", timestampParam(stepUpdate.signalTimeoutAt as Date)]);
    }
    // Schema snapshot for `ctx.validatedSignal` / `ctx.approval` suspends.
    // Absent for plain `ctx.signal()` — those keep the pass-through path.
    if (stepUpdate.signalJsonSchema != null) {
      fields.push(["signal_json_schema", jsonbParam(stepUpdate.signalJsonSchema)]);
    }
    const columns = sql.raw(fields.map(([c]) => c).join(", "));
    const updates = sql.raw(fields.map(([c]) => `${c} = EXCLUDED.${c}`).join(", "));

    await this.fencedStatement({
      workflowId,
      guard,
      statement: (ok) => sql`
        cur AS (
          UPDATE wf_workflows SET status_id = ${WorkflowStatusIds.id.suspended}::int,
            updated_at = ${now}
          WHERE workflow_id = ${workflowId} AND ${ok}
          RETURNING run
        ),
        step AS (
          INSERT INTO wf_workflow_steps (workflow_id, step_name, ${columns})
          SELECT ${workflowId}, ${stepName}, ${sql.join(
            fields.map(([, v]) => v),
            sql`, `,
          )}
          FROM cur
          ON CONFLICT (workflow_id, step_name, run) DO UPDATE SET ${updates}
          RETURNING 1
        )`,
      select: sql`1 AS one`,
    });
  }

  async deliverSignal({ workflowId, signalName, payload }: DeliverSignalParams): Promise<void> {
    await this.db
      .insert(workflowSignals)
      .values({ workflowId, signalName, payload })
      .onConflictDoUpdate({
        target: [workflowSignals.workflowId, workflowSignals.signalName],
        set: { payload, deliveredAt: this.config.clock.now() },
      });
  }

  async setWorkflowMetadata({
    workflowId,
    patch,
    guard,
  }: SetWorkflowMetadataParams): Promise<void> {
    // Postgres jsonb merge on the row's metadata column, in one UPDATE so
    // concurrent patches to different keys all land. `||` shallow-merges
    // top-level keys; null-valued entries in the patch are stripped via a
    // second `- text[]` op so callers can use `null` to remove a key. The
    // merge is parenthesised: `-` binds tighter than `||`, so without the
    // parentheses the keys were removed from the patch, not the result.
    const removeKeys = Object.entries(patch)
      .filter(([, v]) => v === null)
      .map(([k]) => k);
    const writePatch: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(patch)) {
      if (v !== null) writePatch[k] = v;
    }
    await this.fenced({
      workflowId,
      guard,
      write: async (db) => {
        await db
          .update(workflows)
          .set({
            metadata: sql`(COALESCE(${workflows.metadata}, '{}'::jsonb) || ${JSON.stringify(writePatch)}::jsonb)${
              removeKeys.length > 0
                ? sql` - ARRAY[${sql.join(
                    removeKeys.map((k) => sql`${k}`),
                    sql`, `,
                  )}]::text[]`
                : sql``
            }`,
            updatedAt: this.config.clock.now(),
          })
          .where(eq(workflows.workflowId, workflowId));
      },
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

  async tryLock({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ acquired: boolean; token?: string }> {
    // Row locks (the default) live in wf_workflow_locks and carry the
    // bigserial fence_token. The deprecated advisory mode has no row and so
    // no token — see `PostgresStorageConfig.useAdvisoryLocks`.
    if (this.config.useAdvisoryLocks) {
      const acquired = await this.tryAdvisoryLock(workflowId);
      return { acquired };
    }
    return this.tryRowLock(workflowId, lockDurationMs);
  }

  async tryLockAndLoad({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    // Sequenced lock then load (not one transaction): collapses two HTTP
    // round-trips when this storage is fronted by the workflow-remote RPC.
    // The load can observe writes committed after the lock was taken —
    // good enough for the "are we joining an in-flight run?" question the
    // coordinator actually asks.
    const { acquired, token } = await this.tryLock({ workflowId, lockDurationMs });
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock({ workflowId, guard }: ReleaseLockParams): Promise<void> {
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

  async heartbeat({ workflowId, lockDurationMs, guard }: HeartbeatParams): Promise<void> {
    if (this.config.useAdvisoryLocks) return;
    // Expiry is computed on the server clock, same as `tryRowLock`, so the
    // lease length doesn't drift with client/server skew.
    if (guard?.fenceToken) {
      // Only a live lock under this token extends: an expired lease is
      // lost even before anyone takes it over.
      const extended = await this.db
        .update(workflowLocks)
        .set({ expiresAt: serverNowPlusMs(lockDurationMs) })
        .where(
          and(
            eq(workflowLocks.workflowId, workflowId),
            eq(workflowLocks.fenceToken, Number(guard.fenceToken)),
            sql`${workflowLocks.expiresAt} > NOW()`,
          ),
        )
        .returning({ fenceToken: workflowLocks.fenceToken });
      if (extended.length > 0) return;
      // Nothing extended: the lock is gone, expired or held under another
      // token — the caller lost the run. Read it back for the error only.
      const [row] = await execRaw(
        this.db,
        sql`SELECT fence_token::text AS token, expires_at > NOW() AS live
            FROM wf_workflow_locks WHERE workflow_id = ${workflowId}`,
      );
      throw fenceMismatch({
        workflowId,
        provided: guard.fenceToken,
        current: row ? String(row.token) : undefined,
        // Our own token still on the row means the update missed it on expiry.
        expired: row !== undefined && (row.live !== true || String(row.token) === guard.fenceToken),
      });
    }
    await this.db
      .update(workflowLocks)
      .set({ expiresAt: serverNowPlusMs(lockDurationMs) })
      .where(
        and(
          eq(workflowLocks.workflowId, workflowId),
          eq(workflowLocks.lockedBy, this.config.instanceId),
        ),
      );
  }

  /** The token a fenced write must match, or undefined when the write is unfenced. */
  private fenceTokenOf(guard?: FenceGuard): string | undefined {
    // Advisory-lock mode issues no tokens, so there is no row to fence on.
    if (this.config.useAdvisoryLocks) return undefined;
    return guard?.fenceToken || undefined;
  }

  /**
   * Inside a transaction: take `FOR SHARE` on the workflow's lock row and
   * reject unless it is live and carries `token`. The share lock blocks a
   * takeover (`tryRowLock`) or release until the transaction ends, and a
   * takeover that committed first is what this read sees, so the check
   * holds for every write the transaction makes after it. Expiry is judged
   * on the server clock, like the lease itself.
   */
  private async assertFence(params: {
    db: DrizzleDb;
    workflowId: string;
    token: string;
  }): Promise<void> {
    const [row] = await execRaw(
      params.db,
      sql`SELECT fence_token::text AS token, expires_at > NOW() AS live
          FROM wf_workflow_locks WHERE workflow_id = ${params.workflowId} FOR SHARE`,
    );
    if (row && String(row.token) === params.token && row.live === true) return;
    throw fenceMismatch({
      workflowId: params.workflowId,
      provided: params.token,
      current: row ? String(row.token) : undefined,
      expired: row !== undefined && row.live !== true,
    });
  }

  /**
   * Run `write` as one fenced write: with a token, in a transaction that
   * first passes `assertFence`; without one, directly on the pool.
   */
  private async fenced<T>(params: {
    workflowId: string;
    guard?: FenceGuard;
    write: (db: DrizzleDb) => Promise<T>;
  }): Promise<T> {
    const token = this.fenceTokenOf(params.guard);
    if (token === undefined) return params.write(this.db);
    return this.db.transaction(async (tx) => {
      const db = tx as unknown as DrizzleDb;
      await this.assertFence({ db, workflowId: params.workflowId, token });
      return params.write(db);
    });
  }

  /**
   * Run one fenced write as a single statement:
   *
   *   WITH fence AS MATERIALIZED (<live lock row under the token, FOR SHARE>),
   *        <statement>
   *   SELECT EXISTS (SELECT 1 FROM fence) AS fenced, <select>
   *
   * Every data-modifying CTE of `statement` must be gated on `ok` (true
   * only when the fence passed), so a rejected token writes nothing. The
   * share lock is held until the statement commits, so a takeover can't
   * land between the check and the write, the same guarantee
   * `assertFence` gives a transaction. Without a token the fence is a
   * constant row and the write is unfenced. A rejected token throws
   * `FenceTokenMismatchError`; otherwise resolves with the final row.
   */
  private async fencedStatement(params: {
    workflowId: string;
    guard?: FenceGuard;
    statement: (ok: SQL) => SQL;
    select: SQL;
  }): Promise<Record<string, any>> {
    const { workflowId } = params;
    const token = this.fenceTokenOf(params.guard);
    const fence =
      token === undefined
        ? sql`SELECT 1`
        : sql`SELECT 1 FROM wf_workflow_locks
            WHERE workflow_id = ${workflowId} AND fence_token = ${Number(token)}::bigint
              AND expires_at > NOW()
            FOR SHARE`;
    const ok = sql`EXISTS (SELECT 1 FROM fence)`;
    const [row] = await execRaw(
      this.db,
      sql`WITH fence AS MATERIALIZED (${fence}), ${params.statement(ok)}
          SELECT ${ok} AS fenced, ${params.select}`,
    );
    if (row?.fenced === true) return row;
    // Rejected: read the lock back for the error only.
    const [lock] = await execRaw(
      this.db,
      sql`SELECT fence_token::text AS token, expires_at > NOW() AS live
          FROM wf_workflow_locks WHERE workflow_id = ${workflowId}`,
    );
    throw fenceMismatch({
      workflowId,
      provided: token!,
      current: lock ? String(lock.token) : undefined,
      expired: lock !== undefined && lock.live !== true,
    });
  }

  async startFreshRun({ workflowId, guard }: StartFreshRunParams): Promise<number> {
    const now = this.config.clock.now();
    const token = this.fenceTokenOf(guard);

    return this.db.transaction(async (tx) => {
      if (token !== undefined) {
        await this.assertFence({ db: tx as unknown as DrizzleDb, workflowId, token });
      }
      // Row lock: concurrent fresh runs serialize instead of archiving the
      // same run twice.
      const [current] = await tx
        .select()
        .from(workflows)
        .where(eq(workflows.workflowId, workflowId))
        .for("update");
      if (!current) throw new Error(`Workflow ${workflowId} not found`);

      await tx
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

      const [row] = await tx
        .update(workflows)
        .set({
          run: sql`${workflows.run} + 1`,
          statusId: WorkflowStatusIds.id.pending,
          result: null,
          error: null,
          errorTag: null,
          tripwire: null,
          startedAt: null,
          completedAt: null,
          updatedAt: now,
        })
        .where(eq(workflows.workflowId, workflowId))
        .returning({ run: workflows.run });

      // The journal and delivered signals aren't keyed by run: drop them so
      // the new run re-executes its activities and waits for fresh signals
      // instead of replaying the previous run's.
      await tx.delete(activityJournal).where(eq(activityJournal.workflowId, workflowId));
      await tx.delete(workflowSignals).where(eq(workflowSignals.workflowId, workflowId));
      return row!.run;
    });
  }

  async resetSteps({ workflowId, stepNames }: ResetStepsParams): Promise<void> {
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
      // The kept steps start a fresh compensation ledger.
      await tx
        .update(workflowSteps)
        .set({ compensationStatus: null, compensationError: null, compensatedAt: null })
        .where(
          and(
            eq(workflowSteps.workflowId, workflowId),
            eq(workflowSteps.run, wf.run),
            sql`${workflowSteps.compensationStatus} IS NOT NULL`,
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
          errorTag: null,
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

  // ---------------------------------------------------------------------------
  // Scanner / recovery queries
  // ---------------------------------------------------------------------------
  //
  // Each query is driven from a partial index (sleeping / waiting steps,
  // or pending / running workflows) and keyset-paginated on workflow_id.
  // jsonb columns come back as `::text` and are parsed here so the result
  // doesn't depend on the driver's json type parsers.

  /**
   * `AND w.namespace = …` when the storage has a configured namespace —
   * the same scoping `listWorkflows` applies.
   */
  private namespaceScope() {
    return this.config.namespace ? sql` AND w.namespace = ${this.config.namespace}` : sql``;
  }

  async listDueTimers(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const now = params.now.toISOString();
    const after =
      params.afterWorkflowId !== undefined
        ? sql` AND s.workflow_id > ${params.afterWorkflowId}`
        : sql``;
    // Both branches hit their own partial index (wake_at / signal_timeout_at),
    // then join the workflow on its current run while suspended. DISTINCT ON
    // keeps the smallest due step name per run.
    const rows = await execRaw(
      this.db,
      sql`
      SELECT DISTINCT ON (d.workflow_id)
        d.workflow_id, w.workflow_name, w.version, w.input::text AS input_json,
        d.step_name, d.reason, d.signal_name
      FROM (
        SELECT s.workflow_id, s.run, s.step_name, 'sleep' AS reason, NULL::text AS signal_name
        FROM wf_workflow_steps s
        WHERE s.status_id = ${StepStatusIds.id.sleeping}
          AND s.wake_at <= ${now}::timestamptz${after}
        UNION ALL
        SELECT s.workflow_id, s.run, s.step_name, 'signal-timeout' AS reason, s.signal_name
        FROM wf_workflow_steps s
        WHERE s.status_id = ${StepStatusIds.id.waiting_for_signal}
          AND s.signal_timeout_at IS NOT NULL
          AND s.signal_timeout_at <= ${now}::timestamptz${after}
      ) d
      JOIN wf_workflows w
        ON w.workflow_id = d.workflow_id
       AND w.run = d.run
       AND w.status_id = ${WorkflowStatusIds.id.suspended}${this.namespaceScope()}
      ORDER BY d.workflow_id, d.step_name
      LIMIT ${Math.max(0, Math.trunc(params.limit))}
    `,
    );
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      workflowName: r.workflow_name,
      ...(r.version != null ? { version: r.version } : {}),
      input: parseJsonText(r.input_json),
      stepName: r.step_name,
      reason: r.reason as "sleep" | "signal-timeout",
      ...(r.signal_name != null ? { signalName: r.signal_name } : {}),
    }));
  }

  async listSignalWakeups(params: {
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const after =
      params.afterWorkflowId !== undefined
        ? sql` AND s.workflow_id > ${params.afterWorkflowId}`
        : sql``;
    // Waiting steps (partial index on (workflow_id, signal_name)) joined to
    // the delivered signal under the same name and to the workflow's
    // current, suspended run.
    const rows = await execRaw(
      this.db,
      sql`
      SELECT DISTINCT ON (s.workflow_id)
        s.workflow_id, w.workflow_name, w.version, w.input::text AS input_json,
        s.step_name, s.signal_name, g.payload::text AS payload_json
      FROM wf_workflow_steps s
      JOIN wf_workflows w
        ON w.workflow_id = s.workflow_id
       AND w.run = s.run
       AND w.status_id = ${WorkflowStatusIds.id.suspended}${this.namespaceScope()}
      JOIN wf_workflow_signals g
        ON g.workflow_id = s.workflow_id
       AND g.signal_name = s.signal_name
      WHERE s.status_id = ${StepStatusIds.id.waiting_for_signal}${after}
      ORDER BY s.workflow_id, s.step_name
      LIMIT ${Math.max(0, Math.trunc(params.limit))}
    `,
    );
    return rows.map((r) => ({
      workflowId: r.workflow_id,
      workflowName: r.workflow_name,
      ...(r.version != null ? { version: r.version } : {}),
      input: parseJsonText(r.input_json),
      stepName: r.step_name,
      reason: "signal" as const,
      signalName: r.signal_name,
      signalPayload: parseJsonText(r.payload_json),
    }));
  }

  /**
   * Lock state comes from `wf_workflow_locks`; in the deprecated advisory
   * lock mode there are no lock rows, so every pending / running run that
   * matches is returned and the caller's `tryLock` turns away owned ones.
   */
  async listOrphanedRuns(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]> {
    const after =
      params.afterWorkflowId !== undefined
        ? sql` AND w.workflow_id > ${params.afterWorkflowId}`
        : sql``;
    const rows = await execRaw(
      this.db,
      sql`
      SELECT w.workflow_id, w.workflow_name, w.version, w.status_id,
        w.input::text AS input_json, w.metadata::text AS metadata_json
      FROM wf_workflows w
      WHERE w.status_id IN (${WorkflowStatusIds.id.pending}, ${WorkflowStatusIds.id.running}, ${WorkflowStatusIds.id.compensating})
        AND w.updated_at < ${params.updatedBefore.toISOString()}::timestamptz${after}${this.namespaceScope()}
        AND NOT EXISTS (
          SELECT 1 FROM wf_workflow_locks l
          WHERE l.workflow_id = w.workflow_id
            AND l.expires_at > ${params.now.toISOString()}::timestamptz
        )
      ORDER BY w.workflow_id
      LIMIT ${Math.max(0, Math.trunc(params.limit))}
    `,
    );
    return rows.map((r) => {
      const metadata = parseJsonText(r.metadata_json) as Record<string, unknown> | null;
      return {
        workflowId: r.workflow_id,
        workflowName: r.workflow_name,
        ...(r.version != null ? { version: r.version } : {}),
        status: WorkflowStatusIds.toName(Number(r.status_id)) as OrphanedRun["status"],
        input: parseJsonText(r.input_json),
        ...(metadata != null ? { metadata } : {}),
      };
    });
  }

  async loadRunHistory({
    workflowId,
    ...params
  }: LoadRunHistoryParams): Promise<WorkflowRunSummary[]> {
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

    // One transaction: a crash mid-purge never leaves a workflow row with
    // half its dependents gone. Child tables first (FK order), then the
    // workflow itself. Tables with an ON DELETE CASCADE FK would follow the
    // final delete anyway; deleting them explicitly keeps rows orphaned
    // before those FKs existed from surviving.
    await this.db.transaction(async (tx) => {
      await tx.delete(workflowSignals).where(inArray(workflowSignals.workflowId, ids));
      await tx.delete(workflowStepTasks).where(inArray(workflowStepTasks.workflowId, ids));
      await tx.delete(workflowSteps).where(inArray(workflowSteps.workflowId, ids));
      await tx.delete(stepAttempts).where(inArray(stepAttempts.workflowId, ids));
      await tx.delete(workflowRuns).where(inArray(workflowRuns.workflowId, ids));
      await tx.delete(activityJournal).where(inArray(activityJournal.workflowId, ids));
      await tx.delete(signalTokens).where(inArray(signalTokens.workflowId, ids));
      await tx.delete(workflowStreams).where(inArray(workflowStreams.workflowId, ids));
      await tx
        .delete(stepQueue)
        .where(
          and(
            inArray(stepQueue.workflowId, ids),
            sql`${stepQueue.status} IN ('completed', 'failed')`,
          ),
        );
      await tx.delete(workflowLocks).where(inArray(workflowLocks.workflowId, ids));
      await tx.delete(workflows).where(inArray(workflows.workflowId, ids));
    });

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
    // Timestamps come from the server clock (`NOW()`), never bound from the
    // client: every holder judges lease expiry against the same clock, and
    // there's no JS Date for the driver to choke on. On fresh insert the
    // bigserial column populates from its sequence; on expired-lock
    // takeover we force a new value via `nextval(...)` so the token strictly
    // increases across holders. The `WHERE expires_at < NOW()` clause on the
    // UPDATE path only lets a takeover through once the previous lease is
    // dead; the INSERT ... ON CONFLICT row lock makes concurrent attempts
    // from any connection or process resolve to exactly one winner.
    const expiresAt = serverNowPlusMs(lockDurationMs);
    const [result] = await execRaw(
      this.db,
      sql`
      INSERT INTO wf_workflow_locks (workflow_id, locked_at, expires_at, locked_by)
      VALUES (${workflowId}, NOW(), ${expiresAt}, ${this.config.instanceId})
      ON CONFLICT (workflow_id) DO UPDATE
        SET locked_at = NOW(),
            expires_at = ${expiresAt},
            locked_by = ${this.config.instanceId},
            fence_token = nextval(pg_get_serial_sequence('wf_workflow_locks', 'fence_token'))
        WHERE wf_workflow_locks.expires_at < NOW()
      RETURNING fence_token
    `,
    );
    if (!result) return { acquired: false };
    const token = String((result as { fence_token: number | string }).fence_token);
    return { acquired: true, token };
  }

  // ---------------------------------------------------------------------------
  // StepAttemptStore — attempt history (opt-in via recordAttempts config)
  // ---------------------------------------------------------------------------

  async saveStepAttempt({ record, guard }: SaveStepAttemptParams): Promise<void> {
    if (!this.config.recordAttempts) return;
    await this.fenced({
      workflowId: record.workflowId,
      guard,
      write: async (db) => {
        await db.insert(stepAttempts).values({
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
      },
    });
  }

  async loadStepAttempts({
    workflowId,
    stepName,
  }: LoadStepAttemptsParams): Promise<StepAttemptRecord[]> {
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
  // CompensationLedgerStore
  // ---------------------------------------------------------------------------

  async beginCompensation({ guard, ...params }: BeginCompensationParams): Promise<boolean> {
    const { workflowId } = params;
    return this.fenced({
      workflowId,
      guard,
      write: async (db) => {
        const moved = await db
          .update(workflows)
          .set({
            statusId: WorkflowStatusIds.id.compensating,
            error: params.error,
            errorTag: params.errorTag ?? null,
            updatedAt: this.config.clock.now(),
          })
          .where(
            and(
              eq(workflows.workflowId, workflowId),
              inArray(workflows.statusId, CANCELLABLE_STATUS_IDS),
            ),
          )
          .returning({ workflowId: workflows.workflowId });
        if (moved.length > 0) return true;
        const [row] = await db
          .select({ statusId: workflows.statusId })
          .from(workflows)
          .where(eq(workflows.workflowId, workflowId));
        return row?.statusId === WorkflowStatusIds.id.compensating;
      },
    });
  }

  async saveStepCompensation({ guard, ...params }: SaveStepCompensationParams): Promise<void> {
    const { workflowId } = params;
    const now = timestampParam(this.config.clock.now());
    await this.fencedStatement({
      workflowId,
      guard,
      statement: (ok) => sql`
        wf AS (
          UPDATE wf_workflows SET updated_at = ${now}
          WHERE workflow_id = ${workflowId} AND ${ok}
          RETURNING run
        ),
        ledger AS (
          UPDATE wf_workflow_steps s SET compensation_status = ${params.status}::text,
            compensation_error = ${params.error ?? null}::text, compensated_at = ${now}
          FROM wf
          WHERE s.workflow_id = ${workflowId} AND s.step_name = ${params.stepName}
            AND s.run = wf.run
          RETURNING 1
        )`,
      select: sql`1 AS one`,
    });
  }

  // ---------------------------------------------------------------------------
  // JournalStore — .journaled() step support
  // ---------------------------------------------------------------------------

  async loadJournal({ workflowId, stepName }: LoadJournalParams): Promise<JournalEntry[]> {
    const rows = await this.db
      .select()
      .from(activityJournal)
      .where(
        and(eq(activityJournal.workflowId, workflowId), eq(activityJournal.stepName, stepName)),
      )
      .orderBy(activityJournal.activityIndex, activityJournal.branchPath);
    return rows.map(rowToJournalEntry);
  }

  async appendEntry({ guard, ...params }: AppendEntryParams): Promise<void> {
    // Idempotent append — PK conflict on
    // (workflow_id, step_name, activity_index, branch_path) is silently
    // dropped. Storage-level dedup: the engine may re-call append during a
    // retry that crashes after a successful INSERT but before the caller
    // observes completion.
    await this.insertJournalEntry({
      ...params,
      stepType: "activity",
      phase: "completed",
      exit: params.exit,
      guard,
    });
  }

  // ---------------------------------------------------------------------------
  // JournalStore — pending entries (ctx.sleep / ctx.signal)
  // ---------------------------------------------------------------------------

  async appendPendingEntry({ guard, ...params }: AppendPendingEntryParams): Promise<void> {
    await this.insertJournalEntry({ ...params, phase: "pending", exit: null, guard });
  }

  /** Insert one journal row unless its slot is taken, in one fenced statement. */
  private async insertJournalEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    activityName: string;
    payloadHash?: string;
    stepType: JournalStepType;
    phase: "pending" | "completed";
    wakeAt?: Date;
    exit: JournalExit | null;
    guard?: FenceGuard;
  }): Promise<void> {
    await this.fencedStatement({
      workflowId: params.workflowId,
      guard: params.guard,
      statement: (ok) => sql`
        entry AS (
          INSERT INTO wf_activity_journal (workflow_id, step_name, activity_index, branch_path,
            activity_name, step_type, phase, payload_hash, wake_at, exit)
          SELECT ${params.workflowId}, ${params.stepName}, ${params.activityIndex}::int,
            ${params.branchPath ?? ""}::text, ${params.activityName}::text,
            ${params.stepType}::text, ${params.phase}::text, ${params.payloadHash ?? null}::text,
            ${timestampParam(params.wakeAt)}, ${jsonbParam(params.exit)}
          WHERE ${ok}
          ON CONFLICT (workflow_id, step_name, activity_index, branch_path) DO NOTHING
          RETURNING 1
        )`,
      select: sql`1 AS one`,
    });
  }

  async completePendingEntry({
    guard,
    ...params
  }: CompletePendingEntryParams): Promise<CompletePendingResult> {
    const slot = and(
      eq(activityJournal.workflowId, params.workflowId),
      eq(activityJournal.stepName, params.stepName),
      eq(activityJournal.activityIndex, params.activityIndex),
      eq(activityJournal.branchPath, params.branchPath ?? ""),
    );
    // First writer wins: the WHERE clause restricts to still-pending rows,
    // and a concurrent completer's UPDATE re-checks it after the winner
    // commits, so exactly one call completes the row.
    const row = await this.fencedStatement({
      workflowId: params.workflowId,
      guard,
      statement: (ok) => sql`
        won AS (
          UPDATE wf_activity_journal SET phase = 'completed', exit = ${jsonbParam(params.exit)}
          WHERE ${slot} AND phase = 'pending' AND ${ok}
          RETURNING 1
        )`,
      select: sql`EXISTS (SELECT 1 FROM won) AS won`,
    });
    if (row.won === true) return { completed: true, exit: params.exit };
    // Lost (or no such row). A completed row never changes again, so a
    // fresh read (a new statement, a new snapshot) sees the winner's exit.
    const [stored] = await this.db
      .select({ exit: activityJournal.exit })
      .from(activityJournal)
      .where(slot)
      .limit(1);
    return { completed: false, exit: (stored?.exit ?? undefined) as JournalExit | undefined };
  }

  async discardJournalEntries({ guard, ...params }: DiscardJournalEntriesParams): Promise<void> {
    await this.fenced({
      workflowId: params.workflowId,
      guard,
      write: async (db) => {
        if (params.slots.length === 0) return;
        await db
          .delete(activityJournal)
          .where(
            and(
              eq(activityJournal.workflowId, params.workflowId),
              eq(activityJournal.stepName, params.stepName),
              or(
                ...params.slots.map((s) =>
                  and(
                    eq(activityJournal.activityIndex, s.activityIndex),
                    eq(activityJournal.branchPath, s.branchPath),
                  ),
                ),
              ),
            ),
          );
      },
    });
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
    // ON CONFLICT DO NOTHING: a concurrent create with the same idempotency
    // key loses the partial-unique-index race quietly and reads the winner.
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
      .onConflictDoNothing()
      .returning();
    if (inserted) return { record: rowToSignalToken(inserted), isCached: false };
    if (params.idempotencyKey) {
      const [winner] = await this.db
        .select()
        .from(signalTokens)
        .where(
          and(
            eq(signalTokens.workflowId, params.workflowId),
            eq(signalTokens.idempotencyKey, params.idempotencyKey),
          ),
        )
        .limit(1);
      if (winner) return { record: rowToSignalToken(winner), isCached: true };
    }
    throw new Error(`createSignalToken: token id ${params.tokenId} already exists`);
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

  async appendStreamChunk({
    guard,
    ...params
  }: AppendStreamChunkParams): Promise<{ chunkIndex: number }> {
    // `MAX + 1` alone isn't atomic under READ COMMITTED: two appenders read
    // the same MAX and collide on the PK. A transaction-scoped advisory lock
    // on (workflow, stream) serializes appenders of one stream (other
    // streams proceed in parallel) and releases itself at commit, so it is
    // safe on a pooled connection.
    const token = this.fenceTokenOf(guard);
    const rows = await this.db.transaction(async (tx) => {
      if (token !== undefined) {
        await this.assertFence({
          db: tx as unknown as DrizzleDb,
          workflowId: params.workflowId,
          token,
        });
      }
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(${hashToInt32(`wf_streams:${params.workflowId}:${params.streamId}`)})`,
      );
      return execRaw(
        tx as unknown as typeof this.db,
        sql`
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
      `,
      );
    });
    const chunkIndex = rows[0]?.chunk_index as number | undefined;
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
