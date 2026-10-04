// ---------------------------------------------------------------------------
// RedisWorkflowStorage — Redis-backed WorkflowStorage, StepAttemptStore,
// StepCheckpointStore, CompensationLedgerStore and JournalStore.
// ---------------------------------------------------------------------------
//
// Runs on a standalone Redis and on Redis Cluster. Every key of a workflow
// shares the hash tag `{wf:<id>}`, and the cross-workflow indexes share
// `{idx}` (see `redis-workflow-keys.ts`), so each script touches one slot:
//
// - A workflow's own writes (step rows, status, journal, fence check) are
//   one atomic script on its slot.
// - Index maintenance is a second script on the `{idx}` slot, versioned by
//   the workflow hash's `iv` field so it converges whatever the order of
//   concurrent writers. The workflow hash is authoritative; after a crash
//   between the two scripts the index lags until the next write to that
//   workflow, or until a reader (scanners, listing, purge) notices and
//   repairs it.
// - The sleep schedule is a cross-workflow sorted set. Adding to it follows
//   the journal write; removing follows the completion. A member left
//   behind is dropped by `findDueSleeps`, which checks each due member
//   against its journal entry.
// - A fenced child create commits on the parent's slot. The child's row is
//   first written provisional (unindexed, invisible to readers, with a TTL);
//   then one fenced script on the parent's slot records the child in the
//   parent's child intents; then the row is confirmed and indexed. A
//   provisional row counts only once the parent's intent names it, so a
//   parent that lost its lock before the intent never creates the child,
//   and a reader that finds a provisional row with a matching intent
//   confirms it (a crash after the intent loses nothing).
//
// Keys written before this layout need `migrateLegacyKeys()` once, on the
// standalone instance, with workers stopped.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  StepAttemptStore,
  StepCheckpointStore,
  CompensationLedgerStore,
  JournalStore,
  JournalEntry,
  JournalExit,
  CompletePendingResult,
  FenceGuard,
  WorkflowOrderBy,
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
import type {
  WorkflowState,
  WorkflowStatusSnapshot,
  WorkflowStatus,
  WorkflowRunSummary,
  WorkflowSummary,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  RunSource,
} from "@promin/workflow";
import {
  FenceTokenMismatchError,
  WORKFLOW_STATUSES,
  isTerminalWorkflowStatus,
} from "@promin/workflow";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  workflowMetadataMatches,
  encodeRunSource,
  decodeRunSource,
  withoutCompensationLedger,
} from "@promin/workflow/storage-kit";
import { applyMetadataPatch, sortWorkflowRows } from "@promin/workflow/storage-kit";
import type { RedisStoreClient } from "./redis-client.ts";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { RedisWorkflowKeys, escapeGlob } from "./redis-workflow-keys.ts";
import {
  APPEND_ENTRY_LUA,
  APPEND_PENDING_LUA,
  CHECKPOINT_STEP_LUA,
  COMPLETE_PENDING_LUA,
  CREATE_SIGNAL_TOKEN_LUA,
  DISCARD_ENTRIES_LUA,
  FENCE_REJECTED,
  HASH_FIELD_CAS_LUA,
  HEARTBEAT_LUA,
  HMGET_LUA,
  INDEX_FIELDS,
  INDEX_PAGE_LUA,
  INTER_CARD_LUA,
  LOAD_RUN_LUA,
  LRANGE_ALL_LUA,
  PTTL_LUA,
  PURGE_WORKFLOW_LUA,
  READ_FOR_WRITE_LUA,
  RELEASE_LOCK_LUA,
  RESET_STEPS_LUA,
  START_FRESH_RUN_LUA,
  SYNC_INDEX_LUA,
  TRANSITION_STATUS_LUA,
  TRY_LOCK_AND_LOAD_LUA,
  TRY_LOCK_LUA,
  UNINDEX_LUA,
  WRITE_OPS_LUA,
  fencedLua,
} from "./redis-workflow-scripts.ts";
import { migrateLegacyWorkflowKeys } from "./redis-workflow-migration.ts";

export interface RedisWorkflowStorageConfig {
  redis: RedisStoreClient;
  prefix?: string;
  namespace?: string | null;
  instanceId?: string;
  retention?: {
    completedTtlMs?: number;
    maxRunsPerWorkflow?: number;
  };
  /**
   * Time source for client-side timestamps (everything the client
   * serializes into the Redis payload before `HSET`: createdAt, startedAt,
   * completedAt, deliveredAt, purge cutoffs, fresh-run archive stamps).
   * Default: `SystemWallClock`. Pass a `FakeWallClock` for deterministic tests.
   */
  clock?: WallClock;
}

// Fenced variants of every script a lock holder writes through.
const FENCED_CHECK_LUA = fencedLua("return 1");
const FENCED_CHECKPOINT_STEP_LUA = fencedLua(CHECKPOINT_STEP_LUA);
const FENCED_TRANSITION_STATUS_LUA = fencedLua(TRANSITION_STATUS_LUA);
const FENCED_HASH_FIELD_CAS_LUA = fencedLua(HASH_FIELD_CAS_LUA);
const FENCED_START_FRESH_RUN_LUA = fencedLua(START_FRESH_RUN_LUA);
const FENCED_APPEND_ENTRY_LUA = fencedLua(APPEND_ENTRY_LUA);
const FENCED_APPEND_PENDING_LUA = fencedLua(APPEND_PENDING_LUA);
const FENCED_COMPLETE_PENDING_LUA = fencedLua(COMPLETE_PENDING_LUA);
const FENCED_DISCARD_ENTRIES_LUA = fencedLua(DISCARD_ENTRIES_LUA);
const FENCED_WRITE_OPS_LUA = fencedLua(WRITE_OPS_LUA);

/** Statuses a terminal transition may leave. */
const NON_TERMINAL_STATUSES = WORKFLOW_STATUSES.filter((st) => !isTerminalWorkflowStatus(st));
/** Statuses `cancelWorkflow` may leave. */
const CANCELLABLE_STATUSES: readonly WorkflowStatus[] = ["pending", "running", "suspended"];
const TERMINAL_STATUSES_CSV = WORKFLOW_STATUSES.filter(isTerminalWorkflowStatus).join(",");
/** Bound on compare-and-set retries under contention. */
const MAX_CAS_ATTEMPTS = 100;
/** Bound on re-reads while a provisional child row is being replaced under a reader. */
const MAX_SETTLE_ATTEMPTS = 3;
/**
 * Lifetime of a provisional child row: one whose parent's intent has not
 * been confirmed yet. A row whose creator died before the parent's intent
 * expires with it; the parent's next lock holder re-creates the child.
 */
const PROVISIONAL_CHILD_TTL_MS = 60 * 60 * 1_000;
/** Ids per HMGET when reading index records. */
const RECORD_CHUNK = 2_000;
/** Workflows purged concurrently. */
const PURGE_CONCURRENCY = 16;
/** SCAN calls (COUNT 1000 each) the purge spends looking for one workflow's untracked streams. */
const UNTRACKED_STREAM_SCAN_CALLS = 1_000;
/** "No limit" for the index page script; small enough to print as an integer in Lua. */
const UNBOUNDED = 2_147_483_647;

/** Hash fields a `listWorkflowSummaries` row is read from. */
const SUMMARY_FIELDS = [
  "id",
  "workflowName",
  "workflowType",
  "namespace",
  "status",
  "version",
  "run",
  "runSource",
  "runSourceId",
  "metadata",
  "createdAt",
  "startedAt",
  "updatedAt",
  "completedAt",
] as const;

/** Index fields of a workflow hash, as `WRITE_OPS_LUA` & co. return them. */
type IndexSnapshot = ReadonlyArray<string | null>;

/** A workflow's filter and sort fields as the index stores them. */
interface IndexRecord {
  /** workflowName */
  n: string;
  t?: string;
  ns?: string;
  v?: string;
  p?: string;
  rs?: string;
  rsi?: string;
  s: WorkflowStatus;
  /** createdAt / startedAt / completedAt, epoch ms */
  c: number;
  st?: number;
  co?: number;
}

interface ListFilters {
  status?: WorkflowStatus;
  name?: string;
  version?: string;
  type?: string;
  parentId?: string;
  namespace?: string;
  runSource?: RunSource;
  runSourceId?: string;
  metadata?: Record<string, unknown>;
}

interface ListParams extends ListFilters {
  limit?: number;
  offset?: number;
  orderBy?: WorkflowOrderBy;
  orderDir?: "asc" | "desc";
}

/**
 * The error a rejected fenced write throws. `current` is the lock's token
 * at rejection time; null when the lock key is gone — released, or expired
 * through its TTL.
 */
function fenceMismatch(params: {
  workflowId: string;
  provided: string;
  current: string | null;
}): FenceTokenMismatchError {
  const { workflowId, provided, current } = params;
  return new FenceTokenMismatchError({
    workflowId,
    expected: current ?? "(no lock)",
    provided,
    message:
      current === null
        ? `Fenced write for "${workflowId}" rejected — no active lock (released or expired)`
        : `Fenced write for "${workflowId}" rejected — token mismatch (expected "${current}", got "${provided}")`,
  });
}

export class RedisWorkflowStorage
  implements
    WorkflowStorage,
    StepAttemptStore,
    StepCheckpointStore,
    CompensationLedgerStore,
    JournalStore
{
  private readonly redis: RedisStoreClient;
  private readonly keys: RedisWorkflowKeys;
  private readonly namespace: string | null;
  private readonly instanceId: string;
  private readonly completedTtlMs?: number;
  private readonly maxRunsPerWorkflow: number;
  private readonly clock: WallClock;

  constructor(config: RedisWorkflowStorageConfig) {
    this.redis = config.redis;
    this.keys = new RedisWorkflowKeys(config.prefix ?? "wf");
    this.namespace = config.namespace ?? null;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.completedTtlMs = config.retention?.completedTtlMs;
    this.maxRunsPerWorkflow = config.retention?.maxRunsPerWorkflow ?? 5;
    this.clock = config.clock ?? SystemWallClock;
  }

  /**
   * Move keys written by earlier versions of this storage (untagged,
   * standalone-only layout) to the hash-tagged layout, and rebuild the
   * cross-workflow indexes from the moved workflow hashes. Streams appended
   * before stream ids were tracked are found by the same keyspace SCAN and
   * registered, so `purgeCompleted` removes them.
   *
   * Run it once per prefix, against the standalone instance (it renames
   * keys across slots), with every worker stopped. Re-running it is safe:
   * keys already in the new layout are left alone.
   */
  async migrateLegacyKeys(params?: {
    /** SCAN COUNT hint. Default 1000. */
    scanCount?: number;
  }): Promise<{ workflows: number; keys: number }> {
    return migrateLegacyWorkflowKeys({
      redis: this.redis,
      keys: this.keys,
      scanCount: params?.scanCount ?? 1_000,
      reindex: (workflowId) => this.repairIndex(workflowId),
    });
  }

  // -- Serialization helpers ------------------------------------------------

  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
  }

  private serializeDate(d: Date): string {
    return d.toISOString();
  }

  private parseDate(s: string): Date {
    return new Date(s);
  }

  /** Build a WorkflowState from a `LOAD_RUN_LUA` reply. Null for a missing workflow. */
  private assembleWorkflow(reply: unknown): WorkflowState | null {
    const parts = (reply ?? []) as unknown[];
    if (parts.length === 0) return null;
    const raw = flatToRecord(parts[0] as string[]);
    if (!raw.id) return null;
    const stepsRaw = flatToRecord(parts[1] as string[]);
    const tasksByStep = new Map<string, Record<string, string>>();
    for (let i = 2; i + 1 < parts.length; i += 2) {
      tasksByStep.set(parts[i] as string, flatToRecord(parts[i + 1] as string[]));
    }

    const steps: Record<string, StepState> = {};
    for (const [stepName, json] of Object.entries(stepsRaw)) {
      const step = this.parseStepState(json);
      const tasksRaw = step.stepType === "map" ? tasksByStep.get(stepName) : undefined;
      if (tasksRaw && Object.keys(tasksRaw).length > 0) {
        const tasks = Object.values(tasksRaw).map((t) => this.parseTaskState(t));
        tasks.sort((a, b) => a.taskIndex - b.taskIndex);
        steps[stepName] = { ...step, tasks };
      } else {
        steps[stepName] = step;
      }
    }

    return {
      workflowId: raw.id,
      workflowName: raw.workflowName,
      workflowType: raw.workflowType || undefined,
      parentWorkflowId: raw.parentWorkflowId || undefined,
      namespace: raw.namespace || undefined,
      status: raw.status as WorkflowStatus,
      version: raw.version || undefined,
      run: Number(raw.run),
      input: JSON.parse(raw.input),
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      errorTag: raw.errorTag || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      runSource: raw.runSource ? decodeRunSource(Number(raw.runSource)) : undefined,
      runSourceId: raw.runSourceId || undefined,
      metadata: raw.metadata ? JSON.parse(raw.metadata) : undefined,
      steps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      updatedAt: this.parseDate(raw.updatedAt),
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };
  }

  private parseStepState(json: string): StepState {
    const s = JSON.parse(json);
    return {
      ...s,
      startedAt: s.startedAt ? new Date(s.startedAt) : undefined,
      completedAt: s.completedAt ? new Date(s.completedAt) : undefined,
      wakeAt: s.wakeAt ? new Date(s.wakeAt) : undefined,
      signalTimeoutAt: s.signalTimeoutAt ? new Date(s.signalTimeoutAt) : undefined,
      compensatedAt: s.compensatedAt ? new Date(s.compensatedAt) : undefined,
    };
  }

  private parseTaskState(json: string): StepTaskState {
    const t = JSON.parse(json);
    return {
      ...t,
      startedAt: t.startedAt ? new Date(t.startedAt) : undefined,
      completedAt: t.completedAt ? new Date(t.completedAt) : undefined,
    };
  }

  private serializeStepState(step: StepState): string {
    return JSON.stringify({
      ...step,
      // Strip tasks — stored separately
      tasks: undefined,
      startedAt: step.startedAt ? this.serializeDate(step.startedAt) : undefined,
      completedAt: step.completedAt ? this.serializeDate(step.completedAt) : undefined,
      wakeAt: step.wakeAt ? this.serializeDate(step.wakeAt) : undefined,
      signalTimeoutAt: step.signalTimeoutAt ? this.serializeDate(step.signalTimeoutAt) : undefined,
      compensatedAt: step.compensatedAt ? this.serializeDate(step.compensatedAt) : undefined,
    });
  }

  // -- Workflow CRUD --------------------------------------------------------

  async createWorkflow({
    guard,
    ...params
  }: CreateWorkflowParams): Promise<
    { created: true } | { created: false; existing: WorkflowState }
  > {
    if (guard?.fenceToken && params.parentWorkflowId === undefined) {
      throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
    }
    const ns = this.resolveNamespace(params.namespace);
    // Idempotency-key path: check the index first. SET-NX below claims it
    // atomically — concurrent creates serialize, the loser falls through
    // to attach to the winning row.
    if (params.idempotencyKey) {
      const idxKey = this.keys.workflowIdempotency(ns, params.workflowName, params.idempotencyKey);
      const cachedId = await this.redis.get(idxKey);
      if (cachedId) {
        const expiresAt = await this.redis.hget(this.keys.wf(cachedId), "idempotencyExpiresAt");
        if (expiresAt && new Date(expiresAt).getTime() > this.clock.now().getTime()) {
          const existing = await this.loadWorkflow(cachedId);
          if (existing) return { created: false, existing };
        }
      }
    }

    const now = this.serializeDate(this.clock.now());
    const fields: Record<string, string> = {
      id: params.workflowId,
      workflowName: params.workflowName,
      status: "pending",
      run: "1",
      input: JSON.stringify(params.input),
      createdAt: now,
      updatedAt: now,
      iv: "1",
      // Every stream of this row is registered in its stream-ids set.
      streamsTracked: "1",
    };
    if (params.workflowType) fields.workflowType = params.workflowType;
    if (params.parentWorkflowId) fields.parentWorkflowId = params.parentWorkflowId;
    if (ns) fields.namespace = ns;
    if (params.metadata) fields.metadata = JSON.stringify(params.metadata);
    if (params.version) fields.version = params.version;
    if (params.runSource !== undefined) {
      fields.runSource = String(encodeRunSource(params.runSource));
    }
    if (params.runSourceId) fields.runSourceId = params.runSourceId;
    if (params.idempotencyKey) fields.idempotencyKey = params.idempotencyKey;
    if (params.idempotencyExpiresAt) {
      fields.idempotencyExpiresAt = this.serializeDate(params.idempotencyExpiresAt);
    }

    // A fenced child create commits through the parent's child intents (see
    // the file header): the row goes in provisional, under a nonce of this
    // attempt, and counts once the parent's intent names that nonce.
    const parentId = guard?.fenceToken ? params.parentWorkflowId : undefined;
    const wfKey = this.keys.wf(params.workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const nonce = parentId === undefined ? undefined : crypto.randomUUID();
      const rowOps = (precondition: ReadonlyArray<readonly string[]>) => [
        ...precondition,
        ["HSET", wfKey, ...Object.entries(fields).flat(), ...(nonce ? ["provisional", nonce] : [])],
        ...(nonce ? [["PEXPIRE", wfKey, String(PROVISIONAL_CHILD_TTL_MS)]] : []),
        ["INDEXED", wfKey],
      ];

      // The row lands in one script that first checks it is still absent (a
      // concurrent create wins cleanly). It is indexed once the idempotency
      // claim below is settled.
      let { applied, snapshot } = await this.writeOps({
        workflowId: params.workflowId,
        deferIndex: true,
        ops: rowOps([["ABSENT", wfKey]]),
      });
      if (!applied) {
        const existing = await this.loadWorkflow(params.workflowId);
        if (existing) {
          // A stale parent learns it lost the run here too, as on any other
          // fenced call.
          if (parentId !== undefined) {
            await this.evalFenced({
              script: FENCED_CHECK_LUA,
              workflowId: parentId,
              guard,
              keys: [],
              args: [],
            });
          }
          // A create that crashed before indexing its row is healed by the retry.
          await this.repairIndex(params.workflowId);
          return { created: false, existing };
        }
        // A provisional row no parent intent names never counted: replace it.
        const stale = await this.redis.hget(wfKey, "provisional");
        if (stale === null) continue; // gone in between (purged, expired)
        ({ applied, snapshot } = await this.writeOps({
          workflowId: params.workflowId,
          deferIndex: true,
          ops: rowOps([
            ["HEQ", wfKey, "provisional", stale],
            ["DEL", wfKey],
          ]),
        }));
        if (!applied) continue;
      }

      if (parentId !== undefined && nonce !== undefined) {
        // The commit point: the parent's fence and its intent, in one script
        // on the parent's slot. A rejected fence leaves the row uncommitted.
        try {
          await this.writeOps({
            workflowId: parentId,
            guard,
            ops: [["HSET", this.keys.childIntents(parentId), params.workflowId, nonce]],
          });
        } catch (err) {
          if (err instanceof FenceTokenMismatchError) {
            await this.writeOps({
              workflowId: params.workflowId,
              ops: [
                ["HEQ", wfKey, "provisional", nonce],
                ["DEL", wfKey],
              ],
            });
          }
          throw err;
        }
      }

      // Atomic claim of the idempotency index. SET NX with PX expires the
      // index entry exactly at the run's idempotency_expires_at — concurrent
      // creates that race here lose the SET NX and back out below.
      if (params.idempotencyKey && params.idempotencyExpiresAt) {
        const idxKey = this.keys.workflowIdempotency(
          ns,
          params.workflowName,
          params.idempotencyKey,
        );
        const ttlMs = params.idempotencyExpiresAt.getTime() - this.clock.now().getTime();
        if (ttlMs > 0) {
          const won = await this.redis.set(idxKey, params.workflowId, "NX", "PX", ttlMs);
          if (!won) {
            // Lost the race — undo the (not yet indexed) row and resolve to the winner.
            await this.redis.del(wfKey);
            const winnerId = await this.redis.get(idxKey);
            if (winnerId) {
              const existing = await this.loadWorkflow(winnerId);
              if (existing) return { created: false, existing };
            }
            return this.createWorkflow({ ...params, guard });
          }
        }
      }

      if (nonce !== undefined) {
        const confirmed = await this.confirmProvisional({
          workflowId: params.workflowId,
          nonce,
          deferIndex: true,
        });
        if (!confirmed.applied) {
          // A reader confirmed it first, or the row expired before this
          // confirm: then the create starts over.
          if (await this.redis.exists(wfKey)) return { created: true };
          continue;
        }
        snapshot = confirmed.snapshot;
      }

      await this.syncIndex(snapshot);
      return { created: true };
    }
    throw new Error(`createWorkflow: gave up on "${params.workflowId}" after contention`);
  }

  /**
   * Make a provisional child row a plain one: drop the marker and its TTL,
   * provided it still carries `nonce`, and index it.
   */
  private async confirmProvisional(params: {
    workflowId: string;
    nonce: string;
    deferIndex?: boolean;
  }): Promise<{ applied: boolean; snapshot: IndexSnapshot | null }> {
    const wfKey = this.keys.wf(params.workflowId);
    const { applied, snapshot } = await this.writeOps({
      workflowId: params.workflowId,
      deferIndex: params.deferIndex,
      ops: [
        ["HEQ", wfKey, "provisional", params.nonce],
        ["HDEL", wfKey, "provisional"],
        ["PERSIST", wfKey],
        ["INDEXED", wfKey],
      ],
    });
    return { applied, snapshot };
  }

  /**
   * Settle a provisional row a reader found: confirm it when its parent's
   * intent names its nonce. True when the row counts now (confirmed here
   * or by someone else), false when no intent commits it.
   */
  private async settleProvisional(params: {
    workflowId: string;
    parentId: string;
    nonce: string;
  }): Promise<boolean> {
    const intent = await this.redis.hget(
      this.keys.childIntents(params.parentId),
      params.workflowId,
    );
    if (intent !== params.nonce) return false;
    const { applied } = await this.confirmProvisional(params);
    if (applied) return true;
    const [id, marker] = await this.hmget(this.keys.wf(params.workflowId), ["id", "provisional"]);
    return Boolean(id) && !marker;
  }

  async findWorkflowByIdempotencyKey(params: {
    workflowName: string;
    namespace?: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
    const ns = this.resolveNamespace(params.namespace);
    const idxKey = this.keys.workflowIdempotency(ns, params.workflowName, params.idempotencyKey);
    const cachedId = await this.redis.get(idxKey);
    if (!cachedId) return null;
    // Defense-in-depth: confirm the workflow's stored expiry is unexpired
    // before returning. The index has its own PEXPIREAT, but a clock skew
    // between Redis and runner could surface a "live" index entry past
    // the row's expiry.
    const expiresAt = await this.redis.hget(this.keys.wf(cachedId), "idempotencyExpiresAt");
    if (!expiresAt) return null;
    if (new Date(expiresAt).getTime() <= params.now.getTime()) return null;
    return { workflowId: cachedId };
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const key = ns ? this.keys.distinctNamespaceNames(ns) : this.keys.distinctNames;
    return (await this.redis.smembers(key)).sort();
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const key = ns ? this.keys.distinctNamespaceTypes(ns) : this.keys.distinctTypes;
    return (await this.redis.smembers(key)).sort();
  }

  async distinctNamespaces(): Promise<string[]> {
    return (await this.redis.smembers(this.keys.distinctNamespaces)).sort();
  }

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    const [id, status, error, errorTag, nonce, parentId] = await this.hmget(
      this.keys.wf(workflowId),
      ["id", "status", "error", "errorTag", "provisional", "parentWorkflowId"],
    );
    if (!id || !status) return null;
    if (nonce) {
      const counts = await this.settleProvisional({ workflowId, parentId: parentId ?? "", nonce });
      return counts ? this.loadWorkflowStatus(workflowId) : null;
    }
    return {
      status: status as WorkflowStatus,
      ...(error ? { error } : {}),
      ...(errorTag ? { errorTag } : {}),
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const wfKey = this.keys.wf(workflowId);
    return this.settledState({
      workflowId,
      reply: await this.redis.eval(LOAD_RUN_LUA, 1, wfKey, wfKey),
    });
  }

  /**
   * The workflow a `LOAD_RUN_LUA` reply holds. A provisional child row is
   * settled first: null unless its parent's intent commits it, else the
   * confirmed row, re-read.
   */
  private async settledState(params: {
    workflowId: string;
    reply: unknown;
  }): Promise<WorkflowState | null> {
    const wfKey = this.keys.wf(params.workflowId);
    let reply = params.reply;
    for (let attempt = 0; attempt < MAX_SETTLE_ATTEMPTS; attempt++) {
      const pending = provisionalMarker(reply);
      if (!pending) return this.assembleWorkflow(reply);
      if (!(await this.settleProvisional({ workflowId: params.workflowId, ...pending }))) {
        return null;
      }
      reply = await this.redis.eval(LOAD_RUN_LUA, 1, wfKey, wfKey);
    }
    return null;
  }

  // -- Listing ---------------------------------------------------------------
  //
  // Filters on status, name, parent and namespace come from index sets; the
  // other filters and every sort key come from the compact index records
  // (one HMGET for all candidates), so no workflow hash is read until the
  // page is known. Without filters, a createdAt / startedAt / completedAt
  // page comes straight from its sorted set.

  async listWorkflows(params?: ListParams): Promise<WorkflowState[]> {
    const ids = await this.pageIds(params);
    const rows = await Promise.all(ids.map((id) => this.loadWorkflow(id)));
    return this.keepIndexedRows({ ids, rows, status: params?.status });
  }

  async listWorkflowSummaries(params?: ListParams): Promise<WorkflowSummary[]> {
    const ids = await this.pageIds(params);
    const rows = await Promise.all(
      ids.map(async (id) => toSummary(await this.hmget(this.keys.wf(id), SUMMARY_FIELDS))),
    );
    return this.keepIndexedRows({ ids, rows, status: params?.status });
  }

  /** Count over the same filters as `listWorkflows`, without reading any workflow. */
  async countWorkflows(params?: ListFilters): Promise<number> {
    if (!needsRecords(params)) {
      const sets = this.filterSets(params);
      if (sets.length === 0) return this.redis.zcard(this.keys.byCreated);
      if (sets.length === 1) return this.redis.scard(sets[0]!);
      return Number(await this.redis.eval(INTER_CARD_LUA, sets.length, ...sets));
    }
    return (await this.matchingRecords(params)).length;
  }

  /**
   * Drop page rows that are gone (expired through a retention TTL) or no
   * longer match the status filter (the index lags the row), repairing the
   * index for each.
   */
  private async keepIndexedRows<T extends { status: WorkflowStatus }>(params: {
    ids: readonly string[];
    rows: ReadonlyArray<T | null>;
    status?: WorkflowStatus;
  }): Promise<T[]> {
    const out: T[] = [];
    const repairs: Promise<void>[] = [];
    params.rows.forEach((row, i) => {
      const id = params.ids[i]!;
      if (row === null) repairs.push(this.unindexMissing(id));
      else if (params.status !== undefined && row.status !== params.status) {
        repairs.push(this.repairIndex(id));
      } else out.push(row);
    });
    await Promise.all(repairs);
    return out;
  }

  /** Ids of one page, in order. */
  private async pageIds(params?: ListParams): Promise<string[]> {
    const orderBy = params?.orderBy ?? "startedAt";
    const desc = (params?.orderDir ?? "desc") === "desc";
    const offset = params?.offset ?? 0;
    const limit = params?.limit;

    const sets = this.filterSets(params);
    const zset =
      orderBy === "createdAt"
        ? this.keys.byCreated
        : orderBy === "startedAt"
          ? this.keys.byStarted
          : orderBy === "completedAt"
            ? this.keys.byCompleted
            : undefined;
    if (sets.length === 0 && !needsRecords(params) && zset !== undefined) {
      return (await this.redis.eval(
        INDEX_PAGE_LUA,
        2,
        zset,
        this.keys.byCreated,
        String(offset),
        String(limit ?? UNBOUNDED),
        desc ? "1" : "0",
      )) as string[];
    }

    const rows = await this.matchingRecords(params);
    sortWorkflowRows({
      rows,
      orderBy,
      orderDir: desc ? "desc" : "asc",
      fields: ({ rec }) => ({
        workflowName: rec.n,
        status: rec.s,
        createdAtMs: rec.c,
        startedAtMs: rec.st,
        completedAtMs: rec.co,
      }),
    });
    return rows.slice(offset, limit === undefined ? undefined : offset + limit).map((r) => r.id);
  }

  /** Index sets the filters select candidates from. */
  private filterSets(params?: ListFilters): string[] {
    const sets: string[] = [];
    if (params?.status) sets.push(this.keys.status(params.status));
    if (params?.name) sets.push(this.keys.name(params.name));
    if (params?.parentId) sets.push(this.keys.children(params.parentId));
    const ns = params?.namespace ?? this.namespace;
    if (ns) sets.push(this.keys.namespace(ns));
    return sets;
  }

  /** Index records of every workflow matching the filters. */
  private async matchingRecords(
    params?: ListFilters,
  ): Promise<Array<{ id: string; rec: IndexRecord }>> {
    const sets = this.filterSets(params);
    const ids =
      sets.length === 0
        ? await this.redis.zrangebyscore(this.keys.byCreated, "-inf", "+inf")
        : sets.length === 1
          ? await this.redis.smembers(sets[0]!)
          : await this.redis.sinter(...sets);
    const recs = await this.indexRecords(ids);

    let rows: Array<{ id: string; rec: IndexRecord }> = [];
    ids.forEach((id, i) => {
      const rec = recs[i];
      if (!rec) return;
      if (params?.version !== undefined && rec.v !== params.version) return;
      if (params?.type && rec.t !== params.type) return;
      if (params?.runSource !== undefined && rec.rs !== String(encodeRunSource(params.runSource))) {
        return;
      }
      if (params?.runSourceId !== undefined && rec.rsi !== params.runSourceId) return;
      rows.push({ id, rec });
    });

    const metadataFilter = params?.metadata;
    if (metadataFilter) {
      const metadata = await Promise.all(
        rows.map((r) => this.redis.hget(this.keys.wf(r.id), "metadata")),
      );
      rows = rows.filter((_, i) => {
        const raw = metadata[i];
        return workflowMetadataMatches(raw ? JSON.parse(raw) : undefined, metadataFilter);
      });
    }
    return rows;
  }

  private async indexRecords(ids: readonly string[]): Promise<Array<IndexRecord | null>> {
    const chunks: Promise<Array<string | null>>[] = [];
    for (let i = 0; i < ids.length; i += RECORD_CHUNK) {
      chunks.push(this.hmget(this.keys.indexRecords, ids.slice(i, i + RECORD_CHUNK)));
    }
    return (await Promise.all(chunks)).flat().map((json) => (json ? JSON.parse(json) : null));
  }

  // -- Cross-workflow index ---------------------------------------------------

  /** Apply a workflow's index snapshot (from a write script) to the `{idx}` keys. */
  private async syncIndex(snapshot: IndexSnapshot | null | undefined): Promise<void> {
    if (!snapshot) return;
    const f = snapshotFields(snapshot);
    if (!f.id || !f.status || !f.createdAt) return;
    const ms = (iso: string | null | undefined) => (iso ? String(new Date(iso).getTime()) : "");
    const rec: IndexRecord = {
      n: f.workflowName ?? "",
      s: f.status as WorkflowStatus,
      c: new Date(f.createdAt).getTime(),
      ...(f.workflowType ? { t: f.workflowType } : {}),
      ...(f.namespace ? { ns: f.namespace } : {}),
      ...(f.version ? { v: f.version } : {}),
      ...(f.parentWorkflowId ? { p: f.parentWorkflowId } : {}),
      ...(f.runSource ? { rs: f.runSource } : {}),
      ...(f.runSourceId ? { rsi: f.runSourceId } : {}),
      ...(f.startedAt ? { st: new Date(f.startedAt).getTime() } : {}),
      ...(f.completedAt ? { co: new Date(f.completedAt).getTime() } : {}),
    };
    const adds = this.memberSets({
      id: f.id,
      name: rec.n,
      type: rec.t,
      namespace: rec.ns,
      parentId: rec.p,
    });
    await this.redis.eval(
      SYNC_INDEX_LUA,
      5 + WORKFLOW_STATUSES.length + adds.length,
      this.keys.indexVersions,
      this.keys.indexRecords,
      this.keys.byCreated,
      this.keys.byStarted,
      this.keys.byCompleted,
      ...WORKFLOW_STATUSES.map((s) => this.keys.status(s)),
      ...adds.map(([key]) => key),
      f.id,
      f.iv ?? "1",
      JSON.stringify(rec),
      f.status,
      String(rec.c),
      ms(f.startedAt),
      ms(f.completedAt),
      String(WORKFLOW_STATUSES.length),
      ...WORKFLOW_STATUSES,
      ...adds.map(([, member]) => member),
    );
  }

  /** `[set, member]` pairs a workflow belongs to besides its status set. */
  private memberSets(params: {
    id: string;
    name: string;
    type?: string;
    namespace?: string;
    parentId?: string;
  }): Array<[string, string]> {
    const { id, name, type, namespace: ns, parentId } = params;
    const sets: Array<[string, string]> = [
      [this.keys.name(name), id],
      // Distinct-value sets keep every value ever seen, even after purge.
      [this.keys.distinctNames, name],
    ];
    if (type) sets.push([this.keys.distinctTypes, type]);
    if (ns) {
      sets.push([this.keys.namespace(ns), id], [this.keys.distinctNamespaces, ns]);
      sets.push([this.keys.distinctNamespaceNames(ns), name]);
      if (type) sets.push([this.keys.distinctNamespaceTypes(ns), type]);
    }
    if (parentId) sets.push([this.keys.children(parentId), id]);
    return sets;
  }

  /** Re-apply a workflow's current index fields (read-repair after a lag). */
  private async repairIndex(workflowId: string): Promise<void> {
    const snapshot = await this.hmget(this.keys.wf(workflowId), INDEX_FIELDS);
    if (snapshot[0]) await this.syncIndex(snapshot);
    else await this.unindexMissing(workflowId);
  }

  /** Drop a workflow whose hash is gone from the index, using its index record. */
  private async unindexMissing(workflowId: string): Promise<void> {
    const [json] = await this.hmget(this.keys.indexRecords, [workflowId]);
    const rec = json ? (JSON.parse(json) as IndexRecord) : null;
    await this.unindex({
      workflowId,
      name: rec?.n,
      namespace: rec?.ns,
      parentId: rec?.p,
      status: rec?.s,
    });
  }

  private async unindex(params: {
    workflowId: string;
    name?: string;
    namespace?: string;
    parentId?: string;
    status?: string;
  }): Promise<void> {
    const { workflowId } = params;
    const memberSets = (params.status ? [params.status] : WORKFLOW_STATUSES).map((s) =>
      this.keys.status(s),
    );
    if (params.name) memberSets.push(this.keys.name(params.name));
    if (params.namespace) memberSets.push(this.keys.namespace(params.namespace));
    if (params.parentId) memberSets.push(this.keys.children(params.parentId));
    const owned = [this.keys.children(workflowId)];
    await this.redis.eval(
      UNINDEX_LUA,
      5 + memberSets.length + owned.length,
      this.keys.indexVersions,
      this.keys.indexRecords,
      this.keys.byCreated,
      this.keys.byStarted,
      this.keys.byCompleted,
      ...memberSets,
      ...owned,
      workflowId,
      String(owned.length),
    );
  }

  async cancelWorkflow({ workflowId, cascade, guard }: CancelWorkflowParams): Promise<void> {
    await this.transitionStatus({
      workflowId,
      guard,
      to: "failed",
      from: CANCELLABLE_STATUSES,
      fields: { error: CANCELLED_ERROR, errorTag: CANCELLED_ERROR_TAG },
    });

    if (cascade) {
      // The children index, plus the children committed through the
      // parent's intents whose rows are not indexed yet: reading their
      // status settles (confirms) them first.
      const [indexed, intents] = await Promise.all([
        this.redis.smembers(this.keys.children(workflowId)),
        this.redis.hgetall(this.keys.childIntents(workflowId)),
      ]);
      const children = new Set(indexed);
      for (const childId of Object.keys(intents ?? {})) {
        if (children.has(childId)) continue;
        if (await this.loadWorkflowStatus(childId)) children.add(childId);
      }
      for (const childId of children) {
        await this.cancelWorkflow({ workflowId: childId, cascade: true });
      }
    }
  }

  /**
   * Atomically move a workflow to a terminal status when its current
   * status is one of `from`, then index the move. Returns the previous
   * status, or null when the workflow is missing or its status isn't one
   * of `from`. Fenced by `guard` in the same script; with `ttlMs` the
   * run's keys get the retention TTL in that script too.
   */
  private async transitionStatus(params: {
    workflowId: string;
    guard?: FenceGuard;
    to: WorkflowStatus;
    from: readonly WorkflowStatus[];
    fields: Record<string, string>;
    ttlMs?: number;
  }): Promise<WorkflowStatus | null> {
    const nowIso = this.serializeDate(this.clock.now());
    const fieldArgs = Object.entries({ completedAt: nowIso, ...params.fields }).flat();
    const wfKey = this.keys.wf(params.workflowId);
    const result = (await this.evalFenced({
      script: FENCED_TRANSITION_STATUS_LUA,
      workflowId: params.workflowId,
      guard: params.guard,
      keys: [wfKey],
      args: [
        params.to,
        nowIso,
        params.ttlMs ? String(params.ttlMs) : "",
        wfKey,
        String(fieldArgs.length / 2),
        ...fieldArgs,
        ...params.from,
      ],
    })) as [WorkflowStatus, IndexSnapshot] | null;
    if (!result) return null;
    await this.syncIndex(result[1]);
    return result[0];
  }

  // -- Step results ---------------------------------------------------------

  /** Op moving a `pending` run to `running` on its first step write. */
  private markRunningOp(workflowId: string, nowIso: string): string[] {
    return this.statusOp({
      workflowId,
      to: "running",
      from: ["pending"],
      fields: { startedAt: nowIso, updatedAt: nowIso },
    });
  }

  /** Completed step row for `saveStepResult` / `batchSaveStepResults`. */
  private completedStep(params: {
    run: number;
    existing: Partial<StepState>;
    record: {
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    };
    now: Date;
  }): StepState {
    const { run, existing, record, now } = params;
    return {
      stepName: record.stepName,
      run,
      status: "completed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      result: record.result,
      // `record.metadata` wins when provided; otherwise preserve whatever
      // was already on the step (e.g. metadata written at execute time).
      metadata: record.metadata ?? existing.metadata,
      startedAt: record.startedAt,
      completedAt: now,
      durationMs: record.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };
  }

  async saveStepResult({ guard, ...params }: SaveStepResultParams): Promise<void> {
    await this.batchSaveStepResults({ records: [params], guard });
  }

  async batchSaveStepResults({ records, guard }: BatchSaveStepResultsParams): Promise<void> {
    // Per workflow: one script reads the run number and the existing rows
    // of the batch's steps (to keep dependsOn / stepType / attempt), and one
    // fenced script writes the pending → running move, the step rows and
    // `updatedAt`, provided the run is still the one read. A batch is
    // atomic per workflow.
    const byWf = new Map<string, Array<(typeof records)[number]>>();
    for (const r of records) {
      const bucket = byWf.get(r.workflowId);
      if (bucket) bucket.push(r);
      else byWf.set(r.workflowId, [r]);
    }

    const now = this.clock.now();
    const nowIso = this.serializeDate(now);
    for (const [wfId, rs] of byWf) {
      await this.readModifyWrite({
        workflowId: wfId,
        guard,
        stepNames: rs.map((r) => r.stepName),
        ops: ({ run, steps }) => {
          const stepsKey = this.keys.steps(wfId, run);
          const ops: string[][] = [this.markRunningOp(wfId, nowIso)];
          rs.forEach((r, i) => {
            const existingJson = steps[i];
            const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};
            const step = this.completedStep({ run, existing, record: r, now });
            ops.push(["HSET", stepsKey, r.stepName, this.serializeStepState(step)]);
          });
          ops.push(["HSET", this.keys.wf(wfId), "updatedAt", nowIso]);
          return ops;
        },
      });
    }
  }

  /** Failed step row for `saveStepFailure` / `checkpointStep`. */
  private failedStep(params: {
    run: number;
    existing: Partial<StepState>;
    record: {
      stepName: string;
      error: string;
      errorTag?: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    };
    now: Date;
  }): StepState {
    const { run, existing, record, now } = params;
    return {
      stepName: record.stepName,
      run,
      status: "failed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      error: record.error,
      ...(record.errorTag !== undefined && { errorTag: record.errorTag }),
      metadata: record.metadata ?? existing.metadata,
      startedAt: record.startedAt,
      completedAt: now,
      durationMs: record.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };
  }

  async saveStepFailure({ guard, ...params }: SaveStepFailureParams): Promise<void> {
    const now = this.clock.now();
    const nowIso = this.serializeDate(now);
    await this.readModifyWrite({
      workflowId: params.workflowId,
      guard,
      stepNames: [params.stepName],
      ops: ({ run, steps }) => {
        const existing: Partial<StepState> = steps[0] ? JSON.parse(steps[0]) : {};
        const step = this.failedStep({ run, existing, record: params, now });
        return [
          this.markRunningOp(params.workflowId, nowIso),
          [
            "HSET",
            this.keys.steps(params.workflowId, run),
            params.stepName,
            this.serializeStepState(step),
          ],
          ["HSET", this.keys.wf(params.workflowId), "updatedAt", nowIso],
        ];
      },
    });
  }

  /**
   * The step row, its attempt rows, the pending → running move and the
   * status read-back, in one fenced script. The row is built for the
   * existing row the script is expected to find — none on the first try,
   * so a step's first checkpoint is one round trip; when a row is there
   * already (a map step's, a retried step's) the script hands it back and
   * the second try builds on it.
   */
  async checkpointStep({
    guard,
    ...checkpoint
  }: CheckpointStepParams): Promise<WorkflowStatusSnapshot | null> {
    const { workflowId, stepName, outcome } = checkpoint;
    const now = this.clock.now();
    const nowIso = this.serializeDate(now);
    const wfKey = this.keys.wf(workflowId);
    const attempts = checkpoint.attempts.map((record) => this.serializeAttempt(record));
    let expected: string | null = null;
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const existing: Partial<StepState> = expected === null ? {} : JSON.parse(expected);
      const record = { stepName, ...outcome };
      const step =
        outcome.kind === "completed"
          ? this.completedStep({
              run: 0,
              existing,
              record: { ...record, result: outcome.result },
              now,
            })
          : this.failedStep({ run: 0, existing, record: { ...record, error: outcome.error }, now });
      // The script puts the run in.
      const rowJson = this.serializeStepState({ ...step, run: undefined as unknown as number });
      const reply = (await this.evalFenced({
        script: FENCED_CHECKPOINT_STEP_LUA,
        workflowId,
        guard,
        keys: [wfKey, this.keys.attempts(workflowId)],
        args: [
          wfKey,
          stepName,
          expected === null ? "0" : "1",
          expected ?? "",
          rowJson,
          nowIso,
          ...attempts,
        ],
      })) as [number, ...unknown[]];
      const code = Number(reply[0]);
      if (code === -1) return null;
      if (code === 0) {
        expected = (reply[1] as string | null) ?? null;
        continue;
      }
      const [, status, error, errorTag, snapshot] = reply as [
        number,
        WorkflowStatus,
        string,
        string,
        IndexSnapshot | null,
      ];
      await this.syncIndex(snapshot);
      return {
        status,
        ...(error ? { error } : {}),
        ...(errorTag ? { errorTag } : {}),
      };
    }
    throw new Error(`checkpointStep: gave up on "${workflowId}" after contention`);
  }

  // -- Task results ---------------------------------------------------------

  async saveTaskResult({ guard, ...params }: SaveTaskResultParams): Promise<void> {
    await this.saveTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "completed", result: params.result },
      guard,
    });
  }

  async saveTaskFailure({ guard, ...params }: SaveTaskFailureParams): Promise<void> {
    await this.saveTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "failed", error: params.error },
      guard,
    });
  }

  /**
   * Write one map task row with `outcome`, creating the parent step row if
   * it doesn't exist yet, in one fenced script.
   */
  private async saveTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    outcome: { status: "completed"; result: unknown } | { status: "failed"; error: string };
    guard?: FenceGuard;
  }): Promise<void> {
    const now = this.clock.now();
    const taskField = String(params.taskIndex);
    await this.readModifyWrite({
      workflowId: params.workflowId,
      guard: params.guard,
      stepNames: [],
      task: { stepName: params.stepName, field: taskField },
      ops: ({ run, task: existingTaskJson }) => {
        const prev: Partial<StepTaskState> = existingTaskJson ? JSON.parse(existingTaskJson) : {};
        const task: StepTaskState = {
          taskIndex: params.taskIndex,
          ...params.outcome,
          startedAt: prev.startedAt ? new Date(prev.startedAt as unknown as string) : now,
          completedAt: now,
          attempt: ((prev.attempt as number) ?? 0) + 1,
        };
        const parentStep: StepState = {
          stepName: params.stepName,
          run,
          status: "running",
          dependsOn: [],
          stepType: "map",
          attempt: 1,
        };
        return [
          [
            "HSET",
            this.keys.tasks(params.workflowId, run, params.stepName),
            taskField,
            JSON.stringify({
              ...task,
              startedAt: task.startedAt ? this.serializeDate(task.startedAt) : undefined,
              completedAt: task.completedAt ? this.serializeDate(task.completedAt) : undefined,
            }),
          ],
          // Ensure the parent step row exists.
          [
            "HSETNX",
            this.keys.steps(params.workflowId, run),
            params.stepName,
            this.serializeStepState(parentStep),
          ],
          ["HSET", this.keys.wf(params.workflowId), "updatedAt", this.serializeDate(now)],
        ];
      },
    });
  }

  /**
   * Read the run number plus existing step / task rows in one script, then
   * write the ops computed from them in one fenced script that first checks
   * the run is unchanged — retrying when a fresh run got in between. A
   * missing workflow writes nothing.
   */
  private async readModifyWrite(params: {
    workflowId: string;
    guard?: FenceGuard;
    stepNames: readonly string[];
    task?: { stepName: string; field: string };
    ops: (read: {
      run: number;
      steps: ReadonlyArray<string | null>;
      task: string | null;
    }) => ReadonlyArray<readonly string[]>;
  }): Promise<void> {
    const wfKey = this.keys.wf(params.workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const reply = (await this.redis.eval(
        READ_FOR_WRITE_LUA,
        1,
        wfKey,
        wfKey,
        params.task?.stepName ?? "",
        params.task?.field ?? "",
        ...params.stepNames,
      )) as Array<string | null>;
      if (reply.length === 0) return;
      const [run, task, ...steps] = reply;
      const ops = params.ops({ run: Number(run), steps, task: task ?? null });
      const { applied } = await this.writeOps({
        workflowId: params.workflowId,
        guard: params.guard,
        ops: [["HEQ", wfKey, "run", run!], ...ops],
      });
      if (applied) return;
    }
    throw new Error(`write to "${params.workflowId}" gave up after contention`);
  }

  // -- Workflow completion --------------------------------------------------

  async completeWorkflow({ workflowId, result, guard }: CompleteWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "completed",
      fields: { result: JSON.stringify(result) },
    });
  }

  async failWorkflow({ workflowId, error, errorTag, guard }: FailWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "failed",
      fields: { error, ...(errorTag !== undefined && { errorTag }) },
    });
  }

  async tripwireWorkflow({ workflowId, reason, guard }: TripwireWorkflowParams): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "tripwire",
      fields: { tripwire: JSON.stringify(reason) },
    });
  }

  /** Terminal transition from any non-terminal status, with the retention TTL. */
  private async finishWorkflow(params: {
    workflowId: string;
    guard?: FenceGuard;
    to: WorkflowStatus;
    fields: Record<string, string>;
  }): Promise<void> {
    await this.transitionStatus({
      ...params,
      from: NON_TERMINAL_STATUSES,
      ttlMs: this.completedTtlMs,
    });
  }

  // -- Suspend / Signal -----------------------------------------------------

  async suspendWorkflow({
    workflowId,
    stepName,
    stepUpdate,
    guard,
  }: SuspendWorkflowParams): Promise<void> {
    const now = this.clock.now();
    // The step row, the status move and `updatedAt` land in one fenced script.
    await this.readModifyWrite({
      workflowId,
      guard,
      stepNames: [stepName],
      ops: ({ run, steps }) => {
        const existing: Partial<StepState> = steps[0] ? JSON.parse(steps[0]) : {};
        const step = {
          stepName,
          run,
          dependsOn: existing.dependsOn ?? [],
          stepType: existing.stepType ?? "single",
          attempt: existing.attempt ?? 1,
          startedAt: existing.startedAt ?? this.serializeDate(now),
          ...stepUpdate,
        };
        const serialized: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(step)) {
          serialized[k] = v instanceof Date ? this.serializeDate(v) : v;
        }
        return [
          ["HSET", this.keys.steps(workflowId, run), stepName, JSON.stringify(serialized)],
          this.statusOp({
            workflowId,
            to: "suspended",
            from: "*",
            fields: { updatedAt: this.serializeDate(now) },
          }),
        ];
      },
    });
  }

  async deliverSignal({ workflowId, signalName, payload }: DeliverSignalParams): Promise<void> {
    const signal: SignalState = {
      signalName,
      payload,
      deliveredAt: this.clock.now(),
    };
    await this.redis.hset(
      this.keys.signals(workflowId),
      signalName,
      JSON.stringify({
        ...signal,
        deliveredAt: this.serializeDate(signal.deliveredAt),
      }),
    );
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    const raw = await this.redis.hgetall(this.keys.signals(workflowId));
    if (!raw || Object.keys(raw).length === 0) return [];

    return Object.values(raw).map((json) => {
      const s = JSON.parse(json);
      return {
        signalName: s.signalName,
        payload: s.payload,
        deliveredAt: new Date(s.deliveredAt),
      };
    });
  }

  async setWorkflowMetadata({
    workflowId,
    patch,
    guard,
  }: SetWorkflowMetadataParams): Promise<void> {
    // Metadata is one JSON string field on the workflow hash. Merge
    // client-side, then compare-and-set against the value we read, retrying
    // when a concurrent patch landed first — so no patch is lost. The fence
    // is checked in the same script as each compare-and-set.
    const key = this.keys.wf(workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const raw = await this.redis.hget(key, "metadata");
      const merged = applyMetadataPatch({ current: raw ? JSON.parse(raw) : {}, patch });
      const written = await this.evalFenced({
        script: FENCED_HASH_FIELD_CAS_LUA,
        workflowId,
        guard,
        keys: [key],
        args: [
          "metadata",
          raw === null ? "1" : "0",
          raw ?? "",
          JSON.stringify(merged),
          "id",
          "updatedAt",
          this.serializeDate(this.clock.now()),
        ],
      });
      if (written !== 0) return; // 1 = written, -1 = no such workflow
    }
    throw new Error(`setWorkflowMetadata: gave up on "${workflowId}" after contention`);
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
    const record: SignalTokenRecord = {
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
    // Dedup check and insert in one script: two concurrent creates with the
    // same idempotency key resolve to one token. The token → workflow lookup
    // is written next, also on a dedup hit, so a create that crashed in
    // between is completed by its retry.
    const [cached, tokenId] = (await this.redis.eval(
      CREATE_SIGNAL_TOKEN_LUA,
      2,
      this.keys.signalTokens(params.workflowId),
      this.keys.signalTokenIdempotency(params.workflowId),
      params.tokenId,
      serializeSignalToken(record),
      params.idempotencyKey ?? "",
    )) as [number, string];
    await this.redis.set(this.keys.signalTokenLookup(tokenId), params.workflowId);
    if (cached === 1) {
      const existing = await this.findSignalTokenById(tokenId);
      if (!existing) throw new Error(`createSignalToken: deduplicated token ${tokenId} missing`);
      return { record: existing, isCached: true };
    }
    return { record, isCached: false };
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    const workflowId = await this.redis.get(this.keys.signalTokenLookup(tokenId));
    if (!workflowId) return null;
    const raw = await this.redis.hget(this.keys.signalTokens(workflowId), tokenId);
    return raw ? deserializeSignalToken(raw) : null;
  }

  async markSignalTokenCompleted(params: {
    tokenId: string;
    value: unknown;
    now: Date;
  }): Promise<
    | { outcome: "delivered"; record: SignalTokenRecord }
    | { outcome: "already_completed"; record: SignalTokenRecord }
  > {
    // Compare-and-set against the pending record we read: exactly one
    // concurrent completer swaps it, every other one re-reads and sees the
    // completed record.
    const workflowId = await this.redis.get(this.keys.signalTokenLookup(params.tokenId));
    if (!workflowId) throw new Error(`signal token ${params.tokenId} not found`);
    const hashKey = this.keys.signalTokens(workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const raw = await this.redis.hget(hashKey, params.tokenId);
      if (!raw) throw new Error(`signal token ${params.tokenId} not found`);
      const current = deserializeSignalToken(raw);
      if (current.completedAt !== null) {
        return { outcome: "already_completed", record: current };
      }
      const updated: SignalTokenRecord = {
        ...current,
        completedAt: params.now,
        completedValue: params.value,
      };
      const written = await this.redis.eval(
        HASH_FIELD_CAS_LUA,
        1,
        hashKey,
        params.tokenId,
        "0",
        raw,
        serializeSignalToken(updated),
        "",
      );
      if (written === 1) return { outcome: "delivered", record: updated };
    }
    throw new Error(`markSignalTokenCompleted: gave up on ${params.tokenId} after contention`);
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    const raw = await this.redis.hgetall(this.keys.signalTokens(workflowId));
    if (!raw || Object.keys(raw).length === 0) return [];
    return Object.values(raw)
      .map(deserializeSignalToken)
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
  }

  // ---------------------------------------------------------------------------
  // Streams — append-only chunks per (workflow, stream) via Redis lists.
  // ---------------------------------------------------------------------------

  async appendStreamChunk({
    guard,
    ...params
  }: AppendStreamChunkParams): Promise<{ chunkIndex: number }> {
    const { last } = await this.writeOps({
      workflowId: params.workflowId,
      guard,
      ops: [
        ["SADD", this.keys.streamIds(params.workflowId), params.streamId],
        [
          "RPUSH",
          this.keys.stream(params.workflowId, params.streamId),
          JSON.stringify({
            payload: params.payload,
            appendedBy: params.appendedBy,
            appendedAt: this.serializeDate(this.clock.now()),
          }),
        ],
      ],
    });
    // RPUSH returns the new list length; chunkIndex is length - 1.
    return { chunkIndex: Number(last) - 1 };
  }

  async readStreamChunks(params: {
    workflowId: string;
    streamId: string;
    since?: number;
    limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>> {
    const key = this.keys.stream(params.workflowId, params.streamId);
    const start = params.since !== undefined ? params.since + 1 : 0;
    const stop = params.limit !== undefined ? start + params.limit - 1 : -1;
    const items = await this.redis.lrange(key, start, stop);
    return items.map((raw: string, i: number) => {
      const parsed = JSON.parse(raw);
      return {
        chunkIndex: start + i,
        payload: parsed.payload,
        appendedBy: parsed.appendedBy as "workflow" | "external",
        appendedAt: new Date(parsed.appendedAt),
      };
    });
  }

  // -- Locking --------------------------------------------------------------

  async tryLock({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ acquired: boolean; token?: string }> {
    const [acquired, token] = (await this.redis.eval(
      TRY_LOCK_LUA,
      2,
      this.keys.lock(workflowId),
      this.keys.fence(workflowId),
      this.instanceId,
      lockDurationMs.toString(),
    )) as [number, string];
    if (acquired !== 1) return { acquired: false };
    return { acquired: true, token };
  }

  async tryLockAndLoad({
    workflowId,
    lockDurationMs,
  }: TryLockParams): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    // One script: the state is read as of the moment the lock was taken.
    const wfKey = this.keys.wf(workflowId);
    const [acquired, token, load] = (await this.redis.eval(
      TRY_LOCK_AND_LOAD_LUA,
      3,
      this.keys.lock(workflowId),
      this.keys.fence(workflowId),
      wfKey,
      this.instanceId,
      lockDurationMs.toString(),
      wfKey,
    )) as [number, string, unknown];
    const state = await this.settledState({ workflowId, reply: load });
    return acquired === 1 ? { locked: true, token, state } : { locked: false, state };
  }

  async releaseLock({ workflowId, guard }: ReleaseLockParams): Promise<void> {
    await this.redis.eval(
      RELEASE_LOCK_LUA,
      1,
      this.keys.lock(workflowId),
      this.instanceId,
      guard?.fenceToken ?? "",
    );
  }

  async heartbeat({ workflowId, lockDurationMs, guard }: HeartbeatParams): Promise<void> {
    const extended = await this.redis.eval(
      HEARTBEAT_LUA,
      1,
      this.keys.lock(workflowId),
      this.instanceId,
      lockDurationMs.toString(),
      guard?.fenceToken ?? "",
    );
    // A token holder whose lock is gone (released, or expired through its
    // TTL) or re-taken learns it lost the run. The read is for the error only.
    if (guard?.fenceToken && Number(extended) !== 1) {
      const current = await this.redis.hget(this.keys.lock(workflowId), "token");
      throw fenceMismatch({ workflowId, provided: guard.fenceToken, current });
    }
  }

  /**
   * Run a `fencedLua` script for `workflowId`'s lock: the fence check and
   * the script's writes are one atomic script. Without a token in `guard`
   * the script runs unfenced. A rejected fence becomes
   * `FenceTokenMismatchError`.
   */
  private async evalFenced(params: {
    script: string;
    workflowId: string;
    guard?: FenceGuard;
    keys: readonly string[];
    args: readonly string[];
  }): Promise<unknown> {
    const token = params.guard?.fenceToken ?? "";
    try {
      return await this.redis.eval(
        params.script,
        params.keys.length + 1,
        this.keys.lock(params.workflowId),
        ...params.keys,
        token,
        ...params.args,
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const at = message.indexOf(FENCE_REJECTED);
      if (at === -1) throw err;
      const current = message.slice(at + FENCE_REJECTED.length).trim();
      throw fenceMismatch({
        workflowId: params.workflowId,
        provided: token,
        current: current === "" ? null : current,
      });
    }
  }

  /**
   * Run `ops` (see `WRITE_OPS_LUA`) as one fenced script on `workflowId`'s
   * slot; every op's key (its second element) is passed in KEYS. When a
   * status op changed the run's index fields, the index is synced next,
   * unless `deferIndex` hands the snapshot back to the caller.
   * `applied` is false when an ABSENT or HEQ op stopped the script.
   */
  private async writeOps(params: {
    workflowId: string;
    guard?: FenceGuard;
    ops: ReadonlyArray<readonly string[]>;
    deferIndex?: boolean;
  }): Promise<{ applied: boolean; last: unknown; snapshot: IndexSnapshot | null }> {
    const keys: string[] = [];
    const position = new Map<string, number>();
    const encoded = params.ops.map(([command, key, ...rest]) => {
      let at = position.get(key!);
      if (at === undefined) {
        keys.push(key!);
        at = keys.length;
        position.set(key!, at);
      }
      return [command, at, ...rest];
    });
    const reply = (await this.evalFenced({
      script: FENCED_WRITE_OPS_LUA,
      workflowId: params.workflowId,
      guard: params.guard,
      keys,
      args: [JSON.stringify(encoded)],
    })) as [number, unknown?, IndexSnapshot?];
    const applied = Number(reply[0]) === 1;
    const snapshot = applied ? (reply[2] ?? null) : null;
    if (snapshot && !params.deferIndex) await this.syncIndex(snapshot);
    return { applied, last: reply[1], snapshot };
  }

  /** `STATUS` op of `WRITE_OPS_LUA`. */
  private statusOp(params: {
    workflowId: string;
    to: WorkflowStatus;
    from: readonly WorkflowStatus[] | "*";
    fields: Record<string, string>;
  }): string[] {
    return [
      "STATUS",
      this.keys.wf(params.workflowId),
      params.to,
      params.from === "*" ? "*" : params.from.join(","),
      ...Object.entries(params.fields).flat(),
    ];
  }

  private async hmget(key: string, fields: readonly string[]): Promise<Array<string | null>> {
    if (fields.length === 0) return [];
    return ((await this.redis.eval(HMGET_LUA, 1, key, ...fields)) ?? []) as Array<string | null>;
  }

  // -- Scanner / recovery queries -------------------------------------------
  //
  // Candidates come from the status index sets (ids only), sorted and cut at
  // the keyset cursor client-side; each candidate's workflow is then read in
  // parallel chunks, stopping as soon as `limit` rows match. A candidate
  // whose hash disagrees with the index has its index entry repaired.

  /**
   * Walk the ids in the given status index sets in ascending order after
   * `afterWorkflowId`, resolving `pick` for each in parallel chunks, and
   * return the first `limit` non-undefined results in id order.
   */
  private async scanStatusIndex<T>(params: {
    statuses: readonly WorkflowStatus[];
    limit: number;
    afterWorkflowId?: string;
    pick: (workflowId: string) => Promise<T | undefined>;
  }): Promise<T[]> {
    const limit = Math.max(0, Math.trunc(params.limit));
    if (limit === 0) return [];
    const sets = await Promise.all(
      params.statuses.map((s) => this.redis.smembers(this.keys.status(s))),
    );
    const after = params.afterWorkflowId;
    const ids = [...new Set(sets.flat())]
      .filter((id) => after === undefined || id > after)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

    const out: T[] = [];
    const chunkSize = Math.max(limit, 32);
    for (let i = 0; i < ids.length && out.length < limit; i += chunkSize) {
      const picked = await Promise.all(ids.slice(i, i + chunkSize).map((id) => params.pick(id)));
      for (const row of picked) {
        if (row === undefined) continue;
        out.push(row);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  /** Namespace scoping for the scanners — the same rule `listWorkflows` applies. */
  private inScannerNamespace(state: { namespace?: string }): boolean {
    return !this.namespace || state.namespace === this.namespace;
  }

  /** A still-suspended run with its steps sorted by name; repairs a lagging index. */
  private async loadSuspended(
    workflowId: string,
  ): Promise<{ state: WorkflowState; steps: StepState[] } | undefined> {
    const state = await this.loadWorkflow(workflowId);
    if (!state || state.status !== "suspended") {
      await this.repairIndex(workflowId);
      return undefined;
    }
    if (!this.inScannerNamespace(state)) return undefined;
    const steps = Object.values(state.steps).sort((a, b) =>
      a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0,
    );
    return { state, steps };
  }

  private toWakeup(params: {
    state: WorkflowState;
    stepName: string;
    reason: WorkflowWakeup["reason"];
    signalName?: string;
    signalPayload?: unknown;
  }): WorkflowWakeup {
    const { state } = params;
    return {
      workflowId: state.workflowId,
      workflowName: state.workflowName,
      ...(state.version ? { version: state.version } : {}),
      input: state.input,
      stepName: params.stepName,
      reason: params.reason,
      ...(params.signalName !== undefined ? { signalName: params.signalName } : {}),
      ...(params.reason === "signal" ? { signalPayload: params.signalPayload } : {}),
    };
  }

  async listDueTimers(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const nowMs = params.now.getTime();
    const due = (at: Date | undefined): boolean => at !== undefined && at.getTime() <= nowMs;
    return this.scanStatusIndex<WorkflowWakeup>({
      statuses: ["suspended"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        const loaded = await this.loadSuspended(workflowId);
        if (!loaded) return undefined;
        for (const step of loaded.steps) {
          if (step.status === "sleeping" && due(step.wakeAt)) {
            return this.toWakeup({ state: loaded.state, stepName: step.stepName, reason: "sleep" });
          }
          if (step.status === "waiting_for_signal" && due(step.signalTimeoutAt)) {
            return this.toWakeup({
              state: loaded.state,
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
    return this.scanStatusIndex<WorkflowWakeup>({
      statuses: ["suspended"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        // Signals first: most suspended runs have none delivered, and that
        // check is one HGETALL on a usually-missing key.
        const signals = await this.redis.hgetall(this.keys.signals(workflowId));
        if (!signals || Object.keys(signals).length === 0) return undefined;
        const loaded = await this.loadSuspended(workflowId);
        if (!loaded) return undefined;
        for (const step of loaded.steps) {
          if (step.status !== "waiting_for_signal" || step.signalName === undefined) continue;
          const json = signals[step.signalName];
          if (json === undefined) continue;
          return this.toWakeup({
            state: loaded.state,
            stepName: step.stepName,
            reason: "signal",
            signalName: step.signalName,
            signalPayload: JSON.parse(json).payload,
          });
        }
        return undefined;
      },
    });
  }

  /**
   * Locks are keys with a server-side TTL, so a lock that expired is
   * already gone; a live lock counts as expired at `now` when its
   * remaining TTL, measured from this client's clock, ends by then.
   */
  async listOrphanedRuns(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]> {
    const nowMs = params.now.getTime();
    const beforeMs = params.updatedBefore.getTime();
    return this.scanStatusIndex<OrphanedRun>({
      statuses: ["pending", "running", "compensating"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        const [raw, pttl] = await Promise.all([
          this.redis.hgetall(this.keys.wf(workflowId)),
          this.redis.eval(PTTL_LUA, 1, this.keys.lock(workflowId)) as Promise<number>,
        ]);
        if (!raw || !raw.id) {
          await this.unindexMissing(workflowId);
          return undefined;
        }
        if (raw.status !== "pending" && raw.status !== "running" && raw.status !== "compensating") {
          await this.repairIndex(workflowId);
          return undefined;
        }
        if (!this.inScannerNamespace({ namespace: raw.namespace })) return undefined;
        if (this.parseDate(raw.updatedAt).getTime() >= beforeMs) return undefined;
        // PTTL: -2 = no key, -1 = no expiry (treat as held).
        const ttl = Number(pttl);
        if (ttl === -1) return undefined;
        if (ttl >= 0 && this.clock.currentTimeMs() + ttl > nowMs) return undefined;
        return {
          workflowId: raw.id,
          workflowName: raw.workflowName,
          ...(raw.version ? { version: raw.version } : {}),
          status: raw.status,
          input: JSON.parse(raw.input),
          ...(raw.metadata ? { metadata: JSON.parse(raw.metadata) } : {}),
        };
      },
    });
  }

  // -- Run history ----------------------------------------------------------

  /** Summary of the current run of a loaded workflow, for history and archiving. */
  private currentRunSummary(state: WorkflowState): WorkflowRunSummary {
    const steps: Record<string, StepState> = {};
    for (const [name, step] of Object.entries(state.steps)) {
      const { tasks: _tasks, ...row } = step;
      steps[name] = row;
    }
    return {
      run: state.run,
      version: state.version,
      status: state.status,
      result: state.result,
      error: state.error,
      tripwire: state.tripwire,
      steps,
      createdAt: state.createdAt,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
    };
  }

  async startFreshRun({ workflowId, guard }: StartFreshRunParams): Promise<number> {
    const state = await this.loadWorkflow(workflowId);
    if (!state) throw new Error(`Workflow ${workflowId} not found`);
    const summaryJson = JSON.stringify(this.currentRunSummary(state), (_, v) =>
      v instanceof Date ? v.toISOString() : v,
    );

    // Archive + run bump + journal and signal cleanup in one script, so the
    // new run can never observe the previous run's journal or signals.
    const reply = (await this.evalFenced({
      script: FENCED_START_FRESH_RUN_LUA,
      workflowId,
      guard,
      keys: [
        this.keys.wf(workflowId),
        this.keys.runs(workflowId),
        this.keys.signals(workflowId),
        this.keys.journalSteps(workflowId),
        this.keys.attempts(workflowId),
      ],
      args: [
        String(state.run),
        summaryJson,
        String(this.maxRunsPerWorkflow),
        this.serializeDate(this.clock.now()),
        this.keys.journalBase(workflowId),
      ],
    })) as [number, string[]?, IndexSnapshot?];
    const newRun = Number(reply[0]);
    if (newRun === -1) {
      // A concurrent fresh run moved the counter between our read and the
      // script — start over against the new run.
      return this.startFreshRun({ workflowId, guard });
    }
    await Promise.all([
      this.unscheduleSleeps({ workflowId, members: reply[1] ?? [] }),
      this.syncIndex(reply[2]),
    ]);
    return newRun;
  }

  /**
   * Reset step rows of the current run (see `WorkflowStorage.resetSteps`):
   * the listed steps' rows, task rows and journal go, the kept steps lose
   * their compensation ledger, and a terminal run moves back to `running`
   * — one script, compare-and-set against the rows read first.
   */
  async resetSteps({ workflowId, stepNames }: ResetStepsParams): Promise<void> {
    if (stepNames.length === 0) return;
    const wfKey = this.keys.wf(workflowId);
    const reset = new Set(stepNames);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const load = (await this.redis.eval(LOAD_RUN_LUA, 1, wfKey, wfKey)) as unknown[];
      if (!load || load.length === 0) throw new Error(`Workflow ${workflowId} not found`);
      const run = flatToRecord(load[0] as string[]).run!;
      const ledgerSwaps: string[] = [];
      for (const [name, json] of Object.entries(flatToRecord(load[1] as string[]))) {
        if (reset.has(name)) continue;
        const step = this.parseStepState(json);
        if (step.compensationStatus === undefined) continue;
        ledgerSwaps.push(name, json, this.serializeStepState(withoutCompensationLedger(step)));
      }
      const reply = (await this.redis.eval(
        RESET_STEPS_LUA,
        2,
        wfKey,
        this.keys.journalSteps(workflowId),
        wfKey,
        this.keys.journalBase(workflowId),
        run,
        this.serializeDate(this.clock.now()),
        String(stepNames.length),
        ...stepNames,
        ...ledgerSwaps,
      )) as [number, string[]?, IndexSnapshot?];
      const outcome = Number(reply[0]);
      if (outcome === -1) throw new Error(`Workflow ${workflowId} not found`);
      if (outcome === 0) continue;
      await Promise.all([
        this.unscheduleSleeps({ workflowId, members: reply[1] ?? [] }),
        this.syncIndex(reply[2]),
      ]);
      return;
    }
    throw new Error(`resetSteps: gave up on "${workflowId}" after contention`);
  }

  async loadRunHistory({
    workflowId,
    ...params
  }: LoadRunHistoryParams): Promise<WorkflowRunSummary[]> {
    const [state, archivedRaw] = await Promise.all([
      this.loadWorkflow(workflowId),
      this.redis.eval(LRANGE_ALL_LUA, 1, this.keys.runs(workflowId)) as Promise<string[] | null>,
    ]);
    if (!state) return [];

    const archived: WorkflowRunSummary[] = (archivedRaw ?? []).map((json) => {
      const r = JSON.parse(json);
      const steps: Record<string, StepState> = {};
      if (r.steps) {
        for (const [name, step] of Object.entries(r.steps)) {
          const s = step as Record<string, unknown>;
          steps[name] = {
            ...s,
            startedAt: s.startedAt ? new Date(s.startedAt as string) : undefined,
            completedAt: s.completedAt ? new Date(s.completedAt as string) : undefined,
            wakeAt: s.wakeAt ? new Date(s.wakeAt as string) : undefined,
            signalTimeoutAt: s.signalTimeoutAt ? new Date(s.signalTimeoutAt as string) : undefined,
            compensatedAt: s.compensatedAt ? new Date(s.compensatedAt as string) : undefined,
          } as StepState;
        }
      }
      return {
        run: r.run,
        version: r.version || undefined,
        status: r.status,
        result: r.result,
        error: r.error || undefined,
        steps,
        createdAt: new Date(r.createdAt),
        startedAt: r.startedAt ? new Date(r.startedAt) : undefined,
        completedAt: r.completedAt ? new Date(r.completedAt) : undefined,
      };
    });

    const runs = [this.currentRunSummary(state), ...archived];
    runs.sort((a, b) => b.run - a.run);

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? runs.length;
    return runs.slice(offset, offset + limit);
  }

  // -- Purge ----------------------------------------------------------------

  async purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
    let minScore: number;
    let maxScore: number;
    if ("olderThanMs" in params) {
      minScore = 0;
      maxScore = this.clock.currentTimeMs() - params.olderThanMs;
    } else {
      minScore = params.from.getTime();
      maxScore = params.to.getTime();
    }

    const ids = await this.redis.zrangebyscore(
      this.keys.byCompleted,
      minScore,
      maxScore,
      "LIMIT",
      0,
      params.limit,
    );
    let deleted = 0;
    for (let i = 0; i < ids.length; i += PURGE_CONCURRENCY) {
      const outcomes = await Promise.all(
        ids.slice(i, i + PURGE_CONCURRENCY).map((id) => this.purgeWorkflow(id)),
      );
      deleted += outcomes.filter(Boolean).length;
    }
    return deleted;
  }

  /** Delete one finished workflow and its index entries. True when it was purged. */
  private async purgeWorkflow(workflowId: string): Promise<boolean> {
    const wfKey = this.keys.wf(workflowId);
    const reply = (await this.redis.eval(
      PURGE_WORKFLOW_LUA,
      1,
      wfKey,
      wfKey,
      TERMINAL_STATUSES_CSV,
    )) as [number, ...unknown[]];
    const outcome = Number(reply[0]);
    if (outcome === 0) {
      // Already gone (retention TTL) — drop what the index still holds.
      await this.unindexMissing(workflowId);
      return false;
    }
    if (outcome === -1) {
      await this.repairIndex(workflowId);
      return false;
    }
    const [, status, name, parentId, ns, streamsTracked, tokenIds, sleeps] = reply as [
      number,
      string,
      string,
      string,
      string,
      string,
      string[],
      string[],
    ];
    await Promise.all([
      streamsTracked === "1" ? undefined : this.deleteUntrackedStreams(workflowId),
      // Each lookup key is its own slot: one DEL per key.
      ...tokenIds.map((tokenId) => this.redis.del(this.keys.signalTokenLookup(tokenId))),
      this.unscheduleSleeps({ workflowId, members: sleeps }),
      this.unindex({
        workflowId,
        status,
        name: name || undefined,
        parentId: parentId || undefined,
        namespace: ns || undefined,
      }),
    ]);
    return true;
  }

  /**
   * Streams appended before their ids were tracked in the stream-ids set
   * are found by a SCAN for the workflow's stream keys, bounded at
   * `UNTRACKED_STREAM_SCAN_CALLS` calls. Runs only for rows that predate
   * the tracking (no `streamsTracked` marker).
   */
  private async deleteUntrackedStreams(workflowId: string): Promise<void> {
    const match = `${escapeGlob(this.keys.wf(workflowId))}:streams:*`;
    let cursor = "0";
    for (let calls = 0; calls < UNTRACKED_STREAM_SCAN_CALLS; calls++) {
      const [next, found] = await this.redis.scan(cursor, "MATCH", match, "COUNT", 1_000);
      await Promise.all(found.map((key) => this.redis.del(key)));
      cursor = next;
      if (cursor === "0") return;
    }
  }

  /** Take journal sleeps (`<step>::<idx>|<path>` from a script) off the sleep schedule. */
  private async unscheduleSleeps(params: {
    workflowId: string;
    members: readonly string[];
  }): Promise<void> {
    if (params.members.length === 0) return;
    await this.redis.zrem(
      this.keys.sleeps,
      ...params.members.map((m) => `${params.workflowId}::${m}`),
    );
  }

  // -- StepAttemptStore ---------------------------------------------------

  async saveStepAttempt({ record, guard }: SaveStepAttemptParams): Promise<void> {
    await this.writeOps({
      workflowId: record.workflowId,
      guard,
      ops: [["RPUSH", this.keys.attempts(record.workflowId), this.serializeAttempt(record)]],
    });
  }

  private serializeAttempt(record: StepAttemptRecord): string {
    return JSON.stringify({
      ...record,
      startedAt: this.serializeDate(record.startedAt),
      completedAt: this.serializeDate(record.completedAt),
    });
  }

  async loadStepAttempts({
    workflowId,
    stepName,
  }: LoadStepAttemptsParams): Promise<StepAttemptRecord[]> {
    const raw = (await this.redis.eval(LRANGE_ALL_LUA, 1, this.keys.attempts(workflowId))) as
      | string[]
      | null;
    const items = (raw ?? []).map((json) => {
      const r = JSON.parse(json);
      return {
        ...r,
        startedAt: new Date(r.startedAt),
        completedAt: new Date(r.completedAt),
      } as StepAttemptRecord;
    });
    return stepName ? items.filter((a) => a.stepName === stepName) : items;
  }

  // -- CompensationLedgerStore --------------------------------------------

  async beginCompensation({ guard, ...params }: BeginCompensationParams): Promise<boolean> {
    const { workflowId } = params;
    const nowIso = this.serializeDate(this.clock.now());
    // The move and the read-back run in one script: the reply is the status
    // the run has after it.
    const { last } = await this.writeOps({
      workflowId,
      guard,
      ops: [
        this.statusOp({
          workflowId,
          to: "compensating",
          from: CANCELLABLE_STATUSES,
          fields: { error: params.error, errorTag: params.errorTag ?? "", updatedAt: nowIso },
        }),
        ["HGET", this.keys.wf(workflowId), "status"],
      ],
    });
    return last === "compensating";
  }

  async saveStepCompensation({ guard, ...params }: SaveStepCompensationParams): Promise<void> {
    const { workflowId } = params;
    const now = this.clock.now();
    await this.readModifyWrite({
      workflowId,
      guard,
      stepNames: [params.stepName],
      ops: ({ run, steps }) => {
        const existingJson = steps[0];
        if (!existingJson) return [];
        const step: StepState = {
          ...withoutCompensationLedger(this.parseStepState(existingJson)),
          compensationStatus: params.status,
          ...(params.error !== undefined && { compensationError: params.error }),
          compensatedAt: now,
        };
        return [
          [
            "HSET",
            this.keys.steps(workflowId, run),
            params.stepName,
            this.serializeStepState(step),
          ],
          ["HSET", this.keys.wf(workflowId), "updatedAt", this.serializeDate(now)],
        ];
      },
    });
  }

  // -- JournalStore -----------------------------------------------

  async loadJournal({ workflowId, stepName }: LoadJournalParams): Promise<JournalEntry[]> {
    const members = await this.redis.zrangebyscore(
      this.keys.journalIdx(workflowId, stepName),
      "-inf",
      "+inf",
    );
    if (members.length === 0) return [];

    const entries = await Promise.all(
      members.map(async (member) => {
        const { activityIndex, branchPath } = parseJournalMember(member);
        const hash = await this.redis.hgetall(
          this.keys.journalEntry(workflowId, stepName, activityIndex, branchPath),
        );
        if (!hash || Object.keys(hash).length === 0) return null;
        return this.parseJournalEntry(activityIndex, branchPath, hash);
      }),
    );
    return entries.filter((e): e is JournalEntry => e !== null);
  }

  async appendEntry({ guard, ...params }: AppendEntryParams): Promise<void> {
    const branchPath = params.branchPath ?? "";
    await this.evalFenced({
      script: FENCED_APPEND_ENTRY_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [
        this.keys.journalEntry(
          params.workflowId,
          params.stepName,
          params.activityIndex,
          branchPath,
        ),
        this.keys.journalIdx(params.workflowId, params.stepName),
        this.keys.journalSteps(params.workflowId),
      ],
      args: [
        String(params.activityIndex),
        params.activityName,
        JSON.stringify(params.exit),
        this.serializeDate(this.clock.now()),
        params.stepName,
        branchPath,
        params.payloadHash ?? "",
      ],
    });
  }

  // -- JournalStore: pending entries -------------------------------

  async appendPendingEntry({ guard, ...params }: AppendPendingEntryParams): Promise<void> {
    const branchPath = params.branchPath ?? "";
    // Persist wakeAt for signals too: it is the signal's timeout deadline,
    // and replay must read the recorded one rather than recompute it.
    const wakeAtMs = params.wakeAt ? String(params.wakeAt.getTime()) : "";
    const signalName = params.stepType === "signal" ? params.activityName : "";

    const [, phase, storedWakeAt] = (await this.evalFenced({
      script: FENCED_APPEND_PENDING_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [
        this.keys.journalEntry(
          params.workflowId,
          params.stepName,
          params.activityIndex,
          branchPath,
        ),
        this.keys.journalIdx(params.workflowId, params.stepName),
        this.keys.journalSteps(params.workflowId),
        this.keys.journalSignalIdx(params.workflowId, params.stepName),
      ],
      args: [
        String(params.activityIndex),
        params.activityName,
        params.stepType,
        wakeAtMs,
        this.serializeDate(this.clock.now()),
        params.stepName,
        signalName,
        branchPath,
        params.payloadHash ?? "",
      ],
    })) as [number, string, string];
    // A pending sleep goes on the cross-workflow schedule — also when the
    // entry already existed, so a replay re-adds a sleep whose first
    // writer crashed before scheduling it. A sleep without wakeAt can't be
    // scheduled.
    if (params.stepType === "sleep" && phase === "pending" && storedWakeAt) {
      await this.redis.zadd(
        this.keys.sleeps,
        storedWakeAt,
        sleepsMember({
          workflowId: params.workflowId,
          stepName: params.stepName,
          activityIndex: params.activityIndex,
          branchPath,
        }),
      );
    }
  }

  async completePendingEntry({
    guard,
    ...params
  }: CompletePendingEntryParams): Promise<CompletePendingResult> {
    const branchPath = params.branchPath ?? "";
    // The Lua phase check makes the transition atomic, so concurrent
    // completers (a signal delivery racing the body's timeout write) get
    // exactly one winner.
    const [won, storedExit, stepType] = (await this.evalFenced({
      script: FENCED_COMPLETE_PENDING_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [
        this.keys.journalEntry(
          params.workflowId,
          params.stepName,
          params.activityIndex,
          branchPath,
        ),
        this.keys.journalSignalIdx(params.workflowId, params.stepName),
      ],
      args: [JSON.stringify(params.exit)],
    })) as [number, string, string];
    // Off the schedule whoever won: the loser cleans up after a winner that
    // crashed before doing it.
    if (stepType === "sleep") {
      await this.redis.zrem(
        this.keys.sleeps,
        sleepsMember({
          workflowId: params.workflowId,
          stepName: params.stepName,
          activityIndex: params.activityIndex,
          branchPath,
        }),
      );
    }
    if (Number(won) === 1) return { completed: true, exit: params.exit };
    return {
      completed: false,
      exit: storedExit ? (JSON.parse(storedExit) as JournalExit) : undefined,
    };
  }

  async discardJournalEntries({ guard, ...params }: DiscardJournalEntriesParams): Promise<void> {
    // Every slot goes in one fenced script; the sleeps then leave the schedule.
    await this.evalFenced({
      script: FENCED_DISCARD_ENTRIES_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [
        this.keys.journalIdx(params.workflowId, params.stepName),
        this.keys.journalSignalIdx(params.workflowId, params.stepName),
        ...params.slots.map((slot) =>
          this.keys.journalEntry(
            params.workflowId,
            params.stepName,
            slot.activityIndex,
            slot.branchPath,
          ),
        ),
      ],
      args: params.slots.flatMap((slot) => [
        `${slot.activityIndex}|${slot.branchPath}`,
        // Index members written before branch paths existed are the bare index.
        slot.branchPath === "" ? String(slot.activityIndex) : "",
      ]),
    });
    if (params.slots.length > 0) {
      await this.redis.zrem(
        this.keys.sleeps,
        ...params.slots.map((slot) =>
          sleepsMember({ workflowId: params.workflowId, stepName: params.stepName, ...slot }),
        ),
      );
    }
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
    const raw = await this.redis.zrangebyscore(
      this.keys.sleeps,
      "-inf",
      params.now.getTime(),
      "WITHSCORES",
      "LIMIT",
      0,
      params.limit,
    );
    const candidates: Array<{
      member: string;
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }> = [];
    for (let i = 0; i < raw.length; i += 2) {
      const member = raw[i]!;
      const parsed = parseSleepsMember(member);
      if (parsed) candidates.push({ member, ...parsed, wakeAt: new Date(Number(raw[i + 1])) });
    }
    // The schedule is written next to the journal, not with it: keep only
    // members whose entry is still a pending sleep, and drop the rest.
    const phases = await Promise.all(
      candidates.map((c) =>
        this.hmget(
          this.keys.journalEntry(c.workflowId, c.stepName, c.activityIndex, c.branchPath),
          ["phase", "stepType"],
        ),
      ),
    );
    const stale = candidates.filter((_, i) => {
      const [phase, stepType] = phases[i]!;
      return phase !== "pending" || stepType !== "sleep";
    });
    if (stale.length > 0) {
      await this.redis.zrem(this.keys.sleeps, ...stale.map((c) => c.member));
    }
    return candidates.filter((c) => !stale.includes(c)).map(({ member: _member, ...due }) => due);
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const composite = await this.redis.hget(
      this.keys.journalSignalIdx(params.workflowId, params.stepName),
      params.signalName,
    );
    if (composite == null) return null;
    const { activityIndex, branchPath } = parseJournalMember(composite);
    const hash = await this.redis.hgetall(
      this.keys.journalEntry(params.workflowId, params.stepName, activityIndex, branchPath),
    );
    if (!hash || Object.keys(hash).length === 0) return null;
    const entry = this.parseJournalEntry(activityIndex, branchPath, hash);
    if (entry.phase !== "pending") return null;
    return entry;
  }

  // -- Journal helpers ------------------------------------------------------

  private parseJournalEntry(
    activityIndex: number,
    branchPath: string,
    hash: Record<string, string>,
  ): JournalEntry {
    const stepType = hash.stepType as JournalEntry["stepType"];
    const phase = hash.phase as JournalEntry["phase"];
    const exit = hash.exit ? (JSON.parse(hash.exit) as JournalEntry["exit"]) : undefined;
    const wakeAt = hash.wakeAt ? new Date(Number(hash.wakeAt)) : undefined;
    return {
      activityIndex,
      branchPath: hash.branchPath ?? branchPath,
      activityName: hash.activityName!,
      stepType,
      phase,
      payloadHash: hash.payloadHash,
      exit,
      wakeAt,
      createdAt: this.parseDate(hash.createdAt!),
    };
  }
}

/** True when a filter needs index records (not just index sets). */
function needsRecords(params?: ListFilters): boolean {
  return (
    params?.version !== undefined ||
    Boolean(params?.type) ||
    params?.runSource !== undefined ||
    params?.runSourceId !== undefined ||
    params?.metadata !== undefined
  );
}

/** Name the fields of an `INDEX_FIELDS` snapshot. */
function snapshotFields(
  snapshot: IndexSnapshot,
): Partial<Record<(typeof INDEX_FIELDS)[number], string | null>> {
  const out: Partial<Record<(typeof INDEX_FIELDS)[number], string | null>> = {};
  INDEX_FIELDS.forEach((field, i) => {
    out[field] = snapshot[i] ?? null;
  });
  return out;
}

/** A `listWorkflowSummaries` row from `SUMMARY_FIELDS` values; null for a missing workflow. */
function toSummary(values: ReadonlyArray<string | null>): WorkflowSummary | null {
  const f: Partial<Record<(typeof SUMMARY_FIELDS)[number], string>> = {};
  SUMMARY_FIELDS.forEach((field, i) => {
    const v = values[i];
    if (v) f[field] = v;
  });
  if (!f.id || !f.status || !f.createdAt) return null;
  return {
    workflowId: f.id,
    workflowName: f.workflowName ?? "",
    ...(f.workflowType ? { workflowType: f.workflowType } : {}),
    ...(f.namespace ? { namespace: f.namespace } : {}),
    status: f.status as WorkflowStatus,
    ...(f.version ? { version: f.version } : {}),
    run: Number(f.run),
    ...(f.runSource ? { runSource: decodeRunSource(Number(f.runSource)) } : {}),
    ...(f.runSourceId ? { runSourceId: f.runSourceId } : {}),
    ...(f.metadata ? { metadata: JSON.parse(f.metadata) } : {}),
    createdAt: new Date(f.createdAt),
    ...(f.startedAt ? { startedAt: new Date(f.startedAt) } : {}),
    updatedAt: new Date(f.updatedAt ?? f.createdAt),
    ...(f.completedAt ? { completedAt: new Date(f.completedAt) } : {}),
  };
}

/**
 * The provisional marker of a `LOAD_RUN_LUA` reply's workflow hash: the
 * nonce and parent of a fenced child create not confirmed yet, else null.
 */
function provisionalMarker(reply: unknown): { parentId: string; nonce: string } | null {
  const parts = (reply ?? []) as unknown[];
  if (parts.length === 0) return null;
  const raw = flatToRecord(parts[0] as string[]);
  if (!raw.provisional) return null;
  return { parentId: raw.parentWorkflowId ?? "", nonce: raw.provisional };
}

/** `[k1, v1, k2, v2, ...]` (a Lua HGETALL reply) as a record. */
function flatToRecord(flat: readonly string[] | null | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!flat) return out;
  for (let i = 0; i + 1 < flat.length; i += 2) out[flat[i]!] = flat[i + 1]!;
  return out;
}

/** Sleep schedule member: `{wid}::{step}::{idx}|{path}`. */
function sleepsMember(params: {
  workflowId: string;
  stepName: string;
  activityIndex: number;
  branchPath: string;
}): string {
  return `${params.workflowId}::${params.stepName}::${params.activityIndex}|${params.branchPath}`;
}

function parseSleepsMember(member: string): {
  workflowId: string;
  stepName: string;
  activityIndex: number;
  branchPath: string;
} | null {
  // Parse from the right: last "::" separates idx|branchPath; next-to-last
  // separates step. Tolerates "::" inside workflowId but not step name.
  const lastSep = member.lastIndexOf("::");
  if (lastSep < 0) return null;
  const idxAndPath = member.slice(lastSep + 2);
  const rest = member.slice(0, lastSep);
  const midSep = rest.lastIndexOf("::");
  if (midSep < 0) return null;
  const stepName = rest.slice(midSep + 2);
  const workflowId = rest.slice(0, midSep);
  const { activityIndex, branchPath } = parseJournalMember(idxAndPath);
  if (!Number.isFinite(activityIndex)) return null;
  return { workflowId, stepName, activityIndex, branchPath };
}

/**
 * Parse a composite journal member `${idx}|${branchPath}`. Rows written
 * before branch paths existed have just `${idx}` with no pipe — tolerated
 * so old workflows keep loading cleanly.
 */
function parseJournalMember(member: string): { activityIndex: number; branchPath: string } {
  const pipe = member.indexOf("|");
  if (pipe === -1) return { activityIndex: Number(member), branchPath: "" };
  return {
    activityIndex: Number(member.slice(0, pipe)),
    branchPath: member.slice(pipe + 1),
  };
}

function serializeSignalToken(t: SignalTokenRecord): string {
  return JSON.stringify({
    tokenId: t.tokenId,
    workflowId: t.workflowId,
    signalName: t.signalName,
    bearer: t.bearer,
    tags: t.tags,
    idempotencyKey: t.idempotencyKey,
    expiresAt: t.expiresAt.toISOString(),
    completedAt: t.completedAt ? t.completedAt.toISOString() : null,
    completedValue: t.completedValue,
    createdAt: t.createdAt.toISOString(),
  });
}

function deserializeSignalToken(raw: string): SignalTokenRecord {
  const parsed = JSON.parse(raw);
  return {
    tokenId: parsed.tokenId,
    workflowId: parsed.workflowId,
    signalName: parsed.signalName,
    bearer: parsed.bearer,
    tags: parsed.tags ?? [],
    idempotencyKey: parsed.idempotencyKey ?? null,
    expiresAt: new Date(parsed.expiresAt),
    completedAt: parsed.completedAt ? new Date(parsed.completedAt) : null,
    completedValue: parsed.completedValue,
    createdAt: new Date(parsed.createdAt),
  };
}
