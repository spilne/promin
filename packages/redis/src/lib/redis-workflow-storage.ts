// ---------------------------------------------------------------------------
// RedisWorkflowStorage — Redis-backed WorkflowStorage, StepAttemptStorage,
// ActivityJournalStorage, and JournaledSuspendStorage.
// ---------------------------------------------------------------------------
//
// Unsupported (by design — use PostgresWorkflowStorage if you need these):
// - cancelWorkflow cascade: no parent index is maintained.
// - listWorkflows parentId filter: no parent index is maintained.
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  StepAttemptStorage,
  ActivityJournalStorage,
  JournaledSuspendStorage,
  JournalEntry,
  FenceGuard,
} from "@promin/workflow";
import type {
  WorkflowState,
  WorkflowStatus,
  WorkflowRunSummary,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
} from "@promin/workflow";
import { FenceTokenMismatchError } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";
import { SystemClock, type Clock } from "@promin/core";

export interface RedisWorkflowStorageConfig {
  redis: RedisClient;
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
   * Default: `SystemClock`. Pass a `FakeClock` for deterministic tests.
   */
  clock?: Clock;
}

// -- Lua scripts ----------------------------------------------------------

// Lock is stored as a hash with { lockedBy, token } fields + a PEXPIRE TTL.
// Token is minted from a global INCR counter so each holder's stamp is
// strictly greater than any prior one — mutating writes carry it back
// through `checkFence` and a mismatch rejects the stale writer.
//
// TRY_LOCK
// KEYS: [lockKey, counterKey]
// ARGV: [instanceId, lockDurationMs]
// Returns: [acquired (0/1), token (string, empty on miss)]
const TRY_LOCK_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, ''}
end
local token = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'lockedBy', ARGV[1], 'token', token)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, tostring(token)}
`;

// RELEASE_LOCK: honor the fence token when provided, else fall back to
// the instanceId check (matches InMemory/Postgres semantics during the
// migration window where some callers don't yet pass guards).
// KEYS: [lockKey]
// ARGV: [instanceId, fenceToken|'']
const RELEASE_LOCK_LUA = `
if ARGV[2] ~= '' then
  if redis.call('HGET', KEYS[1], 'token') == ARGV[2] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
end
if redis.call('HGET', KEYS[1], 'lockedBy') == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

// HEARTBEAT: same fence-or-instanceId semantics as RELEASE_LOCK.
// KEYS: [lockKey]
// ARGV: [instanceId, lockDurationMs, fenceToken|'']
const HEARTBEAT_LUA = `
if ARGV[3] ~= '' then
  if redis.call('HGET', KEYS[1], 'token') == ARGV[3] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
  end
  return 0
end
if redis.call('HGET', KEYS[1], 'lockedBy') == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

// Journal: append a COMPLETED activity entry. Idempotent on (wid, step, idx).
// KEYS: [entryHash, idxZset, stepsSet]
// ARGV: [idx, activityName, exitJson, createdAt, stepName, branchPath, payloadHash|'']
const APPEND_ENTRY_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'activityName', ARGV[2],
  'stepType', 'activity',
  'phase', 'completed',
  'branchPath', ARGV[6],
  'exit', ARGV[3],
  'createdAt', ARGV[4])
if ARGV[7] ~= '' then
  redis.call('HSET', KEYS[1], 'payloadHash', ARGV[7])
end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. '|' .. ARGV[6])
redis.call('SADD', KEYS[3], ARGV[5])
return 1
`;

// Journal: append a PENDING entry (sleep or signal). Idempotent on (wid, step, idx, branch).
// KEYS: [entryHash, idxZset, stepsSet, sleepsZset (global), signalIdxHash]
// ARGV: [idx, activityName, stepType, wakeAtMs|'', createdAt, stepName, sleepsMember|'', signalName|'', branchPath, payloadHash|'']
const APPEND_PENDING_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'activityName', ARGV[2],
  'stepType', ARGV[3],
  'phase', 'pending',
  'wakeAt', ARGV[4],
  'branchPath', ARGV[9],
  'createdAt', ARGV[5])
if ARGV[10] ~= '' then
  redis.call('HSET', KEYS[1], 'payloadHash', ARGV[10])
end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. '|' .. ARGV[9])
redis.call('SADD', KEYS[3], ARGV[6])
if ARGV[3] == 'sleep' and ARGV[7] ~= '' then
  redis.call('ZADD', KEYS[4], ARGV[4], ARGV[7])
elseif ARGV[3] == 'signal' and ARGV[8] ~= '' then
  redis.call('HSET', KEYS[5], ARGV[8], ARGV[1] .. '|' .. ARGV[9])
end
return 1
`;

// Journal: transition pending -> completed atomically. No-op if already completed.
// KEYS: [entryHash, sleepsZset (global), signalIdxHash]
// ARGV: [exitJson, sleepsMember|'', signalName|'']
const COMPLETE_PENDING_LUA = `
local phase = redis.call('HGET', KEYS[1], 'phase')
if phase ~= 'pending' then return 0 end
local stepType = redis.call('HGET', KEYS[1], 'stepType')
redis.call('HSET', KEYS[1], 'phase', 'completed', 'exit', ARGV[1])
if stepType == 'sleep' and ARGV[2] ~= '' then
  redis.call('ZREM', KEYS[2], ARGV[2])
elseif stepType == 'signal' and ARGV[3] ~= '' then
  redis.call('HDEL', KEYS[3], ARGV[3])
end
return 1
`;

export class RedisWorkflowStorage
  implements WorkflowStorage, StepAttemptStorage, ActivityJournalStorage, JournaledSuspendStorage
{
  private readonly redis: RedisClient;
  private readonly prefix: string;
  private readonly namespace: string | null;
  private readonly instanceId: string;
  private readonly completedTtlMs?: number;
  private readonly maxRunsPerWorkflow: number;
  private readonly clock: Clock;

  constructor(config: RedisWorkflowStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "wf";
    this.namespace = config.namespace ?? null;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.completedTtlMs = config.retention?.completedTtlMs;
    this.maxRunsPerWorkflow = config.retention?.maxRunsPerWorkflow ?? 5;
    this.clock = config.clock ?? SystemClock;
  }

  // -- Key helpers ----------------------------------------------------------

  private wfKey(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private stepsKey(id: string, run: number): string {
    return `${this.prefix}:${id}:steps:${run}`;
  }

  private tasksKey(id: string, run: number, stepName: string): string {
    return `${this.prefix}:${id}:tasks:${run}:${stepName}`;
  }

  private signalsKey(id: string): string {
    return `${this.prefix}:${id}:signals`;
  }

  private runsKey(id: string): string {
    return `${this.prefix}:${id}:runs`;
  }

  private attemptsKey(id: string): string {
    return `${this.prefix}:${id}:attempts`;
  }

  private lockKey(id: string): string {
    return `${this.prefix}:lock:${id}`;
  }

  /** Global monotonic counter key for fence tokens. One per prefix. */
  private get fenceCounterKey(): string {
    return `${this.prefix}:lock-fence-counter`;
  }

  private statusIndexKey(status: string): string {
    return `${this.prefix}:idx:status:${status}`;
  }

  private nameIndexKey(name: string): string {
    return `${this.prefix}:idx:name:${name}`;
  }

  private get completedIndexKey(): string {
    return `${this.prefix}:idx:completed`;
  }

  // -- Journal key helpers --------------------------------------------------

  /** Per-workflow set of step names that have journal entries (for purge/ttl). */
  private journalStepsKey(id: string): string {
    return `${this.prefix}:${id}:journal:steps`;
  }

  /** Per-step sorted set of activity indices, for ordered loadJournal. */
  private journalIdxKey(id: string, stepName: string): string {
    return `${this.prefix}:${id}:journal:${stepName}:idx`;
  }

  /**
   * Per-entry hash: activityName, stepType, phase, exit, wakeAt, createdAt.
   * The entry key encodes (activityIndex, branchPath) — branchPath is `""`
   * for everything pre-`ctx.parallel` (and for sleep/signal yields), so
   * existing keys still resolve unchanged.
   */
  private journalEntryKey(
    id: string,
    stepName: string,
    activityIndex: number,
    branchPath: string,
  ): string {
    // Encoded as `${idx}|${path}` inside the key segment. `|` isn't used
    // elsewhere in the key schema, so it's a safe separator.
    return `${this.prefix}:${id}:journal:${stepName}:entry:${activityIndex}|${branchPath}`;
  }

  /** Per-step hash {signalName → `${activityIndex}|${branchPath}`} for O(1) findPendingSignal. */
  private journalSignalIdxKey(id: string, stepName: string): string {
    return `${this.prefix}:${id}:journal:${stepName}:signal-idx`;
  }

  /** Global sorted set across workflows: score=wakeAt_ms, member="{wid}::{step}::{idx}|{path}". */
  private get sleepsKey(): string {
    return `${this.prefix}:sleeps`;
  }

  private sleepsMember(
    id: string,
    stepName: string,
    activityIndex: number,
    branchPath: string,
  ): string {
    return `${id}::${stepName}::${activityIndex}|${branchPath}`;
  }

  private parseSleepsMember(member: string): {
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
    const pipe = idxAndPath.indexOf("|");
    const idxStr = pipe === -1 ? idxAndPath : idxAndPath.slice(0, pipe);
    const branchPath = pipe === -1 ? "" : idxAndPath.slice(pipe + 1);
    const activityIndex = Number(idxStr);
    if (!Number.isFinite(activityIndex)) return null;
    return { workflowId, stepName, activityIndex, branchPath };
  }

  // -- Index helpers --------------------------------------------------------

  private async moveStatusIndex(workflowId: string, from: string, to: string): Promise<void> {
    if (from === to) return;
    await this.redis.srem(this.statusIndexKey(from), workflowId);
    await this.redis.sadd(this.statusIndexKey(to), workflowId);
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

  /** Build a WorkflowState from the raw workflow hash + steps + tasks. */
  private async assembleWorkflow(raw: Record<string, string>): Promise<WorkflowState> {
    const id = raw.id;
    const run = Number(raw.run);

    // Load steps for current run
    const stepsRaw = await this.redis.hgetall(this.stepsKey(id, run));
    const steps: Record<string, StepState> = {};

    for (const [stepName, json] of Object.entries(stepsRaw)) {
      const step = this.parseStepState(json);

      // Load tasks if step is a map step
      if (step.stepType === "map") {
        const tasksRaw = await this.redis.hgetall(this.tasksKey(id, run, stepName));
        if (tasksRaw && Object.keys(tasksRaw).length > 0) {
          const tasks: StepTaskState[] = [];
          for (const taskJson of Object.values(tasksRaw)) {
            tasks.push(this.parseTaskState(taskJson));
          }
          tasks.sort((a, b) => a.taskIndex - b.taskIndex);
          steps[stepName] = { ...step, tasks };
          continue;
        }
      }

      steps[stepName] = step;
    }

    return {
      workflowId: id,
      workflowName: raw.workflowName,
      workflowType: raw.workflowType || undefined,
      parentWorkflowId: raw.parentWorkflowId || undefined,
      namespace: raw.namespace || undefined,
      status: raw.status as WorkflowStatus,
      version: raw.version || undefined,
      run,
      input: JSON.parse(raw.input),
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
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
    // Check if workflow already exists before creating
    const existingRaw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (existingRaw && existingRaw.id) {
      const existing = await this.loadWorkflow(params.workflowId);
      return { created: false, existing: existing! };
    }

    const now = this.serializeDate(this.clock.now());
    const ns = this.resolveNamespace(params.namespace);

    const fields: Record<string, string> = {
      id: params.workflowId,
      workflowName: params.workflowName,
      status: "pending",
      run: "1",
      input: JSON.stringify(params.input),
      createdAt: now,
      updatedAt: now,
    };
    if (params.workflowType) fields.workflowType = params.workflowType;
    if (params.parentWorkflowId) fields.parentWorkflowId = params.parentWorkflowId;
    if (ns) fields.namespace = ns;
    if (params.metadata) fields.metadata = JSON.stringify(params.metadata);
    if (params.version) fields.version = params.version;

    await this.redis.hset(this.wfKey(params.workflowId), fields);
    await this.redis.sadd(this.statusIndexKey("pending"), params.workflowId);
    await this.redis.sadd(this.nameIndexKey(params.workflowName), params.workflowId);
    return { created: true };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return null;
    return this.assembleWorkflow(raw);
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
    // Collect candidate ID sets based on filters
    const indexKeys: string[] = [];

    if (params?.status) {
      indexKeys.push(this.statusIndexKey(params.status));
    }
    if (params?.name) {
      indexKeys.push(this.nameIndexKey(params.name));
    }

    let candidateIds: string[];

    if (indexKeys.length > 1) {
      candidateIds = await this.redis.sinter(...indexKeys);
    } else if (indexKeys.length === 1) {
      candidateIds = await this.redis.smembers(indexKeys[0]);
    } else {
      // No index filters — scan all status sets
      const allStatuses: WorkflowStatus[] = [
        "pending",
        "running",
        "completed",
        "failed",
        "suspended",
        "compensating",
      ];
      const idSets = await Promise.all(
        allStatuses.map((s) => this.redis.smembers(this.statusIndexKey(s))),
      );
      candidateIds = [...new Set(idSets.flat())];
    }

    const ns = params?.namespace ?? this.namespace;
    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? Infinity;

    // Load and filter each candidate
    const results: WorkflowState[] = [];
    let skipped = 0;

    for (const id of candidateIds) {
      if (results.length >= limit) break;

      const raw = await this.redis.hgetall(this.wfKey(id));
      if (!raw || !raw.id) continue;

      // Apply filters not covered by indexes
      if (ns && raw.namespace !== ns) continue;
      if (params?.type && raw.workflowType !== params.type) continue;
      if (params?.parentId && raw.parentWorkflowId !== params.parentId) continue;

      if (skipped < offset) {
        skipped++;
        continue;
      }

      results.push(await this.assembleWorkflow(raw));
    }

    return results;
  }

  async cancelWorkflow(
    workflowId: string,
    _options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const status = raw.status as WorkflowStatus;
    if (status !== "pending" && status !== "running" && status !== "suspended") return;

    const now = this.serializeDate(this.clock.now());
    await this.redis.hset(this.wfKey(workflowId), {
      status: "failed",
      error: "Cancelled",
      completedAt: now,
      updatedAt: now,
    });

    await this.moveStatusIndex(workflowId, status, "failed");

    // Note: cascade not supported in Phase 1 — no parent index available
  }

  // -- Step results ---------------------------------------------------------

  /** Transition pending → running on first step activity. */
  private async markRunning(workflowId: string, raw: Record<string, string>): Promise<void> {
    if (raw.status !== "pending") return;
    const now = this.serializeDate(this.clock.now());
    await this.redis.hset(this.wfKey(workflowId), {
      status: "running",
      startedAt: now,
      updatedAt: now,
    });
    await this.moveStatusIndex(workflowId, "pending", "running");
    raw.status = "running";
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
    await this.checkFence(params.workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    await this.markRunning(params.workflowId, raw);

    const run = Number(raw.run);
    const now = this.clock.now();

    // Load existing step to preserve fields
    const existingJson = await this.redis.hget(
      this.stepsKey(params.workflowId, run),
      params.stepName,
    );
    const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};

    const step: StepState = {
      stepName: params.stepName,
      run,
      status: "completed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      result: params.result,
      // `params.metadata` wins when provided; otherwise preserve whatever
      // was already on the step (e.g. metadata written at execute time).
      metadata: params.metadata ?? existing.metadata,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };

    await this.redis.hset(
      this.stepsKey(params.workflowId, run),
      params.stepName,
      this.serializeStepState(step),
    );
    await this.redis.hset(this.wfKey(params.workflowId), { updatedAt: this.serializeDate(now) });
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
    const workflowIds = new Set(records.map((r) => r.workflowId));
    for (const id of workflowIds) await this.checkFence(id, guard);
    // Redis doesn't have transactions in the Postgres sense, but a pipeline
    // collapses the round-trip count to one regardless of batch size. We
    // still have to read existing step state up front (preserve dependsOn /
    // stepType / attempt counter) and the workflow row (run number), which
    // stays outside the pipeline to keep the reads sane. Writes go through
    // the pipeline in one burst.
    if (records.length === 0) return;

    // Group by (workflowId, run) — same reason as the Postgres path. For
    // each workflow we fetch its hash once, call markRunning once, and
    // load the steps hash for the current run once.
    const byWf = new Map<string, Array<(typeof records)[number]>>();
    for (const r of records) {
      const bucket = byWf.get(r.workflowId);
      if (bucket) bucket.push(r);
      else byWf.set(r.workflowId, [r]);
    }

    const pipeline = this.redis.pipeline();
    const now = this.clock.now();
    const nowIso = this.serializeDate(now);

    for (const [wfId, rs] of byWf) {
      const raw = await this.redis.hgetall(this.wfKey(wfId));
      if (!raw || !raw.id) continue;
      await this.markRunning(wfId, raw);
      const run = Number(raw.run);
      const stepsHashKey = this.stepsKey(wfId, run);
      const existingAll = await this.redis.hgetall(stepsHashKey);

      for (const r of rs) {
        const existingJson = existingAll?.[r.stepName];
        const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};
        const step: StepState = {
          stepName: r.stepName,
          run,
          status: "completed",
          dependsOn: existing.dependsOn ?? [],
          stepType: existing.stepType ?? "single",
          result: r.result,
          metadata: r.metadata ?? existing.metadata,
          startedAt: r.startedAt,
          completedAt: now,
          durationMs: r.durationMs,
          attempt: ((existing.attempt as number) ?? 0) + 1,
        };
        pipeline.hset(stepsHashKey, { [r.stepName]: this.serializeStepState(step) });
      }
      pipeline.hset(this.wfKey(wfId), { updatedAt: nowIso });
    }

    await pipeline.exec();
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
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    await this.markRunning(params.workflowId, raw);

    const run = Number(raw.run);
    const now = this.clock.now();

    const existingJson = await this.redis.hget(
      this.stepsKey(params.workflowId, run),
      params.stepName,
    );
    const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};

    const step: StepState = {
      stepName: params.stepName,
      run,
      status: "failed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      error: params.error,
      metadata: params.metadata ?? existing.metadata,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };

    await this.redis.hset(
      this.stepsKey(params.workflowId, run),
      params.stepName,
      this.serializeStepState(step),
    );
    await this.redis.hset(this.wfKey(params.workflowId), { updatedAt: this.serializeDate(now) });
  }

  // -- Task results ---------------------------------------------------------

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
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();

    // Load existing task
    const taskField = String(params.taskIndex);
    const existingTaskJson = await this.redis.hget(
      this.tasksKey(params.workflowId, run, params.stepName),
      taskField,
    );
    const prev: Partial<StepTaskState> = existingTaskJson ? JSON.parse(existingTaskJson) : {};

    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "completed",
      result: params.result,
      startedAt: prev.startedAt ? new Date(prev.startedAt as unknown as string) : now,
      completedAt: now,
      attempt: ((prev.attempt as number) ?? 0) + 1,
    };

    await this.redis.hset(
      this.tasksKey(params.workflowId, run, params.stepName),
      taskField,
      JSON.stringify({
        ...task,
        startedAt: task.startedAt ? this.serializeDate(task.startedAt) : undefined,
        completedAt: task.completedAt ? this.serializeDate(task.completedAt) : undefined,
      }),
    );

    // Ensure parent step exists
    const existingStepJson = await this.redis.hget(
      this.stepsKey(params.workflowId, run),
      params.stepName,
    );
    if (!existingStepJson) {
      const step: StepState = {
        stepName: params.stepName,
        run,
        status: "running",
        dependsOn: [],
        stepType: "map",
        attempt: 1,
      };
      await this.redis.hset(
        this.stepsKey(params.workflowId, run),
        params.stepName,
        this.serializeStepState(step),
      );
    }

    await this.redis.hset(this.wfKey(params.workflowId), { updatedAt: this.serializeDate(now) });
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
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();

    const taskField = String(params.taskIndex);
    const existingTaskJson = await this.redis.hget(
      this.tasksKey(params.workflowId, run, params.stepName),
      taskField,
    );
    const prev: Partial<StepTaskState> = existingTaskJson ? JSON.parse(existingTaskJson) : {};

    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      status: "failed",
      error: params.error,
      startedAt: prev.startedAt ? new Date(prev.startedAt as unknown as string) : now,
      completedAt: now,
      attempt: ((prev.attempt as number) ?? 0) + 1,
    };

    await this.redis.hset(
      this.tasksKey(params.workflowId, run, params.stepName),
      taskField,
      JSON.stringify({
        ...task,
        startedAt: task.startedAt ? this.serializeDate(task.startedAt) : undefined,
        completedAt: task.completedAt ? this.serializeDate(task.completedAt) : undefined,
      }),
    );

    // Ensure parent step exists
    const existingStepJson = await this.redis.hget(
      this.stepsKey(params.workflowId, run),
      params.stepName,
    );
    if (!existingStepJson) {
      const step: StepState = {
        stepName: params.stepName,
        run,
        status: "running",
        dependsOn: [],
        stepType: "map",
        attempt: 1,
      };
      await this.redis.hset(
        this.stepsKey(params.workflowId, run),
        params.stepName,
        this.serializeStepState(step),
      );
    }

    await this.redis.hset(this.wfKey(params.workflowId), { updatedAt: this.serializeDate(now) });
  }

  // -- Workflow completion --------------------------------------------------

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const now = this.clock.now();
    const oldStatus = raw.status;

    await this.redis.hset(this.wfKey(workflowId), {
      status: "completed",
      result: JSON.stringify(result),
      completedAt: this.serializeDate(now),
      updatedAt: this.serializeDate(now),
    });

    await this.moveStatusIndex(workflowId, oldStatus, "completed");
    await this.redis.zadd(this.completedIndexKey, now.getTime(), workflowId);

    if (this.completedTtlMs) {
      await this.applyTtl(workflowId, Number(raw.run));
    }
  }

  async failWorkflow(workflowId: string, error: string, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const now = this.clock.now();
    const oldStatus = raw.status;

    await this.redis.hset(this.wfKey(workflowId), {
      status: "failed",
      error,
      completedAt: this.serializeDate(now),
      updatedAt: this.serializeDate(now),
    });

    await this.moveStatusIndex(workflowId, oldStatus, "failed");
    await this.redis.zadd(this.completedIndexKey, now.getTime(), workflowId);

    if (this.completedTtlMs) {
      await this.applyTtl(workflowId, Number(raw.run));
    }
  }

  async tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    await this.checkFence(workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const now = this.clock.now();
    const oldStatus = raw.status;

    await this.redis.hset(this.wfKey(workflowId), {
      status: "tripwire",
      tripwire: JSON.stringify(reason),
      completedAt: this.serializeDate(now),
      updatedAt: this.serializeDate(now),
    });

    await this.moveStatusIndex(workflowId, oldStatus, "tripwire");
    await this.redis.zadd(this.completedIndexKey, now.getTime(), workflowId);

    if (this.completedTtlMs) {
      await this.applyTtl(workflowId, Number(raw.run));
    }
  }

  private async applyTtl(workflowId: string, run: number): Promise<void> {
    if (!this.completedTtlMs) return;
    const ttl = this.completedTtlMs;

    // Apply TTL to workflow hash and sub-keys
    await this.redis.pexpire(this.wfKey(workflowId), ttl);
    await this.redis.pexpire(this.stepsKey(workflowId, run), ttl);
    await this.redis.pexpire(this.signalsKey(workflowId), ttl);
    await this.redis.pexpire(this.runsKey(workflowId), ttl);
    await this.redis.pexpire(this.attemptsKey(workflowId), ttl);

    // TTL task hashes for current run steps
    const stepNames = await this.redis.hkeys(this.stepsKey(workflowId, run));
    for (const stepName of stepNames) {
      await this.redis.pexpire(this.tasksKey(workflowId, run, stepName), ttl);
    }

    // TTL journal keys. The global sleeps zset is shared across workflows
    // and is NOT expired; purgeCompleted + completePendingEntry clean up
    // this workflow's members.
    const journalSteps = await this.redis.smembers(this.journalStepsKey(workflowId));
    if (journalSteps.length > 0) {
      await this.redis.pexpire(this.journalStepsKey(workflowId), ttl);
      for (const stepName of journalSteps) {
        await this.redis.pexpire(this.journalIdxKey(workflowId, stepName), ttl);
        await this.redis.pexpire(this.journalSignalIdxKey(workflowId, stepName), ttl);
        const members = await this.redis.zrangebyscore(
          this.journalIdxKey(workflowId, stepName),
          "-inf",
          "+inf",
        );
        for (const member of members) {
          const { activityIndex, branchPath } = parseJournalMember(member);
          await this.redis.pexpire(
            this.journalEntryKey(workflowId, stepName, activityIndex, branchPath),
            ttl,
          );
        }
      }
    }
  }

  // -- Suspend / Signal -----------------------------------------------------

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    await this.checkFence(workflowId, guard);
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();
    const oldStatus = raw.status;

    // Load existing step
    const existingJson = await this.redis.hget(this.stepsKey(workflowId, run), stepName);
    const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};

    const step = {
      stepName,
      run,
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      attempt: existing.attempt ?? 1,
      startedAt: existing.startedAt ?? this.serializeDate(now),
      ...stepUpdate,
    };

    // Serialize dates in stepUpdate
    const serialized: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(step)) {
      if (v instanceof Date) {
        serialized[k] = this.serializeDate(v);
      } else {
        serialized[k] = v;
      }
    }

    await this.redis.hset(this.stepsKey(workflowId, run), stepName, JSON.stringify(serialized));
    await this.redis.hset(this.wfKey(workflowId), {
      status: "suspended",
      updatedAt: this.serializeDate(now),
    });

    await this.moveStatusIndex(workflowId, oldStatus, "suspended");
  }

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    const signal: SignalState = {
      signalName,
      payload,
      deliveredAt: this.clock.now(),
    };
    await this.redis.hset(
      this.signalsKey(workflowId),
      signalName,
      JSON.stringify({
        ...signal,
        deliveredAt: this.serializeDate(signal.deliveredAt),
      }),
    );
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    const raw = await this.redis.hgetall(this.signalsKey(workflowId));
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

  // -- Locking --------------------------------------------------------------

  async tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: string }> {
    // Atomic via Lua: EXISTS → INCR (monotonic fence counter) → HSET lock
    // hash → PEXPIRE. Redis serializes script execution, so no interleave.
    const result = (await this.redis.eval(
      TRY_LOCK_LUA,
      2,
      this.lockKey(workflowId),
      this.fenceCounterKey,
      this.instanceId,
      lockDurationMs.toString(),
    )) as [number, string];
    const [acquired, token] = result;
    if (acquired !== 1) return { acquired: false };
    return { acquired: true, token };
  }

  async tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    // Sequenced — a Lua script could do this in one round trip, but
    // `loadWorkflow` reads from several keys (wf hash, steps hash,
    // signals hash, per-run history) that don't fit neatly in a single
    // script without reimplementing the deserialization server-side.
    // The real win — collapsing two HTTP round-trips to one — is
    // already captured at the workflow-remote RPC layer (one POST
    // carries the whole tryLockAndLoad call).
    const { acquired, token } = await this.tryLock(workflowId, lockDurationMs);
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    await this.redis.eval(
      RELEASE_LOCK_LUA,
      1,
      this.lockKey(workflowId),
      this.instanceId,
      guard?.fenceToken ?? "",
    );
  }

  async heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    await this.redis.eval(
      HEARTBEAT_LUA,
      1,
      this.lockKey(workflowId),
      this.instanceId,
      lockDurationMs.toString(),
      guard?.fenceToken ?? "",
    );
  }

  /**
   * Reject a mutating call when the caller's fence token doesn't match the
   * current lock hash's `token` field. Calls without a token (legacy path)
   * skip the check so the migration can land additively.
   */
  private async checkFence(workflowId: string, guard?: FenceGuard): Promise<void> {
    if (!guard?.fenceToken) return;
    const current = await this.redis.hget(this.lockKey(workflowId), "token");
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

  // -- Run history ----------------------------------------------------------

  async startFreshRun(workflowId: string): Promise<number> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) throw new Error(`Workflow ${workflowId} not found`);

    const currentRun = Number(raw.run);

    // Archive current run — load steps for current run
    const stepsRaw = await this.redis.hgetall(this.stepsKey(workflowId, currentRun));
    const steps: Record<string, StepState> = {};
    for (const [stepName, json] of Object.entries(stepsRaw)) {
      steps[stepName] = this.parseStepState(json);
    }

    const summary: WorkflowRunSummary = {
      run: currentRun,
      version: raw.version || undefined,
      status: raw.status as WorkflowStatus,
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      steps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };

    await this.redis.rpush(
      this.runsKey(workflowId),
      JSON.stringify(summary, (_, v) => {
        if (v instanceof Date) return v.toISOString();
        return v;
      }),
    );

    // LTRIM to cap run history
    await this.redis.eval(
      `return redis.call('LTRIM', KEYS[1], -ARGV[1], -1)`,
      1,
      this.runsKey(workflowId),
      this.maxRunsPerWorkflow,
    );

    const newRun = currentRun + 1;
    const now = this.serializeDate(this.clock.now());
    const oldStatus = raw.status;

    await this.redis.hset(this.wfKey(workflowId), {
      run: String(newRun),
      status: "pending",
      updatedAt: now,
    });

    // Remove optional fields from completed run
    await this.redis.hdel(
      this.wfKey(workflowId),
      "result",
      "error",
      "tripwire",
      "startedAt",
      "completedAt",
    );

    await this.moveStatusIndex(workflowId, oldStatus, "pending");

    return newRun;
  }

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return [];

    // Load current run as a summary
    const currentRun = Number(raw.run);
    const stepsRaw = await this.redis.hgetall(this.stepsKey(workflowId, currentRun));
    const currentSteps: Record<string, StepState> = {};
    for (const [stepName, json] of Object.entries(stepsRaw)) {
      currentSteps[stepName] = this.parseStepState(json);
    }

    const currentSummary: WorkflowRunSummary = {
      run: currentRun,
      version: raw.version || undefined,
      status: raw.status as WorkflowStatus,
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      steps: currentSteps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };

    // Load archived runs via eval+LRANGE
    const archivedRaw = (await this.redis.eval(
      `return redis.call('LRANGE', KEYS[1], 0, -1)`,
      1,
      this.runsKey(workflowId),
    )) as string[] | null;

    const archived: WorkflowRunSummary[] = (archivedRaw ?? []).map((json) => {
      const r = JSON.parse(json);
      // Parse dates in steps
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

    const runs = [currentSummary, ...archived];
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

    // Find candidate workflow IDs from the completed sorted set
    const ids = await this.redis.zrangebyscore(
      this.completedIndexKey,
      minScore,
      maxScore,
      "LIMIT",
      0,
      params.limit,
    );

    let deleted = 0;

    for (const id of ids) {
      // Load workflow to get run count and status for cleanup
      const raw = await this.redis.hgetall(this.wfKey(id));
      if (!raw || !raw.id) {
        // Already gone — just clean up index
        await this.redis.zrem(this.completedIndexKey, id);
        continue;
      }

      const status = raw.status as WorkflowStatus;
      if (status !== "completed" && status !== "failed") continue;

      const run = Number(raw.run);

      // Collect all keys to delete
      const keysToDelete = [
        this.wfKey(id),
        this.stepsKey(id, run),
        this.signalsKey(id),
        this.runsKey(id),
        this.attemptsKey(id),
      ];

      // Delete task hashes for current run
      const stepNames = await this.redis.hkeys(this.stepsKey(id, run));
      for (const stepName of stepNames) {
        keysToDelete.push(this.tasksKey(id, run, stepName));
      }

      // Delete archived run step/task keys
      const archivedRaw = (await this.redis.eval(
        `return redis.call('LRANGE', KEYS[1], 0, -1)`,
        1,
        this.runsKey(id),
      )) as string[] | null;

      if (archivedRaw) {
        for (const json of archivedRaw) {
          const r = JSON.parse(json);
          const archivedRun = r.run as number;
          keysToDelete.push(this.stepsKey(id, archivedRun));
          if (r.steps) {
            for (const stepName of Object.keys(r.steps)) {
              keysToDelete.push(this.tasksKey(id, archivedRun, stepName));
            }
          }
        }
      }

      // Cascade journal: step list, per-step idx zset + signal-idx hash +
      // entry hashes, plus pending members left in the global sleeps zset.
      const journalStepNames = await this.redis.smembers(this.journalStepsKey(id));
      for (const stepName of journalStepNames) {
        const members = await this.redis.zrangebyscore(
          this.journalIdxKey(id, stepName),
          "-inf",
          "+inf",
        );
        for (const member of members) {
          const { activityIndex, branchPath } = parseJournalMember(member);
          keysToDelete.push(this.journalEntryKey(id, stepName, activityIndex, branchPath));
          // Defensive: ZREM is a no-op if not present.
          await this.redis.zrem(
            this.sleepsKey,
            this.sleepsMember(id, stepName, activityIndex, branchPath),
          );
        }
        keysToDelete.push(this.journalIdxKey(id, stepName));
        keysToDelete.push(this.journalSignalIdxKey(id, stepName));
      }
      if (journalStepNames.length > 0) {
        keysToDelete.push(this.journalStepsKey(id));
      }

      // Delete all keys
      if (keysToDelete.length > 0) {
        await this.redis.del(...keysToDelete);
      }

      // Remove from indexes
      await this.redis.srem(this.statusIndexKey(status), id);
      if (raw.workflowName) {
        await this.redis.srem(this.nameIndexKey(raw.workflowName), id);
      }
      await this.redis.zrem(this.completedIndexKey, id);

      deleted++;
    }

    return deleted;
  }

  // -- StepAttemptStorage ---------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord, guard?: FenceGuard): Promise<void> {
    await this.checkFence(record.workflowId, guard);
    await this.redis.rpush(
      this.attemptsKey(record.workflowId),
      JSON.stringify({
        ...record,
        startedAt: this.serializeDate(record.startedAt),
        completedAt: this.serializeDate(record.completedAt),
      }),
    );
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const raw = (await this.redis.eval(
      `return redis.call('LRANGE', KEYS[1], 0, -1)`,
      1,
      this.attemptsKey(workflowId),
    )) as string[] | null;

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

  // -- ActivityJournalStorage -----------------------------------------------

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const members = await this.redis.zrangebyscore(
      this.journalIdxKey(workflowId, stepName),
      "-inf",
      "+inf",
    );
    if (members.length === 0) return [];

    const entries = await Promise.all(
      members.map(async (member) => {
        const { activityIndex, branchPath } = parseJournalMember(member);
        const hash = await this.redis.hgetall(
          this.journalEntryKey(workflowId, stepName, activityIndex, branchPath),
        );
        if (!hash || Object.keys(hash).length === 0) return null;
        return this.parseJournalEntry(activityIndex, branchPath, hash);
      }),
    );
    return entries.filter((e): e is JournalEntry => e !== null);
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
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const idxKey = this.journalIdxKey(params.workflowId, params.stepName);
    const stepsKey = this.journalStepsKey(params.workflowId);
    const createdAt = this.serializeDate(this.clock.now());
    await this.redis.eval(
      APPEND_ENTRY_LUA,
      3,
      entryKey,
      idxKey,
      stepsKey,
      String(params.activityIndex),
      params.activityName,
      JSON.stringify(params.exit),
      createdAt,
      params.stepName,
      branchPath,
      params.payloadHash ?? "",
    );
  }

  // -- JournaledSuspendStorage ----------------------------------------------

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
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const idxKey = this.journalIdxKey(params.workflowId, params.stepName);
    const stepsKey = this.journalStepsKey(params.workflowId);
    const signalIdxKey = this.journalSignalIdxKey(params.workflowId, params.stepName);
    const wakeAtMs =
      params.stepType === "sleep" && params.wakeAt ? String(params.wakeAt.getTime()) : "";
    // Only register in the global sleeps zset when we have a wakeAt — a sleep
    // entry without one can't be scanned anyway.
    const sleepsMember =
      params.stepType === "sleep" && wakeAtMs
        ? this.sleepsMember(params.workflowId, params.stepName, params.activityIndex, branchPath)
        : "";
    const signalName = params.stepType === "signal" ? params.activityName : "";

    await this.redis.eval(
      APPEND_PENDING_LUA,
      5,
      entryKey,
      idxKey,
      stepsKey,
      this.sleepsKey,
      signalIdxKey,
      String(params.activityIndex),
      params.activityName,
      params.stepType,
      wakeAtMs,
      this.serializeDate(this.clock.now()),
      params.stepName,
      sleepsMember,
      signalName,
      branchPath,
      params.payloadHash ?? "",
    );
  }

  async completePendingEntry(params: {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath?: string;
    exit: NonNullable<JournalEntry["exit"]>;
  }): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const signalIdxKey = this.journalSignalIdxKey(params.workflowId, params.stepName);
    // Load current entry to learn the signal name (if any) so the Lua script
    // can remove it from the signal-idx hash. Safe under the workflow lock;
    // completePendingEntry is always called by the lock holder (workflow
    // resume, scanner, or signal deliverer) and the Lua phase check makes
    // the write itself atomic.
    const current = await this.redis.hgetall(entryKey);
    const stepType = current?.stepType;
    const sleepsMember =
      stepType === "sleep"
        ? this.sleepsMember(params.workflowId, params.stepName, params.activityIndex, branchPath)
        : "";
    const signalName = stepType === "signal" ? (current?.activityName ?? "") : "";

    await this.redis.eval(
      COMPLETE_PENDING_LUA,
      3,
      entryKey,
      this.sleepsKey,
      signalIdxKey,
      JSON.stringify(params.exit),
      sleepsMember,
      signalName,
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
    const raw = await this.redis.zrangebyscore(
      this.sleepsKey,
      "-inf",
      params.now.getTime(),
      "WITHSCORES",
      "LIMIT",
      0,
      params.limit,
    );
    const due: Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }> = [];
    for (let i = 0; i < raw.length; i += 2) {
      const member = raw[i]!;
      const score = Number(raw[i + 1]);
      const parsed = this.parseSleepsMember(member);
      if (!parsed) continue;
      due.push({ ...parsed, wakeAt: new Date(score) });
    }
    return due;
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const composite = await this.redis.hget(
      this.journalSignalIdxKey(params.workflowId, params.stepName),
      params.signalName,
    );
    if (composite == null) return null;
    const { activityIndex, branchPath } = parseJournalMember(composite);
    const hash = await this.redis.hgetall(
      this.journalEntryKey(params.workflowId, params.stepName, activityIndex, branchPath),
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

/**
 * Parse a composite journal member `${idx}|${branchPath}`. Pre-plif rows
 * have just `${idx}` with no pipe — we tolerate that for backward compat
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
