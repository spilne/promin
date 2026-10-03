// ---------------------------------------------------------------------------
// RedisStepQueue — Redis-backed StepQueue for distributed step dispatch
//
// Key structure:
//   {prefix}:task:{id}                             → Hash (task fields)
//   {prefix}:pending                               → Sorted Set (all pending
//                                                    tasks, priority-scored;
//                                                    claim filters by needs,
//                                                    step name and version
//                                                    in Lua)
//   {prefix}:running                               → Set of task IDs
//   {prefix}:done                                  → Sorted Set of terminal
//                                                    task IDs scored by
//                                                    completion time (ms);
//                                                    drives purge()
//   {prefix}:counter                               → Incr for task ID gen
//   {prefix}:claimseq                              → Incr for claim tokens
//   {prefix}:active:{wf}::{step}                   → taskId of the active
//                                                    (pending/running) task
//                                                    — drives idempotent
//                                                    enqueue
//   {prefix}:conc:{len(scope)}:{scope}:{key}       → Set of running task IDs
//                                                    sharing a (concurrency
//                                                    scope, key) — SCARD is
//                                                    the running count that
//                                                    claim checks against
//                                                    the task's limit
// ---------------------------------------------------------------------------

import {
  DEFAULT_MAX_DELIVERIES,
  deadLetterError,
  percentileCont,
  SystemWallClock,
  type StepQueue,
  type StepQueueClaimParams,
  type StepQueueEnqueueParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  type StepTask,
  type StepTaskRecord,
  type StepTaskStatus,
  type WallClock,
} from "@promin/workflow";
import type { RedisStoreClient } from "./redis-client.ts";

// -- Lua scripts -------------------------------------------------------------

/**
 * Lua helper shared by the scripts below: the running-set key for a task's
 * (concurrency scope, concurrency key), or nil when the task isn't keyed.
 * The scope is length-prefixed so `a:b` + `c` can't collide with `a` + `b:c`.
 */
const CONC_KEY_LUA = `
local function conc_key(prefix, scope, key)
  if not scope or scope == '' or not key or key == '' then return nil end
  return prefix .. ':conc:' .. #scope .. ':' .. scope .. ':' .. key
end
`;

/**
 * Atomically enqueue a task with idempotency on (workflow_id, step_name).
 * If an active task exists for the same pair (active key present), return
 * its id. Otherwise: INCR counter, HSET task hash (including needs as
 * JSON), ZADD to the global pending set keyed by priority + FIFO, SET
 * active key to the new task id.
 *
 * KEYS: [active_key, pending_key, counter_key]
 * ARGV: [task_key_prefix, workflowId, stepName, priority, inputJson,
 *        prevResultsJson, needsJson, createdAt, version, namespace,
 *        metadataJson, concurrencyKey, concurrencyScope, concurrencyLimit,
 *        attempt] — optional values are '' when unset
 */
const ENQUEUE_LUA = `
local active_key = KEYS[1]
local pending_key = KEYS[2]
local counter_key = KEYS[3]

local existing = redis.call('GET', active_key)
if existing then return existing end

local seq = redis.call('INCR', counter_key)
local id = tostring(seq)
local task_key = ARGV[1] .. id

local fields = {
  'id', id,
  'workflowId', ARGV[2],
  'stepName', ARGV[3],
  'priority', ARGV[4],
  'input', ARGV[5],
  'prevResults', ARGV[6],
  'needs', ARGV[7],
  'attempt', ARGV[15],
  'deliveries', '0',
  'status', 'pending',
  'createdAt', ARGV[8],
}
local optional = {
  {9, 'version'}, {10, 'namespace'}, {11, 'metadata'},
  {12, 'concurrencyKey'}, {13, 'concurrencyScope'}, {14, 'concurrencyLimit'},
}
for _, o in ipairs(optional) do
  if ARGV[o[1]] ~= '' then
    table.insert(fields, o[2])
    table.insert(fields, ARGV[o[1]])
  end
end
redis.call('HSET', task_key, unpack(fields))

local priority = tonumber(ARGV[4])
local score = priority * 1e12 + (1e12 - seq)
redis.call('ZADD', pending_key, score, id)
redis.call('SET', active_key, id)

return id
`;

/**
 * Claim up to `limit` pending tasks the worker can run: needs ⊆
 * capabilities, step name in `step_names` (when given), version in
 * `versions` or unset (when given), and the (concurrency scope, key) under
 * its limit. Walks the pending zset in priority order a page at a time,
 * skipping everything else, so tasks the worker can't run never block the
 * ones behind them. Every claimed keyed task joins its (scope, key) running
 * set inside the same script, so the cap holds within one call and across
 * concurrent claimers.
 *
 * At most `scan_budget` pending entries are examined per call so a large
 * backlog of blocked tasks can't stall Redis.
 *
 * KEYS: [pending_key, running_key, claimseq_key]
 * ARGV: [worker_id, now, limit, prefix, caps_json, scan_budget,
 *        step_names_json ('' = any), versions_json ('' = any)]
 */
const CLAIM_LUA =
  CONC_KEY_LUA +
  `
local pending_key = KEYS[1]
local running_key = KEYS[2]
local claimseq_key = KEYS[3]
local worker_id = ARGV[1]
local now = ARGV[2]
local limit = tonumber(ARGV[3])
local prefix = ARGV[4]
local caps = cjson.decode(ARGV[5])
local scan_budget = tonumber(ARGV[6])

local caps_set = {}
for _, c in ipairs(caps) do caps_set[c] = true end
local step_set = nil
if ARGV[7] ~= '' then
  step_set = {}
  for _, s in ipairs(cjson.decode(ARGV[7])) do step_set[s] = true end
end
local version_set = nil
if ARGV[8] ~= '' then
  version_set = {}
  for _, v in ipairs(cjson.decode(ARGV[8])) do version_set[v] = true end
end

local page = limit * 4
if page < 16 then page = 16 end

local results = {}
local start = 0
local examined = 0

while #results < limit and examined < scan_budget do
  local candidates = redis.call('ZREVRANGE', pending_key, start, start + page - 1)
  if #candidates == 0 then break end
  local claimed_in_page = 0

  for _, id in ipairs(candidates) do
    if #results >= limit or examined >= scan_budget then break end
    examined = examined + 1
    local task_key = prefix .. ':task:' .. id
    local f = redis.call('HMGET', task_key, 'needs', 'concurrencyKey', 'concurrencyScope',
      'concurrencyLimit', 'stepName', 'version')
    local needs_json, c_key, c_scope, c_limit, step_name, version = f[1], f[2], f[3], f[4], f[5], f[6]

    local ok = true
    if step_set and not step_set[step_name] then ok = false end
    if ok and version_set and version and version ~= '' and not version_set[version] then
      ok = false
    end
    if ok and needs_json and needs_json ~= '' and needs_json ~= '[]' then
      for _, n in ipairs(cjson.decode(needs_json)) do
        if not caps_set[n] then
          ok = false
          break
        end
      end
    end

    local conc = conc_key(prefix, c_scope, c_key)
    if ok and conc and c_limit then
      if redis.call('SCARD', conc) >= tonumber(c_limit) then ok = false end
    end

    if ok then
      local claim_token = worker_id .. ':' .. id .. ':' .. redis.call('INCR', claimseq_key)
      redis.call('ZREM', pending_key, id)
      redis.call('SADD', running_key, id)
      if conc then redis.call('SADD', conc, id) end
      redis.call('HINCRBY', task_key, 'deliveries', 1)
      redis.call('HSET', task_key, 'status', 'running', 'claimedBy', worker_id, 'claimedAt', now,
        'claimToken', claim_token, 'heartbeatAt', '')
      table.insert(results, redis.call('HGETALL', task_key))
      claimed_in_page = claimed_in_page + 1
    end
  end

  -- Claimed entries left the zset, shifting later ranks down.
  start = start + #candidates - claimed_in_page
end

return results
`;

/**
 * Take a running task off the running set and its concurrency slot, then
 * either put it back on the pending zset or — when it has used up
 * `max_deliveries` — dead-letter it (mark it failed, clear its active key,
 * index it as done). No-op unless the task is still running under
 * `claim_token` (empty token skips that check), so a racing complete/fail
 * wins.
 *
 * `mode` = 'requeue' (requeueStuck) or 'release' (release(): never
 * dead-letters, and gives the delivery back).
 *
 * KEYS: [task_key, running_key, pending_key, done_key]
 * ARGV: [id, prefix, claim_token, mode, max_deliveries, dead_letter_error,
 *        completed_at_iso, completed_at_ms]
 * Returns 0 (not running under the token), 1 (back to pending),
 * 2 (dead-lettered).
 */
const REQUEUE_LUA =
  CONC_KEY_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local pending_key = KEYS[3]
local done_key = KEYS[4]
local id = ARGV[1]
local prefix = ARGV[2]
local claim_token = ARGV[3]
local mode = ARGV[4]

local f = redis.call('HMGET', task_key, 'status', 'claimToken', 'priority', 'concurrencyKey',
  'concurrencyScope', 'deliveries', 'workflowId', 'stepName')
if f[1] ~= 'running' then return 0 end
if claim_token ~= '' and f[2] ~= claim_token then return 0 end

redis.call('SREM', running_key, id)
local conc = conc_key(prefix, f[5], f[4])
if conc then redis.call('SREM', conc, id) end

local deliveries = tonumber(f[6]) or 0
if mode == 'requeue' and deliveries >= tonumber(ARGV[5]) then
  redis.call('HSET', task_key,
    'status', 'failed',
    'error', ARGV[6],
    'completedAt', ARGV[7],
    'claimToken', '',
    'heartbeatAt', '')
  redis.call('DEL', prefix .. ':active:' .. f[7] .. '::' .. f[8])
  redis.call('ZADD', done_key, tonumber(ARGV[8]), id)
  return 2
end

if mode == 'release' and deliveries > 0 then
  redis.call('HINCRBY', task_key, 'deliveries', -1)
end
redis.call('HSET', task_key,
  'status', 'pending',
  'claimedBy', '',
  'claimedAt', '',
  'claimToken', '',
  'heartbeatAt', '')
local priority = tonumber(f[3]) or 5
redis.call('ZADD', pending_key, priority * 1e12 + (1e12 - tonumber(id)), id)
return 1
`;

/**
 * Settle a running task as completed or failed.
 *
 * KEYS: [task_key, running_key, done_key]
 * ARGV: [id, claim_token, status, value_field, value, duration_ms,
 *        completed_at_iso, prefix, completed_at_ms]
 */
const SETTLE_LUA =
  CONC_KEY_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local done_key = KEYS[3]
local id = ARGV[1]
local claim_token = ARGV[2]
local prefix = ARGV[8]

local f = redis.call('HMGET', task_key, 'status', 'claimToken', 'workflowId', 'stepName',
  'concurrencyScope', 'concurrencyKey')
if f[1] ~= 'running' then return 0 end
if claim_token ~= '' and f[2] ~= claim_token then return 0 end

redis.call('HSET', task_key,
  'status', ARGV[3],
  ARGV[4], ARGV[5],
  'durationMs', ARGV[6],
  'completedAt', ARGV[7])
redis.call('SREM', running_key, id)
if f[3] and f[4] then
  redis.call('DEL', prefix .. ':active:' .. f[3] .. '::' .. f[4])
end
local conc = conc_key(prefix, f[5], f[6])
if conc then redis.call('SREM', conc, id) end
redis.call('ZADD', done_key, tonumber(ARGV[9]), id)
return 1
`;

const HEARTBEAT_LUA = `
local task_key = KEYS[1]
local claim_token = ARGV[1]
local heartbeat_at = ARGV[2]

local status = redis.call('HGET', task_key, 'status')
local current_token = redis.call('HGET', task_key, 'claimToken')
if status ~= 'running' then return 0 end
if claim_token ~= '' and current_token ~= claim_token then return 0 end

redis.call('HSET', task_key, 'heartbeatAt', heartbeat_at)
return 1
`;

/**
 * Delete up to `batch` terminal tasks completed before `cutoff_ms`.
 *
 * KEYS: [done_key]
 * ARGV: [prefix, cutoff_ms (exclusive), batch]
 * Returns the number of tasks deleted.
 */
const PURGE_LUA = `
local done_key = KEYS[1]
local prefix = ARGV[1]
local ids = redis.call('ZRANGEBYSCORE', done_key, '-inf', '(' .. ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
local purged = 0
for _, id in ipairs(ids) do
  local task_key = prefix .. ':task:' .. id
  local status = redis.call('HGET', task_key, 'status')
  -- A task that left the terminal states isn't ours to delete; it is
  -- re-indexed when it settles again.
  if status == 'completed' or status == 'failed' or not status then
    if status then purged = purged + 1 end
    redis.call('DEL', task_key)
  end
  redis.call('ZREM', done_key, id)
end
return {purged, #ids}
`;

// -- Implementation ----------------------------------------------------------

export interface RedisStepQueueConfig {
  redis: RedisStoreClient;
  /** Key prefix for all queue keys. Default: "sq". */
  prefix?: string;
  /**
   * Upper bound on pending entries one `claim()` examines while skipping
   * tasks the worker can't take (unmet `needs`, other steps or versions,
   * full concurrency key). Default: 1000.
   */
  claimScanLimit?: number;
  /**
   * Deliveries after which `requeueStuck` dead-letters a task instead of
   * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
   */
  maxDeliveries?: number;
  /** Time source for client-side timestamps. Default: `SystemWallClock`. */
  clock?: WallClock;
}

const PURGE_BATCH = 500;

export class RedisStepQueue implements StepQueue {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  private readonly claimScanLimit: number;
  private readonly maxDeliveries: number;
  private readonly clock: WallClock;

  constructor(config: RedisStepQueueConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sq";
    this.claimScanLimit = config.claimScanLimit ?? 1000;
    this.maxDeliveries = config.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    this.clock = config.clock ?? SystemWallClock;
  }

  // -- Key helpers -----------------------------------------------------------

  private taskKey(id: string): string {
    return `${this.prefix}:task:${id}`;
  }

  private get pendingKey(): string {
    return `${this.prefix}:pending`;
  }

  private get runningKey(): string {
    return `${this.prefix}:running`;
  }

  private get doneKey(): string {
    return `${this.prefix}:done`;
  }

  /**
   * Active-task key for idempotent enqueue. Workflow ids are globally
   * unique, so (workflow_id, step_name) is the key.
   */
  private activeKey(workflowId: string, stepName: string): string {
    return `${this.prefix}:active:${workflowId}::${stepName}`;
  }

  // -- StepQueue interface ---------------------------------------------------

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const priority = params.priority ?? 5;
    const needs = params.needs ?? [];
    // One Lua round-trip: check active, maybe insert, atomic.
    const id = (await this.redis.eval(
      ENQUEUE_LUA,
      3,
      this.activeKey(params.workflowId, params.stepName),
      this.pendingKey,
      `${this.prefix}:counter`,
      `${this.prefix}:task:`,
      params.workflowId,
      params.stepName,
      String(priority),
      JSON.stringify(params.input),
      JSON.stringify(params.prevResults),
      JSON.stringify(needs),
      this.clock.now().toISOString(),
      params.version ?? "",
      params.namespace ?? "",
      params.metadata !== undefined ? JSON.stringify(params.metadata) : "",
      params.concurrencyKey ?? "",
      params.concurrencyScope ?? "",
      params.concurrencyLimit !== undefined ? String(params.concurrencyLimit) : "",
      String(params.attempt ?? 1),
    )) as string;
    return String(id);
  }

  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    const limit = Math.max(1, Math.floor(params.limit));
    if (params.stepNames !== undefined && params.stepNames.length === 0) return [];

    const raw = (await this.redis.eval(
      CLAIM_LUA,
      3,
      this.pendingKey,
      this.runningKey,
      `${this.prefix}:claimseq`,
      params.workerId,
      this.clock.now().toISOString(),
      String(limit),
      this.prefix,
      JSON.stringify(params.capabilities ?? []),
      String(Math.max(limit, this.claimScanLimit)),
      params.stepNames !== undefined ? JSON.stringify(params.stepNames) : "",
      params.versions !== undefined ? JSON.stringify(params.versions) : "",
    )) as string[][];

    const claimed: StepTask[] = [];
    for (const arr of raw) {
      const t = this.parseRecord(hashArrayToMap(arr));
      if (t) claimed.push(toTask(t));
    }
    return claimed;
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    if (params.claimToken === "") return false;
    const code = await this.requeue({
      id: params.taskId,
      claimToken: params.claimToken,
      mode: "release",
    });
    return code === 1;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const raw = await this.redis.hgetall(this.taskKey(taskId));
    if (!raw || Object.keys(raw).length === 0) return undefined;
    return this.parseRecord(raw);
  }

  async complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean> {
    return this.settle({
      taskId: params.taskId,
      claimToken: params.claimToken,
      status: "completed",
      field: "result",
      value: JSON.stringify(params.result),
      durationMs: params.durationMs,
    });
  }

  async fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean> {
    return this.settle({
      taskId: params.taskId,
      claimToken: params.claimToken,
      status: "failed",
      field: "error",
      value: params.error,
      durationMs: params.durationMs,
    });
  }

  async heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean> {
    const ok = await this.redis.eval(
      HEARTBEAT_LUA,
      1,
      this.taskKey(params.taskId),
      params.claimToken ?? "",
      this.clock.now().toISOString(),
    );
    return ok === 1;
  }

  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    const runningIds = await this.redis.smembers(this.runningKey);
    let requeued = 0;
    let deadLettered = 0;

    for (const id of runningIds) {
      const raw = await this.redis.hgetall(this.taskKey(id));
      if (!raw || raw.status !== "running") continue;

      if (params.mode === "worker") {
        if (raw.claimedBy !== params.workerId) continue;
      } else {
        const lastActivity = raw.heartbeatAt || raw.claimedAt;
        if (!lastActivity) continue;
        const idleMs = this.clock.currentTimeMs() - new Date(lastActivity).getTime();
        if (idleMs <= params.olderThanMs) continue;
      }

      // Token-guarded, so a task completed between the read above and the
      // requeue stays completed.
      const code = await this.requeue({ id, claimToken: raw.claimToken ?? "", mode: "requeue" });
      if (code === 1) requeued++;
      else if (code === 2) deadLettered++;
    }

    return { requeued, deadLettered };
  }

  /**
   * Delete terminal tasks completed before `completedBefore`. Only tasks
   * indexed in `{prefix}:done` — every task settled by this version — are
   * found.
   */
  async purge(params: { completedBefore: Date }): Promise<number> {
    let purged = 0;
    for (;;) {
      const [n, scanned] = (await this.redis.eval(
        PURGE_LUA,
        1,
        this.doneKey,
        this.prefix,
        String(params.completedBefore.getTime()),
        String(PURGE_BATCH),
      )) as [number, number];
      purged += Number(n);
      if (Number(scanned) < PURGE_BATCH) return purged;
    }
  }

  async metrics(params: { since: Date; until?: Date }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    // Redis has no native time-range index over task hashes, so we read
    // the task namespace and filter in-process. Acceptable for the modest
    // backlog sizes Redis is typically used with (and `purge()` keeps it
    // bounded); for millions of retained rows, use Postgres.
    const sinceMs = params.since.getTime();
    const untilMs = (params.until ?? this.clock.now()).getTime();
    const inWindow = (raw: string | undefined): boolean => {
      if (!raw) return false;
      const t = new Date(raw).getTime();
      return t >= sinceMs && t <= untilMs;
    };

    let pending = 0;
    let running = 0;
    let completed = 0;
    let failed = 0;
    let waitSum = 0;
    let waitN = 0;
    let execSum = 0;
    const execTimes: number[] = [];

    const taskKeys = await this.redis.keys(`${this.prefix}:task:*`);
    for (const key of taskKeys) {
      const h = await this.redis.hgetall(key);
      if (!h) continue;
      const status = h.status;
      if (status === "pending" && inWindow(h.createdAt)) {
        pending++;
      } else if (status === "running" && inWindow(h.claimedAt)) {
        running++;
      } else if ((status === "completed" || status === "failed") && inWindow(h.completedAt)) {
        if (status === "completed") completed++;
        else failed++;
        if (h.createdAt && h.claimedAt) {
          waitSum += new Date(h.claimedAt).getTime() - new Date(h.createdAt).getTime();
          waitN++;
        }
        if (h.durationMs) {
          const d = Number(h.durationMs);
          if (Number.isFinite(d)) {
            execSum += d;
            execTimes.push(d);
          }
        }
      }
    }

    const terminalN = execTimes.length;
    return {
      pending,
      running,
      completed,
      failed,
      avgWaitMs: waitN > 0 ? waitSum / waitN : 0,
      avgExecMs: terminalN > 0 ? execSum / terminalN : 0,
      p95ExecMs: terminalN > 0 ? percentileCont({ values: execTimes, p: 0.95 }) : 0,
    };
  }

  // -- Internal helpers ------------------------------------------------------

  private async settle(params: {
    taskId: string;
    claimToken: string | undefined;
    status: "completed" | "failed";
    field: "result" | "error";
    value: string;
    durationMs: number;
  }): Promise<boolean> {
    const now = this.clock.now();
    const ok = await this.redis.eval(
      SETTLE_LUA,
      3,
      this.taskKey(params.taskId),
      this.runningKey,
      this.doneKey,
      params.taskId,
      params.claimToken ?? "",
      params.status,
      params.field,
      params.value,
      String(params.durationMs),
      now.toISOString(),
      this.prefix,
      String(now.getTime()),
    );
    return ok === 1;
  }

  /** Run REQUEUE_LUA; returns 0 (no-op), 1 (pending again), 2 (dead-lettered). */
  private async requeue(params: {
    id: string;
    claimToken: string;
    mode: "requeue" | "release";
  }): Promise<number> {
    const now = this.clock.now();
    const code = await this.redis.eval(
      REQUEUE_LUA,
      4,
      this.taskKey(params.id),
      this.runningKey,
      this.pendingKey,
      this.doneKey,
      params.id,
      this.prefix,
      params.claimToken,
      params.mode,
      String(this.maxDeliveries),
      deadLetterError(this.maxDeliveries),
      now.toISOString(),
      String(now.getTime()),
    );
    return Number(code);
  }

  private parseRecord(map: Record<string, string>): StepTaskRecord | undefined {
    if (!map.id) return undefined;
    // Optional fields are only set when present so a missing value
    // round-trips as `undefined`, not the empty string the Lua scripts use
    // to clear a field.
    const opt = (k: string): string | undefined => (map[k] ? map[k] : undefined);
    const json = (k: string): unknown => (map[k] ? JSON.parse(map[k]!) : undefined);
    const date = (k: string): Date | undefined => (map[k] ? new Date(map[k]!) : undefined);
    const record: { -readonly [K in keyof StepTaskRecord]: StepTaskRecord[K] } = {
      id: map.id,
      workflowId: map.workflowId ?? "",
      stepName: map.stepName ?? "",
      needs: map.needs ? (JSON.parse(map.needs) as string[]) : [],
      priority: parseInt(map.priority ?? "5", 10),
      input: map.input ? JSON.parse(map.input) : {},
      prevResults: map.prevResults ? JSON.parse(map.prevResults) : {},
      attempt: parseInt(map.attempt || "1", 10),
      deliveries: parseInt(map.deliveries || "0", 10),
      status: (map.status ?? "pending") as StepTaskStatus,
      createdAt: new Date(map.createdAt ?? this.clock.currentTimeMs()),
    };
    if (opt("version")) record.version = map.version;
    if (opt("claimToken")) record.claimToken = map.claimToken;
    if (opt("metadata")) record.metadata = json("metadata") as Record<string, unknown>;
    if (opt("concurrencyKey")) record.concurrencyKey = map.concurrencyKey;
    if (opt("concurrencyScope")) record.concurrencyScope = map.concurrencyScope;
    if (opt("concurrencyLimit")) record.concurrencyLimit = Number(map.concurrencyLimit);
    if (opt("claimedBy")) record.claimedBy = map.claimedBy;
    if (opt("claimedAt")) record.claimedAt = date("claimedAt");
    if (opt("heartbeatAt")) record.heartbeatAt = date("heartbeatAt");
    if (opt("completedAt")) record.completedAt = date("completedAt");
    if (opt("result")) record.result = json("result");
    if (opt("error")) record.error = map.error;
    if (opt("durationMs")) record.durationMs = Number(map.durationMs);
    return record;
  }
}

function hashArrayToMap(arr: string[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) map[arr[i]!] = arr[i + 1]!;
  return map;
}

/** The claim-facing subset of a record. */
function toTask(r: StepTaskRecord): StepTask {
  const {
    claimedBy: _by,
    claimedAt: _at,
    heartbeatAt: _hb,
    completedAt: _done,
    result: _res,
    error: _err,
    durationMs: _dur,
    ...task
  } = r;
  return task;
}
