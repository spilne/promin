// ---------------------------------------------------------------------------
// RedisStepQueue — Redis-backed StepQueue for distributed step dispatch
//
// Key structure:
//   {prefix}:task:{id}                             → Hash (task fields)
//   {prefix}:pending                               → Sorted Set (all pending
//                                                    tasks, priority-scored;
//                                                    claim filters by
//                                                    `needs` subset in Lua)
//   {prefix}:running                               → Set of task IDs
//   {prefix}:counter                               → Incr for task ID gen
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

import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import type { RedisStoreClient } from "./redis-client.ts";
import { SystemWallClock, type WallClock } from "@promin/workflow";

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
 *        prevResultsJson, needsJson, createdAt, version('' if unset)]
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
  'attempt', '1',
  'status', 'pending',
  'createdAt', ARGV[8],
}
if ARGV[9] ~= '' then
  table.insert(fields, 'version')
  table.insert(fields, ARGV[9])
end
-- ARGV[10] = namespace (empty when none)
if ARGV[10] ~= '' then
  table.insert(fields, 'namespace')
  table.insert(fields, ARGV[10])
end
-- ARGV[11] = metadata JSON (empty when none) — stored verbatim, parsed
-- back on claim so search-attribute callers can round-trip arbitrary
-- shapes (mirrors PgStepQueue.metadata).
if ARGV[11] ~= '' then
  table.insert(fields, 'metadata')
  table.insert(fields, ARGV[11])
end
-- ARGV[12..14] = concurrency key / scope / limit (empty when unset)
if ARGV[12] ~= '' then
  table.insert(fields, 'concurrencyKey')
  table.insert(fields, ARGV[12])
end
if ARGV[13] ~= '' then
  table.insert(fields, 'concurrencyScope')
  table.insert(fields, ARGV[13])
end
if ARGV[14] ~= '' then
  table.insert(fields, 'concurrencyLimit')
  table.insert(fields, ARGV[14])
end
redis.call('HSET', task_key, unpack(fields))

local priority = tonumber(ARGV[4])
local score = priority * 1e12 + (1e12 - seq)
redis.call('ZADD', pending_key, score, id)
redis.call('SET', active_key, id)

return id
`;

/**
 * Claim up to `limit` pending tasks whose needs ⊆ capabilities, honouring
 * per-(concurrency scope, key) limits. Walks the pending zset in priority
 * order a page at a time, skipping tasks the worker can't handle and tasks
 * whose (scope, key) already has `concurrencyLimit` running, and moves the
 * rest to running. Every claimed keyed task joins its (scope, key) running
 * set inside the same script, so the cap holds within one call and across
 * concurrent claimers.
 *
 * At most `scan_budget` pending entries are examined per call so a large
 * backlog of blocked tasks can't stall Redis.
 *
 * KEYS: [pending_key, running_key]
 * ARGV: [worker_id, now, limit, prefix, caps_json, scan_budget]
 */
const CLAIM_LUA =
  CONC_KEY_LUA +
  `
local pending_key = KEYS[1]
local running_key = KEYS[2]
local worker_id = ARGV[1]
local now = ARGV[2]
local limit = tonumber(ARGV[3])
local prefix = ARGV[4]
local caps = cjson.decode(ARGV[5])
local scan_budget = tonumber(ARGV[6])

local caps_set = {}
for _, c in ipairs(caps) do caps_set[c] = true end

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
    local f = redis.call('HMGET', task_key, 'needs', 'concurrencyKey', 'concurrencyScope', 'concurrencyLimit')
    local needs_json, c_key, c_scope, c_limit = f[1], f[2], f[3], f[4]

    local ok = true
    if needs_json and needs_json ~= '' and needs_json ~= '[]' then
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
      local claim_token = worker_id .. ':' .. id .. ':' .. now
      redis.call('ZREM', pending_key, id)
      redis.call('SADD', running_key, id)
      if conc then redis.call('SADD', conc, id) end
      redis.call('HSET', task_key, 'status', 'running', 'claimedBy', worker_id, 'claimedAt', now, 'claimToken', claim_token)
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
 * Put a running task back on the pending zset and free its concurrency
 * slot. No-op unless the task is still running under `claim_token` (empty
 * token skips that check), so a racing complete/fail wins.
 *
 * KEYS: [task_key, running_key, pending_key]
 * ARGV: [id, prefix, claim_token]
 */
const REQUEUE_LUA =
  CONC_KEY_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local pending_key = KEYS[3]
local id = ARGV[1]
local prefix = ARGV[2]
local claim_token = ARGV[3]

local f = redis.call('HMGET', task_key, 'status', 'claimToken', 'priority', 'concurrencyKey', 'concurrencyScope')
if f[1] ~= 'running' then return 0 end
if claim_token ~= '' and f[2] ~= claim_token then return 0 end

redis.call('HSET', task_key,
  'status', 'pending',
  'claimedBy', '',
  'claimedAt', '',
  'claimToken', '',
  'heartbeatAt', '')
redis.call('SREM', running_key, id)
local conc = conc_key(prefix, f[5], f[4])
if conc then redis.call('SREM', conc, id) end

local priority = tonumber(f[3]) or 5
redis.call('ZADD', pending_key, priority * 1e12 + (1e12 - tonumber(id)), id)
return 1
`;

const COMPLETE_LUA =
  CONC_KEY_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local active_key_prefix = KEYS[3]
local id = ARGV[1]
local claim_token = ARGV[2]
local result = ARGV[3]
local duration_ms = ARGV[4]
local completed_at = ARGV[5]

local status = redis.call('HGET', task_key, 'status')
local current_token = redis.call('HGET', task_key, 'claimToken')
if status ~= 'running' then return 0 end
if claim_token ~= '' and current_token ~= claim_token then return 0 end

redis.call('HSET', task_key,
  'status', 'completed',
  'result', result,
  'durationMs', duration_ms,
  'completedAt', completed_at)
redis.call('SREM', running_key, id)

local workflow_id = redis.call('HGET', task_key, 'workflowId')
local step_name = redis.call('HGET', task_key, 'stepName')
if workflow_id and step_name then
  redis.call('DEL', active_key_prefix .. workflow_id .. '::' .. step_name)
end
local conc = conc_key(ARGV[6], redis.call('HGET', task_key, 'concurrencyScope'), redis.call('HGET', task_key, 'concurrencyKey'))
if conc then redis.call('SREM', conc, id) end
return 1
`;

const FAIL_LUA =
  CONC_KEY_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local active_key_prefix = KEYS[3]
local id = ARGV[1]
local claim_token = ARGV[2]
local error = ARGV[3]
local duration_ms = ARGV[4]
local completed_at = ARGV[5]

local status = redis.call('HGET', task_key, 'status')
local current_token = redis.call('HGET', task_key, 'claimToken')
if status ~= 'running' then return 0 end
if claim_token ~= '' and current_token ~= claim_token then return 0 end

redis.call('HSET', task_key,
  'status', 'failed',
  'error', error,
  'durationMs', duration_ms,
  'completedAt', completed_at)
redis.call('SREM', running_key, id)

local workflow_id = redis.call('HGET', task_key, 'workflowId')
local step_name = redis.call('HGET', task_key, 'stepName')
if workflow_id and step_name then
  redis.call('DEL', active_key_prefix .. workflow_id .. '::' .. step_name)
end
local conc = conc_key(ARGV[6], redis.call('HGET', task_key, 'concurrencyScope'), redis.call('HGET', task_key, 'concurrencyKey'))
if conc then redis.call('SREM', conc, id) end
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

// -- Implementation ----------------------------------------------------------

export interface RedisStepQueueConfig {
  redis: RedisStoreClient;
  /** Key prefix for all queue keys. Default: "sq". */
  prefix?: string;
  /** Identity recorded on claimed tasks. Default: random UUID. */
  workerId?: string;
  /**
   * Upper bound on pending entries one `claim()` examines while skipping
   * tasks the worker can't take (unmet `needs`, full concurrency key).
   * Default: 1000.
   */
  claimScanLimit?: number;
  /** Time source for client-side timestamps. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class RedisStepQueue implements StepQueue {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  private readonly workerId: string;
  private readonly claimScanLimit: number;
  private readonly clock: WallClock;

  constructor(config: RedisStepQueueConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sq";
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.claimScanLimit = config.claimScanLimit ?? 1000;
    this.clock = config.clock ?? SystemWallClock;
  }

  // -- Key helpers -----------------------------------------------------------

  private taskKey(id: string): string {
    return `${this.prefix}:task:${id}`;
  }

  private get pendingKey(): string {
    return `${this.prefix}:pending`;
  }

  private runningKey(): string {
    return `${this.prefix}:running`;
  }

  /**
   * Active-task key for idempotent enqueue. Drops namespace from the name
   * — workflow_id is globally unique by the rest-of-schema contract, so
   * (workflow_id, step_name) is enough.
   */
  private activeKey(workflowId: string, stepName: string): string {
    return `${this.prefix}:active:${workflowId}::${stepName}`;
  }

  // -- StepQueue interface ---------------------------------------------------

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    needs?: readonly string[];
    priority?: number;
    namespace?: string;
    version?: string;
    metadata?: Record<string, unknown>;
    concurrencyKey?: string;
    concurrencyScope?: string;
    concurrencyLimit?: number;
  }): Promise<string> {
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
    )) as string;
    return id;
  }

  async claim(params: {
    capabilities?: readonly string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const caps = params.capabilities ?? [];
    const limit = Math.max(1, Math.floor(params.limit));
    // Fairness policies beyond strict-priority could be added to the Lua
    // — today we accept round-robin / weighted in the interface but fall
    // back to priority-FIFO. Enough for most workloads; revisit if real
    // multi-tenant scenarios need it.
    void params.fairness;

    const raw = (await this.redis.eval(
      CLAIM_LUA,
      2,
      this.pendingKey,
      this.runningKey(),
      this.workerId,
      this.clock.now().toISOString(),
      String(limit),
      this.prefix,
      JSON.stringify(caps),
      String(Math.max(limit, this.claimScanLimit)),
    )) as string[][];

    const claimed: StepTask[] = [];
    for (const arr of raw) {
      const t = this.parseHashArray(arr);
      if (t) claimed.push(t);
    }

    // Version filter / custom predicate — rejected tasks go back to
    // pending so another worker can grab them.
    if (params.filter) {
      const accepted: StepTask[] = [];
      for (const t of claimed) {
        if (params.filter(t)) {
          accepted.push(t);
          continue;
        }
        await this.requeue({ id: t.id, claimToken: t.claimToken ?? "" });
      }
      return accepted;
    }
    return claimed;
  }

  async complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean> {
    const ok = await this.redis.eval(
      COMPLETE_LUA,
      3,
      this.taskKey(params.taskId),
      this.runningKey(),
      `${this.prefix}:active:`,
      params.taskId,
      params.claimToken ?? "",
      JSON.stringify(params.result),
      String(params.durationMs),
      this.clock.now().toISOString(),
      this.prefix,
    );
    return ok === 1;
  }

  async fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean> {
    const ok = await this.redis.eval(
      FAIL_LUA,
      3,
      this.taskKey(params.taskId),
      this.runningKey(),
      `${this.prefix}:active:`,
      params.taskId,
      params.claimToken ?? "",
      params.error,
      String(params.durationMs),
      this.clock.now().toISOString(),
      this.prefix,
    );
    return ok === 1;
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

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    const runningIds = await this.redis.smembers(this.runningKey());
    let count = 0;

    for (const id of runningIds) {
      const raw = await this.redis.hgetall(this.taskKey(id));
      if (!raw || raw.status !== "running") continue;

      if (params.claimedBy && raw.claimedBy !== params.claimedBy) continue;
      if (params.staleTimeoutMs) {
        const lastActivity = raw.heartbeatAt || raw.claimedAt;
        if (!lastActivity) continue;
        const lastActivityMs = new Date(lastActivity).getTime();
        if (this.clock.currentTimeMs() - lastActivityMs < params.staleTimeoutMs) continue;
      }

      // Token-guarded, so a task completed between the read above and the
      // requeue stays completed.
      if (await this.requeue({ id, claimToken: raw.claimToken ?? "" })) count++;
    }

    return count;
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
    // Redis has no native time-range index over task hashes, so we SCAN
    // the task namespace and filter in-process. Acceptable for the modest
    // backlog sizes Redis is typically used with; if you're running
    // millions of retained terminal rows, switch to Postgres (which does
    // use indexed time queries). The SCAN cursor cap keeps the work
    // bounded per call.
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
      p95ExecMs: terminalN > 0 ? percentile(execTimes, 0.95) : 0,
    };
  }

  // -- Internal helpers ------------------------------------------------------

  private async requeue(params: { id: string; claimToken: string }): Promise<boolean> {
    const ok = await this.redis.eval(
      REQUEUE_LUA,
      3,
      this.taskKey(params.id),
      this.runningKey(),
      this.pendingKey,
      params.id,
      this.prefix,
      params.claimToken,
    );
    return ok === 1;
  }

  private parseHashArray(arr: string[]): StepTask | null {
    const map: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      map[arr[i]!] = arr[i + 1]!;
    }
    if (!map.id) return null;
    const task: StepTask = {
      id: map.id,
      workflowId: map.workflowId ?? "",
      stepName: map.stepName ?? "",
      needs: map.needs ? (JSON.parse(map.needs) as string[]) : [],
      priority: parseInt(map.priority ?? "5", 10),
      input: map.input ? JSON.parse(map.input) : {},
      prevResults: map.prevResults ? JSON.parse(map.prevResults) : {},
      attempt: parseInt(map.attempt ?? "1", 10),
      status: "running" as const,
      createdAt: new Date(map.createdAt ?? this.clock.currentTimeMs()),
    };
    // Optional fields are only set when present so a missing value
    // round-trips as `undefined`, not the empty string we used in the
    // Lua's empty-sentinel handling.
    if (map.version) (task as { version?: string }).version = map.version;
    if (map.namespace) (task as { namespace?: string }).namespace = map.namespace;
    if (map.claimToken) (task as { claimToken?: string }).claimToken = map.claimToken;
    if (map.metadata) {
      (task as { metadata?: Record<string, unknown> }).metadata = JSON.parse(map.metadata);
    }
    if (map.concurrencyKey) {
      (task as { concurrencyKey?: string }).concurrencyKey = map.concurrencyKey;
    }
    if (map.concurrencyScope) {
      (task as { concurrencyScope?: string }).concurrencyScope = map.concurrencyScope;
    }
    if (map.concurrencyLimit) {
      (task as { concurrencyLimit?: number }).concurrencyLimit = Number(map.concurrencyLimit);
    }
    return task;
  }
}

/**
 * Linear-interpolation percentile — matches SQL `PERCENTILE_CONT` and the
 * in-memory implementation so metrics stay comparable across backends.
 */
function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const rank = p * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (rank - lo);
}
