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
//                                                    enqueue (promin-k6mk)
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";

// -- Lua scripts -------------------------------------------------------------

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
redis.call('HSET', task_key, unpack(fields))

local priority = tonumber(ARGV[4])
local score = priority * 1e12 + (1e12 - seq)
redis.call('ZADD', pending_key, score, id)
redis.call('SET', active_key, id)

return id
`;

/**
 * Claim up to `limit` pending tasks whose needs ⊆ capabilities. ZREVRANGE
 * the pending zset (priority-ordered), parse each candidate's needs, skip
 * tasks the worker can't handle, and move qualifying ones to running.
 *
 * `caps_json` is a JSON array of the worker's capabilities; we decode it
 * once at the top and use a set-membership check per need.
 *
 * KEYS: [pending_key, running_key]
 * ARGV: [worker_id, now, limit, task_key_prefix, caps_json]
 */
const CLAIM_LUA = `
local pending_key = KEYS[1]
local running_key = KEYS[2]
local worker_id = ARGV[1]
local now = ARGV[2]
local limit = tonumber(ARGV[3])
local prefix = ARGV[4]
local caps = cjson.decode(ARGV[5])

local caps_set = {}
for _, c in ipairs(caps) do caps_set[c] = true end

-- Scan more than limit so we can skip tasks whose needs aren't met.
-- Scan size grows linearly with limit — fine for the typical load where
-- needs mismatches are a minority.
local scan = limit * 4
if scan < 16 then scan = 16 end

local candidates = redis.call('ZREVRANGE', pending_key, 0, scan - 1)
local results = {}

for _, id in ipairs(candidates) do
  if #results >= limit then break end
  local task_key = prefix .. ':task:' .. id
  local needs_json = redis.call('HGET', task_key, 'needs')
  local ok = true
  if needs_json and needs_json ~= '' and needs_json ~= '[]' then
    local needs = cjson.decode(needs_json)
    for _, n in ipairs(needs) do
      if not caps_set[n] then
        ok = false
        break
      end
    end
  end
  if ok then
    redis.call('ZREM', pending_key, id)
    redis.call('SADD', running_key, id)
    redis.call('HSET', task_key, 'status', 'running', 'claimedBy', worker_id, 'claimedAt', now)
    local task = redis.call('HGETALL', task_key)
    table.insert(results, task)
  end
end

return results
`;

// -- Implementation ----------------------------------------------------------

export class RedisStepQueue implements StepQueue {
  private readonly prefix: string;
  private readonly workerId: string;

  constructor(
    private readonly redis: RedisClient,
    config?: {
      prefix?: string;
      workerId?: string;
    },
  ) {
    this.prefix = config?.prefix ?? "sq";
    this.workerId = config?.workerId ?? crypto.randomUUID();
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
      new Date().toISOString(),
      params.version ?? "",
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
      new Date().toISOString(),
      String(limit),
      this.prefix,
      JSON.stringify(caps),
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
        await this.redis.srem(this.runningKey(), t.id);
        await this.redis.hset(this.taskKey(t.id), {
          status: "pending",
          claimedBy: "",
          claimedAt: "",
        });
        const score = t.priority * 1e12 + (1e12 - Number(t.id));
        await this.redis.zadd(this.pendingKey, score, t.id);
      }
      return accepted;
    }
    return claimed;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const hash = await this.redis.hgetall(this.taskKey(params.taskId));
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "completed",
      result: JSON.stringify(params.result),
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    // Bump a flat counter for metrics() — no per-queue breakdown now.
    await this.redis.incr(`${this.prefix}:counter:completed`);
    if (hash?.workflowId && hash.stepName) {
      await this.redis.del(this.activeKey(hash.workflowId, hash.stepName));
    }
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const hash = await this.redis.hgetall(this.taskKey(params.taskId));
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "failed",
      error: params.error,
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    await this.redis.incr(`${this.prefix}:counter:failed`);
    if (hash?.workflowId && hash.stepName) {
      await this.redis.del(this.activeKey(hash.workflowId, hash.stepName));
    }
  }

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    const runningIds = await this.redis.smembers(this.runningKey());
    let count = 0;

    for (const id of runningIds) {
      const raw = await this.redis.hgetall(this.taskKey(id));
      if (!raw || raw.status !== "running") continue;

      if (params.claimedBy && raw.claimedBy !== params.claimedBy) continue;
      if (params.staleTimeoutMs && raw.claimedAt) {
        const claimedAt = new Date(raw.claimedAt).getTime();
        if (Date.now() - claimedAt < params.staleTimeoutMs) continue;
      }

      const priority = parseInt(raw.priority ?? "5", 10);
      const score = priority * 1e12 + (1e12 - Number(id));
      await this.redis.hset(this.taskKey(id), { status: "pending", claimedBy: "", claimedAt: "" });
      await this.redis.srem(this.runningKey(), id);
      await this.redis.zadd(this.pendingKey, score, id);
      count++;
    }

    return count;
  }

  async metrics(): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
  }> {
    const [pending, running, completedRaw, failedRaw] = await Promise.all([
      this.redis.zcard(this.pendingKey),
      (async () => (await this.redis.smembers(this.runningKey())).length)(),
      this.redis.get(`${this.prefix}:counter:completed`),
      this.redis.get(`${this.prefix}:counter:failed`),
    ]);
    return {
      pending,
      running,
      completed: completedRaw ? Number(completedRaw) : 0,
      failed: failedRaw ? Number(failedRaw) : 0,
    };
  }

  // -- Internal helpers ------------------------------------------------------

  private parseHashArray(arr: string[]): StepTask | null {
    const map: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      map[arr[i]!] = arr[i + 1]!;
    }
    if (!map.id) return null;
    return {
      id: map.id,
      workflowId: map.workflowId ?? "",
      stepName: map.stepName ?? "",
      needs: map.needs ? (JSON.parse(map.needs) as string[]) : [],
      priority: parseInt(map.priority ?? "5", 10),
      input: map.input ? JSON.parse(map.input) : {},
      prevResults: map.prevResults ? JSON.parse(map.prevResults) : {},
      attempt: parseInt(map.attempt ?? "1", 10),
      status: "running" as const,
      createdAt: new Date(map.createdAt ?? Date.now()),
      version: map.version,
    };
  }
}
