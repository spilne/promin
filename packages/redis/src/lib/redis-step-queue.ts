// ---------------------------------------------------------------------------
// RedisStepQueue — Redis-backed StepQueue for distributed step dispatch
//
// Key structure:
//   {prefix}:task:{id}                             → Hash (task fields)
//   {prefix}:pending:{queue}                       → Sorted Set (priority+FIFO)
//   {prefix}:running                               → Set of task IDs
//   {prefix}:counter                               → Incr for task ID generation
//   {prefix}:active:{ns}::{wfId}::{stepName}       → taskId of active
//                                                    (pending/running) task;
//                                                    drives idempotent enqueue
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";

// -- Lua scripts -------------------------------------------------------------

/**
 * Atomically enqueue a task with idempotency on (namespace, workflowId,
 * stepName) — promin-k6mk. If an active task (key `{prefix}:active:...`
 * exists) is already pending or running for the same triple, return its
 * id without creating a new row. Otherwise INCR the counter, HSET the
 * task hash, ZADD to the pending set, and SET the active key.
 *
 * KEYS: [active_key, pending_key, counter_key]
 * ARGV: [task_key_prefix, workflowId, stepName, queue, priority,
 *        inputJson, prevResultsJson, namespace, createdAt, version('' if unset)]
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
  'queue', ARGV[4],
  'priority', ARGV[5],
  'input', ARGV[6],
  'prevResults', ARGV[7],
  'attempt', '1',
  'status', 'pending',
  'namespace', ARGV[8],
  'createdAt', ARGV[9],
}
if ARGV[10] ~= '' then
  table.insert(fields, 'version')
  table.insert(fields, ARGV[10])
end
redis.call('HSET', task_key, unpack(fields))

local priority = tonumber(ARGV[5])
local score = priority * 1e12 + (1e12 - seq)
redis.call('ZADD', pending_key, score, id)
redis.call('SET', active_key, id)

return id
`;

/**
 * Atomically claim tasks: ZREVRANGE top N from pending set, move to running,
 * update task hash status, and return full task hashes.
 */
const CLAIM_LUA = `
local pending_key = KEYS[1]
local running_key = KEYS[2]
local worker_id = ARGV[1]
local now = ARGV[2]
local limit = tonumber(ARGV[3])
local prefix = ARGV[4]

local ids = redis.call('ZREVRANGE', pending_key, 0, limit - 1)
local results = {}

for _, id in ipairs(ids) do
  redis.call('ZREM', pending_key, id)
  redis.call('SADD', running_key, id)
  local task_key = prefix .. ':task:' .. id
  redis.call('HSET', task_key, 'status', 'running', 'claimedBy', worker_id, 'claimedAt', now)
  local task = redis.call('HGETALL', task_key)
  table.insert(results, task)
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

  private pendingKey(queue: string): string {
    return `${this.prefix}:pending:${queue}`;
  }

  private runningKey(): string {
    return `${this.prefix}:running`;
  }

  private completedKey(queue: string): string {
    return `${this.prefix}:completed:${queue}`;
  }

  private failedKey(queue: string): string {
    return `${this.prefix}:failed:${queue}`;
  }

  /**
   * Active-task key for idempotent enqueue. Namespace is defaulted to '' so
   * NULL/empty-string collapse into the same bucket (matches the Postgres
   * backend's COALESCE(namespace, '') behaviour).
   */
  private activeKey(namespace: string, workflowId: string, stepName: string): string {
    return `${this.prefix}:active:${namespace}::${workflowId}::${stepName}`;
  }

  // -- StepQueue interface ---------------------------------------------------

  async enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    priority?: number;
    namespace?: string;
    version?: string;
  }): Promise<string> {
    const namespace = params.namespace ?? "";
    const priority = params.priority ?? 5;
    // Single Lua round-trip does the "check active → maybe insert" atomically.
    // Returns either the existing task id (dedup hit) or the freshly allocated
    // one — caller sees a uniform string either way, matching the interface
    // contract.
    const id = (await this.redis.eval(
      ENQUEUE_LUA,
      3,
      this.activeKey(namespace, params.workflowId, params.stepName),
      this.pendingKey(params.queue),
      `${this.prefix}:counter`,
      `${this.prefix}:task:`,
      params.workflowId,
      params.stepName,
      params.queue,
      String(priority),
      JSON.stringify(params.input),
      JSON.stringify(params.prevResults),
      namespace,
      new Date().toISOString(),
      params.version ?? "",
    )) as string;
    return id;
  }

  async claim(params: {
    queues: string[];
    limit: number;
    fairness?: FairnessPolicy;
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]> {
    const claimed: StepTask[] = [];
    const now = new Date().toISOString();

    for (const queue of params.queues) {
      if (claimed.length >= params.limit) break;
      const remaining = params.limit - claimed.length;

      const result = await this.redis.eval(
        CLAIM_LUA,
        2,
        this.pendingKey(queue),
        this.runningKey(),
        this.workerId,
        now,
        remaining,
        this.prefix,
      );

      if (Array.isArray(result)) {
        for (const taskData of result) {
          if (Array.isArray(taskData)) {
            const task = this.parseHashArray(taskData as string[]);
            if (task) claimed.push(task);
          }
        }
      }
    }

    const sorted = claimed.sort(
      (a, b) => b.priority - a.priority || a.createdAt.getTime() - b.createdAt.getTime(),
    );

    // Apply filter AFTER the Lua claim. Rejected tasks are released back to
    // pending (the Lua script already moved them to running + updated the
    // task hash — unwind those mutations here).
    if (params.filter) {
      const accepted: StepTask[] = [];
      for (const t of sorted) {
        if (params.filter(t)) {
          accepted.push(t);
          continue;
        }
        // Release: remove from running set + restore pending status + put
        // back on the pending zset with original priority score.
        await this.redis.srem(this.runningKey(), t.id);
        await this.redis.hset(this.taskKey(t.id), {
          status: "pending",
          claimedBy: "",
          claimedAt: "",
        });
        // Score mirrors enqueue(): priority*1e12 + offset. We don't have the
        // original seq; use the id (monotonic via INCR) as a stand-in.
        const seq = Number(t.id);
        const score = t.priority * 1e12 + (1e12 - seq);
        await this.redis.zadd(this.pendingKey(t.queue), score, t.id);
      }
      return accepted;
    }

    return sorted;
  }

  async complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void> {
    const hash = await this.redis.hgetall(this.taskKey(params.taskId));
    const queue = hash?.queue;
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "completed",
      result: JSON.stringify(params.result),
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    if (queue) await this.redis.sadd(this.completedKey(queue), params.taskId);
    // Free the active-key dedupe slot so a retry / fresh-run enqueue can go
    // through. Namespace defaults to '' to match the enqueue path's bucket.
    if (hash?.workflowId && hash.stepName) {
      await this.redis.del(this.activeKey(hash.namespace ?? "", hash.workflowId, hash.stepName));
    }
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const hash = await this.redis.hgetall(this.taskKey(params.taskId));
    const queue = hash?.queue;
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "failed",
      error: params.error,
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    if (queue) await this.redis.sadd(this.failedKey(queue), params.taskId);
    if (hash?.workflowId && hash.stepName) {
      await this.redis.del(this.activeKey(hash.namespace ?? "", hash.workflowId, hash.stepName));
    }
  }

  async requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number> {
    const runningIds = await this.redis.smembers(this.runningKey());
    let count = 0;

    for (const id of runningIds) {
      const raw = await this.redis.hgetall(this.taskKey(id));
      if (!raw || raw.status !== "running") continue;

      // Filter by worker if specified
      if (params.claimedBy && raw.claimedBy !== params.claimedBy) continue;

      // Filter by stale timeout if specified
      if (params.staleTimeoutMs && raw.claimedAt) {
        const claimedAt = new Date(raw.claimedAt).getTime();
        if (Date.now() - claimedAt < params.staleTimeoutMs) continue;
      }

      // Move back from running set to pending sorted set
      const priority = parseInt(raw.priority ?? "5", 10);
      const requeueSeq = await this.redis.incr(`${this.prefix}:counter`);
      const score = priority * 1e12 + (1e12 - requeueSeq);
      await this.redis.hset(this.taskKey(id), { status: "pending", claimedBy: "", claimedAt: "" });
      await this.redis.srem(this.runningKey(), id);
      await this.redis.zadd(this.pendingKey(raw.queue ?? "default"), score, id);
      count++;
    }

    return count;
  }

  async metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  > {
    const result: Record<
      string,
      { pending: number; running: number; completed: number; failed: number }
    > = {};

    // Count pending tasks per queue from sorted sets
    const keys = await this.redis.keys(`${this.prefix}:pending:*`);
    for (const key of keys) {
      const queue = key.slice(`${this.prefix}:pending:`.length);
      const pendingCount = await this.redis.zcard(key);
      if (!result[queue]) result[queue] = { pending: 0, running: 0, completed: 0, failed: 0 };
      result[queue]!.pending = pendingCount;
    }

    // Count running tasks per queue from running set
    const runningIds = await this.redis.smembers(this.runningKey());
    for (const id of runningIds) {
      const queue = await this.redis.hget(this.taskKey(id), "queue");
      if (queue) {
        if (!result[queue]) result[queue] = { pending: 0, running: 0, completed: 0, failed: 0 };
        result[queue]!.running++;
      }
    }

    // Count completed/failed from per-queue tracking sets
    const allQueueKeys = new Set<string>();
    for (const key of keys) allQueueKeys.add(key.slice(`${this.prefix}:pending:`.length));
    const completedKeys = await this.redis.keys(`${this.prefix}:completed:*`);
    for (const key of completedKeys)
      allQueueKeys.add(key.slice(`${this.prefix}:completed:`.length));
    const failedKeys = await this.redis.keys(`${this.prefix}:failed:*`);
    for (const key of failedKeys) allQueueKeys.add(key.slice(`${this.prefix}:failed:`.length));

    for (const queue of allQueueKeys) {
      if (!result[queue]) result[queue] = { pending: 0, running: 0, completed: 0, failed: 0 };
      const cMembers = await this.redis.smembers(this.completedKey(queue));
      result[queue]!.completed = cMembers.length;
      const fMembers = await this.redis.smembers(this.failedKey(queue));
      result[queue]!.failed = fMembers.length;
    }

    return result;
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
      queue: map.queue ?? "default",
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
