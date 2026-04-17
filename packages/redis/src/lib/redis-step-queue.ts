// ---------------------------------------------------------------------------
// RedisStepQueue — Redis-backed StepQueue for distributed step dispatch
//
// Key structure:
//   {prefix}:task:{id}        → Hash (task fields)
//   {prefix}:pending:{queue}  → Sorted Set (score = priority*1e12 + offset for FIFO)
//   {prefix}:running          → Set of task IDs
//   {prefix}:counter          → Incr for task ID generation
// ---------------------------------------------------------------------------

import type { StepQueue, StepTask, FairnessPolicy } from "@promin/workflow";
import type { RedisClient } from "./redis-client.ts";

// -- Lua scripts -------------------------------------------------------------

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
    const seq = await this.redis.incr(`${this.prefix}:counter`);
    const id = String(seq);
    const now = Date.now();
    const priority = params.priority ?? 5;

    // Store task as hash. Redis hashes don't support undefined — omit the
    // version field entirely when unset rather than storing an empty string,
    // so `parseHashArray` can distinguish "no version" from "empty string".
    const fields: Record<string, string> = {
      id,
      workflowId: params.workflowId,
      stepName: params.stepName,
      queue: params.queue,
      priority: String(priority),
      input: JSON.stringify(params.input),
      prevResults: JSON.stringify(params.prevResults),
      attempt: "1",
      status: "pending",
      namespace: params.namespace ?? "",
      createdAt: new Date(now).toISOString(),
    };
    if (params.version !== undefined) fields.version = params.version;
    await this.redis.hset(this.taskKey(id), fields);

    // Score: higher priority = higher score = popped first by ZREVRANGE.
    // Within same priority, lower sequence = earlier enqueue = higher offset = FIFO.
    const score = priority * 1e12 + (1e12 - seq);
    await this.redis.zadd(this.pendingKey(params.queue), score, id);

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
    const queue = await this.redis.hget(this.taskKey(params.taskId), "queue");
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "completed",
      result: JSON.stringify(params.result),
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    if (queue) await this.redis.sadd(this.completedKey(queue), params.taskId);
  }

  async fail(params: { taskId: string; error: string; durationMs: number }): Promise<void> {
    const queue = await this.redis.hget(this.taskKey(params.taskId), "queue");
    await this.redis.hset(this.taskKey(params.taskId), {
      status: "failed",
      error: params.error,
      durationMs: String(params.durationMs),
      completedAt: new Date().toISOString(),
    });
    await this.redis.srem(this.runningKey(), params.taskId);
    if (queue) await this.redis.sadd(this.failedKey(queue), params.taskId);
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
