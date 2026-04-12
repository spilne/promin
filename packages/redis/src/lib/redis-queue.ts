import type { AsyncQueue } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

/**
 * Distributed bounded queue with backpressure backed by Redis.
 * Uses a List for data and Lua scripts for atomic capacity checks.
 *
 * Items are pushed left (LPUSH) and popped right (BRPOP) for FIFO order.
 * `takeAsync` blocks on a dedicated connection via BRPOP.
 * `offerAsync` rejects when the queue is at capacity.
 *
 * @example
 * ```ts
 * const jobs = RedisQueue.make<Job>({ redis, key: "work:jobs", capacity: 100 });
 * await jobs.offerAsync({ id: "j-1" });
 * const next = await jobs.takeAsync();
 * ```
 */
export class RedisQueue<T> implements AsyncQueue<T> {
  private _shutdown = false;

  private constructor(
    private readonly redis: RedisClient,
    private readonly dataKey: string,
    private readonly capacity: number,
    private readonly timeoutMs: number,
  ) {}

  static make<T>(params: {
    redis: RedisClient;
    key: string;
    capacity?: number;
    timeoutMs?: number;
  }): RedisQueue<T> {
    return new RedisQueue(
      params.redis,
      params.key,
      params.capacity ?? 256,
      params.timeoutMs ?? 30_000,
    );
  }

  async offerAsync(item: T): Promise<void> {
    if (this._shutdown) throw new Error("Queue is shut down");

    const OFFER_SCRIPT = `
      local len = redis.call('LLEN', KEYS[1])
      if len >= tonumber(ARGV[1]) then return 0 end
      redis.call('LPUSH', KEYS[1], ARGV[2])
      return 1
    `;
    const result = await this.redis.eval(
      OFFER_SCRIPT,
      1,
      this.dataKey,
      this.capacity,
      JSON.stringify(item),
    );
    if (result === 0) throw new Error("Queue is full");
  }

  async takeAsync(): Promise<T> {
    if (this._shutdown) throw new Error("Queue is shut down");

    // Block on dedicated connection via BRPOP (right side = oldest item = FIFO)
    const sub = await this.redis.duplicate();
    try {
      const timeoutSec = Math.ceil(this.timeoutMs / 1000);
      const result = await sub.brpop(this.dataKey, timeoutSec);
      if (!result) throw new Error(`Queue take timeout after ${this.timeoutMs}ms`);
      // brpop returns [key, value]
      const raw = Array.isArray(result) ? result[1] : result;
      return JSON.parse(raw as string) as T;
    } finally {
      if (sub.disconnect) sub.disconnect();
      else sub.close?.();
    }
  }

  async shutdownAsync(): Promise<void> {
    this._shutdown = true;
  }

  async sizeAsync(): Promise<number> {
    const len = await this.redis.llen(this.dataKey);
    return len;
  }
}
