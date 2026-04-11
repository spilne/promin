import type { Singleflight } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

/**
 * Distributed singleflight backed by Redis.
 *
 * When multiple processes call `doAsync` with the same key concurrently,
 * only one executes `fn` — the rest block on BRPOP waiting for the result.
 *
 * Protocol:
 * - Winner: `SET {prefix}:{key} {requestId} NX PX {timeoutMs}`
 * - Joiners: `BRPOP {prefix}:{key}:result {timeoutSec}`
 * - Winner publishes: `RPUSH {prefix}:{key}:result {payload}`
 * - Joiners re-publish for subsequent joiners
 *
 * @example
 * ```ts
 * const sf = RedisSingleflight.make({ redis });
 * const [a, b] = await Promise.all([
 *   sf.doAsync("user:1", () => fetchUser(1)),
 *   sf.doAsync("user:1", () => fetchUser(1)),
 * ]);
 * // fetchUser called only once across all processes
 * ```
 */
export class RedisSingleflight implements Singleflight {
  private constructor(
    private readonly redis: RedisClient,
    private readonly prefix: string,
    private readonly timeoutMs: number,
  ) {}

  static make(params: {
    redis: RedisClient;
    prefix?: string;
    timeoutMs?: number;
  }): RedisSingleflight {
    return new RedisSingleflight(params.redis, params.prefix ?? "sf", params.timeoutMs ?? 30_000);
  }

  async doAsync<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const lockKey = `${this.prefix}:${key}`;
    const resultKey = `${this.prefix}:${key}:result`;
    const requestId = crypto.randomUUID();

    // Try to become the winner
    const acquired = await this.redis.set(lockKey, requestId, "NX", "PX", this.timeoutMs);

    if (acquired) {
      // Winner — execute and broadcast result
      try {
        const value = await fn();
        const payload = JSON.stringify({ error: false, value });
        await this.redis.rpush(resultKey, payload);
        await this.redis.pexpire(resultKey, 5000);
        await this.redis.del(lockKey);
        return value;
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        const payload = JSON.stringify({ error: true, message });
        await this.redis.rpush(resultKey, payload);
        await this.redis.pexpire(resultKey, 5000);
        await this.redis.del(lockKey);
        throw err;
      }
    }

    // Joiner — wait for result
    const timeoutSec = Math.ceil(this.timeoutMs / 1000);
    const result = await this.redis.brpop(resultKey, timeoutSec);
    if (!result) {
      throw new Error(`Singleflight timeout waiting for key: ${key}`);
    }

    const payload = JSON.parse(result[1]) as { error: boolean; value?: T; message?: string };
    // Re-publish for other joiners
    await this.redis.rpush(resultKey, result[1]);
    await this.redis.pexpire(resultKey, 5000);

    if (payload.error) {
      throw new Error(payload.message);
    }
    return payload.value as T;
  }
}
