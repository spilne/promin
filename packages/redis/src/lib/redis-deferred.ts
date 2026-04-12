import type { DeferredValue } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

/**
 * Distributed one-shot synchronization backed by Redis.
 * One process waits, another completes it with a value or error.
 *
 * Uses a value key for the result and a notify list for blocking waiters.
 * Multiple concurrent awaiters are supported via re-publish after BRPOP.
 *
 * @example
 * ```ts
 * const gate = await RedisDeferred.make<Config>({ redis, key: "my:gate" });
 * // Process A:
 * const config = await gate.awaitAsync();
 * // Process B:
 * await gate.succeedAsync(loadedConfig);
 * ```
 */
export class RedisDeferred<T> implements DeferredValue<T> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly valueKey: string,
    private readonly notifyKey: string,
    private readonly timeoutMs: number,
  ) {}

  static make<T>(params: {
    redis: RedisClient;
    key: string;
    timeoutMs?: number;
  }): RedisDeferred<T> {
    return new RedisDeferred(
      params.redis,
      `${params.key}:value`,
      `${params.key}:notify`,
      params.timeoutMs ?? 30_000,
    );
  }

  async succeedAsync(value: T): Promise<boolean> {
    const SUCCEED_SCRIPT = `
      local exists = redis.call('EXISTS', KEYS[1])
      if exists == 1 then return 0 end
      redis.call('SET', KEYS[1], ARGV[1])
      redis.call('RPUSH', KEYS[2], '1')
      return 1
    `;
    const result = await this.redis.eval(
      SUCCEED_SCRIPT,
      2,
      this.valueKey,
      this.notifyKey,
      JSON.stringify({ ok: true, value }),
    );
    return result === 1;
  }

  async failAsync(error: Error): Promise<boolean> {
    const FAIL_SCRIPT = `
      local exists = redis.call('EXISTS', KEYS[1])
      if exists == 1 then return 0 end
      redis.call('SET', KEYS[1], ARGV[1])
      redis.call('RPUSH', KEYS[2], '1')
      return 1
    `;
    const result = await this.redis.eval(
      FAIL_SCRIPT,
      2,
      this.valueKey,
      this.notifyKey,
      JSON.stringify({ ok: false, message: error.message }),
    );
    return result === 1;
  }

  async awaitAsync(): Promise<T> {
    // Fast path: already resolved
    const raw = await this.redis.get(this.valueKey);
    if (raw !== null) return this.decodeOrThrow(raw);

    // Block waiting for notification on dedicated connection
    const sub = await this.redis.duplicate();
    try {
      const timeoutSec = Math.ceil(this.timeoutMs / 1000);
      const result = await sub.brpop(this.notifyKey, timeoutSec);
      if (!result) throw new Error(`RedisDeferred await timeout after ${this.timeoutMs}ms`);

      // Re-publish for other waiters
      await this.redis.rpush(this.notifyKey, "1");
    } finally {
      if (sub.disconnect) sub.disconnect();
      else sub.close?.();
    }

    const resolved = await this.redis.get(this.valueKey);
    if (resolved === null) throw new Error("RedisDeferred value disappeared after notification");
    return this.decodeOrThrow(resolved);
  }

  async isDoneAsync(): Promise<boolean> {
    const exists = await this.redis.exists(this.valueKey);
    return exists === 1;
  }

  private decodeOrThrow(raw: string): T {
    const envelope = JSON.parse(raw) as { ok: boolean; value?: T; message?: string };
    if (envelope.ok) return envelope.value as T;
    throw new Error(envelope.message ?? "RedisDeferred failed");
  }
}
