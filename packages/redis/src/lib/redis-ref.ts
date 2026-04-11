import type { AtomicRef } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

/**
 * Distributed atomic reference backed by Redis.
 * Stores the value as JSON under a single key.
 *
 * Note: `updateAsync` uses a read-modify-write cycle without WATCH/MULTI,
 * so it is NOT atomic across multiple processes. This is acceptable for
 * Phase 1 — use Lua scripting or WATCH/MULTI for true cross-process atomicity.
 *
 * @example
 * ```ts
 * const counter = RedisRef.make({ redis, key: "my:counter", initial: 0 });
 * await counter.setAsync(42);
 * const value = await counter.getAsync(); // 42
 * ```
 */
export class RedisRef<T> implements AtomicRef<T> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly initial: T,
  ) {}

  static make<T>(params: { redis: RedisClient; key: string; initial: T }): RedisRef<T> {
    return new RedisRef(params.redis, params.key, params.initial);
  }

  async getAsync(): Promise<T> {
    const raw = await this.redis.get(this.key);
    if (raw === null) return this.initial;
    return JSON.parse(raw) as T;
  }

  async setAsync(value: T): Promise<void> {
    await this.redis.set(this.key, JSON.stringify(value));
  }

  async updateAsync(fn: (current: T) => T): Promise<void> {
    // Read-modify-write without WATCH/MULTI — not atomic across processes.
    const raw = await this.redis.get(this.key);
    const current = raw === null ? this.initial : (JSON.parse(raw) as T);
    const next = fn(current);
    await this.redis.set(this.key, JSON.stringify(next));
  }
}
