import type { Signal } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

/**
 * Distributed shared mutable value backed by Redis.
 * Stores JSON at a single key. Supports get/set/update.
 *
 * Note: `updateAsync` uses read-modify-write without WATCH/MULTI,
 * so it is NOT atomic across multiple processes — same trade-off as RedisRef.
 *
 * @example
 * ```ts
 * const config = RedisSignal.make({ redis, key: "app:config", initial: defaultConfig });
 * await config.setAsync(newConfig);
 * const current = await config.getAsync();
 * ```
 */
export class RedisSignal<T> implements Signal<T> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly initial: T,
  ) {}

  static make<T>(params: { redis: RedisClient; key: string; initial: T }): RedisSignal<T> {
    return new RedisSignal(params.redis, params.key, params.initial);
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
    const raw = await this.redis.get(this.key);
    const current = raw === null ? this.initial : (JSON.parse(raw) as T);
    const next = fn(current);
    await this.redis.set(this.key, JSON.stringify(next));
  }
}
