// ---------------------------------------------------------------------------
// RedisCacheStore — CacheStore backed by Redis with TTL
// ---------------------------------------------------------------------------

import type { Redis } from "ioredis";
import type { CacheStore } from "@promin/core";

export interface RedisCacheStoreConfig {
  /** ioredis client instance. */
  redis: Redis;
  /** Key prefix to avoid collisions. Default: "cache:". */
  prefix?: string;
  /** Default TTL in ms. */
  ttlMs: number;
}

export class RedisCacheStore<V> implements CacheStore<string, V> {
  private readonly redis: Redis;
  private readonly prefix: string;
  private readonly ttlMs: number;

  constructor(config: RedisCacheStoreConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "cache:";
    this.ttlMs = config.ttlMs;
  }

  private key(k: string): string {
    return `${this.prefix}${k}`;
  }

  async get(key: string): Promise<V | undefined> {
    const raw = await this.redis.get(this.key(key));
    if (raw === null) return undefined;
    return JSON.parse(raw) as V;
  }

  async set(key: string, value: V, ttlMs?: number): Promise<void> {
    const ms = ttlMs ?? this.ttlMs;
    await this.redis.set(this.key(key), JSON.stringify(value), "PX", ms);
  }

  async delete(key: string): Promise<void> {
    await this.redis.del(this.key(key));
  }

  async has(key: string): Promise<boolean> {
    return (await this.redis.exists(this.key(key))) === 1;
  }

  async clear(): Promise<void> {
    const keys = await this.redis.keys(`${this.prefix}*`);
    if (keys.length > 0) await this.redis.del(...keys);
  }

  async size(): Promise<number> {
    const keys = await this.redis.keys(`${this.prefix}*`);
    return keys.length;
  }
}
