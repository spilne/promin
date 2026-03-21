// ---------------------------------------------------------------------------
// RedisStateBackend — StateBackend backed by Redis hashes
//
// All keyed state lives in a single Redis hash per topology group.
// Checkpoints are atomic COPY of the hash to a checkpoint key.
// Restore loads from the checkpoint hash back into the live hash.
// ---------------------------------------------------------------------------

import type { Redis } from "ioredis";
import type { StateBackend } from "@promin/core";

export interface RedisStateBackendConfig {
  /** ioredis client instance. */
  redis: Redis;
  /** Key prefix for the state hash. Default: "state:". */
  prefix?: string;
}

export class RedisStateBackend implements StateBackend<string, unknown> {
  private readonly redis: Redis;
  private readonly prefix: string;

  constructor(config: RedisStateBackendConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "state:";
  }

  private hashKey(): string {
    return `${this.prefix}live`;
  }

  private checkpointKey(name: string): string {
    return `${this.prefix}checkpoint:${name}`;
  }

  async get(key: string): Promise<unknown | undefined> {
    const raw = await this.redis.hget(this.hashKey(), key);
    if (raw === null) return undefined;
    return JSON.parse(raw);
  }

  async put(key: string, value: unknown): Promise<void> {
    await this.redis.hset(this.hashKey(), key, JSON.stringify(value));
  }

  async delete(key: string): Promise<void> {
    await this.redis.hdel(this.hashKey(), key);
  }

  async keys(): Promise<string[]> {
    return this.redis.hkeys(this.hashKey());
  }

  async entries(): Promise<[string, unknown][]> {
    const all = await this.redis.hgetall(this.hashKey());
    return Object.entries(all).map(([k, v]) => [k, JSON.parse(v)]);
  }

  async checkpoint(params: { name: string }): Promise<void> {
    const src = this.hashKey();
    const dst = this.checkpointKey(params.name);

    // Atomic snapshot: dump all fields and write to checkpoint hash
    const all = await this.redis.hgetall(src);
    if (Object.keys(all).length === 0) return;

    const pipeline = this.redis.pipeline();
    pipeline.del(dst);
    // HSET with all fields at once
    pipeline.hset(dst, all);
    await pipeline.exec();
  }

  async restore(params: { name: string }): Promise<void> {
    const src = this.checkpointKey(params.name);
    const dst = this.hashKey();

    const all = await this.redis.hgetall(src);
    if (Object.keys(all).length === 0) return;

    const pipeline = this.redis.pipeline();
    pipeline.del(dst);
    pipeline.hset(dst, all);
    await pipeline.exec();
  }

  async clear(): Promise<void> {
    await this.redis.del(this.hashKey());
  }
}
