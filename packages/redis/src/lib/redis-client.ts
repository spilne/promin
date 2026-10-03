// ---------------------------------------------------------------------------
// RedisStoreClient — the Redis surface the workflow stores need
//
// Extends `@spilne/perfect-redis`'s driver-agnostic `RedisClient` with the
// sorted-set, set and pipeline commands the stores use for indexes and
// batched writes. An ioredis `Redis` instance satisfies it structurally, so
// one connection can back both perfect-redis primitives and these stores.
// ---------------------------------------------------------------------------

import type { RedisClient } from "@spilne/perfect-redis";

export interface RedisStoreClient extends RedisClient {
  // -- Sorted Set --
  zadd(key: string, ...args: Array<string | number>): Promise<number | string | null>;
  zrem(key: string, ...members: string[]): Promise<number>;
  zrangebyscore(key: string, min: string | number, max: string | number): Promise<string[]>;
  zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    limit: "LIMIT",
    offset: number,
    count: number,
  ): Promise<string[]>;
  zrangebyscore(
    key: string,
    min: string | number,
    max: string | number,
    withScores: "WITHSCORES",
    limit: "LIMIT",
    offset: number,
    count: number,
  ): Promise<string[]>;
  zcard(key: string): Promise<number>;

  // -- Set --
  sadd(key: string, ...members: string[]): Promise<number>;
  srem(key: string, ...members: string[]): Promise<number>;
  smembers(key: string): Promise<string[]>;
  scard(key: string): Promise<number>;
  sinter(...keys: string[]): Promise<string[]>;

  // -- Pipeline --
  pipeline(): RedisStorePipeline;
}

/** Batched commands sent in one round-trip by `RedisStoreClient.pipeline()`. */
export interface RedisStorePipeline {
  hset(key: string, fields: Record<string, string>): RedisStorePipeline;
  exec(): Promise<unknown>;
}
