// ---------------------------------------------------------------------------
// RedisClient — driver-agnostic Redis interface
//
// Works with any Redis client:
//   - ioredis
//   - node-redis (@redis/client)
//   - Bun.RedisClient (when available)
//
// Only includes commands we actually use. Add more as needed.
// ---------------------------------------------------------------------------

export interface RedisClient {
  // -- Key/Value --
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ...args: (string | number)[]): Promise<unknown>;
  del(...keys: string[]): Promise<number>;
  exists(key: string): Promise<number>;
  keys(pattern: string): Promise<string[]>;

  // -- List --
  rpush(key: string, ...values: string[]): Promise<number>;
  brpop(key: string, timeout: number): Promise<[string, string] | null>;
  lpush(key: string, ...values: string[]): Promise<number>;

  // -- Scripting --
  eval(script: string, numkeys: number, ...args: (string | number)[]): Promise<unknown>;

  // -- Expiry --
  pexpire(key: string, milliseconds: number): Promise<number>;

  // -- Hash --
  hset(key: string, ...args: (string | Record<string, string>)[]): Promise<number>;
  hget(key: string, field: string): Promise<string | null>;
  hgetall(key: string): Promise<Record<string, string>>;
  hdel(key: string, ...fields: string[]): Promise<number>;
  hkeys(key: string): Promise<string[]>;

  // -- Stream --
  xadd(key: string, id: string, ...fields: string[]): Promise<string>;
  xreadgroup(...args: unknown[]): Promise<unknown>;
  xack(key: string, group: string, ...ids: string[]): Promise<number>;
  xgroup(...args: unknown[]): Promise<unknown>;
  xpending(...args: unknown[]): Promise<unknown>;
  xclaim(...args: unknown[]): Promise<unknown>;
  xinfo(...args: unknown[]): Promise<unknown>;

  // -- Pub/Sub --
  publish(channel: string, message: string): Promise<number>;
  subscribe(...channels: string[]): Promise<unknown>;
  psubscribe(...patterns: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  punsubscribe(...patterns: string[]): Promise<unknown>;
  on(event: string, listener: (...args: any[]) => void): void;

  // -- Pipeline --
  pipeline(): RedisPipeline;

  // -- Connection --
  duplicate(): RedisClient;
  disconnect(): void;
}

export interface RedisPipeline {
  del(key: string): RedisPipeline;
  hset(key: string, ...args: (string | Record<string, string>)[]): RedisPipeline;
  exec(): Promise<unknown>;
}
