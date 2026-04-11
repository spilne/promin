import type { Throttle } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

// Lua script: atomic sliding window acquire
// KEYS[1] = throttle key
// ARGV[1] = now (epoch ms)
// ARGV[2] = windowMs
// ARGV[3] = permits
// ARGV[4] = unique member ID
// Returns: 0 if acquired, >0 = ms to wait until next slot
const ACQUIRE_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local permits = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < permits then
  redis.call('ZADD', key, now, member)
  redis.call('PEXPIRE', key, window)
  return 0
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
return tonumber(oldest[2]) + window - now
`;

export class RedisThrottle implements Throttle {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly permits: number,
    private readonly windowMs: number,
  ) {}

  static make(params: {
    redis: RedisClient;
    key: string;
    permits: number;
    windowMs: number;
  }): RedisThrottle {
    return new RedisThrottle(params.redis, params.key, params.permits, params.windowMs);
  }

  private resolveKey(resource?: string): string {
    return resource ? `${this.key}:${resource}` : this.key;
  }

  async acquireAsync(resource?: string): Promise<void> {
    while (true) {
      const waitMs = await this.tryAcquireInternal(resource);
      if (waitMs === 0) return;
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }

  async tryAcquireAsync(resource?: string): Promise<boolean> {
    const waitMs = await this.tryAcquireInternal(resource);
    return waitMs === 0;
  }

  async withPermitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T> {
    await this.acquireAsync(resource);
    return fn();
  }

  private async tryAcquireInternal(resource?: string): Promise<number> {
    const now = Date.now();
    const member = `${now}:${Math.random().toString(36).slice(2, 10)}`;
    const result = await this.redis.eval(
      ACQUIRE_SCRIPT,
      1,
      this.resolveKey(resource),
      now,
      this.windowMs,
      this.permits,
      member,
    );
    return Number(result);
  }
}
