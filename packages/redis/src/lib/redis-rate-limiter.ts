import { RateLimitExceeded, type RateLimiter } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

// Lua script: sliding window rate limit check
// Returns: -1 if acquired, >0 = retryAfterMs
const RATE_LIMIT_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, member)
  redis.call('PEXPIRE', key, window)
  return -1
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
return tonumber(oldest[2]) + window - now
`;

// Lua script: get remaining capacity
const REMAINING_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
return limit - count
`;

export class RedisRateLimiter implements RateLimiter {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly limit: number,
    private readonly windowMs: number,
  ) {}

  static make(params: {
    redis: RedisClient;
    key: string;
    limit: number;
    windowMs: number;
  }): RedisRateLimiter {
    return new RedisRateLimiter(params.redis, params.key, params.limit, params.windowMs);
  }

  async acquireAsync(): Promise<void> {
    const now = Date.now();
    const member = `${now}:${Math.random().toString(36).slice(2, 10)}`;
    const result = await this.redis.eval(
      RATE_LIMIT_SCRIPT,
      1,
      this.key,
      now,
      this.windowMs,
      this.limit,
      member,
    );
    const retryAfterMs = Number(result);
    if (retryAfterMs > 0) {
      throw new RateLimitExceeded({ retryAfterMs });
    }
  }

  async tryAcquireAsync(): Promise<boolean> {
    try {
      await this.acquireAsync();
      return true;
    } catch {
      return false;
    }
  }

  async withLimitAsync<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquireAsync();
    return fn();
  }

  async remainingAsync(): Promise<number> {
    const now = Date.now();
    const result = await this.redis.eval(
      REMAINING_SCRIPT,
      1,
      this.key,
      now,
      this.windowMs,
      this.limit,
    );
    return Math.max(0, Number(result));
  }
}
