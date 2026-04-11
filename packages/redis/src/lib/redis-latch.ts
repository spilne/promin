import type { Latch } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

export class RedisLatch implements Latch {
  private constructor(
    private readonly redis: RedisClient,
    private readonly countKey: string,
    private readonly notifyKey: string,
  ) {}

  static async make(params: {
    redis: RedisClient;
    key: string;
    count: number;
    timeoutMs?: number;
  }): Promise<RedisLatch> {
    const countKey = `${params.key}:count`;
    const notifyKey = `${params.key}:notify`;
    // Initialize counter (only if not exists — idempotent)
    await params.redis.set(countKey, String(params.count), "NX");
    return new RedisLatch(params.redis, countKey, notifyKey);
  }

  async countDownAsync(): Promise<void> {
    const COUNTDOWN_SCRIPT = `
      local count = redis.call('DECR', KEYS[1])
      if count <= 0 then
        redis.call('RPUSH', KEYS[2], '1')
      end
      return count
    `;
    await this.redis.eval(COUNTDOWN_SCRIPT, 2, this.countKey, this.notifyKey);
  }

  async awaitAsync(): Promise<void> {
    // Fast path: already at zero
    const current = await this.redis.get(this.countKey);
    if (current !== null && parseInt(current, 10) <= 0) return;

    // Block waiting for notification on dedicated connection
    const sub = await this.redis.duplicate();
    try {
      const result = await sub.brpop(this.notifyKey, 30);
      if (result) {
        // Re-publish for other waiters
        await this.redis.rpush(this.notifyKey, "1");
      }
    } finally {
      sub.disconnect ? sub.disconnect() : sub.close?.();
    }
  }

  async remainingAsync(): Promise<number> {
    const raw = await this.redis.get(this.countKey);
    return Math.max(0, parseInt(raw ?? "0", 10));
  }
}
