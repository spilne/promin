import type { Barrier } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

export class RedisBarrier implements Barrier {
  private constructor(
    private readonly redis: RedisClient,
    private readonly countKey: string,
    private readonly notifyKey: string,
    private readonly parties: number,
  ) {}

  static make(params: { redis: RedisClient; key: string; parties: number }): RedisBarrier {
    return new RedisBarrier(
      params.redis,
      `${params.key}:count`,
      `${params.key}:notify`,
      params.parties,
    );
  }

  async awaitAsync(): Promise<void> {
    const ARRIVE_SCRIPT = `
      local count = redis.call('INCR', KEYS[1])
      local parties = tonumber(ARGV[1])
      if count >= parties then
        redis.call('RPUSH', KEYS[2], '1')
        return 1
      end
      return 0
    `;
    const isLast = await this.redis.eval(
      ARRIVE_SCRIPT,
      2,
      this.countKey,
      this.notifyKey,
      this.parties,
    );

    if (Number(isLast) === 1) return;

    // Not last — block waiting for notification
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

  async arrivedAsync(): Promise<number> {
    const raw = await this.redis.get(this.countKey);
    return parseInt(raw ?? "0", 10);
  }
}
