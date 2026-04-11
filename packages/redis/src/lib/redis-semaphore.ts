import type { RedisClient } from "./redis-client.ts";

export class RedisSemaphore {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly permits: number,
    private readonly timeoutMs: number,
  ) {}

  static async make(params: {
    redis: RedisClient;
    key: string;
    permits: number;
    timeoutMs?: number;
  }): Promise<RedisSemaphore> {
    const sem = new RedisSemaphore(
      params.redis,
      params.key,
      params.permits,
      params.timeoutMs ?? 30_000,
    );
    await sem.initialize();
    return sem;
  }

  private async initialize(): Promise<void> {
    const exists = await this.redis.exists(this.key);
    if (exists) return;

    const tokens = Array.from({ length: this.permits }, (_, i) => String(i));
    if (tokens.length > 0) {
      await this.redis.rpush(this.key, ...tokens);
    }
  }

  /** Acquire a permit. Blocks until one is available or timeout. */
  async acquire(): Promise<void> {
    const timeoutSec = Math.ceil(this.timeoutMs / 1000);
    const result = await this.redis.brpop(this.key, timeoutSec);
    if (!result) {
      throw new Error(`Semaphore acquire timeout after ${this.timeoutMs}ms`);
    }
  }

  /** Release a permit back to the semaphore. */
  async release(): Promise<void> {
    await this.redis.rpush(this.key, "1");
  }

  /** Acquire, run fn, release — even on error. */
  async withPermitAsync<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      await this.release();
    }
  }
}
