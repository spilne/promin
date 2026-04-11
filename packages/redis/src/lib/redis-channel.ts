import type { Channel } from "@promin/core";
import type { RedisClient } from "./redis-client.ts";

export class RedisChannel<T> implements Channel<T> {
  private _isClosed = false;

  private constructor(
    private readonly redis: RedisClient,
    private readonly dataKey: string,
    private readonly metaKey: string,
    private readonly capacity: number,
  ) {}

  static make<T>(params: { redis: RedisClient; key: string; capacity?: number }): RedisChannel<T> {
    return new RedisChannel(
      params.redis,
      `${params.key}:data`,
      `${params.key}:meta`,
      params.capacity ?? 16,
    );
  }

  get isClosed(): boolean {
    return this._isClosed;
  }

  async sendAsync(item: T): Promise<void> {
    if (this._isClosed) throw new Error("Channel is closed");

    const SEND_SCRIPT = `
      local dataKey = KEYS[1]
      local metaKey = KEYS[2]
      local capacity = tonumber(ARGV[1])
      local value = ARGV[2]

      local closed = redis.call('HGET', metaKey, 'closed')
      if closed == '1' then return -1 end

      local len = redis.call('LLEN', dataKey)
      if len >= capacity then return 0 end

      redis.call('RPUSH', dataKey, value)
      return 1
    `;

    const result = await this.redis.eval(
      SEND_SCRIPT,
      2,
      this.dataKey,
      this.metaKey,
      this.capacity,
      JSON.stringify(item),
    );

    if (result === -1) {
      this._isClosed = true;
      throw new Error("Channel is closed");
    }
    if (result === 0) {
      throw new Error("Channel is full");
    }
  }

  async closeAsync(): Promise<void> {
    this._isClosed = true;
    await this.redis.hset(this.metaKey, "closed", "1");
  }
}
