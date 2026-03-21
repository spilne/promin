// ---------------------------------------------------------------------------
// RedisPubSub<T> — Redis Pub/Sub implementing Streamable + Sinkable
//
// Fire-and-forget broadcast messaging. Messages are not persisted —
// only active subscribers receive them. Use RedisStream for durable
// consumer-group semantics.
//
// Supports both exact channels and pattern subscriptions (PSUBSCRIBE).
// ---------------------------------------------------------------------------

import { Effect, Stream } from "effect";
import type { Redis } from "ioredis";
import { StreamPipeline, JsonCodec } from "@promin/core";
import type { Streamable, Sinkable, Codec } from "@promin/core";

export interface RedisPubSubConfig<T> {
  /** ioredis client instance for publishing. */
  redis: Redis;
  /** Exact channel name. Mutually exclusive with `pattern`. */
  channel?: string;
  /** Pattern for PSUBSCRIBE (e.g., "user-*", "events.*"). Mutually exclusive with `channel`. */
  pattern?: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
}

export class RedisPubSub<T> implements Streamable<T>, Sinkable<T> {
  readonly codec: Codec<T>;
  private readonly redis: Redis;
  private readonly channel?: string;
  private readonly pattern?: string;

  constructor(config: RedisPubSubConfig<T>) {
    if (!config.channel && !config.pattern) {
      throw new Error("RedisPubSub requires either channel or pattern");
    }
    if (config.channel && config.pattern) {
      throw new Error("RedisPubSub: channel and pattern are mutually exclusive");
    }
    this.redis = config.redis;
    this.channel = config.channel;
    this.pattern = config.pattern;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
  }

  // =========================================================================
  // Sinkable — publish messages
  // =========================================================================

  async publish(value: T): Promise<void> {
    if (!this.channel) {
      throw new Error("Cannot publish to a pattern subscription — use a channel");
    }
    const encoded = JSON.stringify(this.codec.encode(value));
    await this.redis.publish(this.channel, encoded);
  }

  // =========================================================================
  // Streamable — subscribe to messages
  // =========================================================================

  subscribe(): StreamPipeline<T, never> {
    const codec = this.codec;
    const channel = this.channel;
    const pattern = this.pattern;

    // Redis requires a dedicated connection for subscriptions —
    // ioredis .duplicate() creates one sharing the same config.
    const stream = Stream.async<T, never>((emit) => {
      const sub = this.redis.duplicate();

      if (pattern) {
        sub.psubscribe(pattern).catch(() => {});
        sub.on("pmessage", (_pattern: string, _ch: string, message: string) => {
          try {
            const value = codec.decode(JSON.parse(message));
            emit.single(value);
          } catch {
            // Skip malformed messages
          }
        });
      } else if (channel) {
        sub.subscribe(channel).catch(() => {});
        sub.on("message", (_ch: string, message: string) => {
          try {
            const value = codec.decode(JSON.parse(message));
            emit.single(value);
          } catch {
            // Skip malformed messages
          }
        });
      }

      return Effect.promise(async () => {
        if (pattern) await sub.punsubscribe(pattern);
        else if (channel) await sub.unsubscribe(channel);
        sub.disconnect();
      });
    });

    return StreamPipeline.from(stream);
  }
}
