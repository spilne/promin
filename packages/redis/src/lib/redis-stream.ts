// ---------------------------------------------------------------------------
// RedisStream<T> — Redis Streams implementing streaming typeclasses
//
// Implements: Streamable, Sinkable, KeyedSinkable, Acknowledgeable
// Uses ioredis for XADD/XREADGROUP/XACK/XCLAIM.
// ---------------------------------------------------------------------------

import { Chunk, Effect, Stream } from "effect";
import type { RedisClient } from "./redis-client.ts";
import { StreamPipeline, JsonCodec } from "@promin/core";
import type {
  Streamable,
  Sinkable,
  KeyedSinkable,
  Acknowledgeable,
  Envelope,
  Codec,
} from "@promin/core";

export interface RedisStreamConfig<T> {
  /** Redis client instance (ioredis, node-redis, Bun.RedisClient, etc.). */
  redis: RedisClient;
  /** Stream key name. */
  stream: string;
  /** Consumer group name. */
  group: string;
  /** Consumer name within the group. Default: random UUID. */
  consumer?: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Block timeout for XREADGROUP (ms). Default: 5000. */
  blockMs?: number;
  /** Max messages per read. Default: 10. */
  count?: number;
}

export class RedisStream<T>
  implements Streamable<T>, Sinkable<T>, KeyedSinkable<T>, Acknowledgeable<T>
{
  readonly codec: Codec<T>;
  private readonly redis: RedisClient;
  private readonly stream: string;
  private readonly group: string;
  private readonly consumer: string;
  private readonly blockMs: number;
  private readonly count: number;

  constructor(config: RedisStreamConfig<T>) {
    this.redis = config.redis;
    this.stream = config.stream;
    this.group = config.group;
    this.consumer = config.consumer ?? crypto.randomUUID();
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.blockMs = config.blockMs ?? 5000;
    this.count = config.count ?? 10;
  }

  // =========================================================================
  // Setup — ensure consumer group exists
  // =========================================================================

  async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.stream, this.group, "0", "MKSTREAM");
    } catch (err: any) {
      // Group already exists — ignore BUSYGROUP error
      if (!err.message?.includes("BUSYGROUP")) throw err;
    }
  }

  // =========================================================================
  // Sinkable — publish messages (XADD)
  // =========================================================================

  async publish(value: T, params?: { key: string }): Promise<void> {
    const encoded = JSON.stringify(this.codec.encode(value));
    const fields: string[] = ["data", encoded];
    if (params?.key) fields.push("key", params.key);
    await this.redis.xadd(this.stream, "*", ...fields);
  }

  // =========================================================================
  // Streamable — subscribe to messages
  // =========================================================================

  subscribe(_params?: { group?: string }): StreamPipeline<T, never> {
    return this.createReadStream();
  }

  // =========================================================================
  // Acknowledgeable — manual ack/nack
  // =========================================================================

  subscribeAck(_params?: { group?: string }): StreamPipeline<Envelope<T>, never> {
    const self = this;

    const stream = Stream.async<Envelope<T>, never>((emit) => {
      let running = true;

      const poll = async () => {
        while (running) {
          try {
            const results = await self.redis.xreadgroup(
              "GROUP",
              self.group,
              self.consumer,
              "COUNT",
              self.count,
              "BLOCK",
              self.blockMs,
              "STREAMS",
              self.stream,
              ">",
            );

            if (!results) continue;

            // Build the whole batch in memory, then emit once. One
            // fiber-scheduling event per XREADGROUP poll instead of one
            // per message — at 10K+ msg/sec the difference shows up.
            const envelopes: Envelope<T>[] = [];
            for (const [, messages] of results as any[]) {
              for (const [id, fields] of messages) {
                const dataIdx = fields.indexOf("data");
                if (dataIdx === -1) continue;
                const raw = fields[dataIdx + 1]!;
                const value = self.codec.decode(JSON.parse(raw));

                const keyIdx = fields.indexOf("key");
                const key = keyIdx !== -1 ? fields[keyIdx + 1] : undefined;

                envelopes.push({
                  value,
                  ack: async () => {
                    await self.redis.xack(self.stream, self.group, id);
                  },
                  nack: async () => {
                    // Don't ack — message stays in PEL and will be re-claimed
                  },
                  metadata: {
                    id,
                    stream: self.stream,
                    group: self.group,
                    consumer: self.consumer,
                    key,
                  },
                });
              }
            }

            if (envelopes.length > 0) emit.chunk(Chunk.fromIterable(envelopes));
          } catch {
            if (running) await new Promise((r) => setTimeout(r, 1000));
          }
        }
      };

      poll();

      return Effect.sync(() => {
        running = false;
      });
    });

    return StreamPipeline.from(stream);
  }

  // =========================================================================
  // Claim — reclaim messages from dead consumers (XCLAIM)
  // =========================================================================

  async claimPending(params: {
    minIdleMs: number;
    count: number;
  }): Promise<{ id: string; value: T }[]> {
    // Get pending messages
    const pending = await this.redis.xpending(this.stream, this.group, "-", "+", params.count);

    if (!Array.isArray(pending) || pending.length === 0) return [];

    const ids = pending
      .filter((p: any) => Number(p[2]) >= params.minIdleMs)
      .map((p: any) => p[0] as string);

    if (ids.length === 0) return [];

    const claimed = await this.redis.xclaim(
      this.stream,
      this.group,
      this.consumer,
      params.minIdleMs,
      ...ids,
    );

    return (claimed as any[]).map((entry: any) => {
      const [id, fields] = entry;
      const dataIdx = fields.indexOf("data");
      const raw = dataIdx !== -1 ? fields[dataIdx + 1] : "null";
      return { id, value: this.codec.decode(JSON.parse(raw)) };
    });
  }

  // =========================================================================
  // Internal
  // =========================================================================

  private createReadStream(): StreamPipeline<T, never> {
    const self = this;

    const stream = Stream.async<T, never>((emit) => {
      let running = true;

      const poll = async () => {
        while (running) {
          try {
            const results = await self.redis.xreadgroup(
              "GROUP",
              self.group,
              self.consumer,
              "COUNT",
              self.count,
              "BLOCK",
              self.blockMs,
              "STREAMS",
              self.stream,
              ">",
            );

            if (!results) continue;

            // Collect the whole batch, emit in one chunk, XACK all ids in a
            // single call. Net: one fiber-scheduling event + one round-trip
            // to Redis per poll instead of 2×N (emit + ack) per message.
            const values: T[] = [];
            const ids: string[] = [];
            for (const [, messages] of results as any[]) {
              for (const [id, fields] of messages) {
                const dataIdx = fields.indexOf("data");
                if (dataIdx === -1) continue;
                const raw = fields[dataIdx + 1]!;
                values.push(self.codec.decode(JSON.parse(raw)));
                ids.push(id);
              }
            }

            if (values.length === 0) continue;
            emit.chunk(Chunk.fromIterable(values));
            // Auto-ack in subscribe mode — batched XACK.
            await self.redis.xack(self.stream, self.group, ...ids);
          } catch {
            if (running) await new Promise((r) => setTimeout(r, 1000));
          }
        }
      };

      poll();

      return Effect.sync(() => {
        running = false;
      });
    });

    return StreamPipeline.from(stream);
  }

  // =========================================================================
  // Metrics
  // =========================================================================

  async info(): Promise<{
    length: number;
    groups: number;
    lastId: string;
  }> {
    const info = (await this.redis.xinfo("STREAM", this.stream)) as any[];
    const lengthIdx = info.indexOf("length");
    const groupsIdx = info.indexOf("groups");
    const lastIdx = info.indexOf("last-generated-id");
    return {
      length: lengthIdx !== -1 ? Number(info[lengthIdx + 1]) : 0,
      groups: groupsIdx !== -1 ? Number(info[groupsIdx + 1]) : 0,
      lastId: lastIdx !== -1 ? String(info[lastIdx + 1]) : "0-0",
    };
  }
}
