// ---------------------------------------------------------------------------
// PgmqQueue<T> — high-level typed queue implementing core typeclasses
// Streamable + Sinkable + Acknowledgeable
// ---------------------------------------------------------------------------

import { Stream, Effect, Schedule, Duration } from "effect";
import { StreamPipeline } from "@promin/core";
import type { Streamable, Sinkable, Acknowledgeable, Envelope, Codec } from "@promin/core";
import { JsonCodec } from "@promin/core";
import type { DrizzleDb } from "../lib/drizzle-db.ts";
import type { ReadMode, AckMode } from "./types.ts";
import * as pgmq from "./pgmq.ts";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgmqQueueConfig<T> {
  /** Drizzle database instance. */
  db: DrizzleDb;
  /** Queue name. */
  queue: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Default visibility timeout in seconds. Default: 30. */
  defaultVt?: number;
  /** Default read batch size. Default: 10. */
  defaultQty?: number;
  /** Default poll interval in ms (client-side). Default: 1000. */
  defaultPollIntervalMs?: number;
  /** Default ack mode. Default: "delete". */
  defaultAckMode?: AckMode;
}

// ---------------------------------------------------------------------------
// PgmqQueue
// ---------------------------------------------------------------------------

export class PgmqQueue<T> implements Streamable<T>, Sinkable<T>, Acknowledgeable<T> {
  readonly codec: Codec<T>;
  private readonly db: DrizzleDb;
  readonly queue: string;
  private readonly defaultVt: number;
  private readonly defaultQty: number;
  private readonly defaultPollIntervalMs: number;
  private readonly defaultAckMode: AckMode;

  private constructor(config: PgmqQueueConfig<T>) {
    this.db = config.db;
    this.queue = config.queue;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.defaultVt = config.defaultVt ?? 30;
    this.defaultQty = config.defaultQty ?? 10;
    this.defaultPollIntervalMs = config.defaultPollIntervalMs ?? 1000;
    this.defaultAckMode = config.defaultAckMode ?? "delete";
  }

  /**
   * Create a PgmqQueue and ensure the underlying pgmq queue exists.
   *
   * @example
   * ```ts
   * const jobQueue = await PgmqQueue.create<{ userId: string }>(db, "onboard-jobs");
   * ```
   */
  static async create<T>(
    db: DrizzleDb,
    queue: string,
    config?: Omit<PgmqQueueConfig<T>, "db" | "queue">,
  ): Promise<PgmqQueue<T>> {
    await pgmq.createQueue(db, queue);
    return new PgmqQueue({ db, queue, ...config });
  }

  /**
   * Wrap an existing pgmq queue (assumes it already exists).
   */
  static wrap<T>(config: PgmqQueueConfig<T>): PgmqQueue<T> {
    return new PgmqQueue(config);
  }

  // ---------------------------------------------------------------------------
  // Sinkable<T> — publish messages
  // ---------------------------------------------------------------------------

  async publish(
    value: T,
    params?: { delay?: number; headers?: Record<string, string> },
  ): Promise<void> {
    await pgmq.send(this.db, this.queue, {
      data: this.codec.encode(value),
      delay: params?.delay,
      headers: params?.headers,
    });
  }

  async publishBatch(values: T[], params?: { delay?: number }): Promise<number[]> {
    return pgmq.sendBatch(
      this.db,
      this.queue,
      values.map((v) => ({ data: this.codec.encode(v), delay: params?.delay })),
    );
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — subscribe to messages (auto-ack via pop)
  // ---------------------------------------------------------------------------

  subscribe(params?: { group?: string }): StreamPipeline<T, never> {
    const db = this.db;
    const queue = this.queue;
    const codec = this.codec;
    const qty = this.defaultQty;
    const pollMs = this.defaultPollIntervalMs;

    const stream = Stream.repeatEffect(
      Effect.promise(() => pgmq.pop<unknown>(db, queue, qty)),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(pollMs))),
      Stream.flatMap((records) => Stream.fromIterable(records)),
      Stream.map((record) => codec.decode(record.message)),
    );

    return StreamPipeline.from(stream);
  }

  // ---------------------------------------------------------------------------
  // Acknowledgeable<T> — subscribe with manual ack/nack
  // ---------------------------------------------------------------------------

  subscribeAck(params?: {
    group?: string;
    readMode?: ReadMode;
    pollIntervalMs?: number;
    ackMode?: AckMode;
  }): StreamPipeline<Envelope<T>, never> {
    const db = this.db;
    const queue = this.queue;
    const codec = this.codec;
    const pollMs = params?.pollIntervalMs ?? this.defaultPollIntervalMs;
    const ackMode = params?.ackMode ?? this.defaultAckMode;
    const readMode: ReadMode = params?.readMode ?? {
      _tag: "standard",
      vt: this.defaultVt,
      qty: this.defaultQty,
    };

    const ack = async (msgId: number): Promise<void> => {
      if (ackMode === "archive") {
        await pgmq.archive(db, queue, msgId);
      } else {
        await pgmq.deleteMessage(db, queue, msgId);
      }
    };

    const stream = Stream.repeatEffect(
      Effect.promise(() => pgmq.read<unknown>(db, queue, readMode)),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(pollMs))),
      Stream.flatMap((records) => Stream.fromIterable(records)),
      Stream.map(
        (record): Envelope<T> => ({
          value: codec.decode(record.message),
          ack: () => ack(record.msgId),
          nack: () => pgmq.setVt(db, queue, record.msgId, 1).then(() => {}),
          metadata: {
            msgId: record.msgId,
            readCt: record.readCt,
            enqueuedAt: record.enqueuedAt,
            headers: record.headers,
          },
        }),
      ),
    );

    return StreamPipeline.from(stream);
  }

  // ---------------------------------------------------------------------------
  // Queue management
  // ---------------------------------------------------------------------------

  async purge(): Promise<number> {
    return pgmq.purgeQueue(this.db, this.queue);
  }

  async drop(): Promise<boolean> {
    return pgmq.dropQueue(this.db, this.queue);
  }

  async metrics(): Promise<{
    queueName: string;
    queueLength: number;
    newestMsgAgeSec: number | null;
    oldestMsgAgeSec: number | null;
    totalMessages: number;
  }> {
    return pgmq.metrics(this.db, this.queue);
  }

  async enableNotify(throttleIntervalMs?: number): Promise<void> {
    await pgmq.enableNotify(this.db, this.queue, throttleIntervalMs);
  }

  async disableNotify(): Promise<void> {
    await pgmq.disableNotify(this.db, this.queue);
  }
}
