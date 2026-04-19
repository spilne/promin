// ---------------------------------------------------------------------------
// PgmqQueue<T> — high-level typed queue implementing core typeclasses
// Streamable + Sinkable + Acknowledgeable
// ---------------------------------------------------------------------------

import { Stream, Effect, Schedule, Duration } from "effect";
import { StreamPipeline } from "@promin/core";
import type {
  Streamable,
  Sinkable,
  Acknowledgeable,
  Envelope,
  Codec,
  SchemaParser,
} from "@promin/core";
import { JsonCodec } from "@promin/core";
import type { DrizzleDb } from "../lib/drizzle-db.ts";
import type { ReadMode, AckMode } from "./types.ts";
import * as pgmq from "./pgmq.ts";

// ---------------------------------------------------------------------------
// Schema errors
// ---------------------------------------------------------------------------

/**
 * What to do when a read message fails schema validation.
 *
 * - `"throw"` (default) — let the error propagate; the consumer stream
 *   fails. Safest default for initial rollout: loud failures beat silent
 *   bad data. The poison message is NOT deleted — operator must intervene.
 * - `"skip"` — log (TODO once we have a hook), delete the bad message
 *   from the queue so it doesn't re-appear, and continue. Use when bad
 *   messages are acceptable losses (e.g. observability pipelines).
 * - `"dlq"` — publish the raw message to `{queue}_dlq` for inspection,
 *   then delete the original. The DLQ is auto-created on first use.
 */
export type PgmqOnSchemaError = "throw" | "skip" | "dlq";

/**
 * Thrown from the subscribe stream when `onSchemaError: "throw"` and a
 * message fails validation. Carries the original payload + the schema's
 * error so the operator can reproduce locally.
 */
export class PgmqSchemaValidationError extends Error {
  readonly _tag = "PgmqSchemaValidationError";
  constructor(
    readonly queueName: string,
    readonly msgId: number,
    readonly raw: unknown,
    readonly schemaError: unknown,
  ) {
    super(
      `PgmqQueue "${queueName}": message ${msgId} failed schema validation. ` +
        `Set onSchemaError to "skip" or "dlq" to handle invalid messages without ` +
        `killing the consumer.`,
    );
  }
}

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
  /**
   * Optional runtime schema validator for consumed messages. Zod, Valibot,
   * and ArkType all satisfy `SchemaParser<T>` out of the box. Applied AFTER
   * codec decode; catches silent producer-side drift (right JSON shape
   * after JSON.parse, wrong fields after validation).
   *
   * Skip if `T` is self-describing enough for you (e.g. you own both
   * producer + consumer and trust the types). Required if you're reading
   * messages published by another service.
   */
  schema?: SchemaParser<T>;
  /**
   * How to handle messages that fail `schema.safeParse`. Ignored when
   * `schema` is unset. Default: `"throw"` — loud failures are the right
   * default for schema drift; silent drops hide bugs.
   */
  onSchemaError?: PgmqOnSchemaError;
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
  private readonly schema?: SchemaParser<T>;
  private readonly onSchemaError: PgmqOnSchemaError;
  /** Lazy-init flag — DLQ queue is created the first time a message is routed there. */
  private dlqEnsured = false;

  private constructor(config: PgmqQueueConfig<T>) {
    this.db = config.db;
    this.queue = config.queue;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.defaultVt = config.defaultVt ?? 30;
    this.defaultQty = config.defaultQty ?? 10;
    this.defaultPollIntervalMs = config.defaultPollIntervalMs ?? 1000;
    this.defaultAckMode = config.defaultAckMode ?? "delete";
    this.schema = config.schema;
    this.onSchemaError = config.onSchemaError ?? "throw";
  }

  /** Name of the auto-managed dead-letter queue for this PgmqQueue. */
  get dlqName(): string {
    return `${this.queue}_dlq`;
  }

  /**
   * Decode + validate a message. Returns a result carrying the validated
   * value on success or a tagged failure that the caller dispatches via
   * `onSchemaError`.
   */
  private decodeAndValidate(raw: unknown): { ok: true; value: T } | { ok: false; error: unknown } {
    const decoded = this.codec.decode(raw);
    if (!this.schema) return { ok: true, value: decoded };
    const result = this.schema.safeParse(decoded);
    if (result.success) return { ok: true, value: result.data };
    return { ok: false, error: result.error };
  }

  /**
   * Route a failed message per `onSchemaError`. Returns `true` when the
   * caller should consider the message handled (skip emitting); throws
   * when the policy is `"throw"`. Performs all side effects (DLQ publish,
   * original delete) before returning.
   */
  private async handleSchemaError(params: {
    msgId: number;
    rawMessage: unknown;
    error: unknown;
    // When true (read/manual-ack path), we still own the lock — the
    // caller needs us to delete the original so it doesn't retry. When
    // false (pop/auto-ack path), the message is already gone.
    deleteOriginal: boolean;
  }): Promise<void> {
    const { msgId, rawMessage, error, deleteOriginal } = params;
    if (this.onSchemaError === "throw") {
      throw new PgmqSchemaValidationError(this.queue, msgId, rawMessage, error);
    }
    if (this.onSchemaError === "dlq") {
      if (!this.dlqEnsured) {
        await pgmq.createQueue(this.db, this.dlqName);
        this.dlqEnsured = true;
      }
      await pgmq.send(this.db, this.dlqName, { data: rawMessage });
    }
    if (deleteOriginal) {
      await pgmq.deleteMessage(this.db, this.queue, msgId);
    }
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
    void params;
    const db = this.db;
    const queue = this.queue;
    const qty = this.defaultQty;
    const pollMs = this.defaultPollIntervalMs;

    const self = this;
    const stream = Stream.repeatEffect(
      Effect.promise(() => pgmq.pop<unknown>(db, queue, qty)),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(pollMs))),
      Stream.flatMap((records) => Stream.fromIterable(records)),
      // flatMap over a per-record effect that emits 0 values on schema
      // failure (after routing per onSchemaError) and 1 value on success.
      Stream.flatMap((record) =>
        Stream.unwrap(
          Effect.promise(async () => {
            const res = self.decodeAndValidate(record.message);
            if (res.ok) return Stream.succeed(res.value);
            // pop() already removed the message — no delete needed.
            await self.handleSchemaError({
              msgId: record.msgId,
              rawMessage: record.message,
              error: res.error,
              deleteOriginal: false,
            });
            return Stream.empty;
          }),
        ),
      ),
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

    const self = this;
    const stream = Stream.repeatEffect(
      Effect.promise(() => pgmq.read<unknown>(db, queue, readMode)),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(pollMs))),
      Stream.flatMap((records) => Stream.fromIterable(records)),
      Stream.flatMap((record) =>
        Stream.unwrap(
          Effect.promise(async () => {
            const res = self.decodeAndValidate(record.message);
            if (res.ok) {
              const envelope: Envelope<T> = {
                value: res.value,
                ack: () => ack(record.msgId),
                nack: () => pgmq.setVt(db, queue, record.msgId, 1).then(() => {}),
                metadata: {
                  msgId: record.msgId,
                  readCt: record.readCt,
                  enqueuedAt: record.enqueuedAt,
                  headers: record.headers,
                },
              };
              return Stream.succeed(envelope);
            }
            // read() kept the message in the queue (locked via vt) — we
            // delete it ourselves for skip/dlq so it doesn't retry after
            // vt expires. For "throw", leave the lock to expire naturally
            // (the error surfaces again on retry, which is the point).
            await self.handleSchemaError({
              msgId: record.msgId,
              rawMessage: record.message,
              error: res.error,
              deleteOriginal: true,
            });
            return Stream.empty;
          }),
        ),
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
