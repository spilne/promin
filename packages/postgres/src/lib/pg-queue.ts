// ---------------------------------------------------------------------------
// PgQueue<T> — SKIP LOCKED-based queue (no extension required)
//
// Implements Streamable + Sinkable + Acknowledgeable using plain Postgres:
//   - Enqueue: INSERT into queue table
//   - Dequeue: SELECT ... FOR UPDATE SKIP LOCKED
//   - Ack: DELETE (or UPDATE status = 'completed')
//   - Nack: UPDATE visible_at = NOW() + vt (makes visible again after timeout)
//   - Visibility timeout: messages invisible to other consumers until VT expires
//
// Works with any Postgres 9.5+ — no pgmq extension needed.
// ---------------------------------------------------------------------------

import { Effect, Stream, Duration, Schedule } from "effect";
import { sql } from "drizzle-orm";
import { StreamPipeline, JsonCodec } from "@ts-backend/core";
import type { Streamable, Sinkable, Acknowledgeable, Envelope, Codec } from "@ts-backend/core";
import { type DrizzleDb, execRaw } from "./drizzle-db.ts";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgQueueConfig<T> {
  /** Drizzle database instance. */
  db: DrizzleDb;
  /** Queue name (used as table suffix: pgq_{name}). */
  queue: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Default visibility timeout in seconds. Default: 30. */
  defaultVtSeconds?: number;
  /** Default batch size for reads. Default: 10. */
  defaultBatchSize?: number;
  /** Client-side poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Max delivery attempts before message is dead-lettered. Default: 3. */
  maxAttempts?: number;
  /** Whether to archive (keep) or delete completed messages. Default: "delete". */
  ackMode?: "delete" | "archive";
}

// ---------------------------------------------------------------------------
// PgQueue
// ---------------------------------------------------------------------------

/**
 * A typed message queue backed by plain Postgres tables + `SELECT FOR UPDATE SKIP LOCKED`.
 *
 * No pgmq extension required — works with any Postgres 9.5+.
 * Implements `Streamable<T>`, `Sinkable<T>`, and `Acknowledgeable<T>`.
 *
 * @example
 * ```ts
 * import { PgQueue } from "@ts-backend/postgres";
 *
 * const queue = await PgQueue.create<{ userId: string }>(db, "jobs");
 *
 * // Publish
 * await queue.publish({ userId: "u_42" });
 *
 * // Consume with manual ack
 * await StreamPipeline.fromAck(queue)
 *   .forEach(async (envelope) => {
 *     await processUser(envelope.value);
 *     await envelope.ack();
 *   });
 *
 * // Or auto-consume (pop — read + delete in one step)
 * await queue.subscribe()
 *   .forEach((msg) => console.log(msg));
 * ```
 */
export class PgQueue<T> implements Streamable<T>, Sinkable<T>, Acknowledgeable<T> {
  readonly codec: Codec<T>;
  readonly queue: string;
  private readonly db: DrizzleDb;
  private readonly tableName: string;
  private readonly defaultVtSeconds: number;
  private readonly defaultBatchSize: number;
  private readonly pollIntervalMs: number;
  private readonly maxAttempts: number;
  private readonly ackMode: "delete" | "archive";

  private constructor(config: PgQueueConfig<T>) {
    this.db = config.db;
    this.queue = config.queue;
    this.tableName = `pgq_${config.queue}`;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.defaultVtSeconds = config.defaultVtSeconds ?? 30;
    this.defaultBatchSize = config.defaultBatchSize ?? 10;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.maxAttempts = config.maxAttempts ?? 3;
    this.ackMode = config.ackMode ?? "delete";
  }

  /**
   * Create a PgQueue — creates the underlying table if it doesn't exist.
   */
  static async create<T>(
    db: DrizzleDb,
    queue: string,
    config?: Omit<PgQueueConfig<T>, "db" | "queue">,
  ): Promise<PgQueue<T>> {
    const q = new PgQueue<T>({ db, queue, ...config });
    await q.ensureTable();
    return q;
  }

  /** Wrap an existing queue table (assumes it exists). */
  static wrap<T>(config: PgQueueConfig<T>): PgQueue<T> {
    return new PgQueue(config);
  }

  // ---------------------------------------------------------------------------
  // Table management
  // ---------------------------------------------------------------------------

  private async ensureTable(): Promise<void> {
    await this.db.execute(
      sql.raw(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        id BIGSERIAL PRIMARY KEY,
        payload JSONB NOT NULL,
        headers JSONB,
        status TEXT NOT NULL DEFAULT 'pending',
        visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT ${this.maxAttempts},
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        locked_by TEXT
      )
    `),
    );
    await this.db.execute(
      sql.raw(
        `CREATE INDEX IF NOT EXISTS ${this.tableName}_dequeue_idx ON ${this.tableName} (status, visible_at) WHERE status = 'pending'`,
      ),
    );
  }

  // ---------------------------------------------------------------------------
  // Sinkable<T> — publish
  // ---------------------------------------------------------------------------

  async publish(
    value: T,
    params?: { delay?: number; headers?: Record<string, string> },
  ): Promise<void> {
    const payload = JSON.stringify(this.codec.encode(value));
    const headers = params?.headers ? JSON.stringify(params.headers) : null;
    const delaySeconds = params?.delay ?? 0;

    await this.db.execute(
      sql.raw(`
        INSERT INTO ${this.tableName} (payload, headers, visible_at)
        VALUES ('${payload}'::jsonb, ${headers ? `'${headers}'::jsonb` : "NULL"}, NOW() + INTERVAL '${delaySeconds} seconds')
      `),
    );
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — subscribe with auto-pop
  // ---------------------------------------------------------------------------

  subscribe(_params?: { group?: string }): StreamPipeline<T, never> {
    const self = this;
    const s = Stream.repeatEffect(Effect.promise(() => self.pop(self.defaultBatchSize))).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(self.pollIntervalMs))),
      Stream.flatMap((rows) => Stream.fromIterable(rows)),
      Stream.map((row) => self.codec.decode(row.payload)),
    );
    return StreamPipeline.from(s);
  }

  // ---------------------------------------------------------------------------
  // Acknowledgeable<T> — subscribe with manual ack/nack
  // ---------------------------------------------------------------------------

  subscribeAck(params?: {
    group?: string;
    vtSeconds?: number;
  }): StreamPipeline<Envelope<T>, never> {
    const self = this;
    const vt = params?.vtSeconds ?? this.defaultVtSeconds;

    const s = Stream.repeatEffect(
      Effect.promise(() => self.dequeue(self.defaultBatchSize, vt)),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(self.pollIntervalMs))),
      Stream.flatMap((rows) => Stream.fromIterable(rows)),
      Stream.map(
        (row): Envelope<T> => ({
          value: self.codec.decode(row.payload),
          ack: () => self.ack(row.id),
          nack: () => self.nack(row.id),
          metadata: {
            msgId: row.id,
            attemptCount: row.attemptCount,
            createdAt: row.createdAt,
            headers: row.headers,
          },
        }),
      ),
    );
    return StreamPipeline.from(s);
  }

  // ---------------------------------------------------------------------------
  // Core operations
  // ---------------------------------------------------------------------------

  /** Dequeue messages with visibility timeout (SKIP LOCKED). */
  private async dequeue(
    limit: number,
    vtSeconds: number,
  ): Promise<
    { id: number; payload: unknown; attemptCount: number; createdAt: Date; headers: unknown }[]
  > {
    const rows = await execRaw(
      this.db,
      sql.raw(`
        UPDATE ${this.tableName}
        SET status = 'processing',
            visible_at = NOW() + INTERVAL '${vtSeconds} seconds',
            attempt_count = attempt_count + 1,
            locked_by = '${crypto.randomUUID()}'
        WHERE id IN (
          SELECT id FROM ${this.tableName}
          WHERE status = 'pending' AND visible_at <= NOW()
          ORDER BY created_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, payload, attempt_count, created_at, headers
      `),
    );
    return rows.map((r: any) => ({
      id: Number(r.id),
      payload: r.payload,
      attemptCount: r.attempt_count,
      createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
      headers: r.headers,
    }));
  }

  /** Pop (read + delete) — for auto-ack consumers. */
  private async pop(limit: number): Promise<{ id: number; payload: unknown }[]> {
    const rows = await execRaw(
      this.db,
      sql.raw(`
        DELETE FROM ${this.tableName}
        WHERE id IN (
          SELECT id FROM ${this.tableName}
          WHERE status = 'pending' AND visible_at <= NOW()
          ORDER BY created_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, payload
      `),
    );
    return rows.map((r: any) => ({ id: Number(r.id), payload: r.payload }));
  }

  /** Acknowledge — delete or mark completed. */
  private async ack(msgId: number): Promise<void> {
    if (this.ackMode === "delete") {
      await this.db.execute(sql.raw(`DELETE FROM ${this.tableName} WHERE id = ${msgId}`));
    } else {
      await this.db.execute(
        sql.raw(
          `UPDATE ${this.tableName} SET status = 'completed', completed_at = NOW() WHERE id = ${msgId}`,
        ),
      );
    }
  }

  /** Nack — make message visible again immediately. */
  private async nack(msgId: number): Promise<void> {
    await this.db.execute(
      sql.raw(
        `UPDATE ${this.tableName} SET status = 'pending', visible_at = NOW(), locked_by = NULL WHERE id = ${msgId}`,
      ),
    );
  }

  // ---------------------------------------------------------------------------
  // Queue management
  // ---------------------------------------------------------------------------

  /** Get queue metrics. */
  async metrics(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    total: number;
  }> {
    const [row] = await execRaw(
      this.db,
      sql.raw(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'pending') as pending,
          COUNT(*) FILTER (WHERE status = 'processing') as processing,
          COUNT(*) FILTER (WHERE status = 'completed') as completed,
          COUNT(*) as total
        FROM ${this.tableName}
      `),
    );
    return {
      pending: Number(row?.pending ?? 0),
      processing: Number(row?.processing ?? 0),
      completed: Number(row?.completed ?? 0),
      total: Number(row?.total ?? 0),
    };
  }

  /** Purge all messages from the queue. */
  async purge(): Promise<number> {
    const rows = await execRaw(this.db, sql.raw(`DELETE FROM ${this.tableName} RETURNING id`));
    return rows.length;
  }

  /** Drop the queue table entirely. */
  async drop(): Promise<void> {
    await this.db.execute(sql.raw(`DROP TABLE IF EXISTS ${this.tableName}`));
  }

  /** Requeue dead messages (exceeded max attempts but still in processing). */
  async requeueDead(): Promise<number> {
    const rows = await execRaw(
      this.db,
      sql.raw(`
        UPDATE ${this.tableName}
        SET status = 'pending', visible_at = NOW(), locked_by = NULL
        WHERE status = 'processing' AND visible_at < NOW()
        RETURNING id
      `),
    );
    return rows.length;
  }
}
