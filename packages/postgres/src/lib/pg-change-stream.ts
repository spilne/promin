// ---------------------------------------------------------------------------
// PgChangeStream<T> — LISTEN/NOTIFY-based change stream with poll fallback
//
// Implements Streamable<T> and Replayable<T> for real-time CDC:
//   - Primary: LISTEN on a Postgres channel for instant notifications
//   - Fallback: periodic poll to catch any missed events (at-least-once)
//   - Replayable: subscribe from a specific offset (timestamp or sequence)
//
// LISTEN/NOTIFY is lossy — if the consumer is down, notifications are lost.
// The poll-based fallback ensures at-least-once delivery by periodically
// checking for rows newer than the last seen timestamp.
// ---------------------------------------------------------------------------

import { Effect, Stream, Duration, Schedule } from "effect";
import { sql } from "drizzle-orm";
import { StreamPipeline, JsonCodec } from "@promin/core";
import type { Streamable, Replayable, Offset, Codec } from "@promin/core";
import type { DrizzleDb } from "./drizzle-db.ts";
import type postgres from "postgres";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgChangeStreamConfig<T> {
  /** Drizzle database instance (for poll queries). */
  db: DrizzleDb;
  /**
   * Raw postgres-js client (for LISTEN/NOTIFY).
   * Required because Drizzle doesn't expose LISTEN.
   */
  sql: ReturnType<typeof postgres>;
  /** Postgres NOTIFY channel name. */
  channel: string;
  /** Table to poll for changes. Must have a timestamp column for ordering. */
  table: string;
  /** Column name used for ordering/filtering (e.g. "created_at", "updated_at"). */
  timestampColumn?: string;
  /** Column name for a monotonic sequence (e.g. "id"). Used for specific offsets. */
  sequenceColumn?: string;
  /** Payload column to read (e.g. "payload"). Default: entire row as JSON. */
  payloadColumn?: string;
  /** Codec for deserializing payloads. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Poll interval in ms for the fallback poller. Default: 5000. */
  pollIntervalMs?: number;
  /** Max rows per poll batch. Default: 100. */
  pollBatchSize?: number;
}

// ---------------------------------------------------------------------------
// PgChangeStream
// ---------------------------------------------------------------------------

export class PgChangeStream<T> implements Streamable<T>, Replayable<T> {
  readonly codec: Codec<T>;
  private readonly db: DrizzleDb;
  private readonly sqlClient: ReturnType<typeof postgres>;
  private readonly channel: string;
  private readonly table: string;
  private readonly timestampColumn: string;
  private readonly sequenceColumn: string;
  private readonly payloadColumn: string | undefined;
  private readonly pollIntervalMs: number;
  private readonly pollBatchSize: number;

  constructor(config: PgChangeStreamConfig<T>) {
    this.db = config.db;
    this.sqlClient = config.sql;
    this.channel = config.channel;
    this.table = config.table;
    this.timestampColumn = config.timestampColumn ?? "created_at";
    this.sequenceColumn = config.sequenceColumn ?? "id";
    this.payloadColumn = config.payloadColumn;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.pollIntervalMs = config.pollIntervalMs ?? 5000;
    this.pollBatchSize = config.pollBatchSize ?? 100;
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — LISTEN + poll merged stream
  // ---------------------------------------------------------------------------

  subscribe(_params?: { group?: string }): StreamPipeline<T, never> {
    return this.subscribeFrom({ offset: { type: "latest" } });
  }

  // ---------------------------------------------------------------------------
  // Replayable<T> — subscribe from offset
  // ---------------------------------------------------------------------------

  subscribeFrom(params: { offset: Offset; group?: string }): StreamPipeline<T, never> {
    const listenStream = this.createListenStream();
    const pollStream = this.createPollStream(params.offset);

    // Merge both sources — LISTEN for low latency, poll for reliability
    return listenStream.merge(pollStream).dedupe();
  }

  // ---------------------------------------------------------------------------
  // LISTEN stream — real-time notifications
  // ---------------------------------------------------------------------------

  private createListenStream(): StreamPipeline<T, never> {
    const codec = this.codec;
    const sqlClient = this.sqlClient;
    const channel = this.channel;

    const stream = Stream.async<T, never>((emit) => {
      const unlisten = sqlClient.listen(channel, (payload: string) => {
        try {
          const parsed = JSON.parse(payload);
          const decoded = codec.decode(parsed);
          emit.single(decoded);
        } catch {
          // Skip malformed payloads
        }
      });

      // Return cleanup function
      return Effect.promise(async () => {
        const listener = await unlisten;
        await listener.unlisten();
      });
    });

    return StreamPipeline.from(stream);
  }

  // ---------------------------------------------------------------------------
  // Poll stream — periodic catch-up for at-least-once delivery
  // ---------------------------------------------------------------------------

  private createPollStream(offset: Offset): StreamPipeline<T, never> {
    const self = this;
    let cursor = this.offsetToTimestamp(offset);

    const stream = Stream.repeatEffect(
      Effect.promise(async () => {
        const rows = await self.pollSince(cursor);
        if (rows.length > 0) {
          // Advance cursor to latest row's timestamp
          const lastRow = rows[rows.length - 1]!;
          cursor = new Date(lastRow.ts.getTime() + 1);
        }
        return rows.map((r) => r.value);
      }),
    ).pipe(
      Stream.schedule(Schedule.spaced(Duration.millis(self.pollIntervalMs))),
      Stream.flatMap((items) => Stream.fromIterable(items)),
    );

    return StreamPipeline.from(stream);
  }

  private offsetToTimestamp(offset: Offset): Date {
    switch (offset.type) {
      case "earliest":
        return new Date(0);
      case "latest":
        return new Date();
      case "timestamp":
        return new Date(offset.value);
      case "specific":
        // Interpret as ISO timestamp string
        return new Date(offset.value);
    }
  }

  private async pollSince(since: Date): Promise<{ value: T; ts: Date }[]> {
    const tsCol = this.timestampColumn;
    const seqCol = this.sequenceColumn;
    const table = this.table;
    const limit = this.pollBatchSize;
    const payloadExpr = this.payloadColumn ? `${this.payloadColumn}` : `row_to_json(t)`;

    const rows = (await this.db.execute(
      sql.raw(`
        SELECT ${payloadExpr} as payload, "${tsCol}" as ts
        FROM "${table}" t
        WHERE "${tsCol}" >= '${since.toISOString()}'
        ORDER BY "${seqCol}" ASC
        LIMIT ${limit}
      `),
    )) as any[];

    return rows.map((r: any) => ({
      value: this.codec.decode(r.payload),
      ts: r.ts instanceof Date ? r.ts : new Date(r.ts),
    }));
  }

  // ---------------------------------------------------------------------------
  // Publish — NOTIFY helper for producers
  // ---------------------------------------------------------------------------

  /**
   * Send a NOTIFY on the configured channel.
   * Call this after INSERT/UPDATE to push real-time events.
   */
  async notify(value: T): Promise<void> {
    const payload = JSON.stringify(this.codec.encode(value));
    await this.sqlClient.notify(this.channel, payload);
  }

  // ---------------------------------------------------------------------------
  // Trigger helpers — install/remove Postgres trigger for auto-NOTIFY
  // ---------------------------------------------------------------------------

  /**
   * Install a trigger on the table that auto-NOTIFYs on INSERT.
   * The trigger sends the payload column (or row JSON) as the notification payload.
   */
  async installTrigger(): Promise<void> {
    const fnName = `notify_${this.channel}`;
    const triggerName = `trg_notify_${this.channel}`;
    const payloadExpr = this.payloadColumn
      ? `NEW."${this.payloadColumn}"::text`
      : `row_to_json(NEW)::text`;

    await this.db.execute(
      sql.raw(`
      CREATE OR REPLACE FUNCTION ${fnName}() RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify('${this.channel}', ${payloadExpr});
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `),
    );

    await this.db.execute(
      sql.raw(`
      DROP TRIGGER IF EXISTS ${triggerName} ON "${this.table}";
      CREATE TRIGGER ${triggerName}
        AFTER INSERT ON "${this.table}"
        FOR EACH ROW EXECUTE FUNCTION ${fnName}();
    `),
    );
  }

  /** Remove the auto-NOTIFY trigger from the table. */
  async removeTrigger(): Promise<void> {
    const fnName = `notify_${this.channel}`;
    const triggerName = `trg_notify_${this.channel}`;

    await this.db.execute(
      sql.raw(`
      DROP TRIGGER IF EXISTS ${triggerName} ON "${this.table}";
      DROP FUNCTION IF EXISTS ${fnName}();
    `),
    );
  }
}
