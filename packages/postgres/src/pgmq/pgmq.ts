// ---------------------------------------------------------------------------
// pgmq — low-level SQL operations against the pgmq extension
// Matches pgmq 1.11 SQL API: https://pgmq.github.io/pgmq/api/sql/functions/
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { PgmqMessage, PgmqRecord, ReadMode } from "./types.ts";

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function parseRecords<T>(rows: any[]): PgmqRecord<T>[] {
  return rows.map((r) => ({
    msgId: Number(r.msg_id),
    readCt: r.read_ct,
    enqueuedAt: r.enqueued_at instanceof Date ? r.enqueued_at : new Date(r.enqueued_at),
    vt: r.vt instanceof Date ? r.vt : new Date(r.vt),
    message: r.message,
    headers: r.headers
      ? typeof r.headers === "string"
        ? JSON.parse(r.headers)
        : r.headers
      : undefined,
  }));
}

// ---------------------------------------------------------------------------
// Queue management
// ---------------------------------------------------------------------------

export async function createQueue(db: any, queue: string): Promise<void> {
  await db.execute(sql`SELECT pgmq.create(${queue})`);
}

export async function createUnloggedQueue(db: any, queue: string): Promise<void> {
  await db.execute(sql`SELECT pgmq.create_unlogged(${queue})`);
}

export async function createPartitionedQueue(
  db: any,
  queue: string,
  params?: { partitionInterval?: string; retentionInterval?: string },
): Promise<void> {
  const pi = params?.partitionInterval ?? "10000";
  const ri = params?.retentionInterval ?? "100000";
  await db.execute(sql`SELECT pgmq.create_partitioned(${queue}, ${pi}, ${ri})`);
}

export async function dropQueue(db: any, queue: string): Promise<boolean> {
  const [row] = await db.execute(sql`SELECT pgmq.drop_queue(${queue}) as dropped`);
  return row?.dropped === true;
}

export async function listQueues(
  db: any,
): Promise<{ queueName: string; createdAt: Date; isPartitioned: boolean; isUnlogged: boolean }[]> {
  const rows = await db.execute(sql`SELECT * FROM pgmq.list_queues()`);
  return rows.map((r: any) => ({
    queueName: r.queue_name,
    createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
    isPartitioned: r.is_partitioned ?? false,
    isUnlogged: r.is_unlogged ?? false,
  }));
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

export async function send<T>(db: any, queue: string, msg: PgmqMessage<T>): Promise<number> {
  const json = JSON.stringify(msg.data);
  const delay = msg.delay ?? 0;
  const headers = msg.headers ? JSON.stringify(msg.headers) : null;

  if (headers) {
    const [row] = await db.execute(
      sql`SELECT * FROM pgmq.send(${queue}::text, ${json}::jsonb, ${headers}::jsonb, ${delay}::integer)`,
    );
    return Number(row?.send ?? row?.msg_id);
  }
  const [row] = await db.execute(
    sql`SELECT * FROM pgmq.send(${queue}::text, ${json}::jsonb, ${delay}::integer)`,
  );
  return Number(row?.send ?? row?.msg_id);
}

export async function sendBatch<T>(
  db: any,
  queue: string,
  messages: PgmqMessage<T>[],
): Promise<number[]> {
  // Use individual sends for simplicity — pgmq.send_batch requires SQL array literals
  const ids: number[] = [];
  for (const msg of messages) {
    ids.push(await send(db, queue, msg));
  }
  return ids;
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

export async function read<T>(db: any, queue: string, mode: ReadMode): Promise<PgmqRecord<T>[]> {
  let rows: any[];

  switch (mode._tag) {
    case "standard":
      rows = await db.execute(sql`SELECT * FROM pgmq.read(${queue}, ${mode.vt}, ${mode.qty})`);
      break;

    case "poll":
      rows = await db.execute(
        sql`SELECT * FROM pgmq.read_with_poll(${queue}, ${mode.vt}, ${mode.qty}, ${mode.maxPollSeconds ?? 5}, ${mode.pollIntervalMs ?? 100})`,
      );
      break;

    case "grouped":
      rows = await db.execute(
        sql`SELECT * FROM pgmq.read_grouped(${queue}, ${mode.vt}, ${mode.qty})`,
      );
      break;

    case "grouped-poll":
      rows = await db.execute(
        sql`SELECT * FROM pgmq.read_grouped_with_poll(${queue}, ${mode.vt}, ${mode.qty}, ${mode.maxPollSeconds ?? 5}, ${mode.pollIntervalMs ?? 100})`,
      );
      break;

    case "grouped-round-robin":
      rows = await db.execute(
        sql`SELECT * FROM pgmq.read_grouped_rr(${queue}, ${mode.vt}, ${mode.qty})`,
      );
      break;

    case "grouped-round-robin-poll":
      rows = await db.execute(
        sql`SELECT * FROM pgmq.read_grouped_rr_with_poll(${queue}, ${mode.vt}, ${mode.qty}, ${mode.maxPollSeconds ?? 5}, ${mode.pollIntervalMs ?? 100})`,
      );
      break;
  }

  return parseRecords<T>(rows);
}

/** Pop (read + immediate delete) up to `qty` messages. */
export async function pop<T>(db: any, queue: string, qty: number = 1): Promise<PgmqRecord<T>[]> {
  const rows = await db.execute(sql`SELECT * FROM pgmq.pop(${queue}, ${qty})`);
  return parseRecords<T>(rows);
}

// ---------------------------------------------------------------------------
// Delete / Archive
// ---------------------------------------------------------------------------

export async function deleteMessage(db: any, queue: string, msgId: number): Promise<boolean> {
  const [row] = await db.execute(
    sql`SELECT pgmq.delete(${queue}::text, ${BigInt(msgId)}::bigint) as deleted`,
  );
  return row?.deleted === true;
}

export async function deleteBatch(db: any, queue: string, msgIds: number[]): Promise<number[]> {
  const arr = msgIds.map(BigInt);
  const rows = await db.execute(sql`SELECT * FROM pgmq.delete(${queue}, ${arr}::bigint[])`);
  return rows.map((r: any) => Number(r.delete));
}

export async function archive(db: any, queue: string, msgId: number): Promise<boolean> {
  const [row] = await db.execute(
    sql`SELECT pgmq.archive(${queue}::text, ${BigInt(msgId)}::bigint) as archived`,
  );
  return row?.archived === true;
}

export async function archiveBatch(db: any, queue: string, msgIds: number[]): Promise<number[]> {
  const arr = msgIds.map(BigInt);
  const rows = await db.execute(sql`SELECT * FROM pgmq.archive(${queue}, ${arr}::bigint[])`);
  return rows.map((r: any) => Number(r.archive));
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

export async function purgeQueue(db: any, queue: string): Promise<number> {
  const [row] = await db.execute(sql`SELECT pgmq.purge_queue(${queue}) as count`);
  return Number(row?.count ?? 0);
}

export async function setVt<T>(
  db: any,
  queue: string,
  msgId: number,
  vtSeconds: number,
): Promise<PgmqRecord<T> | null> {
  const rows = await db.execute(
    sql`SELECT * FROM pgmq.set_vt(${queue}::text, ${BigInt(msgId)}::bigint, ${vtSeconds}::integer)`,
  );
  const parsed = parseRecords<T>(rows);
  return parsed[0] ?? null;
}

export async function metrics(
  db: any,
  queue: string,
): Promise<{
  queueName: string;
  queueLength: number;
  newestMsgAgeSec: number | null;
  oldestMsgAgeSec: number | null;
  totalMessages: number;
}> {
  const [row] = await db.execute(sql`SELECT * FROM pgmq.metrics(${queue})`);
  return {
    queueName: row.queue_name,
    queueLength: Number(row.queue_length),
    newestMsgAgeSec: row.newest_msg_age_sec != null ? Number(row.newest_msg_age_sec) : null,
    oldestMsgAgeSec: row.oldest_msg_age_sec != null ? Number(row.oldest_msg_age_sec) : null,
    totalMessages: Number(row.total_messages),
  };
}

/** Enable NOTIFY on message insert for LISTEN-based consumers. */
export async function enableNotify(
  db: any,
  queue: string,
  throttleIntervalMs: number = 250,
): Promise<void> {
  await db.execute(sql`SELECT pgmq.enable_notify_insert(${queue}, ${throttleIntervalMs})`);
}

export async function disableNotify(db: any, queue: string): Promise<void> {
  await db.execute(sql`SELECT pgmq.disable_notify_insert(${queue})`);
}

/** Create FIFO index for grouped reads. */
export async function createFifoIndex(db: any, queue: string): Promise<void> {
  await db.execute(sql`SELECT pgmq.create_fifo_index(${queue})`);
}
