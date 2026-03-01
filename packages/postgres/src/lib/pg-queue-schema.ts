// ---------------------------------------------------------------------------
// Schema for SKIP LOCKED-based queue (no pgmq extension required)
// ---------------------------------------------------------------------------

import { pgTable, text, integer, jsonb, timestamp, bigserial } from "drizzle-orm/pg-core";

export function createQueueTable(queueName: string) {
  return pgTable(`pgq_${queueName}`, {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    payload: jsonb("payload").notNull(),
    headers: jsonb("headers"),
    status: text("status").notNull().default("pending"),
    visibleAt: timestamp("visible_at", { withTimezone: true }).notNull().defaultNow(),
    attemptCount: integer("attempt_count").notNull().default(0),
    maxAttempts: integer("max_attempts").notNull().default(3),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    lockedBy: text("locked_by"),
  });
}

export type QueueTable = ReturnType<typeof createQueueTable>;
