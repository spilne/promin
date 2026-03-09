// ---------------------------------------------------------------------------
// Drizzle schema for durable scheduler
// ---------------------------------------------------------------------------

import {
  pgTable,
  text,
  integer,
  boolean,
  jsonb,
  timestamp,
  index,
  bigint,
} from "drizzle-orm/pg-core";

export const durableSchedules = pgTable(
  "wf_schedules",
  {
    id: text("id").primaryKey(),
    name: text("name"),
    cron: text("cron"),
    rrule: text("rrule"),
    intervalMs: bigint("interval_ms", { mode: "number" }),
    timezone: text("timezone").notNull().default("UTC"),
    overlapPolicy: text("overlap_policy").notNull().default("allow"),
    maxCatchUp: integer("max_catch_up").notNull().default(0),
    jitterMs: integer("jitter_ms").notNull().default(0),
    enabled: boolean("enabled").notNull().default(true),
    startAt: timestamp("start_at", { withTimezone: true }),
    endAt: timestamp("end_at", { withTimezone: true }),
    metadata: jsonb("metadata"),
    lastFiredAt: timestamp("last_fired_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index("wf_schedules_enabled_idx").on(t.enabled)],
);

export const durableScheduleTicks = pgTable(
  "wf_schedule_ticks",
  {
    scheduleId: text("schedule_id")
      .notNull()
      .references(() => durableSchedules.id, { onDelete: "cascade" }),
    scheduledAt: timestamp("scheduled_at", { withTimezone: true }).notNull(),
    firedAt: timestamp("fired_at", { withTimezone: true }).notNull().defaultNow(),
    catchUp: boolean("catch_up").notNull().default(false),
    tickNumber: bigint("tick_number", { mode: "number" }).notNull().default(0),
  },
  (t) => [index("wf_schedule_ticks_schedule_idx").on(t.scheduleId)],
);
