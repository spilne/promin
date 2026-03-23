// ---------------------------------------------------------------------------
// Drizzle schema for topology state backend
// ---------------------------------------------------------------------------

import { pgTable, text, jsonb, timestamp, index, primaryKey } from "drizzle-orm/pg-core";

export function createTopologyStateTable(tableName = "topology_state") {
  return pgTable(
    tableName,
    {
      key: text("key").notNull(),
      value: jsonb("value").notNull(),
      checkpoint: text("checkpoint").notNull().default("live"),
      updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    },
    (t) => [
      primaryKey({ columns: [t.key, t.checkpoint] }),
      index(`idx_${tableName}_checkpoint`).on(t.checkpoint),
    ],
  );
}

export const topologyState = createTopologyStateTable();
