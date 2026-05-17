// ---------------------------------------------------------------------------
// PostgresToolHistoryStore — durable backend for the tool catalog history.
//
// Mirror of InMemoryToolHistoryStore against Postgres so the audit trail
// survives a restart and spans every replica. `recordSnapshot` upserts
// each observation: a new identity tuple inserts with first/last seen at
// the database clock; an existing tuple advances last_seen_at and
// refreshes the description.
//
// first/last_seen_at are owned by the database clock (server-side NOW()),
// never the caller — see `agent_tool_history` in `schema.ts` for the DDL.
// ---------------------------------------------------------------------------

import { and, asc, desc, eq, gte, sql } from "drizzle-orm";
import type {
  ToolHistoryQuery,
  ToolHistoryRecord,
  ToolHistoryStore,
  ToolObservation,
} from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { agentToolHistory } from "../schema.ts";

export interface PostgresToolHistoryStoreConfig {
  readonly db: DrizzleDb;
}

export class PostgresToolHistoryStore implements ToolHistoryStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresToolHistoryStoreConfig) {
    this.db = config.db;
  }

  async recordSnapshot(observations: ReadonlyArray<ToolObservation>): Promise<void> {
    // Server clock owns the timestamps — `extract(epoch ...)` yields
    // seconds, ×1000 lands the epoch-ms BIGINT the columns expect.
    const now = sql`(extract(epoch from now()) * 1000)::bigint`;
    for (const o of observations) {
      await this.db
        .insert(agentToolHistory)
        .values({
          name: o.name,
          sourceKind: o.sourceKind,
          sourceDetail: o.sourceDetail,
          schemaHash: o.schemaHash,
          description: o.description,
          firstSeenAt: now,
          lastSeenAt: now,
        })
        .onConflictDoUpdate({
          target: [
            agentToolHistory.name,
            agentToolHistory.sourceKind,
            agentToolHistory.sourceDetail,
            agentToolHistory.schemaHash,
          ],
          set: { lastSeenAt: now, description: o.description },
        });
    }
  }

  async list(query: ToolHistoryQuery = {}): Promise<ToolHistoryRecord[]> {
    const conditions = [];
    if (query.name !== undefined) {
      conditions.push(eq(agentToolHistory.name, query.name));
    }
    if (query.sourceKind !== undefined) {
      conditions.push(eq(agentToolHistory.sourceKind, query.sourceKind));
    }
    if (query.since !== undefined) {
      conditions.push(gte(agentToolHistory.lastSeenAt, query.since));
    }

    let select = this.db
      .select()
      .from(agentToolHistory)
      .where(conditions.length > 0 ? and(...conditions) : undefined)
      .orderBy(desc(agentToolHistory.lastSeenAt), asc(agentToolHistory.name))
      .$dynamic();
    if (query.limit !== undefined) {
      select = select.limit(query.limit);
    }

    const rows = await select;
    return rows.map((row) => ({
      name: row.name,
      sourceKind: row.sourceKind as ToolObservation["sourceKind"],
      sourceDetail: row.sourceDetail,
      schemaHash: row.schemaHash,
      description: row.description,
      firstSeenAt: row.firstSeenAt,
      lastSeenAt: row.lastSeenAt,
    }));
  }
}
