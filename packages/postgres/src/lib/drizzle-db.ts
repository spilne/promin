// ---------------------------------------------------------------------------
// DrizzleDb — shared type alias for Drizzle database instances
//
// Uses the base PgDatabase type so any Postgres driver works:
// postgres-js, bun:sql, node-postgres, neon, etc.
// ---------------------------------------------------------------------------

import { type SQL } from "drizzle-orm";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";

/** Drizzle database instance — driver-agnostic (postgres-js, bun:sql, etc). */
export type DrizzleDb = PgDatabase<PgQueryResultHKT, Record<string, unknown>>;

/** Execute raw SQL and return rows as any[] (for extension/dynamic queries). */
export async function execRaw(db: DrizzleDb, query: SQL): Promise<any[]> {
  return (await db.execute(query)) as any[];
}
