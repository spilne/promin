// ---------------------------------------------------------------------------
// execRaw — loosely typed raw-SQL helper for the store implementations.
//
// Delegates to perfect-postgres's `execRaw` (which normalizes postgres-js
// arrays and node-postgres/Neon result objects) but widens the row type to
// `any` so call sites can read snake_case columns without per-site casts.
// ---------------------------------------------------------------------------

import type { SQL } from "drizzle-orm";
import { type DrizzleDb, execRaw as execRows } from "@spilne/perfect-postgres";

/** Execute raw SQL and return the result rows untyped. */
export async function execRaw(db: DrizzleDb, query: SQL): Promise<any[]> {
  return execRows(db, query);
}
