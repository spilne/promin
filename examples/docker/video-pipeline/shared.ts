// ---------------------------------------------------------------------------
// Shared bootstrap — Postgres connection, workflow storage, step queue.
// Imported by coordinator + every worker + the submit script so they all
// talk to the same DB. Config comes from env vars so the same compiled
// binary runs in dev (localhost) and in the docker-compose setup
// (service-name hostnames) with no code changes.
// ---------------------------------------------------------------------------

import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { PostgresWorkflowStorage, PgStepQueue, migrate, type DrizzleDb } from "@promin/postgres";

export const DATABASE_URL =
  process.env["DATABASE_URL"] ?? "postgres://promin:promin@localhost:5432/promin";

export interface Stack {
  readonly db: DrizzleDb;
  readonly storage: PostgresWorkflowStorage;
  readonly stepQueue: PgStepQueue;
  /** Close everything. Safe to call once at shutdown. */
  readonly close: () => Promise<void>;
}

export async function buildStack(): Promise<Stack> {
  const sql = postgres(DATABASE_URL, { max: 10 });
  const db = drizzle(sql) as DrizzleDb;
  const storage = await PostgresWorkflowStorage.create({ db });
  const stepQueue = new PgStepQueue({ db });
  return {
    db,
    storage,
    stepQueue,
    close: async () => {
      await sql.end({ timeout: 5 });
    },
  };
}

/**
 * Run migrations. Called by the one-shot `migrator` service before the
 * coordinator / workers come up, so every service reads a ready schema.
 */
export async function runMigrations(): Promise<void> {
  const sql = postgres(DATABASE_URL, { max: 1 });
  try {
    const db = drizzle(sql) as DrizzleDb;
    await migrate(db, {
      logger: {
        info: (m) => console.log(`[migrate] ${m}`),
        error: (m, e) => console.error(`[migrate] ${m}`, e),
      },
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
