// ---------------------------------------------------------------------------
// Schema migration — Drizzle migrator + lookup seeding
// ---------------------------------------------------------------------------

import { migrate as drizzleMigrate } from "drizzle-orm/postgres-js/migrator";
import type { DrizzleDb } from "./drizzle-db.ts";
import { seedLookupEnums } from "./lookup-table.ts";
import { LOOKUP_BINDINGS } from "./schema.ts";

export interface MigrateOptions {
  /** Override migrations folder path. Default: `drizzle/` relative to this package. */
  migrationsFolder?: string;
  /** Custom migrations table name. Useful for multi-app databases. Default: "__drizzle_migrations". */
  migrationsTable?: string;
  /** Schema for migrations table. Default: "public". */
  migrationsSchema?: string;
  /** Logger for migration progress. Default: no-op. */
  logger?: {
    info: (message: string) => void;
    error: (message: string, error?: unknown) => void;
  };
}

/**
 * Run migrations and seed lookup data.
 *
 * Uses Drizzle's migrator with SQL files from `drizzle/`.
 * The schema in `schema.ts` is the single source of truth.
 * Idempotent — safe to call on every startup.
 *
 * @example
 * ```ts
 * await migrate(db);
 *
 * // With custom table for multi-app DB
 * await migrate(db, {
 *   migrationsTable: "__drizzle_migrations_workflows",
 *   logger: { info: console.log, error: console.error },
 * });
 * ```
 */
export async function migrate(db: DrizzleDb, options?: MigrateOptions): Promise<void> {
  const logger = options?.logger ?? { info: () => {}, error: () => {} };
  const migrationsFolder =
    options?.migrationsFolder ?? new URL("../../drizzle", import.meta.url).pathname;

  try {
    logger.info("Starting workflow schema migrations...");

    // drizzleMigrate expects a driver-specific type; cast is safe since
    // the migrator only uses db.execute() which all drivers implement.
    await drizzleMigrate(db as any, {
      migrationsFolder,
      migrationsTable: options?.migrationsTable,
      migrationsSchema: options?.migrationsSchema,
    });

    logger.info("Seeding lookup tables...");
    await seedLookupEnums(db, LOOKUP_BINDINGS);

    logger.info("Workflow schema migrations completed.");
  } catch (error) {
    logger.error("Failed to apply workflow migrations:", error);
    throw error;
  }
}
