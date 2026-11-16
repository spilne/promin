import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import { readMigrationFiles } from "drizzle-orm/migrator";
import { fileURLToPath } from "node:url";

export interface SecurityMigrateOptions {
  /** Override the packaged `security-drizzle/` migration directory. */
  migrationsFolder?: string;
  /** Migration history table. Default: `__zorya_security_migrations`. */
  migrationsTable?: string;
  /** Schema containing migration history. Default: `zorya_security`. */
  migrationsSchema?: string;
  /** Optional migration lifecycle logger. */
  logger?: {
    info: (message: string) => void;
    error: (message: string, error?: unknown) => void;
  };
}

/**
 * Apply the independent enterprise-security migration stream.
 *
 * The dedicated history schema/table prevents these migrations from
 * colliding with applications that also use Drizzle's migrator.
 */
export async function migrateSecurityStore(
  db: DrizzleDb,
  options?: SecurityMigrateOptions,
): Promise<void> {
  const logger = options?.logger ?? { info: () => {}, error: () => {} };
  const migrationsFolder =
    options?.migrationsFolder ?? fileURLToPath(new URL("../../security-drizzle", import.meta.url));
  const migrationsTable = options?.migrationsTable ?? "__zorya_security_migrations";
  const migrationsSchema = options?.migrationsSchema ?? "zorya_security";
  const migrations = readMigrationFiles({ migrationsFolder });

  try {
    logger.info("Starting security store migrations...");
    await db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DrizzleDb;
      // `0000` predates the dedicated data schema and intentionally remains
      // immutable. Pin its unqualified DDL to the trusted public schema before
      // taking the migration lock; pg_catalog remains implicitly first for
      // resolution because it is omitted from this path.
      await execRaw(tx, sql.raw("SET LOCAL search_path = public"));
      await execRaw(
        tx,
        sql`SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('zorya-security-migrations', 0))`,
      );
      await execRaw(tx, sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(migrationsSchema)}`);
      await execRaw(
        tx,
        sql`CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)} (
          id serial PRIMARY KEY,
          hash text NOT NULL,
          created_at bigint
        )`,
      );
      const [lastMigration] = await execRaw(
        tx,
        sql`SELECT created_at FROM ${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)}
            ORDER BY created_at DESC LIMIT 1`,
      );
      const lastAppliedAt = lastMigration ? Number(lastMigration.created_at) : undefined;
      for (const migration of migrations) {
        if (lastAppliedAt === undefined || lastAppliedAt < migration.folderMillis) {
          for (const statement of migration.sql) await execRaw(tx, sql.raw(statement));
          await execRaw(
            tx,
            sql`INSERT INTO ${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)}
                (hash, created_at) VALUES (${migration.hash}, ${migration.folderMillis})`,
          );
        }
      }
    });
    logger.info("Security store migrations completed.");
  } catch (error) {
    logger.error("Failed to apply security store migrations.", error);
    throw error;
  }
}
