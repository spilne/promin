import type { DrizzleDb } from "@spilne/perfect-postgres";
import { migrate as drizzleMigrate } from "drizzle-orm/postgres-js/migrator";
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

  try {
    logger.info("Starting security store migrations...");
    await drizzleMigrate(db as Parameters<typeof drizzleMigrate>[0], {
      migrationsFolder,
      migrationsTable: options?.migrationsTable ?? "__zorya_security_migrations",
      migrationsSchema: options?.migrationsSchema ?? "zorya_security",
    });
    logger.info("Security store migrations completed.");
  } catch (error) {
    logger.error("Failed to apply security store migrations.", error);
    throw error;
  }
}
