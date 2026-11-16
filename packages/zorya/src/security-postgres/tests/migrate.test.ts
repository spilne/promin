import { expect, it } from "bun:test";
import { postgresDescribe } from "@promin/postgres/testing";
import { execRaw } from "@spilne/perfect-postgres";
import { getTableColumns, getTableName, sql } from "drizzle-orm";
import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { migrateSecurityStore } from "../migrate.ts";
import { SECURITY_POSTGRES_SCHEMA } from "../schema.ts";

const PACKAGE = resolve(import.meta.dir, "../../..");
const MIGRATIONS = join(PACKAGE, "security-drizzle");

postgresDescribe("PostgreSQL security migrations", { timeout: 120_000 }, (pg) => {
  it("installs cleanly, reruns idempotently, matches the schema and upgrades", async () => {
    await migrateSecurityStore(pg.db);
    await migrateSecurityStore(pg.db);

    const expectedColumns = Object.values(SECURITY_POSTGRES_SCHEMA)
      .flatMap((table) =>
        Object.values(getTableColumns(table)).map(
          (column) => `${getTableName(table)}.${column.name}`,
        ),
      )
      .sort();
    const databaseColumns = await execRaw(
      pg.db,
      sql`
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name LIKE 'zsec_%'
      ORDER BY table_name, ordinal_position
    `,
    );
    const actualColumns = databaseColumns
      .map((row) => `${String(row.table_name)}.${String(row.column_name)}`)
      .sort();
    expect(actualColumns).toEqual(expectedColumns);

    const historyAfterRerun = await execRaw(
      pg.db,
      sql`SELECT count(*)::int AS count FROM zorya_security.__zorya_security_migrations`,
    );
    expect(historyAfterRerun[0]?.count).toBe(1);

    const upgradeDir = await mkdtemp(join(tmpdir(), "promin-zorya-security-upgrade-"));
    try {
      await mkdir(join(upgradeDir, "meta"));
      await copyFile(
        join(MIGRATIONS, "0000_security_baseline.sql"),
        join(upgradeDir, "0000_security_baseline.sql"),
      );
      await writeFile(
        join(upgradeDir, "0001_upgrade_probe.sql"),
        "ALTER TABLE zsec_subject ADD COLUMN migration_probe boolean DEFAULT false NOT NULL;\n",
      );
      await writeFile(
        join(upgradeDir, "meta", "_journal.json"),
        `${JSON.stringify(
          {
            version: "7",
            dialect: "postgresql",
            entries: [
              {
                idx: 0,
                version: "7",
                when: 1795056000000,
                tag: "0000_security_baseline",
                breakpoints: true,
              },
              {
                idx: 1,
                version: "7",
                when: 1795056000001,
                tag: "0001_upgrade_probe",
                breakpoints: true,
              },
            ],
          },
          null,
          2,
        )}\n`,
      );

      await migrateSecurityStore(pg.db, { migrationsFolder: upgradeDir });
    } finally {
      await rm(upgradeDir, { recursive: true, force: true });
    }

    const upgradeColumn = await execRaw(
      pg.db,
      sql`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'zsec_subject'
        AND column_name = 'migration_probe'
    `,
    );
    expect(upgradeColumn).toHaveLength(1);
    const historyAfterUpgrade = await execRaw(
      pg.db,
      sql`SELECT count(*)::int AS count FROM zorya_security.__zorya_security_migrations`,
    );
    expect(historyAfterUpgrade[0]?.count).toBe(2);
  }, 120_000);
});
