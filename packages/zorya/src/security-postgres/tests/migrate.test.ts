import { expect, it } from "bun:test";
import { postgresDescribe } from "@promin/postgres/testing";
import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { getTableColumns, getTableName, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import postgres from "postgres";
import { migrateSecurityStore } from "../migrate.ts";
import { SECURITY_DATA_SCHEMA, SECURITY_POSTGRES_SCHEMA } from "../schema.ts";

const PACKAGE = resolve(import.meta.dir, "../../..");
const MIGRATIONS = join(PACKAGE, "security-drizzle");

postgresDescribe("PostgreSQL security migrations", { timeout: 120_000 }, (pg) => {
  it("installs cleanly, reruns idempotently, matches the schema and upgrades", async () => {
    const legacyDir = await mkdtemp(join(tmpdir(), "promin-zorya-security-legacy-"));
    try {
      await mkdir(join(legacyDir, "meta"));
      await copyFile(
        join(MIGRATIONS, "0000_security_baseline.sql"),
        join(legacyDir, "0000_security_baseline.sql"),
      );
      await writeFile(
        join(legacyDir, "meta", "_journal.json"),
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
            ],
          },
          null,
          2,
        )}\n`,
      );
      await migrateSecurityStore(pg.db, { migrationsFolder: legacyDir });
    } finally {
      await rm(legacyDir, { recursive: true, force: true });
    }
    await execRaw(
      pg.db,
      sql`INSERT INTO public.zsec_subject(kind,id,status,created_at,updated_at)
          VALUES ('user','legacy-user','active',0,0)`,
    );

    await Promise.all(Array.from({ length: 8 }, () => migrateSecurityStore(pg.db)));
    const [upgraded] = await execRaw(
      pg.db,
      sql`SELECT id FROM zorya_security_data.zsec_subject WHERE id='legacy-user'`,
    );
    expect(upgraded?.id).toBe("legacy-user");
    const historyAfterUpgrade = await execRaw(
      pg.db,
      sql`SELECT count(*)::int AS count FROM zorya_security.__zorya_security_migrations`,
    );
    expect(historyAfterUpgrade[0]?.count).toBe(2);

    await execRaw(pg.db, sql`DROP SCHEMA zorya_security_data CASCADE`);
    await execRaw(pg.db, sql`DROP SCHEMA zorya_security CASCADE`);
    await Promise.all(Array.from({ length: 8 }, () => migrateSecurityStore(pg.db)));
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
      WHERE table_schema = ${SECURITY_DATA_SCHEMA} AND table_name LIKE 'zsec_%'
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
    expect(historyAfterRerun[0]?.count).toBe(2);
  }, 120_000);

  it("ignores a hostile search_path and rejects rows outside shared boundaries", async () => {
    await migrateSecurityStore(pg.db);
    await execRaw(pg.db, sql`CREATE SCHEMA hostile_security`);
    await execRaw(
      pg.db,
      sql`CREATE TABLE hostile_security.zsec_subject (kind text, id text, status text)`,
    );

    const hostileClient = postgres(pg.url, {
      max: 8,
      connection: { search_path: "hostile_security, public" },
    });
    try {
      const hostileDb = drizzle(hostileClient) as unknown as DrizzleDb;
      await migrateSecurityStore(hostileDb);
      const [real] = await execRaw(
        hostileDb,
        sql`SELECT count(*)::int AS count FROM zorya_security_data.zsec_subject`,
      );
      const [decoy] = await execRaw(
        hostileDb,
        sql`SELECT count(*)::int AS count FROM hostile_security.zsec_subject`,
      );
      expect(real?.count).toBe(0);
      expect(decoy?.count).toBe(0);
    } finally {
      await hostileClient.end();
    }

    const rejectCheck = async (label: string, query: ReturnType<typeof sql>): Promise<void> => {
      try {
        await execRaw(pg.db, query);
        throw new Error(`expected database CHECK constraint rejection: ${label}`);
      } catch (error) {
        const code =
          (error as { code?: string; cause?: { code?: string } }).code ??
          (error as { cause?: { code?: string } }).cause?.code;
        if (code === undefined) throw error;
        expect(code).toBe("23514");
      }
    };

    await rejectCheck(
      "identifier length",
      sql`
      INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at)
      VALUES ('user',${"x".repeat(257)},'active',0,0)
    `,
    );
    await rejectCheck(
      "unsafe identifier",
      sql`
      INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at)
      VALUES ('user',${"unsafe\u202eidentifier"},'active',0,0)
    `,
    );
    await rejectCheck(
      "unsafe bigint",
      sql`
      INSERT INTO zorya_security_data.zsec_denial_bucket(key,window_started_at,window_ends_at,count,suppressed)
      VALUES ('overflow',0,1,9007199254740992,0)
    `,
    );
    await execRaw(
      pg.db,
      sql`INSERT INTO zorya_security_data.zsec_subject(kind,id,status,created_at,updated_at)
          VALUES ('group','direct-group','active',0,0)`,
    );
    await rejectCheck(
      "group session principal",
      sql`INSERT INTO zorya_security_data.zsec_session(
        token_hash,principal_kind,principal_id,created_at,expires_at,auth_method
      ) VALUES ('group-session','group','direct-group',0,1,'oidc')`,
    );

    await rejectCheck(
      "nested metadata",
      sql`INSERT INTO zorya_security_data.zsec_audit_record(
        id,namespace_id,sequence,occurred_at,recorded_at,event,metadata,hash,key_id
      ) VALUES ('direct-audit','org',1,0,0,'direct',${JSON.stringify({ nested: { forbidden: true } })}::jsonb,'hash','key')`,
    );
    await rejectCheck(
      "oversized metadata",
      sql`INSERT INTO zorya_security_data.zsec_audit_record(
        id,namespace_id,sequence,occurred_at,recorded_at,event,metadata,hash,key_id
      ) VALUES ('large-audit','org',1,0,0,'direct',${JSON.stringify(
        Object.fromEntries(Array.from({ length: 65 }, (_, index) => [`key-${index}`, index])),
      )}::jsonb,'hash','key')`,
    );

    const publicTables = await execRaw(
      pg.db,
      sql`SELECT table_name FROM information_schema.tables
          WHERE table_schema = 'public' AND table_name LIKE 'zsec_%'`,
    );
    expect(publicTables).toHaveLength(0);
  }, 120_000);
});
