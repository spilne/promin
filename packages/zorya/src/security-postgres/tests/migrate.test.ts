import { expect, it } from "bun:test";
import { postgresDescribe } from "@promin/postgres/testing";
import { type DrizzleDb, execRaw } from "@spilne/perfect-postgres";
import { getTableColumns, getTableName, sql } from "drizzle-orm";
import { getTableConfig, PgDialect, type PgTable } from "drizzle-orm/pg-core";
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
          (column) =>
            `${getTableName(table)}.${column.name}:${column.getSQLType()}:${column.notNull}`,
        ),
      )
      .sort();
    const databaseColumns = await execRaw(
      pg.db,
      sql`
      SELECT table_name, column_name, format_type(c.udt_name::regtype, NULL) AS sql_type,
             (is_nullable = 'NO') AS not_null
      FROM information_schema.columns c
      WHERE table_schema = ${SECURITY_DATA_SCHEMA} AND table_name LIKE 'zsec_%'
      ORDER BY table_name, ordinal_position
    `,
    );
    const actualColumns = databaseColumns
      .map(
        (row) =>
          `${String(row.table_name)}.${String(row.column_name)}:${String(row.sql_type)}:${String(row.not_null)}`,
      )
      .sort();
    expect(actualColumns).toEqual(expectedColumns);

    await expectCatalogMatchesSchema(pg.db, Object.values(SECURITY_POSTGRES_SCHEMA));

    const historyAfterRerun = await execRaw(
      pg.db,
      sql`SELECT count(*)::int AS count FROM zorya_security.__zorya_security_migrations`,
    );
    expect(historyAfterRerun[0]?.count).toBe(2);
  }, 120_000);

  it("installs into an empty database despite a hostile initial search_path", async () => {
    const databaseName = `zsec_hostile_${crypto.randomUUID().replaceAll("-", "")}`;
    await execRaw(pg.db, sql`CREATE DATABASE ${sql.identifier(databaseName)}`);
    const hostileUrl = new URL(pg.url);
    hostileUrl.pathname = `/${databaseName}`;
    let hostileClient: ReturnType<typeof postgres> | undefined;

    try {
      hostileClient = postgres(hostileUrl.toString(), {
        max: 8,
        connection: { search_path: "attacker,pg_catalog,public" },
      });
      const hostileDb = drizzle(hostileClient) as unknown as DrizzleDb;
      await execRaw(hostileDb, sql`CREATE SCHEMA attacker`);
      await execRaw(
        hostileDb,
        sql.raw(`
          CREATE FUNCTION attacker.set_config(text, text, boolean) RETURNS text
          LANGUAGE plpgsql AS $shadow$
          BEGIN RAISE EXCEPTION 'attacker set_config called'; END
          $shadow$;
          CREATE FUNCTION attacker.hashtextextended(text, bigint) RETURNS bigint
          LANGUAGE plpgsql AS $shadow$
          BEGIN RAISE EXCEPTION 'attacker hashtextextended called'; END
          $shadow$;
          CREATE FUNCTION attacker.pg_advisory_xact_lock(bigint) RETURNS void
          LANGUAGE plpgsql AS $shadow$
          BEGIN RAISE EXCEPTION 'attacker pg_advisory_xact_lock called'; END
          $shadow$;
        `),
      );
      const [path] = await execRaw(hostileDb, sql`SHOW search_path`);
      expect(path?.search_path).toBe("attacker,pg_catalog,public");

      await Promise.all(Array.from({ length: 8 }, () => migrateSecurityStore(hostileDb)));

      const [history] = await execRaw(
        hostileDb,
        sql`SELECT pg_catalog.count(*)::pg_catalog.int4 AS count
            FROM zorya_security.__zorya_security_migrations`,
      );
      expect(history?.count).toBe(2);
      const [tables] = await execRaw(
        hostileDb,
        sql`SELECT pg_catalog.count(*)::pg_catalog.int4 AS count
            FROM information_schema.tables
            WHERE table_schema='zorya_security_data' AND table_name LIKE 'zsec_%'`,
      );
      expect(tables?.count).toBe(14);
      const publicTables = await execRaw(
        hostileDb,
        sql`SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE 'zsec_%'`,
      );
      expect(publicTables).toHaveLength(0);
      const attackerTables = await execRaw(
        hostileDb,
        sql`SELECT table_name FROM information_schema.tables
            WHERE table_schema='attacker' AND table_name LIKE 'zsec_%'`,
      );
      expect(attackerTables).toHaveLength(0);
    } finally {
      await hostileClient?.end({ timeout: 1 });
      await execRaw(
        pg.db,
        sql`DROP DATABASE IF EXISTS ${sql.identifier(databaseName)} WITH (FORCE)`,
      );
    }
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

async function expectCatalogMatchesSchema(db: DrizzleDb, tables: PgTable[]): Promise<void> {
  const configs = tables.map((table) => getTableConfig(table));

  const expectedDefaults = configs
    .flatMap((config) =>
      config.columns
        .filter((column) => column.hasDefault && column.default !== undefined)
        .map(
          (column) =>
            `${config.name}.${column.name}:${renderDefault(column.default as string | number | boolean)}`,
        ),
    )
    .sort();
  const actualDefaults = (
    await execRaw(
      db,
      sql`SELECT r.relname AS table_name, a.attname AS column_name,
                 pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) AS default_expr
          FROM pg_catalog.pg_attrdef d
          JOIN pg_catalog.pg_class r ON r.oid=d.adrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid=r.oid AND a.attnum=d.adnum
          WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname LIKE 'zsec_%'
          ORDER BY r.relname,a.attname`,
    )
  )
    .map(
      (row) => `${String(row.table_name)}.${String(row.column_name)}:${String(row.default_expr)}`,
    )
    .sort();
  expect(actualDefaults).toEqual(expectedDefaults);

  const expectedPrimaryKeys = configs
    .flatMap((config) =>
      config.primaryKeys.map((key) => ({
        table: config.name,
        name: key.getName(),
        columns: key.columns.map((column) => column.name),
      })),
    )
    .sort(byCatalogIdentity);
  const actualPrimaryKeys = (
    await execRaw(
      db,
      sql`SELECT r.relname AS table, c.conname AS name,
                 ARRAY(
                   SELECT a.attname
                   FROM pg_catalog.unnest(c.conkey) WITH ORDINALITY AS key(attnum,ord)
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.attnum
                   ORDER BY key.ord
                 ) AS columns
          FROM pg_catalog.pg_constraint c
          JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
          WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname LIKE 'zsec_%' AND c.contype='p'
          ORDER BY r.relname,c.conname`,
    )
  )
    .map((row) => ({
      table: String(row.table),
      name: String(row.name),
      columns: row.columns as string[],
    }))
    .sort(byCatalogIdentity);
  expect(actualPrimaryKeys).toEqual(expectedPrimaryKeys);

  const expectedForeignKeys = configs
    .flatMap((config) =>
      config.foreignKeys.map((key) => {
        const reference = key.reference();
        const foreign = getTableConfig(reference.foreignTable);
        return {
          table: config.name,
          name: key.getName(),
          columns: reference.columns.map((column) => column.name),
          foreignSchema: foreign.schema ?? "public",
          foreignTable: foreign.name,
          foreignColumns: reference.foreignColumns.map((column) => column.name),
          onDelete: key.onDelete ?? "no action",
          onUpdate: key.onUpdate ?? "no action",
        };
      }),
    )
    .sort(byCatalogIdentity);
  const actualForeignKeys = (
    await execRaw(
      db,
      sql`SELECT r.relname AS table, c.conname AS name,
                 ARRAY(
                   SELECT a.attname
                   FROM pg_catalog.unnest(c.conkey) WITH ORDINALITY AS key(attnum,ord)
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.attnum
                   ORDER BY key.ord
                 ) AS columns,
                 fn.nspname AS foreign_schema, fr.relname AS foreign_table,
                 ARRAY(
                   SELECT a.attname
                   FROM pg_catalog.unnest(c.confkey) WITH ORDINALITY AS key(attnum,ord)
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=key.attnum
                   ORDER BY key.ord
                 ) AS foreign_columns,
                 CASE c.confdeltype WHEN 'c' THEN 'cascade' WHEN 'r' THEN 'restrict'
                   WHEN 'n' THEN 'set null' WHEN 'd' THEN 'set default' ELSE 'no action' END AS on_delete,
                 CASE c.confupdtype WHEN 'c' THEN 'cascade' WHEN 'r' THEN 'restrict'
                   WHEN 'n' THEN 'set null' WHEN 'd' THEN 'set default' ELSE 'no action' END AS on_update
          FROM pg_catalog.pg_constraint c
          JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
          JOIN pg_catalog.pg_class fr ON fr.oid=c.confrelid
          JOIN pg_catalog.pg_namespace fn ON fn.oid=fr.relnamespace
          WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname LIKE 'zsec_%' AND c.contype='f'
          ORDER BY r.relname,c.conname`,
    )
  )
    .map((row) => ({
      table: String(row.table),
      name: String(row.name),
      columns: row.columns as string[],
      foreignSchema: String(row.foreign_schema),
      foreignTable: String(row.foreign_table),
      foreignColumns: row.foreign_columns as string[],
      onDelete: String(row.on_delete),
      onUpdate: String(row.on_update),
    }))
    .sort(byCatalogIdentity);
  expect(actualForeignKeys).toEqual(expectedForeignKeys);

  const expectedIndexes = configs
    .flatMap((config) => [
      ...config.primaryKeys.map((key) => ({
        table: config.name,
        name: key.getName(),
        unique: true,
        primary: true,
        method: "btree",
        columns: key.columns.map((column) => column.name),
      })),
      ...config.indexes.map((value) => ({
        table: config.name,
        name: value.config.name!,
        unique: value.config.unique,
        primary: false,
        method: value.config.method ?? "btree",
        columns: value.config.columns.map((column) => (column as { name: string }).name),
      })),
    ])
    .sort(byCatalogIdentity);
  const actualIndexes = (
    await execRaw(
      db,
      sql`SELECT r.relname AS table, idx.relname AS name, i.indisunique AS unique,
                 i.indisprimary AS primary, am.amname AS method,
                 ARRAY(
                   SELECT a.attname
                   FROM pg_catalog.unnest(i.indkey::pg_catalog.int2[]) WITH ORDINALITY AS key(attnum,ord)
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum
                   WHERE key.ord <= i.indnkeyatts
                   ORDER BY key.ord
                 ) AS columns
          FROM pg_catalog.pg_index i
          JOIN pg_catalog.pg_class r ON r.oid=i.indrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
          JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
          JOIN pg_catalog.pg_am am ON am.oid=idx.relam
          WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname LIKE 'zsec_%'
          ORDER BY r.relname,idx.relname`,
    )
  )
    .map((row) => ({
      table: String(row.table),
      name: String(row.name),
      unique: Boolean(row.unique),
      primary: Boolean(row.primary),
      method: String(row.method),
      columns: row.columns as string[],
    }))
    .sort(byCatalogIdentity);
  expect(actualIndexes).toEqual(expectedIndexes);

  const expectedCheckNames = configs
    .flatMap((config) => config.checks.map((value) => `${config.name}.${value.name}`))
    .sort();
  const actualCheckNames = (
    await execRaw(
      db,
      sql`SELECT r.relname AS table_name,c.conname AS constraint_name
          FROM pg_catalog.pg_constraint c
          JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
          JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
          WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname LIKE 'zsec_%' AND c.contype='c'
          ORDER BY r.relname,c.conname`,
    )
  ).map((row) => `${String(row.table_name)}.${String(row.constraint_name)}`);
  expect(actualCheckNames).toEqual(expectedCheckNames);

  const dialect = new PgDialect();
  await db.transaction(async (rawTx) => {
    const tx = rawTx as unknown as DrizzleDb;
    let probeIndex = 0;
    for (const config of configs) {
      const qualifier = `"${SECURITY_DATA_SCHEMA}"."${config.name}".`;
      for (const value of config.checks) {
        const rendered = dialect.sqlToQuery(value.value);
        expect(rendered.params).toEqual([]);
        const expression = rendered.sql.replaceAll(qualifier, "");
        const probe = `zcp_${probeIndex++}`;
        await execRaw(
          tx,
          sql.raw(
            `ALTER TABLE ${quoteIdentifier(SECURITY_DATA_SCHEMA)}.${quoteIdentifier(config.name)} ADD CONSTRAINT ${quoteIdentifier(probe)} CHECK (${expression}) NOT VALID`,
          ),
        );
        const definitions = await execRaw(
          tx,
          sql`SELECT c.conname AS name,
                     pg_catalog.pg_get_expr(c.conbin,c.conrelid,true) AS expression
              FROM pg_catalog.pg_constraint c
              JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
              JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
              WHERE n.nspname=${SECURITY_DATA_SCHEMA} AND r.relname=${config.name}
                AND c.conname IN (${value.name},${probe})`,
        );
        const byName = new Map(
          definitions.map((row) => [String(row.name), String(row.expression)] as const),
        );
        expect(byName.get(probe), `${config.name}.${value.name}`).toBe(byName.get(value.name));
        await execRaw(
          tx,
          sql.raw(
            `ALTER TABLE ${quoteIdentifier(SECURITY_DATA_SCHEMA)}.${quoteIdentifier(config.name)} DROP CONSTRAINT ${quoteIdentifier(probe)}`,
          ),
        );
      }
    }
  });
}

function renderDefault(value: string | number | boolean): string {
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'::text`;
  return String(value);
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function byCatalogIdentity(
  left: { table: string; name: string },
  right: { table: string; name: string },
) {
  return `${left.table}.${left.name}`.localeCompare(`${right.table}.${right.name}`);
}
