import { it, expect } from "bun:test";
import { getTableConfig, PgTable } from "drizzle-orm/pg-core";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import * as schema from "../schema.ts";
import * as schedulerSchema from "../scheduler-schema.ts";

// ---------------------------------------------------------------------------
// schema.ts ⇔ drizzle/ migrations drift guard.
//
// Builds a database purely from the migration chain (what CI and fresh
// deploys get) and checks that every table, column, index and foreign key
// (and named CHECK) declared in the drizzle schema exists with the declared shape — and that
// the migrated tables carry nothing the schema doesn't declare. A feature
// that edits only one side fails here instead of in production.
// ---------------------------------------------------------------------------

interface DbColumn {
  table_name: string;
  column_name: string;
  is_nullable: "YES" | "NO";
  data_type: string;
}

const tables = [...Object.values(schema), ...Object.values(schedulerSchema)].filter(
  (v): v is PgTable => v instanceof PgTable,
);

/** Normalise a drizzle SQL type to information_schema.columns.data_type. */
function expectedDataType(sqlType: string): string {
  if (sqlType.endsWith("[]")) return "ARRAY";
  if (sqlType === "bigserial") return "bigint";
  if (sqlType === "serial") return "integer";
  if (sqlType.startsWith("timestamp")) {
    return sqlType.includes("with time zone")
      ? "timestamp with time zone"
      : "timestamp without time zone";
  }
  return sqlType;
}

postgresDescribe("schema.ts matches the migration chain", { migrate }, (pg) => {
  it("declares every migrated table, column, index and foreign key — and nothing else", async () => {
    const columns = (await pg.sql`
      SELECT table_name, column_name, is_nullable, data_type
      FROM information_schema.columns WHERE table_schema = 'public'
    `) as unknown as DbColumn[];
    const indexes = (await pg.sql`
      SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public'
    `) as unknown as Array<{ tablename: string; indexname: string }>;
    const constraintIndexes = new Set(
      (
        (await pg.sql`
          SELECT conname FROM pg_constraint WHERE contype IN ('p', 'u')
        `) as unknown as Array<{ conname: string }>
      ).map((r) => r.conname),
    );
    const checks = new Set(
      (
        (await pg.sql`
          SELECT conname FROM pg_constraint WHERE contype = 'c'
        `) as unknown as Array<{ conname: string }>
      ).map((r) => r.conname),
    );
    const fks = (await pg.sql`
      SELECT c.conrelid::regclass::text AS table_name,
             c.confrelid::regclass::text AS foreign_table,
             array_to_string(ARRAY(
               SELECT a.attname FROM unnest(c.conkey) k
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k
             ), ',') AS cols
      FROM pg_constraint c WHERE c.contype = 'f'
    `) as unknown as Array<{ table_name: string; foreign_table: string; cols: string }>;

    const problems: string[] = [];
    for (const table of tables) {
      const cfg = getTableConfig(table);
      const dbCols = columns.filter((c) => c.table_name === cfg.name);
      if (dbCols.length === 0) {
        problems.push(`table ${cfg.name}: missing from migrations`);
        continue;
      }
      for (const col of cfg.columns) {
        const dbCol = dbCols.find((c) => c.column_name === col.name);
        if (!dbCol) {
          problems.push(`${cfg.name}.${col.name}: column missing from migrations`);
          continue;
        }
        const notNull = col.notNull || col.primary;
        if (notNull !== (dbCol.is_nullable === "NO")) {
          problems.push(`${cfg.name}.${col.name}: NOT NULL mismatch (schema ${notNull})`);
        }
        const want = expectedDataType(col.getSQLType());
        if (want !== dbCol.data_type) {
          problems.push(`${cfg.name}.${col.name}: type ${dbCol.data_type}, schema says ${want}`);
        }
      }
      for (const dbCol of dbCols) {
        if (!cfg.columns.some((c) => c.name === dbCol.column_name)) {
          problems.push(`${cfg.name}.${dbCol.column_name}: column not declared in schema.ts`);
        }
      }

      const declaredIndexes = new Set(cfg.indexes.map((i) => i.config.name));
      const dbIndexes = indexes.filter((i) => i.tablename === cfg.name).map((i) => i.indexname);
      for (const name of declaredIndexes) {
        if (!dbIndexes.includes(name!)) problems.push(`${cfg.name}: index ${name} missing`);
      }
      for (const name of dbIndexes) {
        if (!declaredIndexes.has(name) && !constraintIndexes.has(name)) {
          problems.push(`${cfg.name}: index ${name} not declared in schema.ts`);
        }
      }

      for (const c of cfg.checks) {
        if (!checks.has(c.name)) problems.push(`${cfg.name}: check ${c.name} missing`);
      }

      const declaredFks = cfg.foreignKeys.map((fk) => {
        const ref = fk.reference();
        return {
          cols: ref.columns.map((c) => c.name).join(","),
          foreign: getTableConfig(ref.foreignTable).name,
        };
      });
      const dbFks = fks.filter((f) => f.table_name === cfg.name);
      for (const fk of declaredFks) {
        if (!dbFks.some((f) => f.cols === fk.cols && f.foreign_table === fk.foreign)) {
          problems.push(`${cfg.name}(${fk.cols}) → ${fk.foreign}: foreign key missing`);
        }
      }
      for (const fk of dbFks) {
        if (!declaredFks.some((d) => d.cols === fk.cols && d.foreign === fk.foreign_table)) {
          problems.push(`${cfg.name}(${fk.cols}) → ${fk.foreign_table}: FK not in schema.ts`);
        }
      }
    }

    expect(problems).toEqual([]);
  });
});
