// ---------------------------------------------------------------------------
// Schema utilities — derive CREATE TABLE IF NOT EXISTS from Drizzle schema
//
// Single source of truth: define the table once as a pgTable, then use
// ensureTable(db, table) for quick bootstrap or include the schema in
// your Drizzle migration pipeline for production.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import { getTableConfig } from "drizzle-orm/pg-core";
import type { PgTable } from "drizzle-orm/pg-core";
import type { DrizzleDb } from "./drizzle-db.ts";

/**
 * Create a table from a Drizzle schema definition if it doesn't exist.
 * Derives the DDL from the schema — no hand-written SQL needed.
 *
 * Also creates any indexes defined on the table.
 *
 * @example
 * ```ts
 * import { topologyState } from "@promin/postgres";
 *
 * // Quick bootstrap — no migration files needed
 * await ensureTable(db, topologyState);
 * ```
 */
export async function ensureTable(db: DrizzleDb, table: PgTable): Promise<void> {
  const config = getTableConfig(table);

  // Build column definitions
  const columnDefs = config.columns.map((col) => {
    const parts: string[] = [`"${col.name}"`, col.getSQLType()];

    if (col.notNull) parts.push("NOT NULL");

    if (col.hasDefault && col.default !== undefined) {
      const def = col.default;
      if (def && typeof def === "object" && "toQuery" in def) {
        // SQL expression default (e.g., now())
        const query = (def as { toQuery: (config: any) => { sql: string } }).toQuery({
          escapeName: (n: string) => `"${n}"`,
          escapeParam: (_p: unknown, i: number) => `$${i}`,
          escapeString: (s: string) => `'${s}'`,
        });
        parts.push(`DEFAULT ${query.sql}`);
      } else if (typeof def === "string") {
        parts.push(`DEFAULT '${def.replace(/'/g, "''")}'`);
      } else if (typeof def === "number" || typeof def === "boolean") {
        parts.push(`DEFAULT ${def}`);
      }
    }

    if (col.primary) parts.push("PRIMARY KEY");

    return parts.join(" ");
  });

  // Composite primary key (if any)
  const compositePks = config.primaryKeys.map(
    (pk) => `PRIMARY KEY (${pk.columns.map((c) => `"${c.name}"`).join(", ")})`,
  );

  const allDefs = [...columnDefs, ...compositePks];

  const createSql = `CREATE TABLE IF NOT EXISTS "${config.name}" (\n  ${allDefs.join(",\n  ")}\n)`;
  await db.execute(sql.raw(createSql));

  // Create indexes. Respects drizzle's optional `.using('gin' | ...)` access
  // method and `.where(sql\`...\`)` partial predicate — both needed for
  // promin-k6mk's partial unique + GIN-on-array indexes without having
  // to hand-write the DDL in a separate place.
  //
  // The where predicate is a drizzle SQL fragment; we splice it into a
  // `sql\`\`` template and let db.execute() render both the parameters
  // and the column identifiers. Avoids manually invoking toQuery(), which
  // wants a casing config we don't have here.
  for (const idx of config.indexes) {
    const uniquePrefix = idx.config.unique ? sql.raw("UNIQUE ") : sql.raw("");
    const name = sql.raw(
      `"${
        idx.config.name ??
        `idx_${config.name}_${idx.config.columns.map((c: any) => c.name).join("_")}`
      }"`,
    );
    const table = sql.raw(`"${config.name}"`);
    const colList = sql.raw(`(${idx.config.columns.map((c: any) => `"${c.name}"`).join(", ")})`);
    const methodClause =
      idx.config.method && idx.config.method !== "btree"
        ? sql.raw(` USING ${idx.config.method}`)
        : sql.raw("");
    const whereClause = idx.config.where ? sql` WHERE ${idx.config.where}` : sql.raw("");
    await db.execute(
      sql`CREATE ${uniquePrefix}INDEX IF NOT EXISTS ${name} ON ${table}${methodClause} ${colList}${whereClause}`,
    );
  }
}
