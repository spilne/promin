// ---------------------------------------------------------------------------
// Drizzle lookup table utilities
// ---------------------------------------------------------------------------

import { pgTable, integer, text } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import type { DrizzleDb } from "./drizzle-db.ts";
import type { LookupEntry } from "./lookup.ts";

// ---------------------------------------------------------------------------
// createLookupTable
// ---------------------------------------------------------------------------

export function createLookupTable(tableName: string) {
  return pgTable(tableName, {
    id: integer("id").primaryKey(),
    name: text("name").notNull().unique(),
  });
}

export type LookupTable = ReturnType<typeof createLookupTable>;

// ---------------------------------------------------------------------------
// LookupBinding — pairs a Lookup with its Drizzle table
// ---------------------------------------------------------------------------

/** Structural type for bindings — only needs entries for seeding/validation. */
export interface LookupBinding {
  readonly lookup: { readonly entries: readonly LookupEntry[] };
  readonly table: LookupTable;
}

// ---------------------------------------------------------------------------
// seedLookupEnums — idempotent seeding
// ---------------------------------------------------------------------------

export async function seedLookupEnums(
  db: DrizzleDb,
  bindings: readonly LookupBinding[],
): Promise<void> {
  for (const { lookup, table } of bindings) {
    for (const entry of lookup.entries) {
      await db
        .insert(table)
        .values({ id: entry.id, name: entry.name })
        .onConflictDoUpdate({
          target: table.name,
          set: { id: sql`excluded.id` },
        });
    }
  }
}

// ---------------------------------------------------------------------------
// validateLookupEnums — check code ↔ DB consistency
// ---------------------------------------------------------------------------

export async function validateLookupEnums(
  db: DrizzleDb,
  bindings: readonly LookupBinding[],
): Promise<void> {
  const mismatches: string[] = [];

  for (const { lookup, table } of bindings) {
    const rows: { id: number; name: string }[] = await db
      .select({ id: table.id, name: table.name })
      .from(table);

    const byName = new Map(rows.map((r) => [r.name, r.id]));

    for (const entry of lookup.entries) {
      const dbId = byName.get(entry.name);
      if (dbId === undefined) {
        mismatches.push(`"${entry.name}" (id=${entry.id}) not found in DB`);
      } else if (dbId !== entry.id) {
        mismatches.push(`"${entry.name}" id mismatch — code=${entry.id}, db=${dbId}`);
      }
    }

    const codeNames = new Set(lookup.entries.map((e) => e.name));
    for (const row of rows) {
      if (!codeNames.has(row.name)) {
        mismatches.push(`DB row "${row.name}" (id=${row.id}) has no matching code entry`);
      }
    }
  }

  if (mismatches.length > 0) {
    throw new Error(`Lookup validation failed:\n${mismatches.join("\n")}`);
  }
}
