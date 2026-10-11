// ---------------------------------------------------------------------------
// PostgresNamespaceRegistry — `NamespaceRegistry` over `wf_namespace_registry`.
//
// Every replica shares one table, so a namespace created or archived on one
// Zorya instance is seen by all. Each method is a single statement:
// `create` is an INSERT … ON CONFLICT DO NOTHING (of concurrent creates of
// one id exactly one returns a row), `update` writes only the patched
// columns (concurrent updates of different fields both land). Timestamps
// come from the database clock (`NOW()`); ids sort byte-wise (`COLLATE "C"`)
// like the other backends.
// ---------------------------------------------------------------------------

import { eq, sql } from "drizzle-orm";
import {
  NamespaceExistsError,
  NamespaceNotFoundError,
  isValidNamespaceId,
  type CreateNamespaceInput,
  type NamespaceCapabilities,
  type NamespacePatch,
  type NamespaceRecord,
  type NamespaceRegistry,
  type NamespaceStatus,
} from "@promin/workflow";
import {
  normalizeNamespaceCreate,
  normalizeNamespacePatch,
  parseNamespaceStatus,
} from "@promin/workflow/storage-kit";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { namespaceRegistry } from "./schema.ts";

export interface PostgresNamespaceRegistryConfig {
  readonly db: DrizzleDb;
}

export class PostgresNamespaceRegistry implements NamespaceRegistry {
  /** Drizzle schema for migration pipelines that include the workflow tables. */
  static readonly schema = { namespaceRegistry };

  private readonly db: DrizzleDb;

  constructor(config: PostgresNamespaceRegistryConfig) {
    this.db = config.db;
  }

  async create(input: CreateNamespaceInput): Promise<NamespaceRecord> {
    const fields = normalizeNamespaceCreate(input);
    const rows = await this.db
      .insert(namespaceRegistry)
      .values({
        id: fields.id,
        displayName: fields.displayName,
        description: fields.description,
        status: "active",
        capabilities: fields.capabilities,
        metadata: fields.metadata,
      })
      .onConflictDoNothing({ target: namespaceRegistry.id })
      .returning();
    const row = rows[0];
    if (!row) throw NamespaceExistsError.of(fields.id);
    return toRecord(row);
  }

  async get(id: string): Promise<NamespaceRecord | null> {
    if (!isValidNamespaceId(id)) return null;
    const rows = await this.db
      .select()
      .from(namespaceRegistry)
      .where(eq(namespaceRegistry.id, id))
      .limit(1);
    const row = rows[0];
    return row ? toRecord(row) : null;
  }

  async list(params: { readonly status?: NamespaceStatus } = {}): Promise<NamespaceRecord[]> {
    const status = params.status !== undefined ? parseNamespaceStatus(params.status) : undefined;
    const query = this.db.select().from(namespaceRegistry);
    const filtered =
      status !== undefined ? query.where(eq(namespaceRegistry.status, status)) : query;
    const rows = await filtered.orderBy(sql`${namespaceRegistry.id} COLLATE "C"`);
    return rows.map(toRecord);
  }

  async update(params: {
    readonly id: string;
    readonly patch: NamespacePatch;
  }): Promise<NamespaceRecord> {
    const patch = normalizeNamespacePatch(params);
    const rows = await this.db
      .update(namespaceRegistry)
      .set({
        ...(patch.displayName !== undefined && { displayName: patch.displayName }),
        ...(patch.description !== undefined && { description: patch.description }),
        ...(patch.status !== undefined && { status: patch.status }),
        ...(patch.capabilities !== undefined && { capabilities: patch.capabilities }),
        ...(patch.metadata !== undefined && { metadata: patch.metadata }),
        updatedAt: sql`NOW()`,
      })
      .where(eq(namespaceRegistry.id, params.id))
      .returning();
    const row = rows[0];
    if (!row) throw NamespaceNotFoundError.of(params.id);
    return toRecord(row);
  }

  async delete(id: string): Promise<boolean> {
    const rows = await this.db
      .delete(namespaceRegistry)
      .where(eq(namespaceRegistry.id, id))
      .returning({ id: namespaceRegistry.id });
    return rows.length > 0;
  }
}

function toRecord(row: typeof namespaceRegistry.$inferSelect): NamespaceRecord {
  return {
    id: row.id,
    displayName: row.displayName,
    description: row.description,
    status: parseNamespaceStatus(row.status),
    capabilities: row.capabilities as NamespaceCapabilities,
    metadata: row.metadata as Record<string, unknown>,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}
