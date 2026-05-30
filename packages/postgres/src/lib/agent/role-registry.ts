// ---------------------------------------------------------------------------
// PostgresRoleRegistry — `RoleRegistry` over the `role_registry` table.
//
// Mirrors `SqliteRoleRegistry` so multiple Zorya replicas can share one
// role store. `definition` and `metadata` are JSONB blobs; their shapes
// evolve without schema migrations.
//
// `register()` upserts on `(role_id, version)` — when the row exists,
// `definition` and `metadata` are replaced; `created_at` is preserved.
// `get(id)` without a version returns the most-recently-updated row.
// ---------------------------------------------------------------------------

import { and, desc, eq, sql } from "drizzle-orm";
import type {
  ListRolesParams,
  RegisterRoleInput,
  RegisteredRole,
  RoleDefinition,
  RoleMetadata,
  RoleRegistry,
} from "@promin/agent";
import { DEFAULT_ROLE_VERSION } from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { roleRegistry } from "../schema.ts";

export interface PostgresRoleRegistryConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresRoleRegistry implements RoleRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresRoleRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async register(input: RegisterRoleInput): Promise<RegisteredRole> {
    const version = input.version ?? DEFAULT_ROLE_VERSION;
    const now = this.clock();
    const metadata: RoleMetadata = {
      description: input.metadata?.description ?? null,
      tags: input.metadata?.tags ? [...input.metadata.tags] : [],
      ...(input.metadata?.suggestedSecrets !== undefined && {
        suggestedSecrets: [...input.metadata.suggestedSecrets],
      }),
    };
    const rows = await this.db
      .insert(roleRegistry)
      .values({
        roleId: input.id,
        version,
        definition: input.definition as unknown as Record<string, unknown>,
        metadata: metadata as unknown as Record<string, unknown>,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: [roleRegistry.roleId, roleRegistry.version],
        set: {
          definition: input.definition as unknown as Record<string, unknown>,
          metadata: metadata as unknown as Record<string, unknown>,
          updatedAt: now,
        },
      })
      .returning();
    const row = rows[0]!;
    return rowToRegistered(row);
  }

  async get(id: string, version?: string): Promise<RegisteredRole | null> {
    if (version !== undefined) {
      const rows = await this.db
        .select()
        .from(roleRegistry)
        .where(and(eq(roleRegistry.roleId, id), eq(roleRegistry.version, version)))
        .limit(1);
      const row = rows[0];
      return row ? rowToRegistered(row) : null;
    }
    const rows = await this.db
      .select()
      .from(roleRegistry)
      .where(eq(roleRegistry.roleId, id))
      .orderBy(desc(roleRegistry.updatedAt))
      .limit(1);
    const row = rows[0];
    return row ? rowToRegistered(row) : null;
  }

  async list(params: ListRolesParams = {}): Promise<RegisteredRole[]> {
    const rows = await this.db.select().from(roleRegistry);
    let roles = rows.map(rowToRegistered);

    // capability + tag live inside JSONB blobs; filter in JS.
    if (params.capability !== undefined) {
      roles = roles.filter((r) => (r.definition.capabilities ?? []).includes(params.capability!));
    }
    if (params.tag !== undefined) {
      roles = roles.filter((r) => r.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    roles.sort((a, b) => {
      switch (order) {
        case "createdAsc":
          return a.createdAt - b.createdAt;
        case "createdDesc":
          return b.createdAt - a.createdAt;
        case "idAsc":
          if (a.id !== b.id) return a.id < b.id ? -1 : 1;
          return a.version < b.version ? -1 : 1;
      }
    });

    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? roles.length;
    return roles.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredRole[]> {
    const rows = await this.db
      .select()
      .from(roleRegistry)
      .where(eq(roleRegistry.roleId, id))
      .orderBy(sql`${roleRegistry.createdAt} ASC`, sql`${roleRegistry.version} ASC`);
    return rows.map(rowToRegistered);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      await this.db
        .delete(roleRegistry)
        .where(and(eq(roleRegistry.roleId, id), eq(roleRegistry.version, version)));
      return;
    }
    await this.db.delete(roleRegistry).where(eq(roleRegistry.roleId, id));
  }
}

interface DbRoleRow {
  roleId: string;
  version: string;
  definition: unknown;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}

function rowToRegistered(r: DbRoleRow): RegisteredRole {
  return {
    id: r.roleId,
    version: r.version,
    definition: r.definition as RoleDefinition,
    metadata: r.metadata as RoleMetadata,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}
