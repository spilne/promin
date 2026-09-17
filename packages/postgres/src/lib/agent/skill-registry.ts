// ---------------------------------------------------------------------------
// PostgresSkillRegistry — `SkillRegistry` over the `skill_registry` table.
// Mirrors `SqliteSkillRegistry` so multiple Zorya replicas share one skill
// store. Content fields are columns; `metadata` is a JSONB blob.
//
// `register()` upserts on `(skill_id, version)` — when the row exists the
// content fields + metadata are replaced and `created_at` is preserved.
// `get(id)` without a version returns the most-recently-updated row.
// ---------------------------------------------------------------------------

import { and, desc, eq, sql } from "drizzle-orm";
import type {
  ListSkillsParams,
  RegisterSkillInput,
  RegisteredSkill,
  SkillMetadata,
  SkillRegistry,
} from "@promin/agent";
import { DEFAULT_SKILL_VERSION } from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { skillRegistry } from "../schema.ts";

export interface PostgresSkillRegistryConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresSkillRegistry implements SkillRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresSkillRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async register(input: RegisterSkillInput): Promise<RegisteredSkill> {
    const version = input.version ?? DEFAULT_SKILL_VERSION;
    const now = this.clock();
    const metadata = buildMetadata(input.metadata);
    const whenToUse = input.whenToUse ?? input.description;
    const rows = await this.db
      .insert(skillRegistry)
      .values({
        skillId: input.id,
        version,
        description: input.description,
        whenToUse,
        body: input.body,
        metadata: metadata as unknown as Record<string, unknown>,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: [skillRegistry.skillId, skillRegistry.version],
        set: {
          description: input.description,
          whenToUse,
          body: input.body,
          metadata: metadata as unknown as Record<string, unknown>,
          updatedAt: now,
        },
      })
      .returning();
    return rowToRegistered(rows[0]!);
  }

  async get(id: string, version?: string): Promise<RegisteredSkill | null> {
    if (version !== undefined) {
      const rows = await this.db
        .select()
        .from(skillRegistry)
        .where(and(eq(skillRegistry.skillId, id), eq(skillRegistry.version, version)))
        .limit(1);
      const row = rows[0];
      return row ? rowToRegistered(row) : null;
    }
    const rows = await this.db
      .select()
      .from(skillRegistry)
      .where(eq(skillRegistry.skillId, id))
      .orderBy(desc(skillRegistry.updatedAt))
      .limit(1);
    const row = rows[0];
    return row ? rowToRegistered(row) : null;
  }

  async list(params: ListSkillsParams = {}): Promise<RegisteredSkill[]> {
    const rows = await this.db.select().from(skillRegistry);
    let skills = rows.map(rowToRegistered);

    // capability + tag live inside the metadata JSONB; filter in JS.
    if (params.capability !== undefined) {
      skills = skills.filter((s) => s.metadata.capabilities.includes(params.capability!));
    }
    if (params.tag !== undefined) {
      skills = skills.filter((s) => s.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    skills.sort((a, b) => {
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
    const limit = params.limit ?? skills.length;
    return skills.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredSkill[]> {
    const rows = await this.db
      .select()
      .from(skillRegistry)
      .where(eq(skillRegistry.skillId, id))
      .orderBy(sql`${skillRegistry.createdAt} ASC`, sql`${skillRegistry.version} ASC`);
    return rows.map(rowToRegistered);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      await this.db
        .delete(skillRegistry)
        .where(and(eq(skillRegistry.skillId, id), eq(skillRegistry.version, version)));
      return;
    }
    await this.db.delete(skillRegistry).where(eq(skillRegistry.skillId, id));
  }
}

interface DbSkillRow {
  skillId: string;
  version: string;
  description: string;
  whenToUse: string;
  body: string;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}

function buildMetadata(patch?: Partial<SkillMetadata>): SkillMetadata {
  return {
    capabilities: patch?.capabilities ? [...patch.capabilities] : [],
    tags: patch?.tags ? [...patch.tags] : [],
    ...(patch?.enabled !== undefined && { enabled: patch.enabled }),
    // Pass trust through — the scanner's needs-review default and an
    // operator's Approve both flow via metadata.trust on register.
    ...(patch?.trust !== undefined && { trust: patch.trust }),
  };
}

function rowToRegistered(r: DbSkillRow): RegisteredSkill {
  return {
    id: r.skillId,
    version: r.version,
    description: r.description,
    whenToUse: r.whenToUse,
    body: r.body,
    metadata: r.metadata as SkillMetadata,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}
