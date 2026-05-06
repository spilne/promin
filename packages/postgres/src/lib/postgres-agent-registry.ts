// ---------------------------------------------------------------------------
// PostgresAgentRegistry — `AgentRegistry` over the `agent_registry` table.
//
// Mirrors `SqliteAgentRegistry` so multiple Zorya replicas can share one
// recipe store. Backend and metadata are JSONB blobs; future backend
// variants land without schema migrations.
//
// `register()` upserts on `(agent_id, version)` — when the row exists,
// `backend` and `metadata` are replaced; `created_at` is preserved.
// `get(id)` without a version returns the most-recently-updated row.
// ---------------------------------------------------------------------------

import { and, desc, eq, sql } from "drizzle-orm";
import type {
  AgentBackend,
  AgentMetadata,
  AgentRegistry,
  ListAgentsParams,
  RegisterAgentInput,
  RegisteredAgent,
} from "@promin/agent";
import { DEFAULT_AGENT_VERSION } from "@promin/agent";
import type { DrizzleDb } from "./drizzle-db.ts";
import { agentRegistry } from "./schema.ts";

export interface PostgresAgentRegistryConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresAgentRegistry implements AgentRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresAgentRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async register(input: RegisterAgentInput): Promise<RegisteredAgent> {
    const version = input.version ?? DEFAULT_AGENT_VERSION;
    const now = this.clock();
    const metadata: AgentMetadata = {
      description: input.metadata?.description ?? null,
      capabilities: input.metadata?.capabilities ? [...input.metadata.capabilities] : [],
      tags: input.metadata?.tags ? [...input.metadata.tags] : [],
      ...(input.metadata?.template !== undefined && { template: input.metadata.template }),
      ...(input.metadata?.requiredSecrets !== undefined && {
        requiredSecrets: [...input.metadata.requiredSecrets],
      }),
      ...(input.metadata?.enabled !== undefined && { enabled: input.metadata.enabled }),
    };
    const rows = await this.db
      .insert(agentRegistry)
      .values({
        agentId: input.id,
        version,
        backendType: input.backend.type,
        backend: input.backend as unknown as Record<string, unknown>,
        metadata: metadata as unknown as Record<string, unknown>,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: [agentRegistry.agentId, agentRegistry.version],
        set: {
          backendType: input.backend.type,
          backend: input.backend as unknown as Record<string, unknown>,
          metadata: metadata as unknown as Record<string, unknown>,
          updatedAt: now,
        },
      })
      .returning();
    const row = rows[0]!;
    return rowToRegistered(row);
  }

  async get(id: string, version?: string): Promise<RegisteredAgent | null> {
    if (version !== undefined) {
      const rows = await this.db
        .select()
        .from(agentRegistry)
        .where(and(eq(agentRegistry.agentId, id), eq(agentRegistry.version, version)))
        .limit(1);
      const row = rows[0];
      return row ? rowToRegistered(row) : null;
    }
    const rows = await this.db
      .select()
      .from(agentRegistry)
      .where(eq(agentRegistry.agentId, id))
      .orderBy(desc(agentRegistry.updatedAt))
      .limit(1);
    const row = rows[0];
    return row ? rowToRegistered(row) : null;
  }

  async list(params: ListAgentsParams = {}): Promise<RegisteredAgent[]> {
    const rows = params.backendType
      ? await this.db
          .select()
          .from(agentRegistry)
          .where(eq(agentRegistry.backendType, params.backendType))
      : await this.db.select().from(agentRegistry);

    let agents = rows.map(rowToRegistered);

    // capability + tag live inside the metadata JSONB; filter in JS.
    if (params.capability !== undefined) {
      agents = agents.filter((a) => a.metadata.capabilities.includes(params.capability!));
    }
    if (params.tag !== undefined) {
      agents = agents.filter((a) => a.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    agents.sort((a, b) => {
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
    const limit = params.limit ?? agents.length;
    return agents.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredAgent[]> {
    const rows = await this.db
      .select()
      .from(agentRegistry)
      .where(eq(agentRegistry.agentId, id))
      .orderBy(sql`${agentRegistry.createdAt} ASC`, sql`${agentRegistry.version} ASC`);
    return rows.map(rowToRegistered);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      await this.db
        .delete(agentRegistry)
        .where(and(eq(agentRegistry.agentId, id), eq(agentRegistry.version, version)));
      return;
    }
    await this.db.delete(agentRegistry).where(eq(agentRegistry.agentId, id));
  }
}

interface DbAgentRow {
  agentId: string;
  version: string;
  backendType: string;
  backend: unknown;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}

function rowToRegistered(r: DbAgentRow): RegisteredAgent {
  return {
    id: r.agentId,
    version: r.version,
    backend: r.backend as AgentBackend,
    metadata: r.metadata as AgentMetadata,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}
