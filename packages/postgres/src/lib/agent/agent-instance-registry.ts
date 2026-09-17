// ---------------------------------------------------------------------------
// PostgresAgentInstanceRegistry — Postgres-backed `AgentInstanceRegistry`.
//
// The durable index of long-lived per-(agent, namespace, owner) instances
// that a multi-replica deployment needs — every replica sees the same
// instances. The id (`namespace::recipe::owner`) doubles as the
// `resourceId` for the memory cascade. Matches InMemory / SQLite
// semantics: idempotent resolveOrCreate, deterministic id, void-on-missing
// delete; create-input `displayName` / `metadata` apply on creation only.
// ---------------------------------------------------------------------------

import { and, asc, desc, eq } from "drizzle-orm";
import {
  composeAgentInstanceId,
  type AgentInstance,
  type AgentInstanceRegistry,
  type CreateAgentInstanceInput,
  type ListAgentInstancesParams,
  type UpdateAgentInstancePatch,
} from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { agentInstance } from "../schema.ts";

export interface PostgresAgentInstanceRegistryConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresAgentInstanceRegistry implements AgentInstanceRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresAgentInstanceRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async resolveOrCreate(input: CreateAgentInstanceInput): Promise<AgentInstance> {
    validateNonEmpty("registeredAgentId", input.registeredAgentId);
    validateNonEmpty("namespaceId", input.namespaceId);
    validateNonEmpty("ownerId", input.ownerId);
    const id = composeAgentInstanceId(input);

    // Idempotent: ON CONFLICT DO NOTHING, then read back so concurrent
    // creators converge on the first-written row (and its createdAt).
    await this.db
      .insert(agentInstance)
      .values({
        id,
        registeredAgentId: input.registeredAgentId,
        namespaceId: input.namespaceId,
        ownerId: input.ownerId,
        displayName: input.displayName ?? null,
        metadata: input.metadata ?? {},
        createdAt: this.clock(),
      })
      .onConflictDoNothing();

    const row = await this.get(id);
    if (!row) {
      throw new Error(`PostgresAgentInstanceRegistry: failed to create instance ${id}`);
    }
    return row;
  }

  async get(id: string): Promise<AgentInstance | null> {
    const [row] = await this.db.select().from(agentInstance).where(eq(agentInstance.id, id));
    return row ? rowToInstance(row) : null;
  }

  async list(params: ListAgentInstancesParams = {}): Promise<AgentInstance[]> {
    const conditions = [];
    if (params.namespaceId !== undefined) {
      conditions.push(eq(agentInstance.namespaceId, params.namespaceId));
    }
    if (params.ownerId !== undefined) {
      conditions.push(eq(agentInstance.ownerId, params.ownerId));
    }
    if (params.registeredAgentId !== undefined) {
      conditions.push(eq(agentInstance.registeredAgentId, params.registeredAgentId));
    }
    const orderCol =
      (params.order ?? "createdDesc") === "createdAsc"
        ? asc(agentInstance.createdAt)
        : desc(agentInstance.createdAt);

    let select = this.db
      .select()
      .from(agentInstance)
      .where(conditions.length > 0 ? and(...conditions) : undefined)
      .orderBy(orderCol)
      .$dynamic();
    if (params.limit !== undefined) {
      select = select.limit(params.limit);
    }
    return (await select).map(rowToInstance);
  }

  async update(id: string, patch: UpdateAgentInstancePatch): Promise<AgentInstance> {
    const existing = await this.get(id);
    if (!existing) {
      throw new Error(`AgentInstance not found: ${id}`);
    }
    const next: AgentInstance = {
      ...existing,
      displayName: "displayName" in patch ? (patch.displayName ?? null) : existing.displayName,
      metadata: patch.metadata !== undefined ? { ...patch.metadata } : existing.metadata,
    };
    await this.db
      .update(agentInstance)
      .set({ displayName: next.displayName, metadata: next.metadata })
      .where(eq(agentInstance.id, id));
    return next;
  }

  async delete(id: string): Promise<void> {
    await this.db.delete(agentInstance).where(eq(agentInstance.id, id));
  }
}

function validateNonEmpty(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`AgentInstance: ${name} must be a non-empty string`);
  }
}

function rowToInstance(row: typeof agentInstance.$inferSelect): AgentInstance {
  return {
    id: row.id,
    registeredAgentId: row.registeredAgentId,
    namespaceId: row.namespaceId,
    ownerId: row.ownerId,
    displayName: row.displayName,
    metadata: (row.metadata as Record<string, unknown> | null) ?? {},
    createdAt: row.createdAt,
  };
}
