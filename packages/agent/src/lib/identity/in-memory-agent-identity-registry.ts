// ---------------------------------------------------------------------------
// `InMemoryAgentIdentityRegistry` — reference impl. A single Map keyed
// by identity id. Cheap, deterministic, and the conformance baseline
// every persistent backend must match.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  composeAgentIdentityId,
  type AgentIdentity,
  type AgentIdentityRegistry,
  type CreateAgentIdentityInput,
  type ListAgentIdentitiesParams,
  type UpdateAgentIdentityPatch,
} from "./types.ts";

export interface InMemoryAgentIdentityRegistryConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryAgentIdentityRegistry implements AgentIdentityRegistry {
  private readonly clock: Clock;
  private readonly rows = new Map<string, AgentIdentity>();

  constructor(config: InMemoryAgentIdentityRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  async resolveOrCreate(input: CreateAgentIdentityInput): Promise<AgentIdentity> {
    validateNonEmpty("registeredAgentId", input.registeredAgentId);
    validateNonEmpty("namespaceId", input.namespaceId);
    validateNonEmpty("userId", input.userId);
    const id = composeAgentIdentityId(input);
    const existing = this.rows.get(id);
    if (existing) return existing;

    const now = this.now();
    const next: AgentIdentity = {
      id,
      registeredAgentId: input.registeredAgentId,
      namespaceId: input.namespaceId,
      userId: input.userId,
      displayName: input.displayName ?? null,
      metadata: { ...(input.metadata ?? {}) },
      createdAt: now,
      lastActiveAt: now,
    };
    this.rows.set(id, next);
    return next;
  }

  async get(id: string): Promise<AgentIdentity | null> {
    return this.rows.get(id) ?? null;
  }

  async list(params: ListAgentIdentitiesParams = {}): Promise<AgentIdentity[]> {
    const filtered = Array.from(this.rows.values()).filter((row) => {
      if (params.namespaceId !== undefined && row.namespaceId !== params.namespaceId) return false;
      if (params.userId !== undefined && row.userId !== params.userId) return false;
      if (
        params.registeredAgentId !== undefined &&
        row.registeredAgentId !== params.registeredAgentId
      ) {
        return false;
      }
      return true;
    });
    const sorted = sortIdentities(filtered, params.order ?? "lastActiveDesc");
    return params.limit !== undefined ? sorted.slice(0, params.limit) : sorted;
  }

  async touch(id: string, lastActiveAt?: number): Promise<void> {
    const existing = this.rows.get(id);
    if (!existing) return;
    this.rows.set(id, { ...existing, lastActiveAt: lastActiveAt ?? this.now() });
  }

  async update(id: string, patch: UpdateAgentIdentityPatch): Promise<AgentIdentity> {
    const existing = this.rows.get(id);
    if (!existing) {
      throw new Error(`AgentIdentity not found: ${id}`);
    }
    const next: AgentIdentity = {
      ...existing,
      // Pass `null` to clear, omit to keep.
      displayName: "displayName" in patch ? (patch.displayName ?? null) : existing.displayName,
      metadata: patch.metadata !== undefined ? { ...patch.metadata } : existing.metadata,
    };
    this.rows.set(id, next);
    return next;
  }

  async delete(id: string): Promise<void> {
    this.rows.delete(id);
  }
}

function validateNonEmpty(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`AgentIdentity: ${name} must be a non-empty string`);
  }
}

export function sortIdentities(
  rows: AgentIdentity[],
  order: NonNullable<ListAgentIdentitiesParams["order"]>,
): AgentIdentity[] {
  return rows.slice().sort((a, b) => {
    switch (order) {
      case "createdAsc":
        return a.createdAt - b.createdAt;
      case "createdDesc":
        return b.createdAt - a.createdAt;
      case "lastActiveDesc":
        return b.lastActiveAt - a.lastActiveAt;
    }
  });
}
