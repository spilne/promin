// ---------------------------------------------------------------------------
// `InMemoryAgentInstanceRegistry` — reference impl. A single Map keyed
// by instance id. Cheap, deterministic, and the conformance baseline
// every persistent backend must match.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  composeAgentInstanceId,
  type AgentInstance,
  type AgentInstanceRegistry,
  type CreateAgentInstanceInput,
  type ListAgentInstancesParams,
  type UpdateAgentInstancePatch,
} from "./types.ts";

export interface InMemoryAgentInstanceRegistryConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryAgentInstanceRegistry implements AgentInstanceRegistry {
  private readonly clock: Clock;
  private readonly rows = new Map<string, AgentInstance>();

  constructor(config: InMemoryAgentInstanceRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  async resolveOrCreate(input: CreateAgentInstanceInput): Promise<AgentInstance> {
    validateNonEmpty("registeredAgentId", input.registeredAgentId);
    validateNonEmpty("namespaceId", input.namespaceId);
    validateNonEmpty("ownerId", input.ownerId);
    const id = composeAgentInstanceId(input);
    const existing = this.rows.get(id);
    if (existing) return existing;

    const next: AgentInstance = {
      id,
      registeredAgentId: input.registeredAgentId,
      namespaceId: input.namespaceId,
      ownerId: input.ownerId,
      displayName: input.displayName ?? null,
      metadata: { ...input.metadata },
      createdAt: this.now(),
    };
    this.rows.set(id, next);
    return next;
  }

  async get(id: string): Promise<AgentInstance | null> {
    return this.rows.get(id) ?? null;
  }

  async list(params: ListAgentInstancesParams = {}): Promise<AgentInstance[]> {
    const filtered = Array.from(this.rows.values()).filter((row) => {
      if (params.namespaceId !== undefined && row.namespaceId !== params.namespaceId) return false;
      if (params.ownerId !== undefined && row.ownerId !== params.ownerId) return false;
      if (
        params.registeredAgentId !== undefined &&
        row.registeredAgentId !== params.registeredAgentId
      ) {
        return false;
      }
      return true;
    });
    const sorted = sortInstances(filtered, params.order ?? "createdDesc");
    return params.limit !== undefined ? sorted.slice(0, params.limit) : sorted;
  }

  async update(id: string, patch: UpdateAgentInstancePatch): Promise<AgentInstance> {
    const existing = this.rows.get(id);
    if (!existing) {
      throw new Error(`AgentInstance not found: ${id}`);
    }
    const next: AgentInstance = {
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
    throw new Error(`AgentInstance: ${name} must be a non-empty string`);
  }
}

export function sortInstances(
  rows: AgentInstance[],
  order: NonNullable<ListAgentInstancesParams["order"]>,
): AgentInstance[] {
  return rows.slice().sort((a, b) => {
    switch (order) {
      case "createdAsc":
        return a.createdAt - b.createdAt;
      case "createdDesc":
        return b.createdAt - a.createdAt;
    }
  });
}
