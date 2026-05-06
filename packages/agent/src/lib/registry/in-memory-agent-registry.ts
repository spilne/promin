// ---------------------------------------------------------------------------
// `InMemoryAgentRegistry` — reference implementation. A single Map keyed
// on `<id>|<version>`. Cheap, deterministic, and the conformance baseline
// every other implementation must match.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  DEFAULT_AGENT_METADATA,
  DEFAULT_AGENT_VERSION,
  type AgentBackend,
  type AgentMetadata,
  type AgentRegistry,
  type ListAgentsParams,
  type RegisterAgentInput,
  type RegisteredAgent,
} from "./types.ts";

export interface InMemoryAgentRegistryConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryAgentRegistry implements AgentRegistry {
  private readonly clock: Clock;
  private readonly rows = new Map<string, RegisteredAgent>();

  constructor(config: InMemoryAgentRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  private key(id: string, version: string): string {
    return `${id}|${version}`;
  }

  async register(input: RegisterAgentInput): Promise<RegisteredAgent> {
    const version = input.version ?? DEFAULT_AGENT_VERSION;
    const k = this.key(input.id, version);
    const now = this.now();
    const existing = this.rows.get(k);
    const metadata = mergeMetadata(input.metadata);
    const next: RegisteredAgent = {
      id: input.id,
      version,
      backend: input.backend,
      metadata,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.rows.set(k, next);
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredAgent | null> {
    if (version !== undefined) {
      return this.rows.get(this.key(id, version)) ?? null;
    }
    // Newest-version-of-id: highest updatedAt.
    let latest: RegisteredAgent | null = null;
    for (const row of this.rows.values()) {
      if (row.id !== id) continue;
      if (!latest || row.updatedAt > latest.updatedAt) latest = row;
    }
    return latest;
  }

  async list(params: ListAgentsParams = {}): Promise<RegisteredAgent[]> {
    let out = Array.from(this.rows.values()).filter((r) => matchesFilter(r, params));
    out = sortAgents(out, params.order ?? "createdDesc");
    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? out.length;
    return out.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredAgent[]> {
    return Array.from(this.rows.values())
      .filter((r) => r.id === id)
      .sort((a, b) => a.createdAt - b.createdAt);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      this.rows.delete(this.key(id, version));
      return;
    }
    for (const k of Array.from(this.rows.keys())) {
      const row = this.rows.get(k)!;
      if (row.id === id) this.rows.delete(k);
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers exported to share with PostgreSQL-style adapters.
// ---------------------------------------------------------------------------

export function mergeMetadata(patch?: Partial<AgentMetadata>): AgentMetadata {
  return {
    description: patch?.description ?? DEFAULT_AGENT_METADATA.description,
    capabilities: patch?.capabilities
      ? [...patch.capabilities]
      : [...DEFAULT_AGENT_METADATA.capabilities],
    tags: patch?.tags ? [...patch.tags] : [...DEFAULT_AGENT_METADATA.tags],
    // Template flags carry through additively. Default-undefined when
    // omitted so listings distinguish 'not a template' from 'explicitly
    // opted-out of being one'.
    ...(patch?.template !== undefined && { template: patch.template }),
    ...(patch?.requiredSecrets !== undefined && {
      requiredSecrets: [...patch.requiredSecrets],
    }),
    ...(patch?.enabled !== undefined && { enabled: patch.enabled }),
  };
}

export function matchesFilter(row: RegisteredAgent, params: ListAgentsParams): boolean {
  if (params.backendType !== undefined && row.backend.type !== params.backendType) {
    return false;
  }
  if (params.capability !== undefined && !row.metadata.capabilities.includes(params.capability)) {
    return false;
  }
  if (params.tag !== undefined && !row.metadata.tags.includes(params.tag)) return false;
  return true;
}

export function sortAgents(
  rows: RegisteredAgent[],
  order: NonNullable<ListAgentsParams["order"]>,
): RegisteredAgent[] {
  return rows.slice().sort((a, b) => {
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
}

// Re-export so callers don't need to chase imports.
export type { AgentBackend };
