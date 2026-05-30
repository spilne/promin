// ---------------------------------------------------------------------------
// `InMemoryRoleRegistry` — reference implementation. A single Map keyed on
// `<id>|<version>`. Cheap, deterministic, and the conformance baseline every
// other implementation must match. Mirrors `InMemoryAgentRegistry`.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  DEFAULT_ROLE_METADATA,
  DEFAULT_ROLE_VERSION,
  type ListRolesParams,
  type RegisterRoleInput,
  type RegisteredRole,
  type RoleMetadata,
  type RoleRegistry,
} from "./types.ts";

export interface InMemoryRoleRegistryConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryRoleRegistry implements RoleRegistry {
  private readonly clock: Clock;
  private readonly rows = new Map<string, RegisteredRole>();

  constructor(config: InMemoryRoleRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  private key(id: string, version: string): string {
    return `${id}|${version}`;
  }

  async register(input: RegisterRoleInput): Promise<RegisteredRole> {
    const version = input.version ?? DEFAULT_ROLE_VERSION;
    const k = this.key(input.id, version);
    const now = this.now();
    const existing = this.rows.get(k);
    const next: RegisteredRole = {
      id: input.id,
      version,
      definition: input.definition,
      metadata: mergeRoleMetadata(input.metadata),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.rows.set(k, next);
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredRole | null> {
    if (version !== undefined) {
      return this.rows.get(this.key(id, version)) ?? null;
    }
    // Newest-version-of-id: highest updatedAt.
    let latest: RegisteredRole | null = null;
    for (const row of this.rows.values()) {
      if (row.id !== id) continue;
      if (!latest || row.updatedAt > latest.updatedAt) latest = row;
    }
    return latest;
  }

  async list(params: ListRolesParams = {}): Promise<RegisteredRole[]> {
    let out = Array.from(this.rows.values()).filter((r) => matchesRoleFilter(r, params));
    out = sortRoles(out, params.order ?? "createdDesc");
    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? out.length;
    return out.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredRole[]> {
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
// Helpers exported to share with SQL adapters.
// ---------------------------------------------------------------------------

export function mergeRoleMetadata(patch?: Partial<RoleMetadata>): RoleMetadata {
  return {
    description: patch?.description ?? DEFAULT_ROLE_METADATA.description,
    tags: patch?.tags ? [...patch.tags] : [...DEFAULT_ROLE_METADATA.tags],
    // Default-undefined when omitted so listings distinguish 'no suggested
    // secrets' from 'explicitly empty'.
    ...(patch?.suggestedSecrets !== undefined && {
      suggestedSecrets: [...patch.suggestedSecrets],
    }),
  };
}

export function matchesRoleFilter(row: RegisteredRole, params: ListRolesParams): boolean {
  if (
    params.capability !== undefined &&
    !(row.definition.capabilities ?? []).includes(params.capability)
  ) {
    return false;
  }
  if (params.tag !== undefined && !row.metadata.tags.includes(params.tag)) return false;
  return true;
}

export function sortRoles(
  rows: RegisteredRole[],
  order: NonNullable<ListRolesParams["order"]>,
): RegisteredRole[] {
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
