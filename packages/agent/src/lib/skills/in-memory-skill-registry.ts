// ---------------------------------------------------------------------------
// `InMemorySkillRegistry` — reference implementation, mirroring
// `InMemoryAgentRegistry`. A single Map keyed on `<id>|<version>`. Cheap,
// deterministic, and the conformance baseline every other implementation
// must match.
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import {
  DEFAULT_SKILL_METADATA,
  DEFAULT_SKILL_VERSION,
  type ListSkillsParams,
  type RegisterSkillInput,
  type RegisteredSkill,
  type SkillMetadata,
  type SkillRegistry,
} from "./types.ts";

export interface InMemorySkillRegistryConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemorySkillRegistry implements SkillRegistry {
  private readonly clock: Clock;
  private readonly rows = new Map<string, RegisteredSkill>();

  constructor(config: InMemorySkillRegistryConfig = {}) {
    this.clock = config.clock ?? SystemClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  private key(id: string, version: string): string {
    return `${id}|${version}`;
  }

  async register(input: RegisterSkillInput): Promise<RegisteredSkill> {
    const version = input.version ?? DEFAULT_SKILL_VERSION;
    const k = this.key(input.id, version);
    const now = this.now();
    const existing = this.rows.get(k);
    const next: RegisteredSkill = {
      id: input.id,
      version,
      description: input.description,
      // Fall back to description when an author (e.g. a SKILL.md) folds the
      // trigger into the description rather than stating it separately.
      whenToUse: input.whenToUse ?? input.description,
      body: input.body,
      metadata: mergeSkillMetadata(input.metadata),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.rows.set(k, next);
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredSkill | null> {
    if (version !== undefined) {
      return this.rows.get(this.key(id, version)) ?? null;
    }
    // Newest-version-of-id: highest updatedAt.
    let latest: RegisteredSkill | null = null;
    for (const row of this.rows.values()) {
      if (row.id !== id) continue;
      if (!latest || row.updatedAt > latest.updatedAt) latest = row;
    }
    return latest;
  }

  async list(params: ListSkillsParams = {}): Promise<RegisteredSkill[]> {
    let out = Array.from(this.rows.values()).filter((r) => matchesSkillFilter(r, params));
    out = sortSkills(out, params.order ?? "createdDesc");
    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? out.length;
    return out.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredSkill[]> {
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

export function mergeSkillMetadata(patch?: Partial<SkillMetadata>): SkillMetadata {
  return {
    capabilities: patch?.capabilities
      ? [...patch.capabilities]
      : [...DEFAULT_SKILL_METADATA.capabilities],
    tags: patch?.tags ? [...patch.tags] : [...DEFAULT_SKILL_METADATA.tags],
    // Kill-switch carries through additively. Default-undefined when omitted
    // so listings distinguish 'never set' from 'explicitly enabled/disabled'.
    ...(patch?.enabled !== undefined && { enabled: patch.enabled }),
    // Trust state — same pass-through. The scanner sets this on first
    // discovery via applyDiscoveredSkills; operators flip it via PATCH.
    ...(patch?.trust !== undefined && { trust: patch.trust }),
  };
}

export function matchesSkillFilter(row: RegisteredSkill, params: ListSkillsParams): boolean {
  if (params.capability !== undefined && !row.metadata.capabilities.includes(params.capability)) {
    return false;
  }
  if (params.tag !== undefined && !row.metadata.tags.includes(params.tag)) return false;
  return true;
}

export function sortSkills(
  rows: RegisteredSkill[],
  order: NonNullable<ListSkillsParams["order"]>,
): RegisteredSkill[] {
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
