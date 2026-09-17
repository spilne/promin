// ---------------------------------------------------------------------------
// DagRegistry — versioned store for AgenticDagRecipe.
//
// Mirrors AgentRegistry's shape so operators have one mental model for
// "things you publish, version, and roll back": agent recipes AND
// dag recipes both live in versioned-row storage with the same CRUD
// surface (register / get / list / versions / unregister).
//
// Rows are keyed on (id, version). `register()` upserts (preserve
// createdAt, advance updatedAt). `get(id)` returns latest by updatedAt.
//
// In-memory impl only at this layer; SQLite + Postgres impls land as
// follow-ups (sqlite-dag-registry / postgres-dag-registry mirroring
// the existing agent registry pattern).
// ---------------------------------------------------------------------------

import type { AgenticDagRecipe } from "./types.ts";
import { validateDag } from "./validate.ts";

export interface RegisteredDag extends AgenticDagRecipe {
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface RegisterDagInput {
  readonly id: string;
  /** Defaults to `"v1"`. */
  readonly version?: string;
  readonly nodes: AgenticDagRecipe["nodes"];
  readonly edges: AgenticDagRecipe["edges"];
  readonly entry: AgenticDagRecipe["entry"];
  readonly terminals: AgenticDagRecipe["terminals"];
  readonly metadata?: AgenticDagRecipe["metadata"];
}

export interface ListDagsParams {
  readonly limit?: number;
  readonly tag?: string;
}

export interface DagRegistry {
  register(input: RegisterDagInput): Promise<RegisteredDag>;
  get(id: string, version?: string): Promise<RegisteredDag | null>;
  list(params?: ListDagsParams): Promise<RegisteredDag[]>;
  versions(id: string): Promise<RegisteredDag[]>;
  unregister(id: string, version?: string): Promise<void>;
}

const DEFAULT_DAG_VERSION = "v1";

interface DagRegistryClock {
  currentTimeMs(): number;
}

export interface InMemoryDagRegistryConfig {
  readonly clock?: DagRegistryClock;
}

export class InMemoryDagRegistry implements DagRegistry {
  private readonly rows = new Map<string, RegisteredDag>();
  private readonly clock: DagRegistryClock;

  constructor(config: InMemoryDagRegistryConfig = {}) {
    this.clock = config.clock ?? { currentTimeMs: () => Date.now() };
  }

  private key(id: string, version: string): string {
    return `${id}::${version}`;
  }

  async register(input: RegisterDagInput): Promise<RegisteredDag> {
    const version = input.version ?? DEFAULT_DAG_VERSION;
    // Validate at register time — bad graphs never enter the store.
    // Cheaper to fail fast here than to discover at run time.
    const recipe: AgenticDagRecipe = {
      id: input.id,
      version,
      nodes: input.nodes,
      edges: input.edges,
      entry: input.entry,
      terminals: input.terminals,
      ...(input.metadata !== undefined && { metadata: input.metadata }),
    };
    validateDag(recipe);

    const k = this.key(input.id, version);
    const now = this.clock.currentTimeMs();
    const existing = this.rows.get(k);
    const next: RegisteredDag = {
      ...recipe,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.rows.set(k, next);
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredDag | null> {
    if (version !== undefined) {
      return this.rows.get(this.key(id, version)) ?? null;
    }
    let latest: RegisteredDag | null = null;
    for (const row of this.rows.values()) {
      if (row.id !== id) continue;
      if (!latest || row.updatedAt > latest.updatedAt) latest = row;
    }
    return latest;
  }

  async list(params?: ListDagsParams): Promise<RegisteredDag[]> {
    // Latest version per id, optionally tag-filtered, oldest-first by createdAt.
    const byId = new Map<string, RegisteredDag>();
    for (const row of this.rows.values()) {
      const cur = byId.get(row.id);
      if (!cur || row.updatedAt > cur.updatedAt) byId.set(row.id, row);
    }
    let rows = [...byId.values()];
    if (params?.tag) {
      rows = rows.filter((r) => r.metadata?.tags?.includes(params.tag!));
    }
    rows.sort((a, b) => a.createdAt - b.createdAt);
    if (params?.limit !== undefined) rows = rows.slice(0, params.limit);
    return rows;
  }

  async versions(id: string): Promise<RegisteredDag[]> {
    const out: RegisteredDag[] = [];
    for (const row of this.rows.values()) {
      if (row.id === id) out.push(row);
    }
    out.sort((a, b) => a.createdAt - b.createdAt);
    return out;
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      this.rows.delete(this.key(id, version));
      return;
    }
    for (const k of [...this.rows.keys()]) {
      const row = this.rows.get(k)!;
      if (row.id === id) this.rows.delete(k);
    }
  }
}
