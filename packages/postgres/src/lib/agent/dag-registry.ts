// ---------------------------------------------------------------------------
// PostgresDagRegistry — Postgres-backed `DagRegistry`.
//
// The durable store of operator-authored multi-agent execution graphs a
// multi-replica deployment needs — every replica resolves the same DAG
// recipes. Version-keyed (one row per (id, version)); the graph lives in
// a `body` jsonb blob, `created_at` is preserved across version
// re-writes. `validateDag` rejects bad graphs before they enter the
// store. Matches the SQLite / in-memory DagRegistry semantics.
// ---------------------------------------------------------------------------

import { and, asc, desc, eq } from "drizzle-orm";
import {
  validateDag,
  type AgenticDagRecipe,
  type DagRegistry,
  type ListDagsParams,
  type RegisterDagInput,
  type RegisteredDag,
} from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { agentDag } from "../schema.ts";

export interface PostgresDagRegistryConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

const DEFAULT_DAG_VERSION = "v1";

/** AgenticDagRecipe minus the identity columns — what lives in `body`. */
interface DagBody {
  nodes: AgenticDagRecipe["nodes"];
  edges: AgenticDagRecipe["edges"];
  entry: AgenticDagRecipe["entry"];
  terminals: AgenticDagRecipe["terminals"];
  metadata?: AgenticDagRecipe["metadata"];
}

export class PostgresDagRegistry implements DagRegistry {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresDagRegistryConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async register(input: RegisterDagInput): Promise<RegisteredDag> {
    const version = input.version ?? DEFAULT_DAG_VERSION;
    // Reject bad graphs before they enter the store.
    validateDag({
      id: input.id,
      version,
      nodes: input.nodes,
      edges: input.edges,
      entry: input.entry,
      terminals: input.terminals,
      ...(input.metadata !== undefined && { metadata: input.metadata }),
    });
    const body: DagBody = {
      nodes: input.nodes,
      edges: input.edges,
      entry: input.entry,
      terminals: input.terminals,
      ...(input.metadata !== undefined && { metadata: input.metadata }),
    };
    const now = this.clock();
    // Upsert: created_at stays out of the conflict SET, so re-writing a
    // version preserves the original creation time; the returned row
    // carries the actual created_at (original on conflict, now on insert).
    const [row] = await this.db
      .insert(agentDag)
      .values({ id: input.id, version, body, createdAt: now, updatedAt: now })
      .onConflictDoUpdate({
        target: [agentDag.id, agentDag.version],
        set: { body, updatedAt: now },
      })
      .returning();
    if (!row) {
      throw new Error(`PostgresDagRegistry: register failed for ${input.id}@${version}`);
    }
    return rowToDag(row);
  }

  async get(id: string, version?: string): Promise<RegisteredDag | null> {
    if (version !== undefined) {
      const [row] = await this.db
        .select()
        .from(agentDag)
        .where(and(eq(agentDag.id, id), eq(agentDag.version, version)));
      return row ? rowToDag(row) : null;
    }
    // No version — latest by updated_at.
    const [row] = await this.db
      .select()
      .from(agentDag)
      .where(eq(agentDag.id, id))
      .orderBy(desc(agentDag.updatedAt))
      .limit(1);
    return row ? rowToDag(row) : null;
  }

  async list(params: ListDagsParams = {}): Promise<RegisteredDag[]> {
    const rows = await this.db.select().from(agentDag);
    // Newest version per id; oldest-created first (matches SqliteDagRegistry).
    const latest = new Map<string, typeof agentDag.$inferSelect>();
    for (const r of rows) {
      const cur = latest.get(r.id);
      if (!cur || r.updatedAt > cur.updatedAt) latest.set(r.id, r);
    }
    let out = [...latest.values()].sort((a, b) => a.createdAt - b.createdAt).map(rowToDag);
    if (params.tag) {
      const tag = params.tag;
      out = out.filter((d) => d.metadata?.tags?.includes(tag));
    }
    if (params.limit !== undefined) out = out.slice(0, params.limit);
    return out;
  }

  async versions(id: string): Promise<RegisteredDag[]> {
    const rows = await this.db
      .select()
      .from(agentDag)
      .where(eq(agentDag.id, id))
      .orderBy(asc(agentDag.createdAt));
    return rows.map(rowToDag);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      await this.db.delete(agentDag).where(and(eq(agentDag.id, id), eq(agentDag.version, version)));
      return;
    }
    await this.db.delete(agentDag).where(eq(agentDag.id, id));
  }
}

function rowToDag(row: typeof agentDag.$inferSelect): RegisteredDag {
  const b = row.body as DagBody;
  return {
    id: row.id,
    version: row.version,
    nodes: b.nodes,
    edges: b.edges,
    entry: b.entry,
    terminals: b.terminals,
    ...(b.metadata !== undefined && { metadata: b.metadata }),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}
