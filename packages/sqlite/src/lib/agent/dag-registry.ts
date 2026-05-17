// ---------------------------------------------------------------------------
// SqliteDagRegistry — DagRegistry over SQLite.
//
// Built on the generic VersionedRecipeStore so all the SQL plumbing
// (schema, upsert with createdAt-preserve, list, versions, unregister)
// is shared with future kinds. Only the kind-specific bits live here:
//   - validateDag at register time (bad graphs never enter the store)
//   - body shape: AgentDagRecipe minus identity columns
//   - tag-filter on list (tags live in the body blob, JS-side filter)
// ---------------------------------------------------------------------------

import type {
  AgenticDagRecipe,
  DagRegistry,
  ListDagsParams,
  RegisterDagInput,
  RegisteredDag,
} from "@promin/agent";
import { validateDag } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";
import { createVersionedRecipeStore, type VersionedRecipeStore } from "./versioned-recipe-store.ts";

export interface SqliteDagRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_dag_registry`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

const DEFAULT_DAG_VERSION = "v1";

export class SqliteDagRegistry implements DagRegistry {
  private readonly inner: VersionedRecipeStore<RegisteredDag, RegisterDagInput>;

  private constructor(config: SqliteDagRegistryConfig) {
    const { db, table = "promin_dag_registry", now } = config;
    this.inner = createVersionedRecipeStore<RegisteredDag, RegisterDagInput>({
      db,
      table,
      defaultVersion: DEFAULT_DAG_VERSION,
      ...(now !== undefined && { now }),
      validate: (input, version) => {
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
      },
      toBody: (input) => ({
        nodes: input.nodes,
        edges: input.edges,
        entry: input.entry,
        terminals: input.terminals,
        metadata: input.metadata,
      }),
      fromRow: ({ id, version, body, createdAt, updatedAt }) => {
        const b = body as {
          nodes: AgenticDagRecipe["nodes"];
          edges: AgenticDagRecipe["edges"];
          entry: AgenticDagRecipe["entry"];
          terminals: AgenticDagRecipe["terminals"];
          metadata?: AgenticDagRecipe["metadata"];
        };
        return {
          id,
          version,
          nodes: b.nodes,
          edges: b.edges,
          entry: b.entry,
          terminals: b.terminals,
          ...(b.metadata !== undefined && { metadata: b.metadata }),
          createdAt,
          updatedAt,
        };
      },
    });
  }

  static make(config: SqliteDagRegistryConfig): SqliteDagRegistry {
    return new SqliteDagRegistry(config);
  }

  register(input: RegisterDagInput): Promise<RegisteredDag> {
    return this.inner.register(input);
  }

  get(id: string, version?: string): Promise<RegisteredDag | null> {
    return this.inner.get(id, version);
  }

  async list(params: ListDagsParams = {}): Promise<RegisteredDag[]> {
    const all = await this.inner.list(params.limit !== undefined ? { limit: params.limit } : {});
    if (!params.tag) return all;
    return all.filter((r) => r.metadata?.tags?.includes(params.tag!));
  }

  versions(id: string): Promise<RegisteredDag[]> {
    return this.inner.versions(id);
  }

  unregister(id: string, version?: string): Promise<void> {
    return this.inner.unregister(id, version);
  }
}
