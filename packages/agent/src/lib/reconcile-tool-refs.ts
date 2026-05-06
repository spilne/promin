// ---------------------------------------------------------------------------
// `reconcileToolReferences` — pure-function diff between what's in the
// AgentRegistry's recipes and what the live AgentToolCatalog reports.
// Answers two related questions:
//
//   (1) Per recipe: which tool names DON'T resolve to anything wired?
//       Surfaces broken-recipe warnings on the agent detail page.
//
//   (2) Per missing tool: which recipes still reference it?
//       Surfaces orphan-tool warnings on the catalog page so operators
//       can clean up dangling references.
//
// Only local backends are considered — remote / cursor recipes don't
// pull from the host's local tool registry, so their `backend.tools`
// (if any) wouldn't be checked against the live catalog anyway.
//
// No persistence. The check is computed at view time from live data:
// catalog.listAll() + registry.list(). If we ever need an audit trail
// (which tools EVER existed on this deployment), that's a separate
// system — see promin tracking note in the catalog page docs.
// ---------------------------------------------------------------------------

import type { AgentToolCatalog } from "./tool-catalog.ts";
import type { AgentRegistry, RegisteredAgent } from "./registry/types.ts";

export interface RecipeToolRefHealth {
  readonly recipeId: string;
  readonly version: string;
  readonly resolved: ReadonlyArray<string>;
  readonly missing: ReadonlyArray<string>;
}

export interface OrphanToolEntry {
  readonly toolName: string;
  readonly recipes: ReadonlyArray<{ readonly id: string; readonly version: string }>;
}

export interface ToolRefReport {
  /** Per-recipe breakdown of which tool refs resolve / don't. */
  readonly recipes: ReadonlyArray<RecipeToolRefHealth>;
  /** Inverted view: per missing tool name, which recipes still cite it. */
  readonly orphans: ReadonlyArray<OrphanToolEntry>;
}

export interface ReconcileToolReferencesDeps {
  readonly catalog: AgentToolCatalog;
  readonly registry: AgentRegistry;
}

export async function reconcileToolReferences(
  deps: ReconcileToolReferencesDeps,
): Promise<ToolRefReport> {
  const liveTools = await deps.catalog.listAll();
  const liveNames = new Set(liveTools.map((t) => t.name));
  const allRecipes = await deps.registry.list();

  const recipes: RecipeToolRefHealth[] = [];
  // toolName → list of recipes referencing it (only built for missing names)
  const orphanIndex = new Map<string, Array<{ id: string; version: string }>>();

  for (const recipe of allRecipes) {
    const tools = extractLocalToolList(recipe);
    if (tools === null) continue; // non-local backend — skip
    const resolved: string[] = [];
    const missing: string[] = [];
    for (const name of tools) {
      if (liveNames.has(name)) {
        resolved.push(name);
      } else {
        missing.push(name);
        let bucket = orphanIndex.get(name);
        if (!bucket) {
          bucket = [];
          orphanIndex.set(name, bucket);
        }
        bucket.push({ id: recipe.id, version: recipe.version });
      }
    }
    recipes.push({
      recipeId: recipe.id,
      version: recipe.version,
      resolved,
      missing,
    });
  }

  const orphans: OrphanToolEntry[] = [];
  for (const [toolName, refList] of orphanIndex.entries()) {
    orphans.push({ toolName, recipes: refList });
  }
  // Stable order — orphans alphabetical by tool name; each orphan's
  // recipes by id then version. Matters for golden-test snapshots.
  orphans.sort((a, b) => a.toolName.localeCompare(b.toolName));
  for (const o of orphans) {
    (o.recipes as Array<{ id: string; version: string }>).sort((x, y) =>
      x.id !== y.id ? x.id.localeCompare(y.id) : x.version.localeCompare(y.version),
    );
  }
  recipes.sort((a, b) =>
    a.recipeId !== b.recipeId
      ? a.recipeId.localeCompare(b.recipeId)
      : a.version.localeCompare(b.version),
  );

  return { recipes, orphans };
}

/**
 * Returns the recipe's declared tool list when the backend is local;
 * null otherwise (caller skips the recipe). Other backend types
 * (remote / cursor) don't pull from the host's tool catalog so their
 * tool refs (if any) aren't part of this reconciliation.
 */
function extractLocalToolList(recipe: RegisteredAgent): ReadonlyArray<string> | null {
  if (recipe.backend.type !== "local") return null;
  return recipe.backend.tools;
}
