// ---------------------------------------------------------------------------
// `resolveSystemPrompt` — turn a role's `systemPrompt` field into the
// concrete string the agent sees. Handles all three shapes:
//
//   - `null` / undefined            → `undefined` (no system message)
//   - `string`                      → passed through verbatim
//   - `{ base, layers?: string[] }` → `base\n\n<layer-1>\n\n<layer-2>…`,
//     each layer read from the `FragmentRegistry`
//
// A layer `"<id>@<version>"` reads that exact fragment version, so a role
// version that pins its layers resolves to the same prompt forever. A bare
// `"<id>"` reads the fragment's active version, so promoting a fragment
// version updates every role that layers it. A disabled fragment, or a
// version marked `needs-review`, is left out either way. A layered role with no registry wired falls back to
// `base`.
// ---------------------------------------------------------------------------

import type { RoleDefinition } from "../role/types.ts";
import { FRAGMENT_VERSION_SEPARATOR, type FragmentRegistry } from "./types.ts";

export interface ResolveSystemPromptParams {
  readonly systemPrompt: RoleDefinition["systemPrompt"];
  readonly fragments?: FragmentRegistry;
  /**
   * What to do when a `layers[]` entry doesn't resolve in the registry.
   * Default `"warn"` — concatenate what resolved + console.warn the
   * missing one(s), since a missing fragment usually means a recipe drift
   * (renamed/removed layer) the operator should hear about, but
   * shouldn't crash the resolve.
   */
  readonly onMissingLayer?: "throw" | "skip" | "warn";
}

/** A layer reference split into fragment id and optional pinned version. */
export function parseLayerRef(layer: string): { id: string; version?: string } {
  const at = layer.lastIndexOf(FRAGMENT_VERSION_SEPARATOR);
  if (at <= 0 || at === layer.length - 1) return { id: layer };
  return { id: layer.slice(0, at), version: layer.slice(at + 1) };
}

/**
 * Returns the effective system-prompt string for the role, or `undefined`
 * when it declares none.
 */
export async function resolveSystemPrompt(
  params: ResolveSystemPromptParams,
): Promise<string | undefined> {
  const sp = params.systemPrompt;
  if (sp === null || sp === undefined) return undefined;
  if (typeof sp === "string") return sp;

  const layers = sp.layers ?? [];
  if (layers.length === 0) return sp.base;

  const parts: string[] = [sp.base];
  const onMissing = params.onMissingLayer ?? "warn";

  for (const layer of layers) {
    const ref = parseLayerRef(layer);
    const fragment = params.fragments ? await params.fragments.get(ref) : null;
    // A disabled fragment, or a version nobody reviewed, is left out —
    // pinned or not (fragments follow the same rule as skills).
    if (fragment === null || !fragment.enabled || fragment.trust === "needs-review") {
      if (onMissing === "throw") {
        throw new Error(
          `resolveSystemPrompt: prompt fragment "${layer}" is not registered or is disabled. ` +
            "Register it, or remove it from the role's `systemPrompt.layers`.",
        );
      }
      if (onMissing === "warn") {
        console.warn(
          `[resolveSystemPrompt] fragment "${layer}" is not registered or is disabled — skipping. ` +
            "Wire a FragmentRegistry that supplies it, or drop the layer from the role.",
        );
      }
      // "skip": silent
      continue;
    }
    parts.push(fragment.content);
  }

  return parts.join("\n\n");
}
