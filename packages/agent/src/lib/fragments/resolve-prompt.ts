// ---------------------------------------------------------------------------
// `resolveSystemPrompt` — turn a recipe's `systemPrompt` field into the
// concrete string the agent loop sees. Handles all three shapes:
//
//   - `null` / undefined          → `undefined` (no system message)
//   - `string`                    → passed through verbatim
//   - `{ base, layers?: string[] }` → `base\n\n<layer-1>\n\n<layer-2>…`,
//     each layer resolved from the FragmentRegistry
//
// Sync on purpose — fragments are static markdown, no I/O. A layered
// recipe with no registry wired falls back to `base` (the layers are
// silently skipped, mirroring `onMissing: 'skip'` semantics for skills).
// ---------------------------------------------------------------------------

import type { LocalAgentBackend } from "../registry/types.ts";
import type { FragmentRegistry } from "./types.ts";

export interface ResolveSystemPromptParams {
  readonly systemPrompt: LocalAgentBackend["systemPrompt"];
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

/**
 * Returns the effective system-prompt string for the recipe, or `undefined`
 * when the recipe declares none. Pure (no async / no side effects beyond a
 * `console.warn` on missing layers under the default policy).
 */
export function resolveSystemPrompt(params: ResolveSystemPromptParams): string | undefined {
  const sp = params.systemPrompt;
  if (sp === null || sp === undefined) return undefined;
  if (typeof sp === "string") return sp;

  const layers = sp.layers ?? [];
  if (layers.length === 0) return sp.base;

  const parts: string[] = [sp.base];
  const onMissing = params.onMissingLayer ?? "warn";

  for (const key of layers) {
    const content = params.fragments?.get(key);
    if (content === undefined) {
      if (onMissing === "throw") {
        throw new Error(
          `resolveSystemPrompt: prompt fragment "${key}" not found in the registry. ` +
            "Register it, or remove it from the recipe's `systemPrompt.layers`.",
        );
      }
      if (onMissing === "warn") {
        console.warn(
          `[resolveSystemPrompt] fragment "${key}" not found — skipping. ` +
            "Wire a FragmentRegistry that supplies it, or drop the layer from the recipe.",
        );
      }
      // "skip": silent
      continue;
    }
    parts.push(content);
  }

  return parts.join("\n\n");
}
