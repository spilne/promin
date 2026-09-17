// ---------------------------------------------------------------------------
// `createFileModelCatalog` — load `ModelCatalogItem`s from a directory of
// .ts/.js files and (optionally) hot-reload them. Mirrors
// `createFileToolRegistry` in `lib/tool-registry.ts` so operators get a
// single mental model: "drop a file in, the platform sees it."
//
// Each file default-exports one ModelCatalogItem. Subdirectories are
// scanned recursively (configurable). On file change, the affected item is
// re-imported with a cache-busted URL so the next `get(provider, id)`
// returns the new shape.
// ---------------------------------------------------------------------------

import { watch } from "node:fs";
import { readdir, access } from "node:fs/promises";
import { join, extname } from "node:path";
import { pathToFileURL } from "node:url";
import {
  type ModelCatalog,
  type ModelCatalogItem,
  type SerializedModelCatalogItem,
} from "./model-catalog.ts";

export interface FileModelCatalogConfig {
  /** Directory to scan and watch for model files. */
  dir: string;
  /** Set to false to disable file watching after initial load. Default: true. */
  watch?: boolean;
  /** Scan subdirectories recursively. Default: true. */
  recursive?: boolean;
  onLoad?: (provider: string, id: string) => void;
  onUnload?: (provider: string, id: string) => void;
  onError?: (file: string, error: unknown) => void;
}

const MODEL_EXTENSIONS = new Set([".ts", ".js"]);

function keyOf(provider: string, id: string): string {
  return `${provider}::${id}`;
}

/**
 * Load `ModelCatalogItem`s from a directory and watch for changes.
 *
 * File contract:
 * ```ts
 * // models/anthropic-sonnet.ts
 * import { anthropic } from "@promin/agent";
 * import type { ModelCatalogItem } from "@promin/agent";
 *
 * const item: ModelCatalogItem = {
 *   provider: "anthropic",
 *   id: "claude-sonnet-4-6",
 *   displayName: "Claude Sonnet 4.6",
 *   contextLimit: 200_000,
 *   capabilities: ["chat", "tools", "vision", "thinking"],
 *   costTier: "mid",
 *   llm: anthropic("claude-sonnet-4-6"),
 * };
 * export default item;
 * ```
 *
 * Hot-reload: `watch: true` (the default) re-imports the file on change.
 * The catalog's `get` always returns the latest snapshot, so an in-flight
 * agent that's already resolved its LLM keeps the OLD instance until its
 * next resolve — same staleness window as the tool registry.
 */
export async function createFileModelCatalog(
  config: FileModelCatalogConfig,
): Promise<ModelCatalog & { close: () => void }> {
  const items = new Map<string, ModelCatalogItem>();
  const fileToKey = new Map<string, string>();
  const recursive = config.recursive !== false;
  let watcher: ReturnType<typeof watch> | null = null;

  async function loadFile(filePath: string): Promise<void> {
    try {
      const url = `${pathToFileURL(filePath).href}?t=${Date.now()}`;
      const mod = await import(url);
      const item = mod.default as ModelCatalogItem | undefined;
      if (!item || typeof item.provider !== "string" || typeof item.id !== "string" || !item.llm) {
        config.onError?.(
          filePath,
          new Error("Model file must default-export a ModelCatalogItem with provider, id, and llm"),
        );
        return;
      }
      const key = keyOf(item.provider, item.id);
      const prev = fileToKey.get(filePath);
      // File renamed / file's exported (provider,id) changed: drop the old
      // key first so we don't leave a stale entry in the catalog.
      if (prev && prev !== key) {
        const old = items.get(prev);
        items.delete(prev);
        if (old) config.onUnload?.(old.provider, old.id);
      }
      items.set(key, item);
      fileToKey.set(filePath, key);
      config.onLoad?.(item.provider, item.id);
    } catch (err) {
      config.onError?.(filePath, err);
    }
  }

  function unloadFile(filePath: string): void {
    const key = fileToKey.get(filePath);
    if (!key) return;
    const item = items.get(key);
    items.delete(key);
    fileToKey.delete(filePath);
    if (item) config.onUnload?.(item.provider, item.id);
  }

  // Initial load — readdir with recursive:true returns relative paths.
  const entries = await readdir(config.dir, { recursive });
  await Promise.all(
    (entries as string[])
      .filter((f) => MODEL_EXTENSIONS.has(extname(f)))
      .map((f) => loadFile(join(config.dir, f))),
  );

  if (config.watch !== false) {
    watcher = watch(config.dir, { recursive }, (_, filename) => {
      if (!filename || !MODEL_EXTENSIONS.has(extname(filename))) return;
      const filePath = join(config.dir, filename);
      access(filePath)
        .then(() => loadFile(filePath))
        .catch(() => unloadFile(filePath));
    });
  }

  return {
    get(provider: string, id: string): ModelCatalogItem | undefined {
      return items.get(keyOf(provider, id));
    },
    list(): ModelCatalogItem[] {
      return Array.from(items.values());
    },
    serialize(): SerializedModelCatalogItem[] {
      return Array.from(items.values()).map((item) => {
        const { llm: _llm, ...rest } = item;
        void _llm;
        return rest;
      });
    },
    close(): void {
      watcher?.close();
      items.clear();
      fileToKey.clear();
    },
  };
}
