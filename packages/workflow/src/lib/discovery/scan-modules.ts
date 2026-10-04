// ---------------------------------------------------------------------------
// scanModules — the directory walk both scanners share.
//
// Walks `root` (depth-first, up to `maxDepth`), skips `node_modules`, dot
// directories, tests, benches and declaration files, imports every module
// whose name ends in one of `extensions` and passes `filter`, and hands each
// module's exports to `visit`. Unreadable directories and modules that fail
// to import become warnings; the walk carries on.
// ---------------------------------------------------------------------------

import { loadNodeFs } from "./node-fs.ts";

export const DEFAULT_SCAN_EXTENSIONS: readonly string[] = [".ts", ".tsx", ".js", ".mjs"];
export const DEFAULT_SCAN_MAX_DEPTH = 10;

export interface ScanModulesParams {
  readonly root: string;
  readonly extensions?: readonly string[];
  readonly maxDepth?: number;
  readonly filter?: (absPath: string) => boolean;
  /** Called once per imported module with its exports. */
  readonly visit: (params: {
    readonly exports: Record<string, unknown>;
    readonly path: string;
  }) => void;
  /** Collects a message per directory / module that couldn't be read. */
  readonly warnings: string[];
}

export async function scanModules(params: ScanModulesParams): Promise<void> {
  const extensions = params.extensions ?? DEFAULT_SCAN_EXTENSIONS;
  const maxDepth = params.maxDepth ?? DEFAULT_SCAN_MAX_DEPTH;
  const filter = params.filter ?? (() => true);
  const { readdir, join, pathToFileURL } = await loadNodeFs();

  const walk = async (dir: string, depth: number): Promise<void> => {
    if (depth > maxDepth) return;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      params.warnings.push(`failed to read ${dir}: ${asMessage(err)}`);
      return;
    }

    for (const entry of entries) {
      const name = entry.name;
      const full = join(dir, name);

      if (entry.isDirectory()) {
        if (name === "node_modules" || name.startsWith(".")) continue;
        await walk(full, depth + 1);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!extensions.some((ext) => name.endsWith(ext))) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) continue;
      if (!filter(full)) continue;

      let exports: Record<string, unknown>;
      try {
        exports = (await import(pathToFileURL(full).href)) as Record<string, unknown>;
      } catch (err) {
        params.warnings.push(`failed to import ${full}: ${asMessage(err)}`);
        continue;
      }
      params.visit({ exports, path: full });
    }
  };

  await walk(params.root, 0);
}

export function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
