// ---------------------------------------------------------------------------
// Workflow folder-scan registry — auto-discover Workflow exports from a
// directory tree.
//
// Usage:
//   const { workflows } = await scanWorkflowsFolder("./workflows");
//   const server = new ZoryaServer({ storage, workflows, trigger: ... });
//
// Semantics:
// - Walks the given directory, imports each .ts / .js / .mjs module.
// - For every export that looks like a Workflow (has `.name`, `.dag`,
//   `._definition`), adds it to the returned map keyed by `workflow.name`.
// - On duplicate names, the last scan wins and a warning is pushed into
//   `warnings`.
// ---------------------------------------------------------------------------

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Workflow } from "@promin/workflow";

export interface ScanResult {
  /** All discovered workflows, keyed by name. */
  workflows: Record<string, Workflow<unknown, unknown>>;
  /** Per-workflow source file path, useful for debugging. */
  sources: Record<string, string>;
  /** Non-fatal warnings (duplicates, unsupported files, import errors). */
  warnings: string[];
}

export interface ScanOptions {
  /** File extensions to consider. Default: .ts, .tsx, .js, .mjs. */
  extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: 10. */
  maxDepth?: number;
  /** Predicate to filter candidate files before import. */
  filter?: (relPath: string) => boolean;
  /** Called with (name, workflow, sourcePath) for every discovered workflow. */
  onWorkflow?: (name: string, workflow: Workflow<unknown, unknown>, sourcePath: string) => void;
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

/**
 * Scans `root` and returns every Workflow found in any exported binding.
 * A "Workflow" is detected structurally: has string `name`, object `dag`
 * with `steps` array, and object `_definition`.
 */
export async function scanWorkflowsFolder(
  root: string,
  options: ScanOptions = {},
): Promise<ScanResult> {
  const extensions = options.extensions ?? DEFAULT_EXTENSIONS;
  const maxDepth = options.maxDepth ?? 10;
  const filter = options.filter ?? (() => true);

  const workflows: Record<string, Workflow<unknown, unknown>> = {};
  const sources: Record<string, string> = {};
  const warnings: string[] = [];

  async function walk(dir: string, depth: number): Promise<void> {
    if (depth > maxDepth) return;
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      warnings.push(`failed to read ${dir}: ${asMessage(err)}`);
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
      if (!filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await importModule(full, workflows, sources, warnings, options.onWorkflow);
    }
  }

  await walk(root, 0);
  return { workflows, sources, warnings };
}

async function importModule(
  absPath: string,
  workflows: Record<string, Workflow<unknown, unknown>>,
  sources: Record<string, string>,
  warnings: string[],
  onWorkflow: ScanOptions["onWorkflow"],
): Promise<void> {
  let mod: Record<string, unknown>;
  try {
    mod = (await import(pathToFileURL(absPath).href)) as Record<string, unknown>;
  } catch (err) {
    warnings.push(`failed to import ${absPath}: ${asMessage(err)}`);
    return;
  }

  for (const [, value] of Object.entries(mod)) {
    if (!isWorkflow(value)) continue;
    const name = value.name;
    if (workflows[name]) {
      warnings.push(
        `duplicate workflow name "${name}": ${sources[name]} vs ${absPath} — last wins`,
      );
    }
    workflows[name] = value;
    sources[name] = absPath;
    onWorkflow?.(name, value, absPath);
  }
}

function isWorkflow(v: unknown): v is Workflow<unknown, unknown> {
  if (v === null || typeof v !== "object") return false;
  const o = v as { name?: unknown; dag?: unknown; _definition?: unknown };
  return (
    typeof o.name === "string" &&
    typeof o.dag === "object" &&
    o.dag !== null &&
    typeof o._definition === "object" &&
    o._definition !== null
  );
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
