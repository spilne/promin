// ---------------------------------------------------------------------------
// WorkflowScanner — auto-discover Workflow exports from a directory tree.
//
// Lives in @promin/workflow (rather than @promin/zorya) so workers, agents,
// and any other consumer can construct one without depending on the
// dashboard. ZoryaServer's `scanWorkflowsFolder` helper is a thin wrapper
// around this class, and split-mode workers can share the same configured
// instance.
//
// Detection is structural: an export counts as a Workflow if it has a
// string `name`, an object `dag`, and an object `_definition`. Any
// authoring style — `defineWorkflow(...)`, factory functions, builder
// pattern — works as long as the resulting object matches that shape.
// ---------------------------------------------------------------------------

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Workflow } from "../durable/durable-pipeline.ts";

export interface WorkflowScannerOptions {
  /** File extensions to consider. Default: .ts, .tsx, .js, .mjs. */
  extensions?: ReadonlyArray<string>;
  /** Recursion depth. Default: 10. */
  maxDepth?: number;
  /** Predicate to filter candidate files before import. */
  filter?: (absPath: string) => boolean;
  /** Called for each discovered workflow. */
  onWorkflow?: (name: string, workflow: Workflow<unknown, unknown>, sourcePath: string) => void;
}

export interface WorkflowScanResult {
  workflows: Record<string, Workflow<unknown, unknown>>;
  /** Per-name source path — useful for debugging and dup diagnostics. */
  sources: Record<string, string>;
  warnings: string[];
}

const DEFAULT_EXTENSIONS = [".ts", ".tsx", ".js", ".mjs"];

export class WorkflowScanner {
  private readonly extensions: ReadonlyArray<string>;
  private readonly maxDepth: number;
  private readonly filter: (absPath: string) => boolean;
  private readonly onWorkflow?: WorkflowScannerOptions["onWorkflow"];

  constructor(options: WorkflowScannerOptions = {}) {
    this.extensions = options.extensions ?? DEFAULT_EXTENSIONS;
    this.maxDepth = options.maxDepth ?? 10;
    this.filter = options.filter ?? (() => true);
    this.onWorkflow = options.onWorkflow;
  }

  /** One-shot helper for callers that don't want to hold an instance. */
  static async scanFolder(
    root: string,
    options: WorkflowScannerOptions = {},
  ): Promise<WorkflowScanResult> {
    return await new WorkflowScanner(options).scan(root);
  }

  async scan(root: string): Promise<WorkflowScanResult> {
    const workflows: Record<string, Workflow<unknown, unknown>> = {};
    const sources: Record<string, string> = {};
    const warnings: string[] = [];
    await this.walk(root, 0, workflows, sources, warnings);
    return { workflows, sources, warnings };
  }

  private async walk(
    dir: string,
    depth: number,
    workflows: Record<string, Workflow<unknown, unknown>>,
    sources: Record<string, string>,
    warnings: string[],
  ): Promise<void> {
    if (depth > this.maxDepth) return;
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
        await this.walk(full, depth + 1, workflows, sources, warnings);
        continue;
      }
      if (!entry.isFile()) continue;
      if (!this.extensions.some((ext) => name.endsWith(ext))) continue;
      if (!this.filter(full)) continue;
      if (name.includes(".test.") || name.includes(".bench.") || name.endsWith(".d.ts")) {
        continue;
      }

      await this.importModule(full, workflows, sources, warnings);
    }
  }

  private async importModule(
    absPath: string,
    workflows: Record<string, Workflow<unknown, unknown>>,
    sources: Record<string, string>,
    warnings: string[],
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
      this.onWorkflow?.(name, value, absPath);
    }
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
